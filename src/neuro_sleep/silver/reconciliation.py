from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Literal

from botocore.client import BaseClient

from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)
from neuro_sleep.silver.idempotency import (
    SUCCESS_OBJECT_NAME,
    read_success_manifest,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
)


ReconciliationSeverity = Literal[
    "warning",
    "error",
]


@dataclass(frozen=True)
class ReconciliationIssue:
    code: str
    severity: ReconciliationSeverity
    message: str


@dataclass(frozen=True)
class SilverReconciliationReport:
    bucket: str
    output_prefix: str

    expected_data_object_count: int
    actual_data_object_count: int

    expected_row_count: int
    actual_row_count: int

    verified_payload_checksum_count: int

    issues: tuple[
        ReconciliationIssue,
        ...,
    ]

    @property
    def error_count(self) -> int:
        return sum(
            issue.severity == "error"
            for issue in self.issues
        )

    @property
    def warning_count(self) -> int:
        return sum(
            issue.severity == "warning"
            for issue in self.issues
        )

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    def raise_for_errors(self) -> None:
        errors = [
            issue
            for issue in self.issues
            if issue.severity == "error"
        ]

        if not errors:
            return

        details = "; ".join(
            f"{issue.code}: {issue.message}"
            for issue in errors
        )

        raise ValueError(
            "Silver reconciliation failed: "
            f"{details}"
        )


def add_issue(
    issues: list[ReconciliationIssue],
    code: str,
    severity: ReconciliationSeverity,
    message: str,
) -> None:
    issues.append(
        ReconciliationIssue(
            code=code,
            severity=severity,
            message=message,
        )
    )


def calculate_object_sha256(
    bucket: str,
    object_key: str,
    client: BaseClient,
) -> str:
    response = (
        run_object_storage_operation(
            operation=lambda: (
                client.get_object(
                    Bucket=bucket,
                    Key=object_key,
                )
            ),
            operation_name=(
                f"get_object:{bucket}/"
                f"{object_key}"
            ),
        )
    )

    body = response["Body"]
    digest = sha256()

    try:
        while chunk := body.read(
            1024 * 1024
        ):
            digest.update(chunk)

    finally:
        body.close()

    return digest.hexdigest()


def reconcile_silver_output(
    bucket: str,
    output_prefix: str,
    *,
    verify_payload_checksums: bool = False,
    client: BaseClient | None = None,
) -> SilverReconciliationReport:
    owns_client = client is None

    if client is None:
        client = get_object_storage_client()

    issues: list[
        ReconciliationIssue
    ] = []

    verified_payload_checksum_count = 0
    actual_row_count = 0

    try:
        manifest = read_success_manifest(
            bucket=bucket,
            output_prefix=output_prefix,
            client=client,
        )

        manifest_objects = manifest.get(
            "objects"
        )

        if not isinstance(
            manifest_objects,
            list,
        ):
            raise ValueError(
                "Success manifest objects "
                "must be a list"
            )

        expected_objects: dict[
            str,
            dict[str, object],
        ] = {}

        for item in manifest_objects:
            if not isinstance(item, dict):
                raise ValueError(
                    "Success manifest object "
                    "entry must be a JSON "
                    "object"
                )

            object_key = item.get(
                "object_key"
            )

            if not isinstance(
                object_key,
                str,
            ):
                raise ValueError(
                    "Success manifest object "
                    "entry has invalid "
                    "object_key"
                )

            if object_key in expected_objects:
                add_issue(
                    issues,
                    code=(
                        "DUPLICATE_MANIFEST_KEY"
                    ),
                    severity="error",
                    message=(
                        "duplicate object key in "
                        "success manifest: "
                        f"{object_key}"
                    ),
                )

            expected_objects[
                object_key
            ] = item

        expected_data_object_count = (
            manifest.get(
                "data_object_count"
            )
        )

        if not isinstance(
            expected_data_object_count,
            int,
        ):
            raise ValueError(
                "Success manifest has invalid "
                "data_object_count"
            )

        expected_row_count = manifest.get(
            "row_count"
        )

        if not isinstance(
            expected_row_count,
            int,
        ):
            raise ValueError(
                "Success manifest has invalid "
                "row_count"
            )

        stored_objects = (
            list_object_summaries(
                bucket=bucket,
                prefix=(
                    output_prefix + "/"
                ),
                client=client,
            )
        )

        success_key = (
            f"{output_prefix}/"
            f"{SUCCESS_OBJECT_NAME}"
        )

        actual_data_objects = [
            item
            for item in stored_objects
            if item.object_key != success_key
        ]

        actual_objects = {
            item.object_key: item
            for item in actual_data_objects
        }

        expected_keys = set(
            expected_objects
        )

        actual_keys = set(
            actual_objects
        )

        missing_keys = sorted(
            expected_keys - actual_keys
        )

        unexpected_keys = sorted(
            actual_keys - expected_keys
        )

        for object_key in missing_keys:
            add_issue(
                issues,
                code="MISSING_OBJECT",
                severity="error",
                message=(
                    "expected Silver object is "
                    "missing: "
                    f"{object_key}"
                ),
            )

        for object_key in unexpected_keys:
            add_issue(
                issues,
                code="UNEXPECTED_OBJECT",
                severity="error",
                message=(
                    "unexpected Silver object "
                    "exists: "
                    f"{object_key}"
                ),
            )

        if (
            len(expected_objects)
            != expected_data_object_count
        ):
            add_issue(
                issues,
                code=(
                    "MANIFEST_OBJECT_COUNT_MISMATCH"
                ),
                severity="error",
                message=(
                    "manifest object list does "
                    "not match "
                    "data_object_count"
                ),
            )

        for object_key in sorted(
            expected_keys & actual_keys
        ):
            expected = expected_objects[
                object_key
            ]

            actual = actual_objects[
                object_key
            ]

            expected_size = expected.get(
                "file_size_bytes"
            )

            if not isinstance(
                expected_size,
                int,
            ):
                add_issue(
                    issues,
                    code=(
                        "INVALID_MANIFEST_FILE_SIZE"
                    ),
                    severity="error",
                    message=(
                        "invalid file size in "
                        "manifest: "
                        f"{object_key}"
                    ),
                )

            elif (
                actual.content_length
                != expected_size
            ):
                add_issue(
                    issues,
                    code="FILE_SIZE_MISMATCH",
                    severity="error",
                    message=(
                        "stored object size does "
                        "not match manifest: "
                        f"{object_key}"
                    ),
                )

            head = (
                run_object_storage_operation(
                    operation=lambda key=(
                        object_key
                    ): client.head_object(
                        Bucket=bucket,
                        Key=key,
                    ),
                    operation_name=(
                        f"head_object:{bucket}/"
                        f"{object_key}"
                    ),
                )
            )

            metadata = head.get(
                "Metadata",
                {},
            )

            expected_dataset = expected.get(
                "dataset_name"
            )

            if (
                metadata.get(
                    "dataset_name"
                )
                != str(expected_dataset)
            ):
                add_issue(
                    issues,
                    code=(
                        "DATASET_METADATA_MISMATCH"
                    ),
                    severity="error",
                    message=(
                        "dataset_name metadata "
                        "does not match manifest: "
                        f"{object_key}"
                    ),
                )

            expected_object_row_count = (
                expected.get(
                    "row_count"
                )
            )

            if not isinstance(
                expected_object_row_count,
                int,
            ):
                add_issue(
                    issues,
                    code=(
                        "INVALID_MANIFEST_ROW_COUNT"
                    ),
                    severity="error",
                    message=(
                        "invalid row_count in "
                        "manifest: "
                        f"{object_key}"
                    ),
                )

            else:
                stored_row_count_text = (
                    metadata.get(
                        "row_count"
                    )
                )

                try:
                    stored_row_count = int(
                        str(
                            stored_row_count_text
                        )
                    )

                except (
                    TypeError,
                    ValueError,
                ):
                    add_issue(
                        issues,
                        code=(
                            "INVALID_OBJECT_ROW_COUNT"
                        ),
                        severity="error",
                        message=(
                            "stored row_count "
                            "metadata is invalid: "
                            f"{object_key}"
                        ),
                    )

                else:
                    actual_row_count += (
                        stored_row_count
                    )

                    if (
                        stored_row_count
                        != expected_object_row_count
                    ):
                        add_issue(
                            issues,
                            code=(
                                "ROW_COUNT_MISMATCH"
                            ),
                            severity="error",
                            message=(
                                "stored row_count "
                                "does not match "
                                "manifest: "
                                f"{object_key}"
                            ),
                        )

            expected_checksum = (
                expected.get(
                    "checksum_sha256"
                )
            )

            stored_checksum = metadata.get(
                "checksum_sha256"
            )

            if (
                stored_checksum
                != expected_checksum
            ):
                add_issue(
                    issues,
                    code=(
                        "CHECKSUM_METADATA_MISMATCH"
                    ),
                    severity="error",
                    message=(
                        "stored checksum metadata "
                        "does not match manifest: "
                        f"{object_key}"
                    ),
                )

            if verify_payload_checksums:
                actual_checksum = (
                    calculate_object_sha256(
                        bucket=bucket,
                        object_key=object_key,
                        client=client,
                    )
                )

                verified_payload_checksum_count += (
                    1
                )

                if (
                    actual_checksum
                    != expected_checksum
                ):
                    add_issue(
                        issues,
                        code=(
                            "PAYLOAD_CHECKSUM_MISMATCH"
                        ),
                        severity="error",
                        message=(
                            "stored object bytes do "
                            "not match manifest "
                            "checksum: "
                            f"{object_key}"
                        ),
                    )

        if (
            not missing_keys
            and actual_row_count
            != expected_row_count
        ):
            add_issue(
                issues,
                code=(
                    "TOTAL_ROW_COUNT_MISMATCH"
                ),
                severity="error",
                message=(
                    "summed stored row counts do "
                    "not match manifest "
                    "row_count"
                ),
            )

        return SilverReconciliationReport(
            bucket=bucket,
            output_prefix=output_prefix,
            expected_data_object_count=(
                expected_data_object_count
            ),
            actual_data_object_count=len(
                actual_data_objects
            ),
            expected_row_count=(
                expected_row_count
            ),
            actual_row_count=(
                actual_row_count
            ),
            verified_payload_checksum_count=(
                verified_payload_checksum_count
            ),
            issues=tuple(issues),
        )

    finally:
        if owns_client:
            client.close()
