from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from botocore.client import BaseClient

from neuro_sleep.raw.file_registry import (
    list_raw_files_by_bucket_prefix,
)
from neuro_sleep.storage.object_storage import (
    get_object_metadata,
    list_object_summaries,
)


ReconciliationStatus = Literal[
    "healthy",
    "missing_in_storage",
    "missing_in_registry",
    "metadata_mismatch",
]


@dataclass(frozen=True)
class BronzeReconciliationResult:
    bucket: str
    object_key: str
    status: ReconciliationStatus
    reason: str

    registry_file_id: UUID | None
    registry_status: str | None
    registry_size_bytes: int | None
    registry_checksum_sha256: str | None

    storage_size_bytes: int | None
    storage_checksum_sha256: str | None

    @property
    def healthy(self) -> bool:
        return self.status == "healthy"


def reconcile_bronze_prefix(
    bucket: str,
    prefix: str,
    client: BaseClient,
) -> list[BronzeReconciliationResult]:
    registry_records = (
        list_raw_files_by_bucket_prefix(
            bucket=bucket,
            prefix=prefix,
        )
    )

    storage_summaries = list_object_summaries(
        bucket=bucket,
        prefix=prefix,
        client=client,
    )

    registry_by_key = {
        record.object_key: record
        for record in registry_records
    }

    storage_by_key = {
        summary.object_key: summary
        for summary in storage_summaries
    }

    all_object_keys = sorted(
        set(registry_by_key)
        | set(storage_by_key)
    )

    results: list[
        BronzeReconciliationResult
    ] = []

    for object_key in all_object_keys:
        registry_record = registry_by_key.get(
            object_key
        )

        storage_summary = storage_by_key.get(
            object_key
        )

        if registry_record is None:
            metadata = get_object_metadata(
                bucket=bucket,
                object_key=object_key,
                client=client,
            )

            results.append(
                BronzeReconciliationResult(
                    bucket=bucket,
                    object_key=object_key,
                    status=(
                        "missing_in_registry"
                    ),
                    reason=(
                        "Object exists in MinIO "
                        "but has no registry row."
                    ),
                    registry_file_id=None,
                    registry_status=None,
                    registry_size_bytes=None,
                    registry_checksum_sha256=None,
                    storage_size_bytes=(
                        metadata.get(
                            "content_length"
                        )
                    ),
                    storage_checksum_sha256=(
                        metadata.get(
                            "checksum_sha256"
                        )
                    ),
                )
            )

            continue

        if storage_summary is None:
            results.append(
                BronzeReconciliationResult(
                    bucket=bucket,
                    object_key=object_key,
                    status=(
                        "missing_in_storage"
                    ),
                    reason=(
                        "Registry row exists "
                        "but MinIO object is "
                        "missing."
                    ),
                    registry_file_id=(
                        registry_record.file_id
                    ),
                    registry_status=(
                        registry_record.status
                    ),
                    registry_size_bytes=(
                        registry_record
                        .file_size_bytes
                    ),
                    registry_checksum_sha256=(
                        registry_record
                        .checksum_sha256
                    ),
                    storage_size_bytes=None,
                    storage_checksum_sha256=None,
                )
            )

            continue

        metadata = get_object_metadata(
            bucket=bucket,
            object_key=object_key,
            client=client,
        )

        storage_size = metadata.get(
            "content_length"
        )

        storage_checksum = metadata.get(
            "checksum_sha256"
        )

        mismatches: list[str] = []

        if registry_record.status != "uploaded":
            mismatches.append(
                "registry status is "
                f"'{registry_record.status}'"
            )

        if (
            registry_record.file_size_bytes
            != storage_size
        ):
            mismatches.append(
                "file size differs"
            )

        if (
            registry_record.checksum_sha256
            != storage_checksum
        ):
            mismatches.append(
                "SHA256 checksum differs"
            )

        if mismatches:
            status: ReconciliationStatus = (
                "metadata_mismatch"
            )

            reason = "; ".join(mismatches)

        else:
            status = "healthy"

            reason = (
                "MinIO object and registry "
                "metadata match."
            )

        results.append(
            BronzeReconciliationResult(
                bucket=bucket,
                object_key=object_key,
                status=status,
                reason=reason,
                registry_file_id=(
                    registry_record.file_id
                ),
                registry_status=(
                    registry_record.status
                ),
                registry_size_bytes=(
                    registry_record
                    .file_size_bytes
                ),
                registry_checksum_sha256=(
                    registry_record
                    .checksum_sha256
                ),
                storage_size_bytes=(
                    storage_size
                ),
                storage_checksum_sha256=(
                    storage_checksum
                ),
            )
        )

    return results
