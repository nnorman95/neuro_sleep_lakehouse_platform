from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal
from uuid import UUID

from botocore.client import BaseClient

from neuro_sleep.config import (
    Settings,
    get_settings,
)
from neuro_sleep.raw.file_registry import (
    get_raw_file_by_object_key,
)
from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)
from neuro_sleep.silver.parquet_schemas import (
    SCHEMA_VERSION,
)
from neuro_sleep.silver.silver_object_writer import (
    SilverObjectWriteResult,
    calculate_file_sha256,
    upload_silver_table,
)
from neuro_sleep.silver.subject_metadata import (
    merge_subject_metadata_bundles,
    parse_sc_workbook,
    parse_st_workbook,
)
from neuro_sleep.silver.subject_parquet import (
    recording_contexts_to_table,
    subjects_to_table,
)
from neuro_sleep.sources.sleep_edf import (
    BRONZE_BUCKET,
    SOURCE_SYSTEM,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
    put_bytes_object,
)


SILVER_BUCKET = "silver"
TRANSFORM_VERSION = "1.0.0"
SUCCESS_FILE_NAME = "_SUCCESS.json"

SubjectMetadataStatus = Literal[
    "written",
    "skipped",
]


@dataclass(frozen=True)
class SourceMetadataFile:
    collection: str
    bucket: str
    object_key: str
    file_id: UUID
    file_size_bytes: int
    checksum_sha256: str


@dataclass(frozen=True)
class SubjectMetadataPipelineResult:
    status: SubjectMetadataStatus
    output_prefix: str
    input_fingerprint: str
    subject_count: int
    recording_context_count: int
    data_object_count: int
    total_object_count: int
    recovered_partial_output: bool


def build_metadata_object_keys(
    dataset_version: str,
) -> dict[str, str]:
    return {
        "sleep-cassette": (
            "physionet/sleep-edfx/"
            f"{dataset_version}/"
            "SC-subjects.xls"
        ),
        "sleep-telemetry": (
            "physionet/sleep-edfx/"
            f"{dataset_version}/"
            "ST-subjects.xls"
        ),
    }


def load_source_metadata_file(
    *,
    collection: str,
    bucket: str,
    object_key: str,
) -> SourceMetadataFile:
    record = get_raw_file_by_object_key(
        bucket=bucket,
        object_key=object_key,
    )

    if record is None:
        raise FileNotFoundError(
            "Raw metadata file is not "
            f"registered: {bucket}/{object_key}"
        )

    if record.status != "uploaded":
        raise RuntimeError(
            "Raw metadata file is not in "
            "uploaded status: "
            f"{bucket}/{object_key}; "
            f"status={record.status}"
        )

    if record.file_size_bytes is None:
        raise RuntimeError(
            "Raw metadata file size is "
            f"missing: {bucket}/{object_key}"
        )

    if not record.checksum_sha256:
        raise RuntimeError(
            "Raw metadata checksum is "
            f"missing: {bucket}/{object_key}"
        )

    return SourceMetadataFile(
        collection=collection,
        bucket=bucket,
        object_key=object_key,
        file_id=record.file_id,
        file_size_bytes=(
            record.file_size_bytes
        ),
        checksum_sha256=(
            record.checksum_sha256
        ),
    )


def load_source_metadata_files(
    dataset_version: str,
) -> tuple[
    SourceMetadataFile,
    SourceMetadataFile,
]:
    object_keys = build_metadata_object_keys(
        dataset_version
    )

    cassette = load_source_metadata_file(
        collection="sleep-cassette",
        bucket=BRONZE_BUCKET,
        object_key=(
            object_keys["sleep-cassette"]
        ),
    )

    telemetry = load_source_metadata_file(
        collection="sleep-telemetry",
        bucket=BRONZE_BUCKET,
        object_key=(
            object_keys["sleep-telemetry"]
        ),
    )

    return cassette, telemetry


def calculate_input_fingerprint(
    *,
    dataset_version: str,
    source_files: tuple[
        SourceMetadataFile,
        ...,
    ],
) -> str:
    payload = {
        "source_system": SOURCE_SYSTEM,
        "dataset_version": dataset_version,
        "schema_version": SCHEMA_VERSION,
        "transform_version": (
            TRANSFORM_VERSION
        ),
        "source_files": [
            {
                "collection": item.collection,
                "bucket": item.bucket,
                "object_key": (
                    item.object_key
                ),
                "checksum_sha256": (
                    item.checksum_sha256
                ),
            }
            for item in sorted(
                source_files,
                key=lambda value: (
                    value.collection
                ),
            )
        ],
    }

    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )

    return sha256(
        canonical.encode("utf-8")
    ).hexdigest()


def build_output_prefix(
    *,
    root_prefix: str,
    input_fingerprint: str,
) -> str:
    normalized_root = root_prefix.strip(
        "/"
    )

    if not normalized_root:
        raise ValueError(
            "root_prefix cannot be empty"
        )

    return (
        f"{normalized_root}/"
        f"schema_version={SCHEMA_VERSION}/"
        "transform_version="
        f"{TRANSFORM_VERSION}/"
        "input_fingerprint="
        f"{input_fingerprint}"
    )


def success_object_key(
    output_prefix: str,
) -> str:
    return (
        f"{output_prefix}/"
        f"{SUCCESS_FILE_NAME}"
    )


def data_object_keys(
    output_prefix: str,
) -> tuple[str, str]:
    return (
        f"{output_prefix}/subjects.parquet",
        (
            f"{output_prefix}/"
            "recording_contexts.parquet"
        ),
    )


def download_and_verify_source(
    *,
    source_file: SourceMetadataFile,
    destination: Path,
    client: BaseClient,
) -> None:
    run_object_storage_operation(
        operation=lambda: client.download_file(
            Bucket=source_file.bucket,
            Key=source_file.object_key,
            Filename=str(destination),
        ),
        operation_name=(
            "download_file:"
            f"{source_file.bucket}/"
            f"{source_file.object_key}"
        ),
    )

    actual_size = destination.stat().st_size

    if (
        actual_size
        != source_file.file_size_bytes
    ):
        raise RuntimeError(
            "Downloaded metadata file size "
            "mismatch: "
            f"{source_file.object_key}; "
            f"expected="
            f"{source_file.file_size_bytes}, "
            f"actual={actual_size}"
        )

    actual_checksum = calculate_file_sha256(
        destination
    )

    if (
        actual_checksum
        != source_file.checksum_sha256
    ):
        raise RuntimeError(
            "Downloaded metadata checksum "
            "mismatch: "
            f"{source_file.object_key}"
        )


def read_success_manifest(
    *,
    bucket: str,
    object_key: str,
    client: BaseClient,
) -> dict[str, object] | None:
    summaries = list_object_summaries(
        bucket=bucket,
        prefix=object_key,
        client=client,
    )

    if not any(
        item.object_key == object_key
        for item in summaries
    ):
        return None

    response = run_object_storage_operation(
        operation=lambda: client.get_object(
            Bucket=bucket,
            Key=object_key,
        ),
        operation_name=(
            f"get_object:{bucket}/"
            f"{object_key}"
        ),
    )

    body = response["Body"]

    try:
        payload = body.read()

    finally:
        body.close()

    parsed = json.loads(
        payload.decode("utf-8")
    )

    if not isinstance(parsed, dict):
        raise RuntimeError(
            "Subject metadata success "
            "manifest must be an object"
        )

    return parsed


def validate_completed_output(
    *,
    bucket: str,
    output_prefix: str,
    input_fingerprint: str,
    client: BaseClient,
) -> SubjectMetadataPipelineResult | None:
    manifest_key = success_object_key(
        output_prefix
    )

    manifest = read_success_manifest(
        bucket=bucket,
        object_key=manifest_key,
        client=client,
    )

    if manifest is None:
        return None

    if (
        manifest.get("input_fingerprint")
        != input_fingerprint
    ):
        raise RuntimeError(
            "Subject metadata success "
            "manifest fingerprint mismatch"
        )

    expected_objects = manifest.get(
        "data_objects"
    )

    if not isinstance(
        expected_objects,
        list,
    ):
        raise RuntimeError(
            "Subject metadata success "
            "manifest has no data_objects"
        )

    summaries = list_object_summaries(
        bucket=bucket,
        prefix=f"{output_prefix}/",
        client=client,
    )

    actual_sizes = {
        item.object_key: (
            item.content_length
        )
        for item in summaries
    }

    for item in expected_objects:
        if not isinstance(item, dict):
            raise RuntimeError(
                "Invalid data object entry "
                "in success manifest"
            )

        object_key = item.get("object_key")
        expected_size = item.get(
            "file_size_bytes"
        )
        expected_checksum = item.get(
            "checksum_sha256"
        )

        if not isinstance(
            object_key,
            str,
        ):
            raise RuntimeError(
                "Manifest object key is "
                "invalid"
            )

        if (
            actual_sizes.get(object_key)
            != expected_size
        ):
            raise RuntimeError(
                "Completed subject metadata "
                "object is missing or has "
                f"wrong size: {object_key}"
            )

        head = run_object_storage_operation(
            operation=lambda object_key=object_key: (
                client.head_object(
                    Bucket=bucket,
                    Key=object_key,
                )
            ),
            operation_name=(
                f"head_object:{bucket}/"
                f"{object_key}"
            ),
        )

        metadata = head.get(
            "Metadata",
            {},
        )

        if (
            metadata.get(
                "checksum_sha256"
            )
            != expected_checksum
        ):
            raise RuntimeError(
                "Completed subject metadata "
                "checksum metadata mismatch: "
                f"{object_key}"
            )

    subject_count = manifest.get(
        "subject_count"
    )
    context_count = manifest.get(
        "recording_context_count"
    )

    if not isinstance(
        subject_count,
        int,
    ) or not isinstance(
        context_count,
        int,
    ):
        raise RuntimeError(
            "Success manifest row counts "
            "are invalid"
        )

    return SubjectMetadataPipelineResult(
        status="skipped",
        output_prefix=output_prefix,
        input_fingerprint=(
            input_fingerprint
        ),
        subject_count=subject_count,
        recording_context_count=(
            context_count
        ),
        data_object_count=len(
            expected_objects
        ),
        total_object_count=(
            len(expected_objects) + 1
        ),
        recovered_partial_output=False,
    )


def delete_output_prefix(
    *,
    bucket: str,
    output_prefix: str,
    client: BaseClient,
) -> int:
    summaries = list_object_summaries(
        bucket=bucket,
        prefix=f"{output_prefix}/",
        client=client,
    )

    for item in summaries:
        run_object_storage_operation(
            operation=lambda object_key=item.object_key: (
                client.delete_object(
                    Bucket=bucket,
                    Key=object_key,
                )
            ),
            operation_name=(
                f"delete_object:{bucket}/"
                f"{item.object_key}"
            ),
        )

    return len(summaries)


def result_to_manifest_object(
    result: SilverObjectWriteResult,
) -> dict[str, object]:
    return {
        "bucket": result.bucket,
        "object_key": result.object_key,
        "dataset_name": (
            result.dataset_name
        ),
        "row_count": result.row_count,
        "file_size_bytes": (
            result.file_size_bytes
        ),
        "checksum_sha256": (
            result.checksum_sha256
        ),
        "etag": result.etag,
    }


def run_subject_metadata_pipeline(
    *,
    silver_bucket: str = SILVER_BUCKET,
    root_prefix: str,
    settings: Settings | None = None,
    client: BaseClient | None = None,
) -> SubjectMetadataPipelineResult:
    if settings is None:
        settings = get_settings()

    source_files = (
        load_source_metadata_files(
            settings.sleep_edf_version
        )
    )

    input_fingerprint = (
        calculate_input_fingerprint(
            dataset_version=(
                settings.sleep_edf_version
            ),
            source_files=source_files,
        )
    )

    output_prefix = build_output_prefix(
        root_prefix=root_prefix,
        input_fingerprint=(
            input_fingerprint
        ),
    )

    owns_client = client is None

    if client is None:
        client = get_object_storage_client(
            settings
        )

    try:
        completed = validate_completed_output(
            bucket=silver_bucket,
            output_prefix=output_prefix,
            input_fingerprint=(
                input_fingerprint
            ),
            client=client,
        )

        if completed is not None:
            return completed

        recovered_object_count = (
            delete_output_prefix(
                bucket=silver_bucket,
                output_prefix=(
                    output_prefix
                ),
                client=client,
            )
        )

        with TemporaryDirectory(
            prefix=(
                "neuro_sleep_subject_"
                "metadata_"
            )
        ) as temporary_directory:
            temporary_root = Path(
                temporary_directory
            )

            local_paths: dict[
                str,
                Path,
            ] = {}

            for source_file in source_files:
                local_path = (
                    temporary_root
                    / Path(
                        source_file.object_key
                    ).name
                )

                download_and_verify_source(
                    source_file=source_file,
                    destination=local_path,
                    client=client,
                )

                local_paths[
                    source_file.collection
                ] = local_path

            cassette_bundle = (
                parse_sc_workbook(
                    local_paths[
                        "sleep-cassette"
                    ]
                )
            )

            telemetry_bundle = (
                parse_st_workbook(
                    local_paths[
                        "sleep-telemetry"
                    ]
                )
            )

            bundle = (
                merge_subject_metadata_bundles(
                    cassette_bundle,
                    telemetry_bundle,
                )
            )

            source_object_keys = {
                item.collection: (
                    item.object_key
                )
                for item in source_files
            }

            subjects_table = (
                subjects_to_table(
                    bundle.subjects,
                    source_system=(
                        SOURCE_SYSTEM
                    ),
                    dataset_version=(
                        settings
                        .sleep_edf_version
                    ),
                    source_bucket=(
                        BRONZE_BUCKET
                    ),
                    source_object_keys=(
                        source_object_keys
                    ),
                )
            )

            contexts_table = (
                recording_contexts_to_table(
                    bundle.recording_contexts,
                    source_system=(
                        SOURCE_SYSTEM
                    ),
                    dataset_version=(
                        settings
                        .sleep_edf_version
                    ),
                    source_bucket=(
                        BRONZE_BUCKET
                    ),
                    source_object_keys=(
                        source_object_keys
                    ),
                )
            )

            subjects_key, contexts_key = (
                data_object_keys(
                    output_prefix
                )
            )

            write_results = (
                upload_silver_table(
                    table=subjects_table,
                    bucket=silver_bucket,
                    object_key=subjects_key,
                    client=client,
                ),
                upload_silver_table(
                    table=contexts_table,
                    bucket=silver_bucket,
                    object_key=contexts_key,
                    client=client,
                ),
            )

            manifest = {
                "source_system": (
                    SOURCE_SYSTEM
                ),
                "dataset_version": (
                    settings
                    .sleep_edf_version
                ),
                "schema_version": (
                    SCHEMA_VERSION
                ),
                "transform_version": (
                    TRANSFORM_VERSION
                ),
                "input_fingerprint": (
                    input_fingerprint
                ),
                "output_prefix": (
                    output_prefix
                ),
                "subject_count": (
                    subjects_table.num_rows
                ),
                "recording_context_count": (
                    contexts_table.num_rows
                ),
                "source_files": [
                    {
                        "collection": (
                            item.collection
                        ),
                        "file_id": str(
                            item.file_id
                        ),
                        "bucket": item.bucket,
                        "object_key": (
                            item.object_key
                        ),
                        "file_size_bytes": (
                            item.file_size_bytes
                        ),
                        "checksum_sha256": (
                            item.checksum_sha256
                        ),
                    }
                    for item in source_files
                ],
                "data_objects": [
                    result_to_manifest_object(
                        item
                    )
                    for item in write_results
                ],
            }

            manifest_bytes = json.dumps(
                manifest,
                sort_keys=True,
                indent=2,
            ).encode("utf-8")

            put_bytes_object(
                bucket=silver_bucket,
                object_key=(
                    success_object_key(
                        output_prefix
                    )
                ),
                data=manifest_bytes,
                content_type=(
                    "application/json"
                ),
                client=client,
            )

            completed = (
                validate_completed_output(
                    bucket=silver_bucket,
                    output_prefix=(
                        output_prefix
                    ),
                    input_fingerprint=(
                        input_fingerprint
                    ),
                    client=client,
                )
            )

            if completed is None:
                raise RuntimeError(
                    "Subject metadata output "
                    "was not completed"
                )

            return (
                SubjectMetadataPipelineResult(
                    status="written",
                    output_prefix=(
                        output_prefix
                    ),
                    input_fingerprint=(
                        input_fingerprint
                    ),
                    subject_count=(
                        subjects_table.num_rows
                    ),
                    recording_context_count=(
                        contexts_table.num_rows
                    ),
                    data_object_count=2,
                    total_object_count=3,
                    recovered_partial_output=(
                        recovered_object_count
                        > 0
                    ),
                )
            )

    finally:
        if owns_client:
            client.close()
