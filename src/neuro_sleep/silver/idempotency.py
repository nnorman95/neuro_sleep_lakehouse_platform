from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Literal
from uuid import UUID

from botocore.client import BaseClient

from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)
from neuro_sleep.silver.parquet_schemas import (
    SCHEMA_VERSION,
)
from neuro_sleep.silver.signal_extractor import (
    DEFAULT_CHUNK_DURATION_SECONDS,
)
from neuro_sleep.silver.source_lineage import (
    SilverSourceLineage,
    build_source_pair_id,
    canonical_source_pair_text,
    resolve_silver_source_lineage,
)
from neuro_sleep.silver.silver_recording_writer import (
    QualityReportHandler,
    SilverRecordingWriteResult,
    validate_output_prefix,
    write_silver_recording,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
)


SILVER_TRANSFORM_VERSION = "1.1.0"
SUCCESS_OBJECT_NAME = "_SUCCESS.json"
SUCCESS_CONTENT_TYPE = "application/json"

IdempotencyStatus = Literal[
    "written",
    "skipped",
]


class PartialSilverOutputError(
    RuntimeError
):
    """Raised when data exists without a success marker."""


@dataclass(frozen=True)
class SilverIdempotentWriteResult:
    status: IdempotencyStatus
    source_pair_id: str
    input_fingerprint: str
    config_id: str
    output_prefix: str

    psg_file_id: UUID
    hypnogram_file_id: UUID
    psg_checksum_sha256: str
    hypnogram_checksum_sha256: str

    data_object_count: int
    total_object_count: int

    recording_id: UUID
    write_result: (
        SilverRecordingWriteResult
        | None
    )

    recovered_partial_output: bool = False
    recovered_object_count: int = 0

    @property
    def skipped(self) -> bool:
        return self.status == "skipped"


def canonical_transform_config_text(
    signal_chunk_duration_seconds: float,
    signal_start_seconds: float,
    signal_stop_seconds: float | None,
    include_signals: bool = True,
) -> str:
    stop_text = (
        "full"
        if signal_stop_seconds is None
        else format(
            signal_stop_seconds,
            ".17g",
        )
    )

    parts = [
        f"schema_version={SCHEMA_VERSION}",
        f"transform_version={SILVER_TRANSFORM_VERSION}",
        "signal_chunk_duration_seconds="
        f"{format(signal_chunk_duration_seconds, '.17g')}",
        "signal_start_seconds="
        f"{format(signal_start_seconds, '.17g')}",
        f"signal_stop_seconds={stop_text}",
    ]

    if not include_signals:
        parts.append("include_signals=false")

    return "\n".join(parts)


def build_config_id(
    signal_chunk_duration_seconds: float,
    signal_start_seconds: float,
    signal_stop_seconds: float | None,
    include_signals: bool = True,
) -> str:
    canonical_text = canonical_transform_config_text(
        signal_chunk_duration_seconds=signal_chunk_duration_seconds,
        signal_start_seconds=signal_start_seconds,
        signal_stop_seconds=signal_stop_seconds,
        include_signals=include_signals,
    )

    return sha256(
        canonical_text.encode("utf-8")
    ).hexdigest()


def build_idempotent_output_prefix(
    root_prefix: str,
    source_pair_id: str,
    input_fingerprint: str,
    config_id: str,
) -> str:
    cleaned_root = validate_output_prefix(
        root_prefix
    )

    return (
        f"{cleaned_root}/"
        f"schema_version={SCHEMA_VERSION}/"
        "transform_version="
        f"{SILVER_TRANSFORM_VERSION}/"
        f"source_pair_id={source_pair_id}/"
        "input_fingerprint="
        f"{input_fingerprint}/"
        f"config_id={config_id}"
    )


def build_success_object_key(
    output_prefix: str,
) -> str:
    return (
        f"{output_prefix}/"
        f"{SUCCESS_OBJECT_NAME}"
    )


def list_prefix_objects(
    bucket: str,
    output_prefix: str,
    client: BaseClient,
):
    return list_object_summaries(
        bucket=bucket,
        prefix=f"{output_prefix}/",
        client=client,
    )


def delete_prefix_objects(
    bucket: str,
    output_prefix: str,
    client: BaseClient,
) -> None:
    objects = list_prefix_objects(
        bucket=bucket,
        output_prefix=output_prefix,
        client=client,
    )

    for item in objects:
        run_object_storage_operation(
            operation=lambda object_key=(
                item.object_key
            ): client.delete_object(
                Bucket=bucket,
                Key=object_key,
            ),
            operation_name=(
                f"delete_object:{bucket}/"
                f"{item.object_key}"
            ),
        )


def recover_partial_output_prefix(
    bucket: str,
    output_prefix: str,
    client: BaseClient,
) -> int:
    existing_objects = list_prefix_objects(
        bucket=bucket,
        output_prefix=output_prefix,
        client=client,
    )

    if not existing_objects:
        return 0

    success_object_key = (
        build_success_object_key(
            output_prefix
        )
    )

    existing_keys = {
        item.object_key
        for item in existing_objects
    }

    if success_object_key in existing_keys:
        raise PartialSilverOutputError(
            "Automatic recovery refuses to "
            "delete a Silver prefix that "
            f"contains {SUCCESS_OBJECT_NAME}: "
            f"{output_prefix}"
        )

    recovered_object_count = len(
        existing_objects
    )

    delete_prefix_objects(
        bucket=bucket,
        output_prefix=output_prefix,
        client=client,
    )

    remaining_objects = list_prefix_objects(
        bucket=bucket,
        output_prefix=output_prefix,
        client=client,
    )

    if remaining_objects:
        remaining_keys = ", ".join(
            item.object_key
            for item in remaining_objects
        )

        raise PartialSilverOutputError(
            "Partial Silver output recovery "
            "did not remove every object: "
            f"{remaining_keys}"
        )

    return recovered_object_count


def build_success_manifest(
    write_result: (
        SilverRecordingWriteResult
    ),
    source_lineage: SilverSourceLineage,
    config_id: str,
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
    signal_chunk_duration_seconds: float,
    signal_start_seconds: float,
    signal_stop_seconds: float | None,
    include_signals: bool = True,
) -> dict[str, object]:
    object_results = (
        *write_result.metadata_objects,
        *write_result.signal_objects,
    )

    return {
        "status": "complete",
        "lakehouse_layer": "silver",
        "schema_version": SCHEMA_VERSION,
        "transform_version": (
            SILVER_TRANSFORM_VERSION
        ),
        "source_pair_id": (
            source_lineage.source_pair_id
        ),
        "input_fingerprint": (
            source_lineage.input_fingerprint
        ),
        "config_id": config_id,
        "recording_id": str(
            write_result.bundle.recording_id
        ),
        "source": {
            "source_system": (
                source_lineage.source_system
            ),
            "psg_file_id": str(
                source_lineage.psg_file_id
            ),
            "hypnogram_file_id": str(
                source_lineage
                .hypnogram_file_id
            ),
            "psg_bucket": psg_bucket,
            "psg_object_key": (
                psg_object_key
            ),
            "hypnogram_bucket": (
                hypnogram_bucket
            ),
            "hypnogram_object_key": (
                hypnogram_object_key
            ),
            "psg_checksum_sha256": (
                source_lineage
                .psg_checksum_sha256
            ),
            "hypnogram_checksum_sha256": (
                source_lineage
                .hypnogram_checksum_sha256
            ),
        },
        "transform_config": {
            "signal_chunk_duration_seconds": (
                signal_chunk_duration_seconds
            ),
            "signal_start_seconds": (
                signal_start_seconds
            ),
            "signal_stop_seconds": (
                signal_stop_seconds
            ),
            "include_signals": include_signals,
        },
        "quality": {
            "error_count": (
                write_result
                .quality_report
                .error_count
            ),
            "warning_count": (
                write_result
                .quality_report
                .warning_count
            ),
            "warning_codes": [
                issue.code
                for issue in (
                    write_result
                    .quality_report
                    .issues
                )
                if issue.severity
                == "warning"
            ],
        },
        "data_object_count": len(
            object_results
        ),
        "row_count": (
            write_result.row_count
        ),
        "objects": [
            {
                "bucket": item.bucket,
                "object_key": (
                    item.object_key
                ),
                "dataset_name": (
                    item.dataset_name
                ),
                "row_count": (
                    item.row_count
                ),
                "file_size_bytes": (
                    item.file_size_bytes
                ),
                "checksum_sha256": (
                    item.checksum_sha256
                ),
                "etag": item.etag,
            }
            for item in object_results
        ],
    }


def upload_success_manifest(
    bucket: str,
    output_prefix: str,
    manifest: dict[str, object],
    client: BaseClient,
) -> None:
    object_key = build_success_object_key(
        output_prefix
    )

    body = json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    run_object_storage_operation(
        operation=lambda: client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=body,
            ContentLength=len(body),
            ContentType=(
                SUCCESS_CONTENT_TYPE
            ),
            Metadata={
                "lakehouse_layer": "silver",
                "artifact": (
                    "success_manifest"
                ),
                "schema_version": (
                    SCHEMA_VERSION
                ),
                "source_pair_id": (
                    str(
                        manifest[
                            "source_pair_id"
                        ]
                    )
                ),
                "input_fingerprint": (
                    str(
                        manifest[
                            "input_fingerprint"
                        ]
                    )
                ),
                "config_id": (
                    str(
                        manifest[
                            "config_id"
                        ]
                    )
                ),
            },
        ),
        operation_name=(
            f"put_object:{bucket}/"
            f"{object_key}"
        ),
    )


def read_success_manifest(
    bucket: str,
    output_prefix: str,
    client: BaseClient,
) -> dict[str, object]:
    object_key = build_success_object_key(
        output_prefix
    )

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

    try:
        body = response[
            "Body"
        ].read()

    finally:
        response["Body"].close()

    manifest = json.loads(
        body.decode("utf-8")
    )

    if not isinstance(manifest, dict):
        raise ValueError(
            "Silver success manifest must "
            "be a JSON object"
        )

    return manifest


def validate_existing_manifest(
    manifest: dict[str, object],
    source_lineage: SilverSourceLineage,
    config_id: str,
) -> UUID:
    if manifest.get("status") != (
        "complete"
    ):
        raise ValueError(
            "Silver success manifest is not "
            "complete"
        )

    if (
        manifest.get("source_pair_id")
        != source_lineage.source_pair_id
    ):
        raise ValueError(
            "Silver success manifest source "
            "pair does not match"
        )

    if (
        manifest.get("input_fingerprint")
        != source_lineage.input_fingerprint
    ):
        raise ValueError(
            "Silver success manifest input "
            "fingerprint does not match"
        )

    source = manifest.get("source")

    if not isinstance(source, dict):
        raise ValueError(
            "Silver success manifest source "
            "lineage is invalid"
        )

    if (
        source.get("psg_checksum_sha256")
        != source_lineage
        .psg_checksum_sha256
    ):
        raise ValueError(
            "Silver success manifest PSG "
            "checksum does not match"
        )

    if (
        source.get(
            "hypnogram_checksum_sha256"
        )
        != source_lineage
        .hypnogram_checksum_sha256
    ):
        raise ValueError(
            "Silver success manifest "
            "Hypnogram checksum does not match"
        )

    if (
        manifest.get("config_id")
        != config_id
    ):
        raise ValueError(
            "Silver success manifest config "
            "does not match"
        )

    if (
        manifest.get("schema_version")
        != SCHEMA_VERSION
    ):
        raise ValueError(
            "Silver success manifest schema "
            "version does not match"
        )

    recording_id_text = manifest.get(
        "recording_id"
    )

    if not isinstance(
        recording_id_text,
        str,
    ):
        raise ValueError(
            "Silver success manifest has no "
            "recording_id"
        )

    recording_id = UUID(
        recording_id_text
    )

    if recording_id.version != 7:
        raise ValueError(
            "Manifest recording_id is not "
            "UUIDv7"
        )

    return recording_id


def write_silver_recording_idempotent(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
    silver_bucket: str,
    root_prefix: str,
    *,
    signal_chunk_duration_seconds: float = (
        DEFAULT_CHUNK_DURATION_SECONDS
    ),
    signal_start_seconds: float = 0.0,
    signal_stop_seconds: float | None = None,
    include_signals: bool = True,
    quality_report_handler: (
        QualityReportHandler | None
    ) = None,
    client: BaseClient | None = None,
) -> SilverIdempotentWriteResult:
    source_lineage = (
        resolve_silver_source_lineage(
            psg_bucket=psg_bucket,
            psg_object_key=psg_object_key,
            hypnogram_bucket=(
                hypnogram_bucket
            ),
            hypnogram_object_key=(
                hypnogram_object_key
            ),
        )
    )

    source_pair_id = (
        source_lineage.source_pair_id
    )

    input_fingerprint = (
        source_lineage.input_fingerprint
    )

    config_id = build_config_id(
        signal_chunk_duration_seconds=(
            signal_chunk_duration_seconds
        ),
        signal_start_seconds=(
            signal_start_seconds
        ),
        signal_stop_seconds=(
            signal_stop_seconds
        ),
        include_signals=include_signals,
    )

    output_prefix = (
        build_idempotent_output_prefix(
            root_prefix=root_prefix,
            source_pair_id=source_pair_id,
            input_fingerprint=(
                input_fingerprint
            ),
            config_id=config_id,
        )
    )

    success_object_key = (
        build_success_object_key(
            output_prefix
        )
    )

    owns_client = client is None

    if client is None:
        client = get_object_storage_client()

    recovered_partial_output = False
    recovered_object_count = 0

    try:
        existing_objects = (
            list_prefix_objects(
                bucket=silver_bucket,
                output_prefix=(
                    output_prefix
                ),
                client=client,
            )
        )

        existing_keys = {
            item.object_key
            for item in existing_objects
        }

        if success_object_key in (
            existing_keys
        ):
            manifest = (
                read_success_manifest(
                    bucket=silver_bucket,
                    output_prefix=(
                        output_prefix
                    ),
                    client=client,
                )
            )

            recording_id = (
                validate_existing_manifest(
                    manifest=manifest,
                    source_lineage=(
                        source_lineage
                    ),
                    config_id=config_id,
                )
            )

            data_object_count = (
                len(existing_objects) - 1
            )

            expected_data_object_count = (
                manifest.get(
                    "data_object_count"
                )
            )

            if (
                expected_data_object_count
                != data_object_count
            ):
                raise PartialSilverOutputError(
                    "Silver success manifest "
                    "object count does not "
                    "match stored objects"
                )

            return (
                SilverIdempotentWriteResult(
                    status="skipped",
                    source_pair_id=(
                        source_pair_id
                    ),
                    input_fingerprint=(
                        input_fingerprint
                    ),
                    config_id=config_id,
                    psg_file_id=(
                        source_lineage.psg_file_id
                    ),
                    hypnogram_file_id=(
                        source_lineage
                        .hypnogram_file_id
                    ),
                    psg_checksum_sha256=(
                        source_lineage
                        .psg_checksum_sha256
                    ),
                    hypnogram_checksum_sha256=(
                        source_lineage
                        .hypnogram_checksum_sha256
                    ),
                    output_prefix=(
                        output_prefix
                    ),
                    data_object_count=(
                        data_object_count
                    ),
                    total_object_count=len(
                        existing_objects
                    ),
                    recording_id=(
                        recording_id
                    ),
                    write_result=None,
                    recovered_partial_output=(
                        False
                    ),
                    recovered_object_count=0,
                )
            )

        if existing_objects:
            recovered_object_count = (
                recover_partial_output_prefix(
                    bucket=silver_bucket,
                    output_prefix=(
                        output_prefix
                    ),
                    client=client,
                )
            )

            recovered_partial_output = (
                recovered_object_count > 0
            )

        write_result = (
            write_silver_recording(
                psg_bucket=psg_bucket,
                psg_object_key=(
                    psg_object_key
                ),
                hypnogram_bucket=(
                    hypnogram_bucket
                ),
                hypnogram_object_key=(
                    hypnogram_object_key
                ),
                silver_bucket=(
                    silver_bucket
                ),
                output_prefix=(
                    output_prefix
                ),
                signal_chunk_duration_seconds=(
                    signal_chunk_duration_seconds
                ),
                signal_start_seconds=(
                    signal_start_seconds
                ),
                signal_stop_seconds=(
                    signal_stop_seconds
                ),
                include_signals=include_signals,
                quality_report_handler=(
                    quality_report_handler
                ),
                client=client,
            )
        )

        manifest = build_success_manifest(
            write_result=write_result,
            source_lineage=source_lineage,
            config_id=config_id,
            psg_bucket=psg_bucket,
            psg_object_key=psg_object_key,
            hypnogram_bucket=(
                hypnogram_bucket
            ),
            hypnogram_object_key=(
                hypnogram_object_key
            ),
            signal_chunk_duration_seconds=(
                signal_chunk_duration_seconds
            ),
            signal_start_seconds=(
                signal_start_seconds
            ),
            signal_stop_seconds=(
                signal_stop_seconds
            ),
            include_signals=include_signals,
        )

        try:
            upload_success_manifest(
                bucket=silver_bucket,
                output_prefix=(
                    output_prefix
                ),
                manifest=manifest,
                client=client,
            )

        except Exception:
            delete_prefix_objects(
                bucket=silver_bucket,
                output_prefix=(
                    output_prefix
                ),
                client=client,
            )

            raise

        return SilverIdempotentWriteResult(
            status="written",
            source_pair_id=(
                source_pair_id
            ),
            input_fingerprint=(
                input_fingerprint
            ),
            config_id=config_id,
            psg_file_id=(
                source_lineage.psg_file_id
            ),
            hypnogram_file_id=(
                source_lineage
                .hypnogram_file_id
            ),
            psg_checksum_sha256=(
                source_lineage
                .psg_checksum_sha256
            ),
            hypnogram_checksum_sha256=(
                source_lineage
                .hypnogram_checksum_sha256
            ),
            output_prefix=(
                output_prefix
            ),
            data_object_count=(
                write_result.object_count
            ),
            total_object_count=(
                write_result.object_count
                + 1
            ),
            recording_id=(
                write_result
                .bundle
                .recording_id
            ),
            write_result=write_result,
            recovered_partial_output=(
                recovered_partial_output
            ),
            recovered_object_count=(
                recovered_object_count
            ),
        )

    finally:
        if owns_client:
            client.close()
