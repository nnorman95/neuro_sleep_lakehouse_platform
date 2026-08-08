from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Callable

from botocore.client import BaseClient

from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)
from neuro_sleep.silver.bronze_edf_reader import (
    open_bronze_edf_pair,
)
from neuro_sleep.silver.parquet_tables import (
    channels_to_table,
    epochs_to_table,
    intervals_to_table,
    recording_to_table,
    signal_chunk_to_table,
)
from neuro_sleep.silver.quality_checks import (
    SilverQualityReport,
    run_silver_quality_checks,
)
from neuro_sleep.silver.recording_builder import (
    SilverRecordingBundle,
    build_silver_recording_from_documents,
)
from neuro_sleep.silver.signal_extractor import (
    DEFAULT_CHUNK_DURATION_SECONDS,
    iter_recording_signal_chunks,
)
from neuro_sleep.silver.silver_object_writer import (
    SilverObjectWriteResult,
    upload_silver_table,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
)


QualityReportHandler = Callable[
    [
        SilverRecordingBundle,
        SilverQualityReport,
        str,
    ],
    None,
]


@dataclass(frozen=True)
class SilverRecordingWriteResult:
    bundle: SilverRecordingBundle
    quality_report: SilverQualityReport

    metadata_objects: tuple[
        SilverObjectWriteResult,
        ...,
    ]

    signal_objects: tuple[
        SilverObjectWriteResult,
        ...,
    ]

    output_prefix: str

    @property
    def object_count(self) -> int:
        return (
            len(self.metadata_objects)
            + len(self.signal_objects)
        )

    @property
    def row_count(self) -> int:
        return sum(
            result.row_count
            for result in (
                *self.metadata_objects,
                *self.signal_objects,
            )
        )


def validate_output_prefix(
    output_prefix: str,
) -> str:
    cleaned_prefix = (
        output_prefix.strip().strip("/")
    )

    if not cleaned_prefix:
        raise ValueError(
            "output_prefix cannot be empty"
        )

    if "\\" in cleaned_prefix:
        raise ValueError(
            "output_prefix must use forward "
            "slashes"
        )

    prefix_path = PurePosixPath(
        cleaned_prefix
    )

    if ".." in prefix_path.parts:
        raise ValueError(
            "Parent path traversal is not "
            "allowed"
        )

    return cleaned_prefix


def build_metadata_object_keys(
    output_prefix: str,
) -> dict[str, str]:
    return {
        "recordings": (
            f"{output_prefix}/"
            "recordings/"
            "part-00000.parquet"
        ),
        "channels": (
            f"{output_prefix}/"
            "channels/"
            "part-00000.parquet"
        ),
        "sleep_stage_intervals": (
            f"{output_prefix}/"
            "sleep_stage_intervals/"
            "part-00000.parquet"
        ),
        "sleep_stage_epochs": (
            f"{output_prefix}/"
            "sleep_stage_epochs/"
            "part-00000.parquet"
        ),
    }


def build_signal_object_key(
    output_prefix: str,
    normalized_channel_name: str,
    start_sample_index: int,
    stop_sample_index: int,
) -> str:
    return (
        f"{output_prefix}/signals/"
        f"channel={normalized_channel_name}/"
        f"part-{start_sample_index:012d}-"
        f"{stop_sample_index:012d}.parquet"
    )


def delete_uploaded_objects(
    client: BaseClient,
    results: list[
        SilverObjectWriteResult
    ],
) -> None:
    for result in reversed(results):
        run_object_storage_operation(
            operation=lambda item=result: (
                client.delete_object(
                    Bucket=item.bucket,
                    Key=item.object_key,
                )
            ),
            operation_name=(
                f"delete_object:"
                f"{result.bucket}/"
                f"{result.object_key}"
            ),
        )


def write_silver_recording(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
    silver_bucket: str,
    output_prefix: str,
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
) -> SilverRecordingWriteResult:
    cleaned_prefix = (
        validate_output_prefix(
            output_prefix
        )
    )

    owns_client = client is None

    if client is None:
        client = get_object_storage_client()

    uploaded_results: list[
        SilverObjectWriteResult
    ] = []

    try:
        with open_bronze_edf_pair(
            psg_bucket=psg_bucket,
            psg_object_key=psg_object_key,
            hypnogram_bucket=(
                hypnogram_bucket
            ),
            hypnogram_object_key=(
                hypnogram_object_key
            ),
            client=client,
        ) as pair:
            bundle = (
                build_silver_recording_from_documents(
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
                    psg_document=(
                        pair.psg.document
                    ),
                    hypnogram_document=(
                        pair.hypnogram
                        .document
                    ),
                )
            )

            quality_report = (
                run_silver_quality_checks(
                    bundle
                )
            )

            if (
                quality_report_handler
                is not None
            ):
                quality_report_handler(
                    bundle,
                    quality_report,
                    cleaned_prefix,
                )

            quality_report.raise_for_errors()

            metadata_tables = {
                "recordings": (
                    recording_to_table(
                        bundle.recording
                    )
                ),
                "channels": (
                    channels_to_table(
                        bundle.channels
                    )
                ),
                "sleep_stage_intervals": (
                    intervals_to_table(
                        bundle.intervals
                    )
                ),
                "sleep_stage_epochs": (
                    epochs_to_table(
                        bundle.epochs
                    )
                ),
            }

            metadata_keys = (
                build_metadata_object_keys(
                    cleaned_prefix
                )
            )

            metadata_results: list[
                SilverObjectWriteResult
            ] = []

            for (
                dataset_name,
                table,
            ) in metadata_tables.items():
                result = upload_silver_table(
                    table=table,
                    bucket=silver_bucket,
                    object_key=(
                        metadata_keys[
                            dataset_name
                        ]
                    ),
                    client=client,
                )

                metadata_results.append(
                    result
                )

                uploaded_results.append(
                    result
                )

            signal_results: list[
                SilverObjectWriteResult
            ] = []

            if include_signals:
                for chunk in (
                    iter_recording_signal_chunks(
                        recording_id=(
                            bundle.recording_id
                        ),
                        channels=(
                            bundle.channels
                        ),
                        psg_document=(
                            pair.psg.document
                        ),
                        recording_duration_seconds=(
                            bundle.recording
                            .duration_seconds
                        ),
                        chunk_duration_seconds=(
                            signal_chunk_duration_seconds
                        ),
                        start_seconds=(
                            signal_start_seconds
                        ),
                        stop_seconds=(
                            signal_stop_seconds
                        ),
                    )
                ):
                    signal_table = (
                        signal_chunk_to_table(
                            chunk
                        )
                    )

                    object_key = (
                        build_signal_object_key(
                            output_prefix=(
                                cleaned_prefix
                            ),
                            normalized_channel_name=(
                                chunk
                                .normalized_name
                            ),
                            start_sample_index=(
                                chunk
                                .start_sample_index
                            ),
                            stop_sample_index=(
                                chunk
                                .stop_sample_index
                            ),
                        )
                    )

                    result = upload_silver_table(
                        table=signal_table,
                        bucket=silver_bucket,
                        object_key=object_key,
                        client=client,
                    )

                    signal_results.append(
                        result
                    )

                    uploaded_results.append(
                        result
                    )

        return SilverRecordingWriteResult(
            bundle=bundle,
            quality_report=quality_report,
            metadata_objects=tuple(
                metadata_results
            ),
            signal_objects=tuple(
                signal_results
            ),
            output_prefix=(
                cleaned_prefix
            ),
        )

    except Exception:
        delete_uploaded_objects(
            client=client,
            results=uploaded_results,
        )

        raise

    finally:
        if owns_client:
            client.close()
