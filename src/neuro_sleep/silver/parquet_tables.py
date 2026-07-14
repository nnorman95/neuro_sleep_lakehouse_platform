from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from uuid import UUID

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from neuro_sleep.silver.models import (
    SilverChannel,
    SilverRecording,
    SleepStageEpoch,
    SleepStageInterval,
)
from neuro_sleep.silver.parquet_schemas import (
    CHANNELS_SCHEMA,
    RECORDINGS_SCHEMA,
    SIGNALS_SCHEMA,
    SLEEP_STAGE_EPOCHS_SCHEMA,
    SLEEP_STAGE_INTERVALS_SCHEMA,
)
from neuro_sleep.silver.signal_extractor import (
    SignalSampleChunk,
)


EPOCH_SECONDS = 30.0
PARQUET_COMPRESSION = "zstd"


def uuid_text(value: UUID) -> str:
    return str(value)


def recording_to_table(
    recording: SilverRecording,
) -> pa.Table:
    row = {
        "recording_id": uuid_text(
            recording.recording_id
        ),
        "source_system": (
            recording.source_system
        ),
        "psg_bucket": recording.psg_bucket,
        "psg_object_key": (
            recording.psg_object_key
        ),
        "hypnogram_bucket": (
            recording.hypnogram_bucket
        ),
        "hypnogram_object_key": (
            recording.hypnogram_object_key
        ),
        "recording_start": (
            recording.recording_start
        ),
        "duration_seconds": (
            recording.duration_seconds
        ),
        "channel_count": (
            recording.channel_count
        ),
        "annotation_count": (
            recording.annotation_count
        ),
        "in_range_epoch_count": (
            recording.in_range_epoch_count
        ),
        "out_of_range_epoch_count": (
            recording.out_of_range_epoch_count
        ),
        "trailing_overhang_seconds": (
            recording
            .trailing_overhang_seconds
        ),
    }

    return pa.Table.from_pylist(
        [row],
        schema=RECORDINGS_SCHEMA,
    )


def channels_to_table(
    channels: Iterable[SilverChannel],
) -> pa.Table:
    rows = [
        {
            "channel_id": uuid_text(
                channel.channel_id
            ),
            "recording_id": uuid_text(
                channel.recording_id
            ),
            "position": channel.position,
            "source_label": (
                channel.source_label
            ),
            "normalized_name": (
                channel.normalized_name
            ),
            "sampling_frequency_hz": (
                channel
                .sampling_frequency_hz
            ),
            "physical_dimension": (
                channel.physical_dimension
            ),
            "physical_min": (
                channel.physical_min
            ),
            "physical_max": (
                channel.physical_max
            ),
            "digital_min": (
                channel.digital_min
            ),
            "digital_max": (
                channel.digital_max
            ),
            "samples_per_data_record": (
                channel
                .samples_per_data_record
            ),
            "prefiltering": (
                channel.prefiltering
            ),
        }
        for channel in channels
    ]

    if not rows:
        raise ValueError(
            "At least one channel is "
            "required"
        )

    return pa.Table.from_pylist(
        rows,
        schema=CHANNELS_SCHEMA,
    )


def intervals_to_table(
    intervals: Iterable[
        SleepStageInterval
    ],
) -> pa.Table:
    rows = [
        {
            "interval_id": uuid_text(
                interval.interval_id
            ),
            "recording_id": uuid_text(
                interval.recording_id
            ),
            "source_annotation_index": (
                interval
                .source_annotation_index
            ),
            "onset_seconds": (
                interval.onset_seconds
            ),
            "duration_seconds": (
                interval.duration_seconds
            ),
            "end_seconds": (
                interval.end_seconds
            ),
            "source_label": (
                interval.source_label
            ),
            "normalized_stage": (
                interval.normalized_stage
            ),
            "overlap_status": (
                interval.overlap_status
            ),
        }
        for interval in intervals
    ]

    if not rows:
        raise ValueError(
            "At least one interval is "
            "required"
        )

    return pa.Table.from_pylist(
        rows,
        schema=(
            SLEEP_STAGE_INTERVALS_SCHEMA
        ),
    )


def epochs_to_table(
    epochs: Iterable[SleepStageEpoch],
) -> pa.Table:
    rows = [
        {
            "epoch_id": uuid_text(
                epoch.epoch_id
            ),
            "recording_id": uuid_text(
                epoch.recording_id
            ),
            "source_interval_id": (
                uuid_text(
                    epoch.source_interval_id
                )
            ),
            "source_annotation_index": (
                epoch
                .source_annotation_index
            ),
            "epoch_number": (
                epoch.epoch_number
            ),
            "start_seconds": (
                epoch.start_seconds
            ),
            "duration_seconds": (
                epoch.duration_seconds
            ),
            "end_seconds": (
                epoch.end_seconds
            ),
            "source_label": (
                epoch.source_label
            ),
            "normalized_stage": (
                epoch.normalized_stage
            ),
        }
        for epoch in epochs
    ]

    if not rows:
        raise ValueError(
            "At least one epoch is required"
        )

    return pa.Table.from_pylist(
        rows,
        schema=SLEEP_STAGE_EPOCHS_SCHEMA,
    )


def signal_chunk_to_table(
    chunk: SignalSampleChunk,
) -> pa.Table:
    sample_count = chunk.sample_count

    if sample_count <= 0:
        raise ValueError(
            "Signal chunk must contain "
            "samples"
        )

    if len(chunk.values) != sample_count:
        raise ValueError(
            "Signal values length does not "
            "match chunk sample range"
        )

    samples_per_epoch_exact = (
        chunk.sampling_frequency_hz
        * EPOCH_SECONDS
    )

    samples_per_epoch = round(
        samples_per_epoch_exact
    )

    if not np.isclose(
        samples_per_epoch_exact,
        samples_per_epoch,
        atol=1e-9,
    ):
        raise ValueError(
            "Channel sampling frequency "
            "does not align with 30-second "
            "epochs"
        )

    sample_indexes = np.arange(
        chunk.start_sample_index,
        chunk.stop_sample_index,
        dtype=np.int64,
    )

    elapsed_seconds = (
        sample_indexes.astype(
            np.float64
        )
        / chunk.sampling_frequency_hz
    )

    epoch_numbers = (
        sample_indexes
        // samples_per_epoch
    ).astype(
        np.int32,
        copy=False,
    )

    table = pa.Table.from_arrays(
        [
            pa.array(
                [
                    uuid_text(
                        chunk.recording_id
                    )
                ]
                * sample_count,
                type=pa.string(),
            ),
            pa.array(
                [
                    uuid_text(
                        chunk.channel_id
                    )
                ]
                * sample_count,
                type=pa.string(),
            ),
            pa.array(
                sample_indexes,
                type=pa.int64(),
            ),
            pa.array(
                elapsed_seconds,
                type=pa.float64(),
            ),
            pa.array(
                epoch_numbers,
                type=pa.int32(),
            ),
            pa.array(
                chunk.values,
                type=pa.float64(),
            ),
        ],
        schema=SIGNALS_SCHEMA,
    )

    if table.num_rows != sample_count:
        raise RuntimeError(
            "Signal Arrow table row count "
            "mismatch"
        )

    return table


def validate_silver_table(
    table: pa.Table,
) -> None:
    if table.num_rows <= 0:
        raise ValueError(
            "Silver table cannot be empty"
        )

    metadata = table.schema.metadata

    if (
        metadata is None
        or metadata.get(
            b"lakehouse_layer"
        )
        != b"silver"
    ):
        raise ValueError(
            "Arrow table is not a Silver "
            "dataset"
        )


def write_silver_parquet(
    table: pa.Table,
    output_path: Path,
) -> Path:
    validate_silver_table(table)

    output_path = (
        output_path
        .expanduser()
        .resolve()
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = (
        output_path.parent
        / f".{output_path.name}.tmp"
    )

    temporary_path.unlink(
        missing_ok=True
    )

    try:
        pq.write_table(
            table,
            temporary_path,
            compression=(
                PARQUET_COMPRESSION
            ),
            use_dictionary=True,
            write_statistics=True,
        )

        restored_schema = (
            pq.read_schema(
                temporary_path
            )
        )

        if not restored_schema.equals(
            table.schema,
            check_metadata=True,
        ):
            raise RuntimeError(
                "Written Parquet schema does "
                "not match Arrow table"
            )

        temporary_path.replace(
            output_path
        )

    except Exception:
        temporary_path.unlink(
            missing_ok=True
        )

        raise

    return output_path
