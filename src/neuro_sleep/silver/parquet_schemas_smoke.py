from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from neuro_sleep.silver.parquet_schemas import (
    CHANNELS_SCHEMA,
    RECORDINGS_SCHEMA,
    SCHEMA_VERSION,
    SIGNALS_SCHEMA,
    SILVER_SCHEMAS,
    SLEEP_STAGE_EPOCHS_SCHEMA,
    SLEEP_STAGE_INTERVALS_SCHEMA,
    get_silver_schema,
)


SAMPLE_ROWS = {
    "recordings": {
        "recording_id": (
            "019f0000-0000-7000-"
            "8000-000000000001"
        ),
        "source_system": (
            "physionet_sleep_edf"
        ),
        "psg_bucket": "bronze",
        "psg_object_key": (
            "physionet/sleep-edfx/"
            "1.0.0/sleep-cassette/"
            "SC4001E0-PSG.edf"
        ),
        "hypnogram_bucket": "bronze",
        "hypnogram_object_key": (
            "physionet/sleep-edfx/"
            "1.0.0/sleep-cassette/"
            "SC4001EC-Hypnogram.edf"
        ),
        "recording_start": datetime(
            1989,
            4,
            24,
            16,
            13,
        ),
        "duration_seconds": 79500.0,
        "channel_count": 7,
        "annotation_count": 154,
        "in_range_epoch_count": 2650,
        "out_of_range_epoch_count": 230,
        "trailing_overhang_seconds": (
            6900.0
        ),
    },
    "channels": {
        "channel_id": (
            "019f0000-0000-7000-"
            "8000-000000000002"
        ),
        "recording_id": (
            "019f0000-0000-7000-"
            "8000-000000000001"
        ),
        "position": 6,
        "source_label": "Temp rectal",
        "normalized_name": "temp_rectal",
        "sampling_frequency_hz": 1.0,
        "physical_dimension": None,
        "physical_min": 34.0,
        "physical_max": 40.0,
        "digital_min": -2849,
        "digital_max": 2731,
        "samples_per_data_record": 30,
        "prefiltering": None,
    },
    "sleep_stage_intervals": {
        "interval_id": (
            "019f0000-0000-7000-"
            "8000-000000000003"
        ),
        "recording_id": (
            "019f0000-0000-7000-"
            "8000-000000000001"
        ),
        "source_annotation_index": 1,
        "onset_seconds": 30630.0,
        "duration_seconds": 120.0,
        "end_seconds": 30750.0,
        "source_label": "Sleep stage 1",
        "normalized_stage": "N1",
        "overlap_status": "inside_psg",
    },
    "sleep_stage_epochs": {
        "epoch_id": (
            "019f0000-0000-7000-"
            "8000-000000000004"
        ),
        "recording_id": (
            "019f0000-0000-7000-"
            "8000-000000000001"
        ),
        "source_interval_id": (
            "019f0000-0000-7000-"
            "8000-000000000003"
        ),
        "source_annotation_index": 1,
        "epoch_number": 1021,
        "start_seconds": 30630.0,
        "duration_seconds": 30.0,
        "end_seconds": 30660.0,
        "source_label": "Sleep stage 1",
        "normalized_stage": "N1",
    },
    "signals": {
        "recording_id": (
            "019f0000-0000-7000-"
            "8000-000000000001"
        ),
        "channel_id": (
            "019f0000-0000-7000-"
            "8000-000000000002"
        ),
        "sample_index": 3063000,
        "elapsed_seconds": 30630.0,
        "epoch_number": 1021,
        "signal_value": -4.25,
    },
}


def assert_schema_metadata(
    dataset_name: str,
    schema: pa.Schema,
) -> None:
    metadata = schema.metadata

    if metadata is None:
        raise RuntimeError(
            "Schema metadata is missing"
        )

    if (
        metadata.get(b"lakehouse_layer")
        != b"silver"
    ):
        raise RuntimeError(
            "Unexpected layer metadata"
        )

    if (
        metadata.get(b"dataset_name")
        != dataset_name.encode("utf-8")
    ):
        raise RuntimeError(
            "Unexpected dataset metadata"
        )

    if (
        metadata.get(b"schema_version")
        != SCHEMA_VERSION.encode("utf-8")
    ):
        raise RuntimeError(
            "Unexpected schema version"
        )


def run_smoke_test() -> None:
    if len(SILVER_SCHEMAS) != 5:
        raise RuntimeError(
            "Expected five Silver schemas"
        )

    if (
        RECORDINGS_SCHEMA
        .field("recording_start")
        .type
        != pa.timestamp("us")
    ):
        raise RuntimeError(
            "recording_start must preserve "
            "the source-local timestamp "
            "without inventing a timezone"
        )

    if not (
        CHANNELS_SCHEMA
        .field("physical_dimension")
        .nullable
    ):
        raise RuntimeError(
            "physical_dimension must be "
            "nullable"
        )

    if not (
        CHANNELS_SCHEMA
        .field("prefiltering")
        .nullable
    ):
        raise RuntimeError(
            "prefiltering must be nullable"
        )

    for schema in SILVER_SCHEMAS.values():
        field_names = schema.names

        if len(field_names) != len(
            set(field_names)
        ):
            raise RuntimeError(
                "Duplicate schema fields"
            )

    with TemporaryDirectory(
        prefix="neuro_sleep_parquet_schema_"
    ) as temporary_directory:
        root = Path(
            temporary_directory
        )

        for (
            dataset_name,
            schema,
        ) in SILVER_SCHEMAS.items():
            assert_schema_metadata(
                dataset_name=dataset_name,
                schema=schema,
            )

            table = pa.Table.from_pylist(
                [SAMPLE_ROWS[dataset_name]],
                schema=schema,
            )

            if table.num_rows != 1:
                raise RuntimeError(
                    "Unexpected Arrow row "
                    f"count: {dataset_name}"
                )

            output_path = (
                root
                / f"{dataset_name}.parquet"
            )

            pq.write_table(
                table,
                output_path,
                compression="zstd",
            )

            restored = pq.read_table(
                output_path
            )

            if not restored.schema.equals(
                schema,
                check_metadata=True,
            ):
                raise RuntimeError(
                    "Parquet schema round-trip "
                    f"failed: {dataset_name}"
                )

            if restored.num_rows != 1:
                raise RuntimeError(
                    "Parquet row round-trip "
                    f"failed: {dataset_name}"
                )

    if (
        get_silver_schema("signals")
        != SIGNALS_SCHEMA
    ):
        raise RuntimeError(
            "Schema lookup failed"
        )

    try:
        get_silver_schema(
            "unsupported_dataset"
        )

    except ValueError:
        print(
            "unsupported_dataset_blocked="
            "true"
        )

    else:
        raise RuntimeError(
            "Unsupported dataset was not "
            "blocked"
        )

    print("silver_schema_count=5")
    print(
        "recordings_schema_valid=true"
    )
    print("channels_schema_valid=true")
    print(
        "sleep_stage_intervals_schema_valid="
        "true"
    )
    print(
        "sleep_stage_epochs_schema_valid="
        "true"
    )
    print("signals_schema_valid=true")
    print(
        "nullable_channel_fields_valid=true"
    )
    print(
        "schema_metadata_valid=true"
    )
    print(
        "parquet_zstd_round_trip=true"
    )
    print(
        "parquet_schemas_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
