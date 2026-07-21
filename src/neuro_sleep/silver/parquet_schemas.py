from __future__ import annotations

import pyarrow as pa


SCHEMA_VERSION = "1.0.0"


def build_silver_schema(
    dataset_name: str,
    fields: list[pa.Field],
) -> pa.Schema:
    if not dataset_name.strip():
        raise ValueError(
            "dataset_name cannot be empty"
        )

    return pa.schema(
        fields,
        metadata={
            b"lakehouse_layer": b"silver",
            b"dataset_name": (
                dataset_name.encode("utf-8")
            ),
            b"schema_version": (
                SCHEMA_VERSION.encode("utf-8")
            ),
        },
    )


RECORDINGS_SCHEMA = build_silver_schema(
    dataset_name="recordings",
    fields=[
        pa.field(
            "recording_id",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "source_system",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "psg_bucket",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "psg_object_key",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "hypnogram_bucket",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "hypnogram_object_key",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "recording_start",
            pa.timestamp("us"),
            nullable=False,
        ),
        pa.field(
            "duration_seconds",
            pa.float64(),
            nullable=False,
        ),
        pa.field(
            "channel_count",
            pa.int16(),
            nullable=False,
        ),
        pa.field(
            "annotation_count",
            pa.int32(),
            nullable=False,
        ),
        pa.field(
            "in_range_epoch_count",
            pa.int32(),
            nullable=False,
        ),
        pa.field(
            "out_of_range_epoch_count",
            pa.int32(),
            nullable=False,
        ),
        pa.field(
            "trailing_overhang_seconds",
            pa.float64(),
            nullable=False,
        ),
    ],
)


CHANNELS_SCHEMA = build_silver_schema(
    dataset_name="channels",
    fields=[
        pa.field(
            "channel_id",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "recording_id",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "position",
            pa.int16(),
            nullable=False,
        ),
        pa.field(
            "source_label",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "normalized_name",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "sampling_frequency_hz",
            pa.float64(),
            nullable=False,
        ),
        pa.field(
            "physical_dimension",
            pa.string(),
            nullable=True,
        ),
        pa.field(
            "physical_min",
            pa.float64(),
            nullable=False,
        ),
        pa.field(
            "physical_max",
            pa.float64(),
            nullable=False,
        ),
        pa.field(
            "digital_min",
            pa.int32(),
            nullable=False,
        ),
        pa.field(
            "digital_max",
            pa.int32(),
            nullable=False,
        ),
        pa.field(
            "samples_per_data_record",
            pa.int32(),
            nullable=False,
        ),
        pa.field(
            "prefiltering",
            pa.string(),
            nullable=True,
        ),
    ],
)


SLEEP_STAGE_INTERVALS_SCHEMA = (
    build_silver_schema(
        dataset_name=(
            "sleep_stage_intervals"
        ),
        fields=[
            pa.field(
                "interval_id",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "recording_id",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "source_annotation_index",
                pa.int32(),
                nullable=False,
            ),
            pa.field(
                "onset_seconds",
                pa.float64(),
                nullable=False,
            ),
            pa.field(
                "duration_seconds",
                pa.float64(),
                nullable=False,
            ),
            pa.field(
                "end_seconds",
                pa.float64(),
                nullable=False,
            ),
            pa.field(
                "source_label",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "normalized_stage",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "overlap_status",
                pa.string(),
                nullable=False,
            ),
        ],
    )
)


SLEEP_STAGE_EPOCHS_SCHEMA = (
    build_silver_schema(
        dataset_name="sleep_stage_epochs",
        fields=[
            pa.field(
                "epoch_id",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "recording_id",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "source_interval_id",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "source_annotation_index",
                pa.int32(),
                nullable=False,
            ),
            pa.field(
                "epoch_number",
                pa.int32(),
                nullable=False,
            ),
            pa.field(
                "start_seconds",
                pa.float64(),
                nullable=False,
            ),
            pa.field(
                "duration_seconds",
                pa.float64(),
                nullable=False,
            ),
            pa.field(
                "end_seconds",
                pa.float64(),
                nullable=False,
            ),
            pa.field(
                "source_label",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "normalized_stage",
                pa.string(),
                nullable=False,
            ),
        ],
    )
)


SIGNALS_SCHEMA = build_silver_schema(
    dataset_name="signals",
    fields=[
        pa.field(
            "recording_id",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "channel_id",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "sample_index",
            pa.int64(),
            nullable=False,
        ),
        pa.field(
            "elapsed_seconds",
            pa.float64(),
            nullable=False,
        ),
        pa.field(
            "epoch_number",
            pa.int32(),
            nullable=False,
        ),
        pa.field(
            "signal_value",
            pa.float64(),
            nullable=False,
        ),
    ],
)


SILVER_SCHEMAS: dict[
    str,
    pa.Schema,
] = {
    "recordings": RECORDINGS_SCHEMA,
    "channels": CHANNELS_SCHEMA,
    "sleep_stage_intervals": (
        SLEEP_STAGE_INTERVALS_SCHEMA
    ),
    "sleep_stage_epochs": (
        SLEEP_STAGE_EPOCHS_SCHEMA
    ),
    "signals": SIGNALS_SCHEMA,
}


def get_silver_schema(
    dataset_name: str,
) -> pa.Schema:
    try:
        return SILVER_SCHEMAS[
            dataset_name
        ]

    except KeyError as error:
        supported = ", ".join(
            sorted(SILVER_SCHEMAS)
        )

        raise ValueError(
            "Unsupported Silver dataset: "
            f"{dataset_name!r}. "
            f"Supported: {supported}"
        ) from error
