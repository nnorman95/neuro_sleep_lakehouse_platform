from __future__ import annotations

from pyspark.sql import types as T

from neuro_sleep.spark.feature_integration import (
    EpochLabelIntegrationContext,
    RecordingChannelIntegrationContext,
    build_epoch_label_context_frame,
    build_integrated_feature_frame,
    build_recording_channel_context_frame,
)
from neuro_sleep.spark.session import create_spark_session


_GOLD_SCHEMA = T.StructType(
    [
        T.StructField("source_system", T.StringType(), False),
        T.StructField("dataset_version", T.StringType(), False),
        T.StructField("collection", T.StringType(), False),
        T.StructField("recording_key", T.StringType(), False),
        T.StructField("recording_id", T.StringType(), False),
        T.StructField("channel_id", T.StringType(), False),
        T.StructField("channel_position", T.IntegerType(), False),
        T.StructField("source_label", T.StringType(), False),
        T.StructField("normalized_name", T.StringType(), False),
        T.StructField("sampling_frequency_hz", T.DoubleType(), False),
        T.StructField("epoch_number", T.IntegerType(), False),
        T.StructField("window_start_seconds", T.DoubleType(), False),
        T.StructField("window_end_seconds", T.DoubleType(), False),
        T.StructField("window_duration_seconds", T.DoubleType(), False),
        T.StructField("is_partial_window", T.BooleanType(), False),
        T.StructField("sample_count", T.LongType(), False),
        T.StructField("expected_sample_count", T.LongType(), False),
        T.StructField("sample_coverage_pct", T.DoubleType(), False),
        T.StructField("first_sample_index", T.LongType(), False),
        T.StructField("last_sample_index", T.LongType(), False),
        T.StructField("first_sample_elapsed_seconds", T.DoubleType(), False),
        T.StructField("last_sample_elapsed_seconds", T.DoubleType(), False),
        T.StructField("invalid_signal_sample_count", T.LongType(), False),
        T.StructField("mean", T.DoubleType(), False),
        T.StructField("stddev_pop", T.DoubleType(), False),
        T.StructField("min", T.DoubleType(), False),
        T.StructField("max", T.DoubleType(), False),
        T.StructField("peak_to_peak", T.DoubleType(), False),
        T.StructField("rms", T.DoubleType(), False),
        T.StructField("samples_per_full_window", T.LongType(), False),
        T.StructField("feature_version", T.StringType(), False),
    ]
)


def _gold_row(epoch_number: int) -> tuple[object, ...]:
    start = float(epoch_number * 30)
    return (
        "synthetic", "1.0.0", "synthetic-collection", "SYN0001",
        "recording-1", "channel-1", 0, "EEG synthetic", "eeg_synthetic",
        100.0, epoch_number, start, start + 30.0, 30.0, False,
        3000, 3000, 100.0, epoch_number * 3000,
        epoch_number * 3000 + 2999, start, start + 29.99, 0,
        1.0, 0.5, 0.0, 2.0, 2.0, 1.1, 3000, "1.0.0",
    )


def run_smoke_test() -> None:
    spark = create_spark_session(
        "neurosleep-feature-integration-smoke",
        master="local[1]",
        ui_enabled=False,
    )
    try:
        gold = spark.createDataFrame(
            [_gold_row(0), _gold_row(1)],
            schema=_GOLD_SCHEMA,
        )
        recording_context = build_recording_channel_context_frame(
            spark,
            contexts=(
                RecordingChannelIntegrationContext(
                    recording_id="recording-1",
                    channel_id="channel-1",
                    subject_sk="subject-sk-1",
                    subject_key="subject-1",
                    age_years=42,
                    sex="F",
                    recording_sk="recording-sk-1",
                    channel_sk="channel-sk-1",
                    night_number=1,
                    treatment=None,
                    lights_off_seconds=120.0,
                ),
            ),
        )
        epoch_context = build_epoch_label_context_frame(
            spark,
            contexts=(
                EpochLabelIntegrationContext(
                    recording_id="recording-1",
                    epoch_number=0,
                    sleep_epoch_sk="sleep-epoch-sk-1",
                    sleep_stage_sk=3,
                    silver_epoch_id="silver-epoch-1",
                    epoch_start_seconds=0.0,
                    epoch_end_seconds=30.0,
                    sleep_stage_source_label="Sleep stage 2",
                    silver_stage_code="N2",
                    analytical_stage_code="N2",
                ),
            ),
        )
        integrated = build_integrated_feature_frame(
            gold,
            recording_channel_context_frame=recording_context,
            epoch_label_context_frame=epoch_context,
        )
        rows = sorted(integrated.collect(), key=lambda row: row["epoch_number"])

        if len(rows) != 2:
            raise RuntimeError("Feature integration must preserve every Gold row")
        if not rows[0]["has_warehouse_context"]:
            raise RuntimeError("Synthetic Warehouse context was not joined")
        if not rows[0]["has_sleep_stage_label"]:
            raise RuntimeError("Labeled synthetic epoch was not joined")
        if rows[0]["silver_stage_code"] != "N2":
            raise RuntimeError("Synthetic stage mapping is incorrect")
        if rows[1]["has_sleep_stage_label"]:
            raise RuntimeError("Unlabeled synthetic window must remain unlabeled")
        if rows[1]["sleep_epoch_sk"] is not None or rows[1]["silver_stage_code"] is not None:
            raise RuntimeError("Unlabeled synthetic window must keep nullable label fields")

        print("feature_integration_row_preservation=success")
        print("feature_integration_labeled_left_join=success")
        print("feature_integration_unlabeled_preservation=success")
        print("feature_integration_smoke_status=success")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_smoke_test()
