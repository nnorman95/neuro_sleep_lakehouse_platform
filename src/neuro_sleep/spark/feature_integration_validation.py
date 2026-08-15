from __future__ import annotations

from collections import Counter
import math

from pyspark.sql import functions as F

from neuro_sleep.config import get_settings
from neuro_sleep.gold.signal_feature_publication import (
    GOLD_BUCKET,
    GoldSignalFeaturePublication,
    build_data_prefix,
    build_gold_output_prefix,
    inspect_publication_state,
)
from neuro_sleep.spark.feature_integration import (
    build_epoch_label_context_frame,
    build_integrated_feature_frame,
    build_recording_channel_context_frame,
    fetch_epoch_label_contexts,
    fetch_recording_channel_contexts,
)
from neuro_sleep.spark.s3a import (
    assert_s3a_runtime,
    build_s3a_path,
    configure_minio_s3a,
)
from neuro_sleep.spark.session import create_spark_session
from neuro_sleep.spark.signal_features import expected_window_count, fetch_signal_channel_contexts
from neuro_sleep.spark.signal_input import discover_selected_signal_inputs
from neuro_sleep.storage.object_storage import get_object_storage_client


def run_validation() -> None:
    settings = get_settings()
    inputs = discover_selected_signal_inputs(
        settings=settings,
        verify_live_objects=True,
    )
    selected_ids = {item.recording_id for item in inputs}

    signal_channel_contexts = fetch_signal_channel_contexts(settings=settings)
    integration_channel_contexts = tuple(
        item for item in fetch_recording_channel_contexts(settings=settings)
        if item.recording_id in selected_ids
    )
    epoch_contexts = tuple(
        item for item in fetch_epoch_label_contexts(settings=settings)
        if item.recording_id in selected_ids
    )

    channels_by_recording = Counter(
        item.recording_id for item in integration_channel_contexts
    )
    epochs_by_recording = Counter(
        item.recording_id for item in epoch_contexts
    )

    spark = create_spark_session(
        "neurosleep-feature-integration-validation",
        master="local[*]",
        ui_enabled=False,
    )
    client = get_object_storage_client(settings=settings)

    try:
        assert_s3a_runtime(spark)
        configure_minio_s3a(spark, settings=settings)

        gold_paths: list[str] = []
        expected_total_rows = 0
        expected_labeled_rows = 0

        for item in inputs:
            selected_signal_channels = [
                context for context in signal_channel_contexts
                if context.recording_id == item.recording_id
            ]
            if not selected_signal_channels:
                raise RuntimeError(
                    "Selected Gold recording has no Warehouse channel context: "
                    f"{item.recording_key}"
                )

            channel_count = len(selected_signal_channels)
            if channels_by_recording[item.recording_id] != channel_count:
                raise RuntimeError(
                    "Integration channel context count mismatch for "
                    f"{item.recording_key}"
                )

            durations = {
                context.recording_duration_seconds
                for context in selected_signal_channels
            }
            if len(durations) != 1:
                raise RuntimeError(
                    "Warehouse channels disagree on recording duration for "
                    f"{item.recording_key}"
                )

            duration = next(iter(durations))
            windows = expected_window_count(duration)
            expected_rows = windows * channel_count
            remainder = math.fmod(duration, 30.0)
            expected_partial = (
                channel_count
                if not math.isclose(remainder, 0.0, abs_tol=1e-9)
                else 0
            )

            state = inspect_publication_state(
                item=item,
                expected_row_count=expected_rows,
                expected_partial_window_count=expected_partial,
                client=client,
            )
            if not isinstance(state, GoldSignalFeaturePublication):
                raise RuntimeError(
                    "Expected completed Gold publication for "
                    f"{item.recording_key}"
                )

            gold_paths.append(
                build_s3a_path(
                    bucket=GOLD_BUCKET,
                    object_key=build_data_prefix(build_gold_output_prefix(item)),
                )
            )
            expected_total_rows += expected_rows
            expected_labeled_rows += (
                epochs_by_recording[item.recording_id] * channel_count
            )

        recording_context_frame = build_recording_channel_context_frame(
            spark,
            contexts=integration_channel_contexts,
        )
        epoch_context_frame = build_epoch_label_context_frame(
            spark,
            contexts=epoch_contexts,
        )
        gold = spark.read.parquet(*gold_paths)
        integrated = build_integrated_feature_frame(
            gold,
            recording_channel_context_frame=recording_context_frame,
            epoch_label_context_frame=epoch_context_frame,
        )

        aggregate = integrated.agg(
            F.count("*").alias("rows"),
            F.countDistinct(
                F.struct("recording_id", "channel_id", "epoch_number")
            ).alias("grain_rows"),
            F.sum(
                F.when(F.col("has_warehouse_context"), F.lit(1)).otherwise(F.lit(0))
            ).alias("warehouse_context_rows"),
            F.sum(
                F.when(F.col("has_sleep_stage_label"), F.lit(1)).otherwise(F.lit(0))
            ).alias("labeled_rows"),
            F.sum(
                F.when(~F.col("has_sleep_stage_label"), F.lit(1)).otherwise(F.lit(0))
            ).alias("unlabeled_rows"),
        ).first()

        if aggregate is None:
            raise RuntimeError("Feature integration validation returned no aggregate")

        actual_rows = int(aggregate["rows"])
        actual_grain_rows = int(aggregate["grain_rows"])
        actual_context_rows = int(aggregate["warehouse_context_rows"])
        actual_labeled_rows = int(aggregate["labeled_rows"])
        actual_unlabeled_rows = int(aggregate["unlabeled_rows"])
        expected_unlabeled_rows = expected_total_rows - expected_labeled_rows

        if actual_rows != expected_total_rows:
            raise RuntimeError(
                f"Integrated row count mismatch: expected={expected_total_rows} actual={actual_rows}"
            )
        if actual_grain_rows != actual_rows:
            raise RuntimeError("Integrated feature grain is not unique")
        if actual_context_rows != actual_rows:
            raise RuntimeError(
                "At least one Gold feature row has no Warehouse recording/channel context"
            )
        if actual_labeled_rows != expected_labeled_rows:
            raise RuntimeError(
                f"Integrated labeled-row count mismatch: expected={expected_labeled_rows} "
                f"actual={actual_labeled_rows}"
            )
        if actual_unlabeled_rows != expected_unlabeled_rows:
            raise RuntimeError(
                f"Integrated unlabeled-row count mismatch: expected={expected_unlabeled_rows} "
                f"actual={actual_unlabeled_rows}"
            )

        invalid_labeled = integrated.filter(
            F.col("has_sleep_stage_label")
            & (
                F.col("sleep_epoch_sk").isNull()
                | F.col("sleep_stage_sk").isNull()
                | F.col("silver_stage_code").isNull()
                | F.col("analytical_stage_code").isNull()
            )
        ).count()
        invalid_unlabeled = integrated.filter(
            (~F.col("has_sleep_stage_label"))
            & (
                F.col("sleep_epoch_sk").isNotNull()
                | F.col("sleep_stage_sk").isNotNull()
                | F.col("silver_stage_code").isNotNull()
                | F.col("analytical_stage_code").isNotNull()
            )
        ).count()

        if invalid_labeled != 0:
            raise RuntimeError("Labeled integrated rows contain missing sleep-stage context")
        if invalid_unlabeled != 0:
            raise RuntimeError("Unlabeled integrated rows contain partial sleep-stage context")

        per_recording = (
            integrated.groupBy("recording_id", "recording_key")
            .agg(
                F.count("*").alias("rows"),
                F.sum(
                    F.when(F.col("has_sleep_stage_label"), F.lit(1)).otherwise(F.lit(0))
                ).alias("labeled"),
                F.sum(
                    F.when(~F.col("has_sleep_stage_label"), F.lit(1)).otherwise(F.lit(0))
                ).alias("unlabeled"),
                F.min(
                    F.when(~F.col("has_sleep_stage_label"), F.col("epoch_number"))
                ).alias("first_unlabeled_epoch"),
                F.max(
                    F.when(~F.col("has_sleep_stage_label"), F.col("epoch_number"))
                ).alias("last_unlabeled_epoch"),
            )
            .collect()
        )

        if len(per_recording) != len(inputs):
            raise RuntimeError("Integrated output is missing selected recordings")

        for row in sorted(per_recording, key=lambda value: value["recording_key"]):
            print(
                f"{row['recording_key']}: "
                f"rows={int(row['rows'])} "
                f"labeled={int(row['labeled'])} "
                f"unlabeled={int(row['unlabeled'])} "
                f"first_unlabeled_epoch={row['first_unlabeled_epoch']} "
                f"last_unlabeled_epoch={row['last_unlabeled_epoch']} "
                "validation=success"
            )

        print()
        print(f"feature_integration_recordings={len(inputs)}")
        print(f"feature_integration_rows={actual_rows}")
        print(f"feature_integration_labeled_rows={actual_labeled_rows}")
        print(f"feature_integration_unlabeled_rows={actual_unlabeled_rows}")
        print(f"feature_integration_warehouse_context_rows={actual_context_rows}")
        print("feature_integration_validation_status=success")
    finally:
        client.close()
        spark.stop()


if __name__ == "__main__":
    run_validation()
