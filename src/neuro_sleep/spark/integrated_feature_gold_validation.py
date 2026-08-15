from __future__ import annotations

import math

from pyspark.sql import functions as F

from neuro_sleep.config import get_settings
from neuro_sleep.gold.integrated_feature_publication import (
    GOLD_BUCKET,
    IntegratedFeaturePublication,
    build_data_prefix,
    build_integrated_output_prefix,
    build_warehouse_context_fingerprint,
    inspect_publication_state,
    inspect_source_gold_lineage,
)
from neuro_sleep.spark.feature_integration import (
    fetch_epoch_label_contexts,
    fetch_recording_channel_contexts,
)
from neuro_sleep.spark.s3a import (
    assert_s3a_runtime,
    build_s3a_path,
    configure_minio_s3a,
)
from neuro_sleep.spark.session import create_spark_session
from neuro_sleep.spark.signal_features import (
    expected_window_count,
    fetch_signal_channel_contexts,
)
from neuro_sleep.spark.signal_input import discover_selected_signal_inputs
from neuro_sleep.storage.object_storage import get_object_storage_client


def run_validation() -> None:
    settings = get_settings()
    inputs = discover_selected_signal_inputs(
        settings=settings,
        verify_live_objects=True,
    )
    signal_contexts = fetch_signal_channel_contexts(settings=settings)
    recording_contexts = fetch_recording_channel_contexts(settings=settings)
    epoch_contexts = fetch_epoch_label_contexts(settings=settings)

    spark = create_spark_session(
        "neurosleep-integrated-gold-validation",
        master="local[*]",
        ui_enabled=False,
    )
    client = get_object_storage_client(settings=settings)

    total_rows = 0
    total_labeled = 0
    total_unlabeled = 0
    total_files = 0

    try:
        assert_s3a_runtime(spark)
        configure_minio_s3a(spark, settings=settings)

        for item in inputs:
            item_signal = tuple(
                value
                for value in signal_contexts
                if value.recording_id == item.recording_id
            )
            item_recording = tuple(
                value
                for value in recording_contexts
                if value.recording_id == item.recording_id
            )
            item_epochs = tuple(
                value
                for value in epoch_contexts
                if value.recording_id == item.recording_id
            )

            if not item_signal or not item_recording:
                raise RuntimeError(
                    f"Missing integration context for {item.recording_key}"
                )
            if len(item_signal) != len(item_recording):
                raise RuntimeError(
                    f"Channel context mismatch for {item.recording_key}"
                )

            durations = {
                value.recording_duration_seconds
                for value in item_signal
            }
            if len(durations) != 1:
                raise RuntimeError(
                    f"Duration mismatch for {item.recording_key}"
                )
            duration = next(iter(durations))
            channels = len(item_signal)
            expected_rows = expected_window_count(duration) * channels
            expected_labeled = len(item_epochs) * channels
            expected_unlabeled = expected_rows - expected_labeled
            expected_partial = (
                channels
                if not math.isclose(
                    math.fmod(duration, 30.0),
                    0.0,
                    abs_tol=1e-9,
                )
                else 0
            )

            source_gold = inspect_source_gold_lineage(
                item=item,
                expected_row_count=expected_rows,
                expected_partial_window_count=expected_partial,
                client=client,
            )
            fingerprint = build_warehouse_context_fingerprint(
                recording_contexts=item_recording,
                epoch_contexts=item_epochs,
            )
            state = inspect_publication_state(
                item=item,
                warehouse_context_sha256=fingerprint,
                recording_context_count=len(item_recording),
                epoch_context_count=len(item_epochs),
                expected_row_count=expected_rows,
                expected_labeled_row_count=expected_labeled,
                expected_unlabeled_row_count=expected_unlabeled,
                expected_partial_window_count=expected_partial,
                source_gold=source_gold,
                client=client,
            )
            if not isinstance(state, IntegratedFeaturePublication):
                raise RuntimeError(
                    "Expected completed integrated Gold publication for "
                    f"{item.recording_key}"
                )

            output_prefix = build_integrated_output_prefix(
                item=item,
                warehouse_context_sha256=fingerprint,
            )
            frame = spark.read.parquet(
                build_s3a_path(
                    bucket=GOLD_BUCKET,
                    object_key=build_data_prefix(output_prefix),
                )
            )
            stats = frame.agg(
                F.count("*").alias("rows"),
                F.countDistinct(
                    F.struct(
                        "recording_id",
                        "channel_id",
                        "epoch_number",
                    )
                ).alias("grain_rows"),
                F.countDistinct("channel_id").alias("channels"),
                F.sum(
                    F.when(
                        F.col("has_sleep_stage_label"),
                        F.lit(1),
                    ).otherwise(F.lit(0))
                ).alias("labeled"),
                F.sum(
                    F.when(
                        ~F.col("has_sleep_stage_label"),
                        F.lit(1),
                    ).otherwise(F.lit(0))
                ).alias("unlabeled"),
                F.sum(
                    F.when(
                        F.col("is_partial_window"),
                        F.lit(1),
                    ).otherwise(F.lit(0))
                ).alias("partial"),
                F.sum(
                    F.when(
                        F.col("has_warehouse_context"),
                        F.lit(1),
                    ).otherwise(F.lit(0))
                ).alias("warehouse_context"),
            ).first()
            if stats is None:
                raise RuntimeError(
                    f"No integrated Gold statistics for {item.recording_key}"
                )

            actual = {
                "rows": int(stats["rows"]),
                "grain_rows": int(stats["grain_rows"]),
                "channels": int(stats["channels"]),
                "labeled": int(stats["labeled"]),
                "unlabeled": int(stats["unlabeled"]),
                "partial": int(stats["partial"]),
                "warehouse_context": int(stats["warehouse_context"]),
            }
            expected = {
                "rows": expected_rows,
                "grain_rows": expected_rows,
                "channels": channels,
                "labeled": expected_labeled,
                "unlabeled": expected_unlabeled,
                "partial": expected_partial,
                "warehouse_context": expected_rows,
            }
            if actual != expected:
                raise RuntimeError(
                    "Integrated Gold physical validation failed for "
                    f"{item.recording_key}: expected={expected} actual={actual}"
                )

            total_rows += expected_rows
            total_labeled += expected_labeled
            total_unlabeled += expected_unlabeled
            total_files += 1

            print(
                f"{item.recording_key}: "
                "manifest=valid "
                "data_files=1 "
                f"rows={expected_rows} "
                f"channels={channels} "
                f"labeled={expected_labeled} "
                f"unlabeled={expected_unlabeled} "
                f"partial_rows={expected_partial} "
                "validation=success"
            )

        print()
        print(f"integrated_gold_validation_recordings={len(inputs)}")
        print(f"integrated_gold_validation_data_files={total_files}")
        print(f"integrated_gold_validation_rows={total_rows}")
        print(f"integrated_gold_validation_labeled_rows={total_labeled}")
        print(f"integrated_gold_validation_unlabeled_rows={total_unlabeled}")
        print("integrated_gold_validation_status=success")
    finally:
        client.close()
        spark.stop()


if __name__ == "__main__":
    run_validation()
