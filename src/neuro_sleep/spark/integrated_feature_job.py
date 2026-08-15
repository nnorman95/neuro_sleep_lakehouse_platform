from __future__ import annotations

import math

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from neuro_sleep.config import get_settings
from neuro_sleep.gold.integrated_feature_publication import (
    GOLD_BUCKET,
    IntegratedFeaturePublication,
    build_data_prefix,
    build_integrated_output_prefix,
    build_success_manifest,
    build_warehouse_context_fingerprint,
    inspect_publication_state,
    inspect_source_gold_lineage,
    inspect_written_data_object,
    remove_spark_success_marker,
    upload_success_manifest,
)
from neuro_sleep.spark.feature_integration import (
    INTEGRATION_VERSION,
    EpochLabelIntegrationContext,
    RecordingChannelIntegrationContext,
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
from neuro_sleep.spark.signal_features import (
    FEATURE_VERSION,
    SignalChannelContext,
    expected_window_count,
    fetch_signal_channel_contexts,
)
from neuro_sleep.spark.signal_input import (
    SelectedSignalInput,
    discover_selected_signal_inputs,
)
from neuro_sleep.storage.object_storage import get_object_storage_client


def _signal_contexts_for_input(
    contexts: tuple[SignalChannelContext, ...],
    item: SelectedSignalInput,
) -> tuple[SignalChannelContext, ...]:
    selected = tuple(
        value
        for value in contexts
        if value.recording_id == item.recording_id
    )
    if not selected:
        raise RuntimeError(
            f"No signal channel context for {item.recording_key}"
        )
    return selected


def _recording_contexts_for_input(
    contexts: tuple[RecordingChannelIntegrationContext, ...],
    item: SelectedSignalInput,
) -> tuple[RecordingChannelIntegrationContext, ...]:
    selected = tuple(
        value
        for value in contexts
        if value.recording_id == item.recording_id
    )
    if not selected:
        raise RuntimeError(
            f"No integration channel context for {item.recording_key}"
        )
    return selected


def _epoch_contexts_for_input(
    contexts: tuple[EpochLabelIntegrationContext, ...],
    item: SelectedSignalInput,
) -> tuple[EpochLabelIntegrationContext, ...]:
    return tuple(
        value
        for value in contexts
        if value.recording_id == item.recording_id
    )


def _expected_counts(
    *,
    signal_contexts: tuple[SignalChannelContext, ...],
    recording_contexts: tuple[RecordingChannelIntegrationContext, ...],
    epoch_contexts: tuple[EpochLabelIntegrationContext, ...],
) -> tuple[int, int, int, int]:
    if len(signal_contexts) != len(recording_contexts):
        raise RuntimeError(
            "Signal and integration channel context counts disagree"
        )

    signal_ids = {
        value.channel_id
        for value in signal_contexts
    }
    integration_ids = {
        value.channel_id
        for value in recording_contexts
    }
    if signal_ids != integration_ids:
        raise RuntimeError(
            "Signal and integration channel identities disagree"
        )

    durations = {
        value.recording_duration_seconds
        for value in signal_contexts
    }
    if len(durations) != 1:
        raise RuntimeError(
            "Warehouse channels disagree on recording duration"
        )

    duration = next(iter(durations))
    channel_count = len(signal_contexts)
    row_count = expected_window_count(duration) * channel_count

    remainder = math.fmod(duration, 30.0)
    partial_count = (
        channel_count
        if not math.isclose(remainder, 0.0, abs_tol=1e-9)
        else 0
    )

    labeled_row_count = len(epoch_contexts) * channel_count
    if labeled_row_count > row_count:
        raise RuntimeError(
            "Warehouse sleep labels exceed integrated signal windows"
        )

    unlabeled_row_count = row_count - labeled_row_count
    return (
        row_count,
        labeled_row_count,
        unlabeled_row_count,
        partial_count,
    )


def _validate_before_publish(
    frame: DataFrame,
    *,
    item: SelectedSignalInput,
    expected_row_count: int,
    expected_labeled_row_count: int,
    expected_unlabeled_row_count: int,
    expected_partial_window_count: int,
) -> None:
    stats = frame.agg(
        F.count("*").alias("rows"),
        F.countDistinct(
            F.struct(
                "recording_id",
                "channel_id",
                "epoch_number",
            )
        ).alias("grain_rows"),
        F.countDistinct("recording_id").alias("recording_ids"),
        F.min("recording_id").alias("min_recording_id"),
        F.max("recording_id").alias("max_recording_id"),
        F.sum(
            F.when(
                F.col("has_warehouse_context"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("warehouse_context_rows"),
        F.sum(
            F.when(
                F.col("has_sleep_stage_label"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("labeled_rows"),
        F.sum(
            F.when(
                ~F.col("has_sleep_stage_label"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("unlabeled_rows"),
        F.sum(
            F.when(
                F.col("is_partial_window"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("partial_rows"),
        F.countDistinct("feature_version").alias("feature_versions"),
        F.min("feature_version").alias("min_feature_version"),
        F.max("feature_version").alias("max_feature_version"),
        F.countDistinct("integration_version").alias("integration_versions"),
        F.min("integration_version").alias("min_integration_version"),
        F.max("integration_version").alias("max_integration_version"),
    ).first()

    if stats is None:
        raise RuntimeError(
            "Integrated Gold pre-publish validation returned no result"
        )

    checks = {
        "rows": (int(stats["rows"]), expected_row_count),
        "grain_rows": (int(stats["grain_rows"]), expected_row_count),
        "recording_ids": (int(stats["recording_ids"]), 1),
        "warehouse_context_rows": (
            int(stats["warehouse_context_rows"]),
            expected_row_count,
        ),
        "labeled_rows": (
            int(stats["labeled_rows"]),
            expected_labeled_row_count,
        ),
        "unlabeled_rows": (
            int(stats["unlabeled_rows"]),
            expected_unlabeled_row_count,
        ),
        "partial_rows": (
            int(stats["partial_rows"]),
            expected_partial_window_count,
        ),
        "feature_versions": (int(stats["feature_versions"]), 1),
        "integration_versions": (int(stats["integration_versions"]), 1),
    }
    failures = [
        (name, actual, expected)
        for name, (actual, expected) in checks.items()
        if actual != expected
    ]
    if failures:
        details = "; ".join(
            f"{name}: expected={expected} actual={actual}"
            for name, actual, expected in failures
        )
        raise RuntimeError(
            "Integrated Gold pre-publish validation failed for "
            f"{item.recording_key}: {details}"
        )

    if (
        str(stats["min_recording_id"]) != item.recording_id
        or str(stats["max_recording_id"]) != item.recording_id
    ):
        raise RuntimeError(
            "Integrated Gold recording_id does not match selected input"
        )
    if (
        str(stats["min_feature_version"]) != FEATURE_VERSION
        or str(stats["max_feature_version"]) != FEATURE_VERSION
    ):
        raise RuntimeError("Integrated Gold feature version mismatch")
    if (
        str(stats["min_integration_version"]) != INTEGRATION_VERSION
        or str(stats["max_integration_version"]) != INTEGRATION_VERSION
    ):
        raise RuntimeError("Integrated Gold integration version mismatch")

    invalid_labeled = frame.filter(
        F.col("has_sleep_stage_label")
        & (
            F.col("sleep_epoch_sk").isNull()
            | F.col("sleep_stage_sk").isNull()
            | F.col("silver_epoch_id").isNull()
            | F.col("silver_stage_code").isNull()
            | F.col("analytical_stage_code").isNull()
            | F.col("labeled_epoch_start_seconds").isNull()
            | F.col("labeled_epoch_end_seconds").isNull()
        )
    ).count()
    if invalid_labeled:
        raise RuntimeError(
            "Integrated labeled rows contain incomplete sleep-stage context"
        )

    invalid_unlabeled = frame.filter(
        (~F.col("has_sleep_stage_label"))
        & (
            F.col("sleep_epoch_sk").isNotNull()
            | F.col("sleep_stage_sk").isNotNull()
            | F.col("silver_epoch_id").isNotNull()
            | F.col("silver_stage_code").isNotNull()
            | F.col("analytical_stage_code").isNotNull()
            | F.col("labeled_epoch_start_seconds").isNotNull()
            | F.col("labeled_epoch_end_seconds").isNotNull()
        )
    ).count()
    if invalid_unlabeled:
        raise RuntimeError(
            "Integrated unlabeled rows contain partial sleep-stage context"
        )

    timing_mismatch = frame.filter(
        F.col("has_sleep_stage_label")
        & (
            (
                F.abs(
                    F.col("window_start_seconds")
                    - F.col("labeled_epoch_start_seconds")
                )
                > F.lit(1e-9)
            )
            | (
                F.abs(
                    F.col("window_end_seconds")
                    - F.col("labeled_epoch_end_seconds")
                )
                > F.lit(1e-9)
            )
        )
    ).count()
    if timing_mismatch:
        raise RuntimeError(
            "Integrated sleep-stage labels are not aligned to "
            "the 30-second signal windows"
        )


def _validate_written_integrated_gold(
    spark,
    *,
    item: SelectedSignalInput,
    output_prefix: str,
    expected_row_count: int,
    expected_labeled_row_count: int,
    expected_unlabeled_row_count: int,
    expected_partial_window_count: int,
) -> None:
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
        F.sum(
            F.when(
                F.col("has_sleep_stage_label"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("labeled_rows"),
        F.sum(
            F.when(
                ~F.col("has_sleep_stage_label"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("unlabeled_rows"),
        F.sum(
            F.when(
                F.col("is_partial_window"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("partial_rows"),
        F.sum(
            F.when(
                F.col("has_warehouse_context"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("warehouse_context_rows"),
        F.countDistinct("recording_id").alias("recording_ids"),
        F.min("recording_id").alias("min_recording_id"),
        F.max("recording_id").alias("max_recording_id"),
    ).first()

    if stats is None:
        raise RuntimeError(
            "Integrated Gold read-back validation returned no result"
        )

    expected = {
        "rows": expected_row_count,
        "grain_rows": expected_row_count,
        "labeled_rows": expected_labeled_row_count,
        "unlabeled_rows": expected_unlabeled_row_count,
        "partial_rows": expected_partial_window_count,
        "warehouse_context_rows": expected_row_count,
        "recording_ids": 1,
    }
    for key, expected_value in expected.items():
        if int(stats[key]) != expected_value:
            raise RuntimeError(
                "Integrated Gold read-back "
                f"{key} mismatch for {item.recording_key}"
            )

    if (
        str(stats["min_recording_id"]) != item.recording_id
        or str(stats["max_recording_id"]) != item.recording_id
    ):
        raise RuntimeError(
            "Integrated Gold read-back recording_id validation failed"
        )


def run_job() -> None:
    settings = get_settings()
    inputs = discover_selected_signal_inputs(
        settings=settings,
        verify_live_objects=True,
    )
    signal_contexts = fetch_signal_channel_contexts(settings=settings)
    recording_contexts = fetch_recording_channel_contexts(settings=settings)
    epoch_contexts = fetch_epoch_label_contexts(settings=settings)

    spark = create_spark_session(
        "neurosleep-integrated-signal-features",
        master="local[*]",
        ui_enabled=False,
    )
    client = get_object_storage_client(settings=settings)

    written = 0
    skipped = 0
    recovered_objects = 0
    total_rows = 0
    total_labeled_rows = 0
    total_unlabeled_rows = 0

    try:
        assert_s3a_runtime(spark)
        configure_minio_s3a(spark, settings=settings)

        for item in inputs:
            item_signal_contexts = _signal_contexts_for_input(
                signal_contexts,
                item,
            )
            item_recording_contexts = _recording_contexts_for_input(
                recording_contexts,
                item,
            )
            item_epoch_contexts = _epoch_contexts_for_input(
                epoch_contexts,
                item,
            )
            (
                expected_rows,
                expected_labeled,
                expected_unlabeled,
                expected_partial,
            ) = _expected_counts(
                signal_contexts=item_signal_contexts,
                recording_contexts=item_recording_contexts,
                epoch_contexts=item_epoch_contexts,
            )

            source_gold = inspect_source_gold_lineage(
                item=item,
                expected_row_count=expected_rows,
                expected_partial_window_count=expected_partial,
                client=client,
            )
            warehouse_fingerprint = build_warehouse_context_fingerprint(
                recording_contexts=item_recording_contexts,
                epoch_contexts=item_epoch_contexts,
            )

            state = inspect_publication_state(
                item=item,
                warehouse_context_sha256=warehouse_fingerprint,
                recording_context_count=len(item_recording_contexts),
                epoch_context_count=len(item_epoch_contexts),
                expected_row_count=expected_rows,
                expected_labeled_row_count=expected_labeled,
                expected_unlabeled_row_count=expected_unlabeled,
                expected_partial_window_count=expected_partial,
                source_gold=source_gold,
                client=client,
            )
            if isinstance(state, IntegratedFeaturePublication):
                skipped += 1
                total_rows += state.row_count
                total_labeled_rows += state.labeled_row_count
                total_unlabeled_rows += state.unlabeled_row_count
                print(
                    f"{item.recording_key}: "
                    "status=skipped "
                    f"rows={state.row_count} "
                    f"labeled={state.labeled_row_count} "
                    f"unlabeled={state.unlabeled_row_count} "
                    f"prefix={state.output_prefix}"
                )
                continue

            action, recovered_count = state
            if action != "write":
                raise RuntimeError(
                    "Unexpected integrated Gold publication action"
                )
            recovered_objects += recovered_count

            source_frame = spark.read.parquet(
                build_s3a_path(
                    bucket=GOLD_BUCKET,
                    object_key=source_gold.data_object_key,
                )
            )
            recording_frame = build_recording_channel_context_frame(
                spark,
                contexts=item_recording_contexts,
            )
            epoch_frame = build_epoch_label_context_frame(
                spark,
                contexts=item_epoch_contexts,
            )
            integrated_frame = build_integrated_feature_frame(
                source_frame,
                recording_channel_context_frame=recording_frame,
                epoch_label_context_frame=epoch_frame,
            ).persist(StorageLevel.MEMORY_AND_DISK)

            output_prefix = build_integrated_output_prefix(
                item=item,
                warehouse_context_sha256=warehouse_fingerprint,
            )
            data_path = build_s3a_path(
                bucket=GOLD_BUCKET,
                object_key=build_data_prefix(output_prefix),
            )

            try:
                _validate_before_publish(
                    integrated_frame,
                    item=item,
                    expected_row_count=expected_rows,
                    expected_labeled_row_count=expected_labeled,
                    expected_unlabeled_row_count=expected_unlabeled,
                    expected_partial_window_count=expected_partial,
                )
                (
                    integrated_frame
                    .coalesce(1)
                    .write
                    .mode("errorifexists")
                    .option("compression", "snappy")
                    .parquet(data_path)
                )
                remove_spark_success_marker(
                    output_prefix=output_prefix,
                    client=client,
                )
                data_object = inspect_written_data_object(
                    output_prefix=output_prefix,
                    client=client,
                )
                _validate_written_integrated_gold(
                    spark,
                    item=item,
                    output_prefix=output_prefix,
                    expected_row_count=expected_rows,
                    expected_labeled_row_count=expected_labeled,
                    expected_unlabeled_row_count=expected_unlabeled,
                    expected_partial_window_count=expected_partial,
                )

                manifest = build_success_manifest(
                    item=item,
                    output_prefix=output_prefix,
                    warehouse_context_sha256=warehouse_fingerprint,
                    recording_context_count=len(item_recording_contexts),
                    epoch_context_count=len(item_epoch_contexts),
                    row_count=expected_rows,
                    labeled_row_count=expected_labeled,
                    unlabeled_row_count=expected_unlabeled,
                    partial_window_count=expected_partial,
                    source_gold=source_gold,
                    data_object=data_object,
                    spark_version=spark.version,
                )
                upload_success_manifest(
                    output_prefix=output_prefix,
                    manifest=manifest,
                    client=client,
                )
                validated = inspect_publication_state(
                    item=item,
                    warehouse_context_sha256=warehouse_fingerprint,
                    recording_context_count=len(item_recording_contexts),
                    epoch_context_count=len(item_epoch_contexts),
                    expected_row_count=expected_rows,
                    expected_labeled_row_count=expected_labeled,
                    expected_unlabeled_row_count=expected_unlabeled,
                    expected_partial_window_count=expected_partial,
                    source_gold=source_gold,
                    client=client,
                )
                if not isinstance(validated, IntegratedFeaturePublication):
                    raise RuntimeError(
                        "Integrated Gold publication was not valid "
                        "after success manifest upload"
                    )

                written += 1
                total_rows += expected_rows
                total_labeled_rows += expected_labeled
                total_unlabeled_rows += expected_unlabeled
                print(
                    f"{item.recording_key}: "
                    "status=written "
                    f"rows={expected_rows} "
                    f"labeled={expected_labeled} "
                    f"unlabeled={expected_unlabeled} "
                    "data_files=1 "
                    f"partial_rows={expected_partial} "
                    f"recovered_objects={recovered_count} "
                    f"prefix={output_prefix}"
                )
            finally:
                integrated_frame.unpersist()

        print()
        print(f"integrated_gold_recordings={len(inputs)}")
        print(f"integrated_gold_written={written}")
        print(f"integrated_gold_skipped={skipped}")
        print(f"integrated_gold_recovered_objects={recovered_objects}")
        print(f"integrated_gold_rows={total_rows}")
        print(f"integrated_gold_labeled_rows={total_labeled_rows}")
        print(f"integrated_gold_unlabeled_rows={total_unlabeled_rows}")
        print("integrated_gold_job_status=success")
    finally:
        client.close()
        spark.stop()


if __name__ == "__main__":
    run_job()
