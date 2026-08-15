from __future__ import annotations

import math
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from neuro_sleep.config import get_settings
from neuro_sleep.spark.s3a import (
    assert_s3a_runtime,
    build_s3a_path,
    configure_minio_s3a,
)
from neuro_sleep.spark.session import (
    create_spark_session,
)
from neuro_sleep.spark.signal_features import (
    SignalChannelContext,
    build_channel_context_frame,
    build_signal_feature_frame,
    expected_window_count,
    fetch_signal_channel_contexts,
)
from neuro_sleep.spark.signal_input import (
    SelectedSignalInput,
    discover_selected_signal_inputs,
)


_ALLOWLIST_ENV = (
    "SPARK_SIGNAL_RECORDING_KEYS"
)


def _filter_inputs(
    inputs: tuple[
        SelectedSignalInput,
        ...,
    ],
) -> tuple[
    SelectedSignalInput,
    ...,
]:
    raw = os.environ.get(
        _ALLOWLIST_ENV,
        "",
    ).strip()

    if not raw:
        return inputs

    requested = tuple(
        part.strip()
        for part in raw.split(",")
        if part.strip()
    )
    if not requested:
        raise RuntimeError(
            f"{_ALLOWLIST_ENV} is set but "
            "contains no recording keys"
        )

    if len(requested) != len(
        set(requested)
    ):
        raise RuntimeError(
            f"{_ALLOWLIST_ENV} contains "
            "duplicate recording keys"
        )

    by_key = {
        item.recording_key: item
        for item in inputs
    }
    missing = [
        key
        for key in requested
        if key not in by_key
    ]
    if missing:
        raise RuntimeError(
            "Spark signal recording allowlist "
            "contains unavailable keys: "
            + ", ".join(missing)
        )

    return tuple(
        by_key[key]
        for key in requested
    )


def _contexts_for_input(
    contexts: tuple[
        SignalChannelContext,
        ...,
    ],
    item: SelectedSignalInput,
) -> tuple[
    SignalChannelContext,
    ...,
]:
    selected = tuple(
        context
        for context in contexts
        if (
            context.recording_id
            == item.recording_id
        )
    )

    if not selected:
        raise RuntimeError(
            "No Warehouse channel context for "
            f"{item.recording_key}"
        )

    recording_keys = {
        context.recording_key
        for context in selected
    }
    if recording_keys != {
        item.recording_key
    }:
        raise RuntimeError(
            "Warehouse channel context does not "
            "match the selected recording key: "
            f"{item.recording_key}"
        )

    durations = {
        context.recording_duration_seconds
        for context in selected
    }
    if len(durations) != 1:
        raise RuntimeError(
            "Warehouse channels disagree on "
            "recording duration: "
            f"{item.recording_key}"
        )

    return selected


def _validate_feature_frame(
    frame: DataFrame,
    *,
    recording_key: str,
    recording_duration_seconds: float,
    channel_count: int,
) -> tuple[int, int]:
    windows_per_channel = (
        expected_window_count(
            recording_duration_seconds
        )
    )
    expected_rows = (
        windows_per_channel
        * channel_count
    )

    remainder = math.fmod(
        recording_duration_seconds,
        30.0,
    )
    has_partial = not math.isclose(
        remainder,
        0.0,
        abs_tol=1e-9,
    )
    expected_partial_rows = (
        channel_count
        if has_partial
        else 0
    )

    mismatch_condition = (
        F.col("source_system").isNull()
        | F.col("dataset_version").isNull()
        | F.col("collection").isNull()
        | F.col("recording_key").isNull()
        | F.col(
            "sampling_frequency_hz"
        ).isNull()
    )

    sample_count_mismatch = (
        F.col("sample_count")
        != F.col(
            "expected_sample_count"
        )
    )

    index_mismatch = (
        (
            F.col(
                "first_sample_index"
            )
            != (
                F.col("epoch_number")
                * F.col(
                    "samples_per_full_window"
                )
            )
        )
        | (
            F.col(
                "last_sample_index"
            )
            != (
                F.col(
                    "first_sample_index"
                )
                + F.col("sample_count")
                - F.lit(1)
            )
        )
    )

    elapsed_tolerance = F.lit(
        1e-9
    )
    elapsed_mismatch = (
        (
            F.abs(
                F.col(
                    "first_sample_elapsed_seconds"
                )
                - (
                    F.col(
                        "first_sample_index"
                    )
                    / F.col(
                        "sampling_frequency_hz"
                    )
                )
            )
            > elapsed_tolerance
        )
        | (
            F.abs(
                F.col(
                    "last_sample_elapsed_seconds"
                )
                - (
                    F.col(
                        "last_sample_index"
                    )
                    / F.col(
                        "sampling_frequency_hz"
                    )
                )
            )
            > elapsed_tolerance
        )
    )

    feature_non_finite = (
        F.col("mean").isNull()
        | F.isnan("mean")
        | F.col("stddev_pop").isNull()
        | F.isnan("stddev_pop")
        | F.col("min").isNull()
        | F.isnan("min")
        | F.col("max").isNull()
        | F.isnan("max")
        | F.col("rms").isNull()
        | F.isnan("rms")
    )

    stats = (
        frame
        .agg(
            F.count("*").alias(
                "feature_rows"
            ),
            F.sum(
                F.when(
                    mismatch_condition,
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "missing_context_rows"
            ),
            F.sum(
                "invalid_signal_sample_count"
            ).alias(
                "invalid_signal_samples"
            ),
            F.sum(
                F.when(
                    sample_count_mismatch,
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "sample_count_mismatch_rows"
            ),
            F.sum(
                F.when(
                    index_mismatch,
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "index_mismatch_rows"
            ),
            F.sum(
                F.when(
                    elapsed_mismatch,
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "elapsed_mismatch_rows"
            ),
            F.sum(
                F.when(
                    F.abs(
                        F.col(
                            "sample_coverage_pct"
                        )
                        - F.lit(100.0)
                    )
                    > F.lit(1e-9),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "coverage_mismatch_rows"
            ),
            F.sum(
                F.when(
                    feature_non_finite,
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "non_finite_feature_rows"
            ),
            F.sum(
                F.when(
                    F.col(
                        "is_partial_window"
                    ),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "partial_rows"
            ),
            F.min(
                "epoch_number"
            ).alias(
                "min_epoch_number"
            ),
            F.max(
                "epoch_number"
            ).alias(
                "max_epoch_number"
            ),
        )
        .first()
    )

    if stats is None:
        raise RuntimeError(
            "Feature validation produced "
            "no aggregate result"
        )

    feature_rows = int(
        stats["feature_rows"]
    )
    partial_rows = int(
        stats["partial_rows"]
    )

    checks = {
        "feature_rows": (
            feature_rows,
            expected_rows,
        ),
        "missing_context_rows": (
            int(
                stats[
                    "missing_context_rows"
                ]
            ),
            0,
        ),
        "invalid_signal_samples": (
            int(
                stats[
                    "invalid_signal_samples"
                ]
            ),
            0,
        ),
        "sample_count_mismatch_rows": (
            int(
                stats[
                    "sample_count_mismatch_rows"
                ]
            ),
            0,
        ),
        "index_mismatch_rows": (
            int(
                stats[
                    "index_mismatch_rows"
                ]
            ),
            0,
        ),
        "elapsed_mismatch_rows": (
            int(
                stats[
                    "elapsed_mismatch_rows"
                ]
            ),
            0,
        ),
        "coverage_mismatch_rows": (
            int(
                stats[
                    "coverage_mismatch_rows"
                ]
            ),
            0,
        ),
        "non_finite_feature_rows": (
            int(
                stats[
                    "non_finite_feature_rows"
                ]
            ),
            0,
        ),
        "partial_rows": (
            partial_rows,
            expected_partial_rows,
        ),
        "min_epoch_number": (
            int(
                stats[
                    "min_epoch_number"
                ]
            ),
            0,
        ),
        "max_epoch_number": (
            int(
                stats[
                    "max_epoch_number"
                ]
            ),
            windows_per_channel - 1,
        ),
    }

    failures = [
        (
            name,
            actual,
            expected,
        )
        for (
            name,
            (
                actual,
                expected,
            ),
        ) in checks.items()
        if actual != expected
    ]

    if failures:
        details = "; ".join(
            (
                f"{name}: "
                f"expected={expected} "
                f"actual={actual}"
            )
            for (
                name,
                actual,
                expected,
            ) in failures
        )
        raise RuntimeError(
            "Signal feature validation failed "
            f"for {recording_key}: "
            f"{details}"
        )

    return (
        feature_rows,
        partial_rows,
    )


def run_validation() -> None:
    settings = get_settings()

    discovered = (
        discover_selected_signal_inputs(
            settings=settings,
            verify_live_objects=True,
        )
    )
    inputs = _filter_inputs(
        discovered
    )

    all_contexts = (
        fetch_signal_channel_contexts(
            settings=settings
        )
    )

    spark = create_spark_session(
        "neurosleep-signal-feature-validation",
        master="local[*]",
        ui_enabled=False,
    )

    total_signal_rows = 0
    total_feature_rows = 0
    total_partial_rows = 0

    try:
        assert_s3a_runtime(spark)
        configure_minio_s3a(
            spark,
            settings=settings,
        )

        for item in inputs:
            contexts = (
                _contexts_for_input(
                    all_contexts,
                    item,
                )
            )
            duration_seconds = (
                contexts[0]
                .recording_duration_seconds
            )

            paths = [
                build_s3a_path(
                    bucket=item.bucket,
                    object_key=object_key,
                )
                for object_key in (
                    item.signal_object_keys
                )
            ]

            signal_frame = (
                spark.read.parquet(
                    *paths
                )
            )

            context_frame = (
                build_channel_context_frame(
                    spark,
                    contexts=contexts,
                )
            )

            feature_frame = (
                build_signal_feature_frame(
                    signal_frame,
                    channel_context_frame=(
                        context_frame
                    ),
                )
            )

            (
                feature_rows,
                partial_rows,
            ) = _validate_feature_frame(
                feature_frame,
                recording_key=(
                    item.recording_key
                ),
                recording_duration_seconds=(
                    duration_seconds
                ),
                channel_count=len(
                    contexts
                ),
            )

            expected_windows = (
                expected_window_count(
                    duration_seconds
                )
            )

            total_signal_rows += (
                item.signal_row_count
            )
            total_feature_rows += (
                feature_rows
            )
            total_partial_rows += (
                partial_rows
            )

            print(
                f"{item.recording_key}: "
                f"signal_rows="
                f"{item.signal_row_count} "
                f"channels={len(contexts)} "
                f"windows_per_channel="
                f"{expected_windows} "
                f"feature_rows="
                f"{feature_rows} "
                f"partial_rows="
                f"{partial_rows} "
                "validation=success"
            )

        print()
        print(
            "signal_feature_recordings="
            f"{len(inputs)}"
        )
        print(
            "signal_feature_input_rows="
            f"{total_signal_rows}"
        )
        print(
            "signal_feature_output_rows="
            f"{total_feature_rows}"
        )
        print(
            "signal_feature_partial_rows="
            f"{total_partial_rows}"
        )
        print(
            "signal_feature_validation_status="
            "success"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run_validation()
