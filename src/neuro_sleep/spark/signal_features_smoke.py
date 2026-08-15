from __future__ import annotations

import math

from pyspark.sql import types as T

from neuro_sleep.spark.session import (
    create_spark_session,
)
from neuro_sleep.spark.signal_features import (
    SignalChannelContext,
    build_channel_context_frame,
    build_signal_feature_frame,
)


_SIGNAL_SCHEMA = T.StructType(
    [
        T.StructField(
            "recording_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "channel_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "sample_index",
            T.LongType(),
            False,
        ),
        T.StructField(
            "elapsed_seconds",
            T.DoubleType(),
            False,
        ),
        T.StructField(
            "epoch_number",
            T.IntegerType(),
            False,
        ),
        T.StructField(
            "signal_value",
            T.DoubleType(),
            False,
        ),
    ]
)


def _assert_close(
    actual: float,
    expected: float,
    *,
    name: str,
) -> None:
    if not math.isclose(
        actual,
        expected,
        rel_tol=1e-12,
        abs_tol=1e-12,
    ):
        raise RuntimeError(
            f"{name} mismatch: "
            f"expected={expected} "
            f"actual={actual}"
        )


def run_smoke_test() -> None:
    spark = create_spark_session(
        "neurosleep-signal-feature-smoke",
        master="local[2]",
        ui_enabled=False,
    )

    try:
        rows = [
            (
                "recording-1",
                "channel-1",
                index,
                float(index),
                index // 30,
                float(index),
            )
            for index in range(35)
        ]

        signal_frame = (
            spark.createDataFrame(
                rows,
                schema=_SIGNAL_SCHEMA,
            )
        )

        contexts = (
            SignalChannelContext(
                source_system="test",
                dataset_version="1.0.0",
                collection="test",
                recording_key="R1",
                recording_id="recording-1",
                recording_duration_seconds=35.0,
                channel_id="channel-1",
                channel_position=1,
                source_label="signal",
                normalized_name="signal",
                sampling_frequency_hz=1.0,
            ),
        )

        context_frame = (
            build_channel_context_frame(
                spark,
                contexts=contexts,
            )
        )

        features = (
            build_signal_feature_frame(
                signal_frame,
                channel_context_frame=(
                    context_frame
                ),
            )
            .orderBy("epoch_number")
            .collect()
        )

        if len(features) != 2:
            raise RuntimeError(
                "Expected two feature windows, "
                f"got {len(features)}"
            )

        first = features[0]
        second = features[1]

        if first["epoch_number"] != 0:
            raise RuntimeError(
                "First epoch number mismatch"
            )
        if first["sample_count"] != 30:
            raise RuntimeError(
                "First sample count mismatch"
            )
        if (
            first["expected_sample_count"]
            != 30
        ):
            raise RuntimeError(
                "First expected sample count "
                "mismatch"
            )
        if first["is_partial_window"]:
            raise RuntimeError(
                "First window must be full"
            )

        _assert_close(
            first["window_duration_seconds"],
            30.0,
            name="first.window_duration_seconds",
        )
        _assert_close(
            first["sample_coverage_pct"],
            100.0,
            name="first.sample_coverage_pct",
        )
        _assert_close(
            first["mean"],
            14.5,
            name="first.mean",
        )
        _assert_close(
            first["min"],
            0.0,
            name="first.min",
        )
        _assert_close(
            first["max"],
            29.0,
            name="first.max",
        )
        _assert_close(
            first["peak_to_peak"],
            29.0,
            name="first.peak_to_peak",
        )

        expected_rms = math.sqrt(
            sum(
                float(index * index)
                for index in range(30)
            )
            / 30.0
        )
        _assert_close(
            first["rms"],
            expected_rms,
            name="first.rms",
        )

        if second["epoch_number"] != 1:
            raise RuntimeError(
                "Second epoch number mismatch"
            )
        if second["sample_count"] != 5:
            raise RuntimeError(
                "Second sample count mismatch"
            )
        if (
            second["expected_sample_count"]
            != 5
        ):
            raise RuntimeError(
                "Second expected sample count "
                "mismatch"
            )
        if not second["is_partial_window"]:
            raise RuntimeError(
                "Second window must be partial"
            )

        _assert_close(
            second[
                "window_duration_seconds"
            ],
            5.0,
            name=(
                "second."
                "window_duration_seconds"
            ),
        )
        _assert_close(
            second["sample_coverage_pct"],
            100.0,
            name="second.sample_coverage_pct",
        )
        _assert_close(
            second["mean"],
            32.0,
            name="second.mean",
        )
        _assert_close(
            second["peak_to_peak"],
            4.0,
            name="second.peak_to_peak",
        )

        if (
            first[
                "invalid_signal_sample_count"
            ]
            != 0
            or second[
                "invalid_signal_sample_count"
            ]
            != 0
        ):
            raise RuntimeError(
                "Synthetic signal unexpectedly "
                "contains invalid samples"
            )

        print(
            "signal_feature_full_window_check="
            "success"
        )
        print(
            "signal_feature_partial_window_check="
            "success"
        )
        print(
            "signal_feature_math_smoke_status="
            "success"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run_smoke_test()
