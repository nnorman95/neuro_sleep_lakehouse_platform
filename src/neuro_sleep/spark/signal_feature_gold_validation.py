from __future__ import annotations

import math
import os

from pyspark.sql import functions as F

from neuro_sleep.config import get_settings
from neuro_sleep.gold.signal_feature_publication import (
    GOLD_BUCKET,
    GoldSignalFeaturePublication,
    build_data_prefix,
    build_gold_output_prefix,
    inspect_publication_state,
)
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
    expected_window_count,
    fetch_signal_channel_contexts,
)
from neuro_sleep.spark.signal_input import (
    SelectedSignalInput,
    discover_selected_signal_inputs,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
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
            "Unavailable recording keys: "
            + ", ".join(missing)
        )

    return tuple(
        by_key[key]
        for key in requested
    )


def _counts(
    contexts: tuple[
        SignalChannelContext,
        ...,
    ],
    item: SelectedSignalInput,
) -> tuple[int, int]:
    selected = tuple(
        value
        for value in contexts
        if value.recording_id
        == item.recording_id
    )
    if not selected:
        raise RuntimeError(
            "Missing channel context"
        )

    durations = {
        value.recording_duration_seconds
        for value in selected
    }
    if len(durations) != 1:
        raise RuntimeError(
            "Inconsistent recording duration"
        )

    duration = next(
        iter(durations)
    )
    rows = (
        expected_window_count(duration)
        * len(selected)
    )
    partial = (
        len(selected)
        if not math.isclose(
            math.fmod(
                duration,
                30.0,
            ),
            0.0,
            abs_tol=1e-9,
        )
        else 0
    )

    return rows, partial


def run_validation() -> None:
    settings = get_settings()
    inputs = _filter_inputs(
        discover_selected_signal_inputs(
            settings=settings,
            verify_live_objects=True,
        )
    )
    contexts = (
        fetch_signal_channel_contexts(
            settings=settings
        )
    )

    spark = create_spark_session(
        "neurosleep-gold-signal-feature-validation",
        master="local[*]",
        ui_enabled=False,
    )
    client = get_object_storage_client(
        settings=settings
    )

    total_rows = 0
    total_files = 0

    try:
        assert_s3a_runtime(spark)
        configure_minio_s3a(
            spark,
            settings=settings,
        )

        for item in inputs:
            rows, partial = _counts(
                contexts,
                item,
            )
            state = inspect_publication_state(
                item=item,
                expected_row_count=rows,
                expected_partial_window_count=(
                    partial
                ),
                client=client,
            )

            if not isinstance(
                state,
                GoldSignalFeaturePublication,
            ):
                raise RuntimeError(
                    "Expected completed Gold "
                    "publication for "
                    f"{item.recording_key}"
                )

            output_prefix = (
                build_gold_output_prefix(
                    item
                )
            )
            data_path = build_s3a_path(
                bucket=GOLD_BUCKET,
                object_key=build_data_prefix(
                    output_prefix
                ),
            )

            frame = spark.read.parquet(
                data_path
            )
            stats = (
                frame.agg(
                    F.count("*").alias(
                        "rows"
                    ),
                    F.countDistinct(
                        "channel_id"
                    ).alias(
                        "channels"
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
                        "partial"
                    ),
                    F.countDistinct(
                        "recording_id"
                    ).alias(
                        "recording_ids"
                    ),
                )
                .first()
            )

            if stats is None:
                raise RuntimeError(
                    "Gold validation returned "
                    "no statistics"
                )

            actual_rows = int(
                stats["rows"]
            )
            actual_partial = int(
                stats["partial"]
            )

            if (
                actual_rows != rows
                or actual_partial != partial
                or int(
                    stats[
                        "recording_ids"
                    ]
                )
                != 1
            ):
                raise RuntimeError(
                    "Gold read validation "
                    "failed for "
                    f"{item.recording_key}"
                )

            total_rows += actual_rows
            total_files += 1

            print(
                f"{item.recording_key}: "
                "manifest=valid "
                "data_files=1 "
                f"rows={actual_rows} "
                "channels="
                f"{int(stats['channels'])} "
                "partial_rows="
                f"{actual_partial} "
                "validation=success"
            )

        print()
        print(
            "gold_validation_recordings="
            f"{len(inputs)}"
        )
        print(
            "gold_validation_data_files="
            f"{total_files}"
        )
        print(
            "gold_validation_rows="
            f"{total_rows}"
        )
        print(
            "gold_signal_feature_"
            "validation_status=success"
        )

    finally:
        client.close()
        spark.stop()


if __name__ == "__main__":
    run_validation()
