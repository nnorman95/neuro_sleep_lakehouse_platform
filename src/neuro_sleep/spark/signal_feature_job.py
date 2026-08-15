from __future__ import annotations

import math
import os

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from neuro_sleep.config import get_settings
from neuro_sleep.gold.signal_feature_publication import (
    GOLD_BUCKET,
    GoldSignalFeaturePublication,
    build_data_prefix,
    build_gold_output_prefix,
    build_success_manifest,
    inspect_publication_state,
    inspect_written_data_object,
    remove_spark_success_marker,
    upload_success_manifest,
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
    FEATURE_VERSION,
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

    if {
        context.recording_key
        for context in selected
    } != {
        item.recording_key
    }:
        raise RuntimeError(
            "Warehouse channel context does "
            "not match selected recording key"
        )

    return selected


def _expected_counts(
    contexts: tuple[
        SignalChannelContext,
        ...,
    ],
) -> tuple[int, int]:
    durations = {
        context.recording_duration_seconds
        for context in contexts
    }
    if len(durations) != 1:
        raise RuntimeError(
            "Warehouse channels disagree on "
            "recording duration"
        )

    duration = next(
        iter(durations)
    )
    window_count = (
        expected_window_count(duration)
    )
    row_count = (
        window_count
        * len(contexts)
    )

    remainder = math.fmod(
        duration,
        30.0,
    )
    has_partial = not math.isclose(
        remainder,
        0.0,
        abs_tol=1e-9,
    )
    partial_count = (
        len(contexts)
        if has_partial
        else 0
    )

    return (
        row_count,
        partial_count,
    )


def _validate_before_publish(
    frame: DataFrame,
    *,
    item: SelectedSignalInput,
    expected_row_count: int,
    expected_partial_count: int,
) -> None:
    non_finite = (
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
        frame.agg(
            F.count("*").alias(
                "row_count"
            ),
            F.countDistinct(
                "recording_id"
            ).alias(
                "recording_id_count"
            ),
            F.min(
                "recording_id"
            ).alias(
                "min_recording_id"
            ),
            F.max(
                "recording_id"
            ).alias(
                "max_recording_id"
            ),
            F.sum(
                "invalid_signal_sample_count"
            ).alias(
                "invalid_samples"
            ),
            F.sum(
                F.when(
                    F.col("sample_count")
                    != F.col(
                        "expected_sample_count"
                    ),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "sample_count_mismatch_rows"
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
                    non_finite,
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "non_finite_rows"
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
            F.countDistinct(
                "feature_version"
            ).alias(
                "feature_version_count"
            ),
            F.min(
                "feature_version"
            ).alias(
                "min_feature_version"
            ),
            F.max(
                "feature_version"
            ).alias(
                "max_feature_version"
            ),
        )
        .first()
    )

    if stats is None:
        raise RuntimeError(
            "Gold feature validation "
            "returned no result"
        )

    checks = {
        "row_count": (
            int(stats["row_count"]),
            expected_row_count,
        ),
        "recording_id_count": (
            int(
                stats[
                    "recording_id_count"
                ]
            ),
            1,
        ),
        "invalid_samples": (
            int(
                stats[
                    "invalid_samples"
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
        "coverage_mismatch_rows": (
            int(
                stats[
                    "coverage_mismatch_rows"
                ]
            ),
            0,
        ),
        "non_finite_rows": (
            int(
                stats[
                    "non_finite_rows"
                ]
            ),
            0,
        ),
        "partial_rows": (
            int(
                stats[
                    "partial_rows"
                ]
            ),
            expected_partial_count,
        ),
        "feature_version_count": (
            int(
                stats[
                    "feature_version_count"
                ]
            ),
            1,
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
            "Gold signal feature pre-publish "
            f"validation failed for "
            f"{item.recording_key}: "
            f"{details}"
        )

    if (
        str(stats["min_recording_id"])
        != item.recording_id
        or str(
            stats["max_recording_id"]
        )
        != item.recording_id
    ):
        raise RuntimeError(
            "Gold feature recording_id "
            "does not match selected Silver "
            f"input: {item.recording_key}"
        )

    if (
        str(
            stats[
                "min_feature_version"
            ]
        )
        != FEATURE_VERSION
        or str(
            stats[
                "max_feature_version"
            ]
        )
        != FEATURE_VERSION
    ):
        raise RuntimeError(
            "Gold feature version mismatch"
        )


def _validate_written_gold(
    spark,
    *,
    item: SelectedSignalInput,
    output_prefix: str,
    expected_row_count: int,
    expected_partial_count: int,
) -> None:
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
                "row_count"
            ),
            F.countDistinct(
                "recording_id"
            ).alias(
                "recording_id_count"
            ),
            F.min(
                "recording_id"
            ).alias(
                "min_recording_id"
            ),
            F.max(
                "recording_id"
            ).alias(
                "max_recording_id"
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
        )
        .first()
    )

    if stats is None:
        raise RuntimeError(
            "Gold read-back validation "
            "returned no result"
        )

    if int(
        stats["row_count"]
    ) != expected_row_count:
        raise RuntimeError(
            "Gold read-back row count mismatch"
        )

    if int(
        stats["partial_rows"]
    ) != expected_partial_count:
        raise RuntimeError(
            "Gold read-back partial-window "
            "count mismatch"
        )

    if (
        int(
            stats[
                "recording_id_count"
            ]
        )
        != 1
        or str(
            stats["min_recording_id"]
        )
        != item.recording_id
        or str(
            stats["max_recording_id"]
        )
        != item.recording_id
    ):
        raise RuntimeError(
            "Gold read-back recording_id "
            "validation failed"
        )


def run_job() -> None:
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
    contexts = (
        fetch_signal_channel_contexts(
            settings=settings
        )
    )

    spark = create_spark_session(
        "neurosleep-gold-signal-features",
        master="local[*]",
        ui_enabled=False,
    )
    client = get_object_storage_client(
        settings=settings
    )

    written = 0
    skipped = 0
    recovered_objects = 0
    total_rows = 0

    try:
        assert_s3a_runtime(spark)
        configure_minio_s3a(
            spark,
            settings=settings,
        )

        for item in inputs:
            item_contexts = (
                _contexts_for_input(
                    contexts,
                    item,
                )
            )
            (
                expected_rows,
                expected_partial,
            ) = _expected_counts(
                item_contexts
            )

            state = inspect_publication_state(
                item=item,
                expected_row_count=(
                    expected_rows
                ),
                expected_partial_window_count=(
                    expected_partial
                ),
                client=client,
            )

            if isinstance(
                state,
                GoldSignalFeaturePublication,
            ):
                skipped += 1
                total_rows += state.row_count
                print(
                    f"{item.recording_key}: "
                    "status=skipped "
                    f"rows={state.row_count} "
                    f"prefix={state.output_prefix}"
                )
                continue

            action, recovered_count = state
            if action != "write":
                raise RuntimeError(
                    "Unexpected Gold "
                    "publication action"
                )

            recovered_objects += (
                recovered_count
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
                    contexts=item_contexts,
                )
            )
            feature_frame = (
                build_signal_feature_frame(
                    signal_frame,
                    channel_context_frame=(
                        context_frame
                    ),
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
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

            try:
                _validate_before_publish(
                    feature_frame,
                    item=item,
                    expected_row_count=(
                        expected_rows
                    ),
                    expected_partial_count=(
                        expected_partial
                    ),
                )

                (
                    feature_frame
                    .coalesce(1)
                    .write
                    .mode("errorifexists")
                    .option(
                        "compression",
                        "snappy",
                    )
                    .parquet(data_path)
                )

                remove_spark_success_marker(
                    output_prefix=(
                        output_prefix
                    ),
                    client=client,
                )

                data_object = (
                    inspect_written_data_object(
                        output_prefix=(
                            output_prefix
                        ),
                        client=client,
                    )
                )

                _validate_written_gold(
                    spark,
                    item=item,
                    output_prefix=(
                        output_prefix
                    ),
                    expected_row_count=(
                        expected_rows
                    ),
                    expected_partial_count=(
                        expected_partial
                    ),
                )

                manifest = (
                    build_success_manifest(
                        item=item,
                        output_prefix=(
                            output_prefix
                        ),
                        row_count=(
                            expected_rows
                        ),
                        partial_window_count=(
                            expected_partial
                        ),
                        data_object=(
                            data_object
                        ),
                        spark_version=(
                            spark.version
                        ),
                    )
                )

                upload_success_manifest(
                    output_prefix=(
                        output_prefix
                    ),
                    manifest=manifest,
                    client=client,
                )

                validated = (
                    inspect_publication_state(
                        item=item,
                        expected_row_count=(
                            expected_rows
                        ),
                        expected_partial_window_count=(
                            expected_partial
                        ),
                        client=client,
                    )
                )

                if not isinstance(
                    validated,
                    GoldSignalFeaturePublication,
                ):
                    raise RuntimeError(
                        "Gold publication was not "
                        "valid after success "
                        "manifest upload"
                    )

                written += 1
                total_rows += (
                    expected_rows
                )

                print(
                    f"{item.recording_key}: "
                    "status=written "
                    f"rows={expected_rows} "
                    "data_files=1 "
                    "partial_rows="
                    f"{expected_partial} "
                    "recovered_objects="
                    f"{recovered_count} "
                    f"prefix={output_prefix}"
                )

            finally:
                feature_frame.unpersist()

        print()
        print(
            "gold_signal_feature_"
            f"recordings={len(inputs)}"
        )
        print(
            "gold_signal_feature_"
            f"written={written}"
        )
        print(
            "gold_signal_feature_"
            f"skipped={skipped}"
        )
        print(
            "gold_signal_feature_"
            "recovered_objects="
            f"{recovered_objects}"
        )
        print(
            "gold_signal_feature_"
            f"rows={total_rows}"
        )
        print(
            "gold_signal_feature_job_status="
            "success"
        )

    finally:
        client.close()
        spark.stop()


if __name__ == "__main__":
    run_job()
