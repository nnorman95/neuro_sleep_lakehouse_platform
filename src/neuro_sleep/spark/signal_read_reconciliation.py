from __future__ import annotations

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
from neuro_sleep.spark.signal_input import (
    discover_selected_signal_inputs,
)


EXPECTED_SIGNAL_COLUMNS = (
    "recording_id",
    "channel_id",
    "sample_index",
    "elapsed_seconds",
    "epoch_number",
    "signal_value",
)


def run_reconciliation() -> None:
    settings = get_settings()

    inputs = (
        discover_selected_signal_inputs(
            settings=settings,
            verify_live_objects=True,
        )
    )

    spark = create_spark_session(
        "neurosleep-selected-signal-reconciliation",
        master="local[*]",
        ui_enabled=False,
    )

    total_expected_rows = 0
    total_actual_rows = 0
    total_expected_files = 0
    total_spark_files = 0

    try:
        hadoop_version = (
            assert_s3a_runtime(spark)
        )
        configure_minio_s3a(
            spark,
            settings=settings,
        )

        print(
            f"hadoop={hadoop_version}"
        )
        print(
            "s3a_runtime_status=success"
        )
        print()

        for item in inputs:
            paths = [
                build_s3a_path(
                    bucket=item.bucket,
                    object_key=object_key,
                )
                for object_key in (
                    item.signal_object_keys
                )
            ]

            frame = spark.read.parquet(
                *paths
            )

            columns = tuple(frame.columns)
            if columns != EXPECTED_SIGNAL_COLUMNS:
                raise RuntimeError(
                    "Unexpected Silver signal "
                    "schema for "
                    f"{item.recording_key}: "
                    f"{columns}"
                )

            spark_file_count = len(
                frame.inputFiles()
            )

            if (
                spark_file_count
                != item.signal_file_count
            ):
                raise RuntimeError(
                    "Spark input file count does "
                    "not match the selected "
                    "Silver manifest for "
                    f"{item.recording_key}: "
                    "expected="
                    f"{item.signal_file_count} "
                    "actual="
                    f"{spark_file_count}"
                )

            stats = (
                frame.agg(
                    F.count("*").alias(
                        "row_count"
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
                )
                .first()
            )

            if stats is None:
                raise RuntimeError(
                    "Spark returned no aggregate "
                    "result for "
                    f"{item.recording_key}"
                )

            actual_rows = int(
                stats["row_count"]
            )

            if (
                actual_rows
                != item.signal_row_count
            ):
                raise RuntimeError(
                    "Spark row count does not "
                    "match the selected Silver "
                    "manifest for "
                    f"{item.recording_key}: "
                    "expected="
                    f"{item.signal_row_count} "
                    "actual="
                    f"{actual_rows}"
                )

            min_recording_id = str(
                stats["min_recording_id"]
            )
            max_recording_id = str(
                stats["max_recording_id"]
            )

            if (
                min_recording_id
                != item.recording_id
                or max_recording_id
                != item.recording_id
            ):
                raise RuntimeError(
                    "Spark input contains an "
                    "unexpected recording_id for "
                    f"{item.recording_key}: "
                    "expected="
                    f"{item.recording_id} "
                    "min="
                    f"{min_recording_id} "
                    "max="
                    f"{max_recording_id}"
                )

            partitions = (
                frame.rdd.getNumPartitions()
            )

            total_expected_rows += (
                item.signal_row_count
            )
            total_actual_rows += (
                actual_rows
            )
            total_expected_files += (
                item.signal_file_count
            )
            total_spark_files += (
                spark_file_count
            )

            print(
                f"{item.recording_key}: "
                f"files={spark_file_count} "
                f"partitions={partitions} "
                f"rows={actual_rows} "
                "recording_id_check=success"
            )

        if (
            total_actual_rows
            != total_expected_rows
        ):
            raise RuntimeError(
                "Total Spark signal row count "
                "does not reconcile: "
                "expected="
                f"{total_expected_rows} "
                "actual="
                f"{total_actual_rows}"
            )

        if (
            total_spark_files
            != total_expected_files
        ):
            raise RuntimeError(
                "Total Spark signal file count "
                "does not reconcile: "
                "expected="
                f"{total_expected_files} "
                "actual="
                f"{total_spark_files}"
            )

        print()
        print(
            "spark_selected_signal_recordings="
            f"{len(inputs)}"
        )
        print(
            "spark_selected_signal_files="
            f"{total_spark_files}"
        )
        print(
            "spark_expected_signal_rows="
            f"{total_expected_rows}"
        )
        print(
            "spark_actual_signal_rows="
            f"{total_actual_rows}"
        )
        print(
            "spark_selected_signal_"
            "reconciliation_status=success"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    run_reconciliation()
