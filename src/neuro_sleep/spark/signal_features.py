from __future__ import annotations

from dataclasses import dataclass
import math

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from neuro_sleep.config import Settings
from neuro_sleep.db.postgres import (
    get_postgres_connection,
)


FEATURE_VERSION = "1.0.0"
WINDOW_SECONDS = 30.0


@dataclass(frozen=True)
class SignalChannelContext:
    source_system: str
    dataset_version: str
    collection: str
    recording_key: str
    recording_id: str
    recording_duration_seconds: float
    channel_id: str
    channel_position: int
    source_label: str
    normalized_name: str
    sampling_frequency_hz: float


def fetch_signal_channel_contexts(
    *,
    settings: Settings,
) -> tuple[SignalChannelContext, ...]:
    with get_postgres_connection(
        settings=settings
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    r.source_system,
                    r.dataset_version,
                    r.collection,
                    r.recording_key,
                    r.silver_recording_id,
                    r.duration_seconds,
                    c.silver_channel_id,
                    c.position,
                    c.source_label,
                    c.normalized_name,
                    c.sampling_frequency_hz
                from warehouse.dim_channel c
                inner join warehouse.dim_recording r
                    on r.recording_sk = c.recording_sk
                order by
                    r.source_system,
                    r.dataset_version,
                    r.collection,
                    r.recording_key,
                    c.position;
                """
            )
            rows = cursor.fetchall()

    contexts = tuple(
        SignalChannelContext(
            source_system=str(row[0]),
            dataset_version=str(row[1]),
            collection=str(row[2]),
            recording_key=str(row[3]),
            recording_id=str(row[4]),
            recording_duration_seconds=float(
                row[5]
            ),
            channel_id=str(row[6]),
            channel_position=int(row[7]),
            source_label=str(row[8]),
            normalized_name=str(row[9]),
            sampling_frequency_hz=float(
                row[10]
            ),
        )
        for row in rows
    )

    if not contexts:
        raise RuntimeError(
            "Warehouse contains no channel "
            "context rows"
        )

    identities = {
        (
            item.recording_id,
            item.channel_id,
        )
        for item in contexts
    }
    if len(identities) != len(contexts):
        raise RuntimeError(
            "Warehouse channel context contains "
            "duplicate recording/channel "
            "identities"
        )

    return contexts


_CONTEXT_SCHEMA = T.StructType(
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
            "source_system",
            T.StringType(),
            False,
        ),
        T.StructField(
            "dataset_version",
            T.StringType(),
            False,
        ),
        T.StructField(
            "collection",
            T.StringType(),
            False,
        ),
        T.StructField(
            "recording_key",
            T.StringType(),
            False,
        ),
        T.StructField(
            "recording_duration_seconds",
            T.DoubleType(),
            False,
        ),
        T.StructField(
            "channel_position",
            T.IntegerType(),
            False,
        ),
        T.StructField(
            "source_label",
            T.StringType(),
            False,
        ),
        T.StructField(
            "normalized_name",
            T.StringType(),
            False,
        ),
        T.StructField(
            "sampling_frequency_hz",
            T.DoubleType(),
            False,
        ),
    ]
)


def build_channel_context_frame(
    spark: SparkSession,
    *,
    contexts: tuple[
        SignalChannelContext,
        ...,
    ],
) -> DataFrame:
    if not contexts:
        raise ValueError(
            "At least one channel context "
            "is required"
        )

    rows = [
        (
            item.recording_id,
            item.channel_id,
            item.source_system,
            item.dataset_version,
            item.collection,
            item.recording_key,
            item.recording_duration_seconds,
            item.channel_position,
            item.source_label,
            item.normalized_name,
            item.sampling_frequency_hz,
        )
        for item in contexts
    ]

    return spark.createDataFrame(
        rows,
        schema=_CONTEXT_SCHEMA,
    )


def expected_window_count(
    recording_duration_seconds: float,
) -> int:
    if recording_duration_seconds <= 0:
        raise ValueError(
            "recording_duration_seconds "
            "must be positive"
        )

    return math.ceil(
        recording_duration_seconds
        / WINDOW_SECONDS
    )


def build_signal_feature_frame(
    signal_frame: DataFrame,
    *,
    channel_context_frame: DataFrame,
) -> DataFrame:
    required_signal_columns = {
        "recording_id",
        "channel_id",
        "sample_index",
        "elapsed_seconds",
        "epoch_number",
        "signal_value",
    }
    missing = (
        required_signal_columns
        - set(signal_frame.columns)
    )
    if missing:
        raise RuntimeError(
            "Signal frame is missing required "
            "columns: "
            + ", ".join(sorted(missing))
        )

    aggregated = (
        signal_frame
        .groupBy(
            "recording_id",
            "channel_id",
            "epoch_number",
        )
        .agg(
            F.count("*").alias(
                "sample_count"
            ),
            F.min(
                "sample_index"
            ).alias(
                "first_sample_index"
            ),
            F.max(
                "sample_index"
            ).alias(
                "last_sample_index"
            ),
            F.min(
                "elapsed_seconds"
            ).alias(
                "first_sample_elapsed_seconds"
            ),
            F.max(
                "elapsed_seconds"
            ).alias(
                "last_sample_elapsed_seconds"
            ),
            F.sum(
                F.when(
                    F.col(
                        "signal_value"
                    ).isNull()
                    | F.isnan(
                        "signal_value"
                    ),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "invalid_signal_sample_count"
            ),
            F.avg(
                "signal_value"
            ).alias(
                "mean"
            ),
            F.stddev_pop(
                "signal_value"
            ).alias(
                "stddev_pop"
            ),
            F.min(
                "signal_value"
            ).alias(
                "min"
            ),
            F.max(
                "signal_value"
            ).alias(
                "max"
            ),
            F.avg(
                F.col("signal_value")
                * F.col("signal_value")
            ).alias(
                "_mean_square"
            ),
        )
    )

    joined = aggregated.join(
        F.broadcast(
            channel_context_frame
        ),
        on=[
            "recording_id",
            "channel_id",
        ],
        how="left",
    )

    with_windows = (
        joined
        .withColumn(
            "window_start_seconds",
            F.col("epoch_number").cast(
                "double"
            )
            * F.lit(WINDOW_SECONDS),
        )
        .withColumn(
            "window_duration_seconds",
            F.least(
                F.lit(WINDOW_SECONDS),
                F.greatest(
                    F.lit(0.0),
                    F.col(
                        "recording_duration_seconds"
                    )
                    - F.col(
                        "window_start_seconds"
                    ),
                ),
            ),
        )
        .withColumn(
            "window_end_seconds",
            F.col(
                "window_start_seconds"
            )
            + F.col(
                "window_duration_seconds"
            ),
        )
        .withColumn(
            "is_partial_window",
            F.col(
                "window_duration_seconds"
            )
            < F.lit(WINDOW_SECONDS),
        )
        .withColumn(
            "samples_per_full_window",
            F.round(
                F.col(
                    "sampling_frequency_hz"
                )
                * F.lit(WINDOW_SECONDS)
            ).cast("long"),
        )
        .withColumn(
            "expected_sample_count",
            F.round(
                F.col(
                    "sampling_frequency_hz"
                )
                * F.col(
                    "window_duration_seconds"
                )
            ).cast("long"),
        )
        .withColumn(
            "sample_coverage_pct",
            F.when(
                F.col(
                    "expected_sample_count"
                )
                > F.lit(0),
                (
                    F.col("sample_count")
                    / F.col(
                        "expected_sample_count"
                    )
                    * F.lit(100.0)
                ),
            ),
        )
        .withColumn(
            "peak_to_peak",
            F.col("max")
            - F.col("min"),
        )
        .withColumn(
            "rms",
            F.sqrt(
                F.col("_mean_square")
            ),
        )
        .withColumn(
            "feature_version",
            F.lit(FEATURE_VERSION),
        )
    )

    return with_windows.select(
        "source_system",
        "dataset_version",
        "collection",
        "recording_key",
        "recording_id",
        "channel_id",
        "channel_position",
        "source_label",
        "normalized_name",
        "sampling_frequency_hz",
        "epoch_number",
        "window_start_seconds",
        "window_end_seconds",
        "window_duration_seconds",
        "is_partial_window",
        "sample_count",
        "expected_sample_count",
        "sample_coverage_pct",
        "first_sample_index",
        "last_sample_index",
        "first_sample_elapsed_seconds",
        "last_sample_elapsed_seconds",
        "invalid_signal_sample_count",
        "mean",
        "stddev_pop",
        "min",
        "max",
        "peak_to_peak",
        "rms",
        "samples_per_full_window",
        "feature_version",
    )
