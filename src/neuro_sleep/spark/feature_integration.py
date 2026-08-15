from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from neuro_sleep.config import Settings
from neuro_sleep.db.postgres import get_postgres_connection


INTEGRATION_VERSION = "1.0.0"


@dataclass(frozen=True)
class RecordingChannelIntegrationContext:
    recording_id: str
    channel_id: str
    subject_sk: str
    subject_key: str
    age_years: int
    sex: str
    recording_sk: str
    channel_sk: str
    night_number: int
    treatment: str | None
    lights_off_seconds: float | None


@dataclass(frozen=True)
class EpochLabelIntegrationContext:
    recording_id: str
    epoch_number: int
    sleep_epoch_sk: str
    sleep_stage_sk: int
    silver_epoch_id: str
    epoch_start_seconds: float
    epoch_end_seconds: float
    sleep_stage_source_label: str
    silver_stage_code: str
    analytical_stage_code: str


def fetch_recording_channel_contexts(*, settings: Settings) -> tuple[RecordingChannelIntegrationContext, ...]:
    with get_postgres_connection(settings=settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                select
                    r.silver_recording_id,
                    c.silver_channel_id,
                    r.subject_sk,
                    s.subject_key,
                    s.age_years,
                    s.sex,
                    r.recording_sk,
                    c.channel_sk,
                    r.night_number,
                    r.treatment,
                    r.lights_off_seconds
                from warehouse.dim_channel as c
                inner join warehouse.dim_recording as r
                    on r.recording_sk = c.recording_sk
                inner join warehouse.dim_subject as s
                    on s.subject_sk = r.subject_sk
                order by
                    r.silver_recording_id,
                    c.position;
                '''
            )
            rows = cursor.fetchall()

    contexts = tuple(
        RecordingChannelIntegrationContext(
            recording_id=str(row[0]),
            channel_id=str(row[1]),
            subject_sk=str(row[2]),
            subject_key=str(row[3]),
            age_years=int(row[4]),
            sex=str(row[5]),
            recording_sk=str(row[6]),
            channel_sk=str(row[7]),
            night_number=int(row[8]),
            treatment=None if row[9] is None else str(row[9]),
            lights_off_seconds=None if row[10] is None else float(row[10]),
        )
        for row in rows
    )
    if not contexts:
        raise RuntimeError("Warehouse contains no recording/channel integration contexts")

    identities = {(item.recording_id, item.channel_id) for item in contexts}
    if len(identities) != len(contexts):
        raise RuntimeError("Warehouse recording/channel integration context contains duplicate identities")

    return contexts


def fetch_epoch_label_contexts(*, settings: Settings) -> tuple[EpochLabelIntegrationContext, ...]:
    with get_postgres_connection(settings=settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                select
                    f.silver_recording_id,
                    f.epoch_number,
                    f.sleep_epoch_sk,
                    f.sleep_stage_sk,
                    f.silver_epoch_id,
                    f.start_seconds,
                    f.end_seconds,
                    f.source_label,
                    f.silver_stage_code,
                    d.analytical_stage_code
                from warehouse.fact_sleep_epoch as f
                inner join warehouse.dim_sleep_stage as d
                    on d.sleep_stage_sk = f.sleep_stage_sk
                order by
                    f.silver_recording_id,
                    f.epoch_number;
                '''
            )
            rows = cursor.fetchall()

    contexts = tuple(
        EpochLabelIntegrationContext(
            recording_id=str(row[0]),
            epoch_number=int(row[1]),
            sleep_epoch_sk=str(row[2]),
            sleep_stage_sk=int(row[3]),
            silver_epoch_id=str(row[4]),
            epoch_start_seconds=float(row[5]),
            epoch_end_seconds=float(row[6]),
            sleep_stage_source_label=str(row[7]),
            silver_stage_code=str(row[8]),
            analytical_stage_code=str(row[9]),
        )
        for row in rows
    )
    if not contexts:
        raise RuntimeError("Warehouse contains no sleep-epoch integration contexts")

    identities = {(item.recording_id, item.epoch_number) for item in contexts}
    if len(identities) != len(contexts):
        raise RuntimeError("Warehouse epoch integration context contains duplicate recording/epoch identities")

    return contexts


_RECORDING_CHANNEL_SCHEMA = T.StructType(
    [
        T.StructField("recording_id", T.StringType(), False),
        T.StructField("channel_id", T.StringType(), False),
        T.StructField("subject_sk", T.StringType(), False),
        T.StructField("subject_key", T.StringType(), False),
        T.StructField("age_years", T.IntegerType(), False),
        T.StructField("sex", T.StringType(), False),
        T.StructField("recording_sk", T.StringType(), False),
        T.StructField("channel_sk", T.StringType(), False),
        T.StructField("night_number", T.IntegerType(), False),
        T.StructField("treatment", T.StringType(), True),
        T.StructField("lights_off_seconds", T.DoubleType(), True),
    ]
)

_EPOCH_LABEL_SCHEMA = T.StructType(
    [
        T.StructField("recording_id", T.StringType(), False),
        T.StructField("epoch_number", T.IntegerType(), False),
        T.StructField("sleep_epoch_sk", T.StringType(), False),
        T.StructField("sleep_stage_sk", T.IntegerType(), False),
        T.StructField("silver_epoch_id", T.StringType(), False),
        T.StructField("epoch_start_seconds", T.DoubleType(), False),
        T.StructField("epoch_end_seconds", T.DoubleType(), False),
        T.StructField("sleep_stage_source_label", T.StringType(), False),
        T.StructField("silver_stage_code", T.StringType(), False),
        T.StructField("analytical_stage_code", T.StringType(), False),
    ]
)


def build_recording_channel_context_frame(
    spark: SparkSession,
    *,
    contexts: tuple[RecordingChannelIntegrationContext, ...],
) -> DataFrame:
    if not contexts:
        raise ValueError("At least one recording/channel integration context is required")
    rows = [
        (
            item.recording_id,
            item.channel_id,
            item.subject_sk,
            item.subject_key,
            item.age_years,
            item.sex,
            item.recording_sk,
            item.channel_sk,
            item.night_number,
            item.treatment,
            item.lights_off_seconds,
        )
        for item in contexts
    ]
    return spark.createDataFrame(rows, schema=_RECORDING_CHANNEL_SCHEMA)


def build_epoch_label_context_frame(
    spark: SparkSession,
    *,
    contexts: tuple[EpochLabelIntegrationContext, ...],
) -> DataFrame:
    if not contexts:
        raise ValueError("At least one sleep-epoch integration context is required")
    rows = [
        (
            item.recording_id,
            item.epoch_number,
            item.sleep_epoch_sk,
            item.sleep_stage_sk,
            item.silver_epoch_id,
            item.epoch_start_seconds,
            item.epoch_end_seconds,
            item.sleep_stage_source_label,
            item.silver_stage_code,
            item.analytical_stage_code,
        )
        for item in contexts
    ]
    return spark.createDataFrame(rows, schema=_EPOCH_LABEL_SCHEMA)


def build_integrated_feature_frame(
    gold_feature_frame: DataFrame,
    *,
    recording_channel_context_frame: DataFrame,
    epoch_label_context_frame: DataFrame,
) -> DataFrame:
    required_gold_columns = {
        "source_system", "dataset_version", "collection", "recording_key",
        "recording_id", "channel_id", "channel_position", "source_label",
        "normalized_name", "sampling_frequency_hz", "epoch_number",
        "window_start_seconds", "window_end_seconds", "window_duration_seconds",
        "is_partial_window", "sample_count", "expected_sample_count",
        "sample_coverage_pct", "first_sample_index", "last_sample_index",
        "first_sample_elapsed_seconds", "last_sample_elapsed_seconds",
        "invalid_signal_sample_count", "mean", "stddev_pop", "min", "max",
        "peak_to_peak", "rms", "samples_per_full_window", "feature_version",
    }
    missing = required_gold_columns - set(gold_feature_frame.columns)
    if missing:
        raise RuntimeError(
            "Gold feature frame is missing required columns: "
            + ", ".join(sorted(missing))
        )

    gold = gold_feature_frame.alias("g")
    recording_context = F.broadcast(recording_channel_context_frame).alias("c")
    epoch_context = F.broadcast(epoch_label_context_frame).alias("e")

    joined = (
        gold.join(
            recording_context,
            on=(
                (F.col("g.recording_id") == F.col("c.recording_id"))
                & (F.col("g.channel_id") == F.col("c.channel_id"))
            ),
            how="left",
        )
        .join(
            epoch_context,
            on=(
                (F.col("g.recording_id") == F.col("e.recording_id"))
                & (F.col("g.epoch_number") == F.col("e.epoch_number"))
            ),
            how="left",
        )
    )

    return joined.select(
        F.col("g.source_system").alias("source_system"),
        F.col("g.dataset_version").alias("dataset_version"),
        F.col("g.collection").alias("collection"),
        F.col("g.recording_key").alias("recording_key"),
        F.col("g.recording_id").alias("recording_id"),
        F.col("g.channel_id").alias("channel_id"),
        F.col("g.epoch_number").alias("epoch_number"),
        F.col("c.subject_sk").alias("subject_sk"),
        F.col("c.subject_key").alias("subject_key"),
        F.col("c.age_years").alias("age_years"),
        F.col("c.sex").alias("sex"),
        F.col("c.recording_sk").alias("recording_sk"),
        F.col("c.channel_sk").alias("channel_sk"),
        F.col("c.night_number").alias("night_number"),
        F.col("c.treatment").alias("treatment"),
        F.col("c.lights_off_seconds").alias("lights_off_seconds"),
        F.col("e.sleep_epoch_sk").alias("sleep_epoch_sk"),
        F.col("e.sleep_stage_sk").alias("sleep_stage_sk"),
        F.col("e.silver_epoch_id").alias("silver_epoch_id"),
        F.col("e.sleep_stage_source_label").alias("sleep_stage_source_label"),
        F.col("e.silver_stage_code").alias("silver_stage_code"),
        F.col("e.analytical_stage_code").alias("analytical_stage_code"),
        F.col("e.epoch_start_seconds").alias("labeled_epoch_start_seconds"),
        F.col("e.epoch_end_seconds").alias("labeled_epoch_end_seconds"),
        F.col("g.channel_position").alias("channel_position"),
        F.col("g.source_label").alias("channel_source_label"),
        F.col("g.normalized_name").alias("normalized_name"),
        F.col("g.sampling_frequency_hz").alias("sampling_frequency_hz"),
        F.col("g.window_start_seconds").alias("window_start_seconds"),
        F.col("g.window_end_seconds").alias("window_end_seconds"),
        F.col("g.window_duration_seconds").alias("window_duration_seconds"),
        F.col("g.is_partial_window").alias("is_partial_window"),
        F.col("g.sample_count").alias("sample_count"),
        F.col("g.expected_sample_count").alias("expected_sample_count"),
        F.col("g.sample_coverage_pct").alias("sample_coverage_pct"),
        F.col("g.first_sample_index").alias("first_sample_index"),
        F.col("g.last_sample_index").alias("last_sample_index"),
        F.col("g.first_sample_elapsed_seconds").alias("first_sample_elapsed_seconds"),
        F.col("g.last_sample_elapsed_seconds").alias("last_sample_elapsed_seconds"),
        F.col("g.invalid_signal_sample_count").alias("invalid_signal_sample_count"),
        F.col("g.mean").alias("mean"),
        F.col("g.stddev_pop").alias("stddev_pop"),
        F.col("g.min").alias("min"),
        F.col("g.max").alias("max"),
        F.col("g.peak_to_peak").alias("peak_to_peak"),
        F.col("g.rms").alias("rms"),
        F.col("g.samples_per_full_window").alias("samples_per_full_window"),
        F.col("g.feature_version").alias("feature_version"),
        F.col("c.recording_sk").isNotNull().alias("has_warehouse_context"),
        F.col("e.sleep_epoch_sk").isNotNull().alias("has_sleep_stage_label"),
        F.lit(INTEGRATION_VERSION).alias("integration_version"),
    )
