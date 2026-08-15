# Architecture

This document describes the platform as implemented through **Phase 9:
Feature Integration**. Bronze, Silver, PostgreSQL staging, the Warehouse Core,
relational marts, and the Phase 8 signal-feature path remain in place. Phase 9
adds a separate integrated Gold representation combining reusable signal
features with Warehouse analytical context.

## 1. Source

```text
Dataset: Sleep-EDF Database Expanded
Version: 1.0.0
Source system: physionet_sleep_edf
Collections: sleep-cassette, sleep-telemetry
External access model: open
Internal patient-level access policy: restricted
```

The source can be downloaded without credentials, but subject-level sleep data
and quasi-identifying metadata are still handled as restricted data inside the
platform.

## 2. Storage and relational layers

The project uses separate names for object-storage layers and PostgreSQL schemas.
They are related, but they are not interchangeable.

### MinIO

```text
Bronze      immutable source EDF/XLS objects
Silver      validated and versioned Parquet datasets
Gold        curated versioned signal-feature and integrated-feature Parquet
Quarantine  large rejected/diagnostic payloads when needed
Logs        persisted runtime log artifacts when needed
```

### PostgreSQL

```text
raw         source-object registry and ingestion metadata
staging     verified relational landing for selected Silver publications
warehouse   dimensional analytical core
mart        consumption-ready relational analytics
ops         pipeline runs and file-attempt history
quality     quarantine metadata and durable quality results
governance  source registry, contracts, and column classifications
```

## 3. End-to-end flow

```text
PhysioNet
  -> RECORDS + SHA256SUMS.txt
  -> streaming extract
  -> SHA-256 verification
  -> MinIO Bronze
  -> raw.file_registry
  -> Python / edfio / NumPy / PyArrow
  -> Silver quality gate
  -> versioned Parquet + _SUCCESS.json
  -> PostgreSQL staging for metadata and epochs
  -> dbt fail-closed version selection
  -> Warehouse dimensions + sleep-epoch fact
  -> reusable analytical intermediate models
  -> recording and dataset marts

Selected Warehouse recording representations
  -> exact Silver signal manifest objects
  -> Spark 4.2 + S3A
  -> 30-second recording/channel features
  -> MinIO Gold signal_features + _SUCCESS.json

Gold signal_features + Warehouse subject/recording/channel/epoch context
  -> Spark feature integration
  -> preserve unlabeled signal windows
  -> MinIO Gold integrated_signal_features + _SUCCESS.json
```

Operational execution is tracked in:

```text
ops.pipeline_run
ops.file_attempt
quality.quality_check_results
quality.quarantine_records
```
## 4. Bronze architecture

Bronze keeps the source layout under:

```text
bronze/physionet/sleep-edfx/1.0.0/
```

The ingestion path includes retryable HTTP/object-storage operations, official
checksum validation, verified-object recovery, idempotent registration, advisory
locks, heartbeats, safe interruption cleanup, `.part` cleanup, and storage/registry
reconciliation.

Bronze objects are never rewritten to “fix” source data.

## 5. Silver recording architecture

Each PSG/Hypnogram pair can produce:

```text
recordings
channels
sleep_stage_intervals
sleep_stage_epochs
signals
```

The first four datasets are small enough for the relational analytical path.
`signals` is high-volume sample data and stays in Parquet.

Silver publication includes:

- explicit Arrow schemas;
- Zstandard-compressed Parquet;
- source-preserving and normalized sleep-stage values;
- quality errors that block publication;
- warnings that stay visible without necessarily blocking publication;
- payload checksums and verified uploads;
- versioned output prefixes;
- `_SUCCESS.json` manifests;
- partial-output recovery and idempotent reruns;
- reconciliation and durable quality results.

A Silver quality-gate error is also routed to `quality.quarantine_records`. The
same active incident is refreshed on repeated failures and resolved after a
successful written or skipped rerun. Runtime/network/database/storage errors are
kept as operational failures instead of being classified as bad data.

## 6. Subject metadata

The metadata pipeline reads:

```text
SC-subjects.xls
ST-subjects.xls
```

and publishes:

```text
subjects.parquet
recording_contexts.parquet
_SUCCESS.json
```

The two datasets are separate because subject demographics and recording-level
context have different grains. Night number, treatment, and lights-off values
belong to the recording context, not to the subject row.

Current metadata publication:

```text
100 subjects
197 recording contexts
```

## 7. Identity and versioning

The project keeps logical identity separate from one concrete processed version.

```text
recording_key
  logical Sleep-EDF recording/night

source_pair_id
  identity of PSG/Hypnogram object locations

input_fingerprint
  identity of the verified source bytes

config_id
  identity of the Silver transform configuration

recording_id
  UUIDv7 for one concrete materialized Silver representation
```

A logical recording can therefore have more than one valid historical
representation without overwriting older data.

The Warehouse current-state selection is **fail-closed**. If a logical recording
has zero or more than one compatible representation, dbt does not choose by load
time or UUID order.

## 8. Analytical cohort and process boundary

Phase 7 uses two deliberately different scopes:

```text
Full-signal subset
  5 recordings
  116,242,840 Silver signal rows

Relational analytical cohort
  18 recordings
  9 represented subjects
  110 channels
  3,263 annotation intervals
  35,710 emitted sleep-stage epochs
```

The additional 13 recordings were processed in metadata-only mode. Their
recording metadata, channels, intervals, and epochs are available to PostgreSQL
and dbt, but signal Parquet was not generated because Phase 7 does not use it.

This keeps the workflow aligned with the actual analytical requirement and
avoids turning a relational-mart task into unnecessary high-volume signal
processing.

## 9. PostgreSQL staging

Current verified staging state:

```text
staging.silver_subjects                 100
staging.silver_recording_contexts       197
staging.silver_recordings                18
staging.silver_channels                 110
staging.silver_sleep_stage_intervals   3,263
staging.silver_sleep_stage_epochs     35,710
```

The subject loader validates publication identity, object inventory, file sizes,
checksums, exact Parquet schemas, and subject relationships before writing both
subject tables in one transaction.

The recording loader validates the current compatible `_SUCCESS.json`
publications, object size/checksum/schema/row-count metadata, logical recording
identity, and parent-child relationships before writing the four recording
staging tables.

The cohort expansion run wrote 13 new publications and skipped 5 existing ones.
The immediate rerun skipped all 18 and wrote zero rows.

## 10. Warehouse Core

```text
warehouse.dim_subject          100
warehouse.dim_recording         18
warehouse.dim_channel          110
warehouse.dim_sleep_stage        8
warehouse.fact_sleep_epoch  35,710
```

The Warehouse uses deterministic surrogate keys and keeps source/Silver lineage
alongside them. dbt tests enforce grains, accepted values, relationships,
selected-version consistency, and source-to-Warehouse reconciliation.

`warehouse.dim_sleep_stage` preserves the original Silver stage and provides the
controlled analytical mapping:

```text
N3 -> N3
N4 -> N3
```

`UNKNOWN` and `MOVEMENT` remain explicit.

## 11. Analytics marts

Phase 7 adds two reusable ephemeral intermediate models:

```text
int_recording_stage_metrics
int_recording_sleep_metrics
```

and three physical PostgreSQL tables:

```text
mart.mart_recording_sleep_summary       18 rows
mart.mart_recording_stage_distribution 126 rows
mart.mart_dataset_coverage                6 rows
```

The physical marts are created by dbt in the existing `mart` schema. The
recording-stage mart contains a complete 18 x 7 grid, including zero-duration
stages.

The marts are descriptive. They expose coverage and sleep-stage composition but
do not add arbitrary scientific exclusion thresholds or clinical conclusions.

See [`analytics_marts.md`](analytics_marts.md) for grains and formulas.

## 12. Spark signal feature path

Phase 8 processes only the signal representations selected by the Warehouse.

```text
warehouse.dim_recording
  -> current silver_recording_id + silver_output_prefix
  -> exact signals/*.parquet inventory from Silver _SUCCESS.json
  -> Spark S3A read from MinIO
  -> group by recording_id + channel_id + epoch_number
  -> descriptive 30-second features
  -> pre-publication validation
  -> one compact Gold Parquet data file per recording
  -> Spark read-back validation
  -> Gold _SUCCESS.json written last
```

Current verified input and output:

```text
selected recordings:          5
Silver signal files:      1,416
Silver signal rows:  116,242,840
Gold feature rows:          83,909
Gold data files:                 5
Gold manifests:                  5
```

The feature set includes mean, population standard deviation, minimum, maximum,
peak-to-peak, RMS, sample coverage, window timing, and lineage fields. A final
partial signal window is preserved; ST7011J currently contributes five partial
feature rows, one per channel.

Gold publication is immutable by selected Silver `recording_id`. An unchanged
completed publication is skipped. An incomplete exact prefix without
`_SUCCESS.json` can be recovered and rebuilt. A completed but invalid prefix
fails closed and is not auto-deleted.

See [`spark_signal_features.md`](spark_signal_features.md).

## 13. Scale boundary

PostgreSQL is used for operational metadata, lineage, quality metadata, staging,
dimensional models, and low-volume relational marts.

Sample-level signals are not loaded row-by-row into PostgreSQL. The current
116,242,840 selected signal rows remain trusted Silver Parquet. Spark performs the
high-volume aggregation close to object storage, and Gold contains the compact
83,909-row feature representation.

The current local data size does not justify a separate Spark cluster. `local[*]`
keeps the execution path simpler while still exercising Spark, S3A, Parquet
aggregation, small-file handling, and Gold publication.

## 14. Current validation baseline

Relational baseline:

```text
Core smoke tests:         15/15
Reliability smoke tests:  17/17
Silver smoke tests:       26/26
dbt models:               14
dbt data tests:           249
Full dbt build:           257/257 PASS
```

Phase 8 has additionally verified:

```text
Spark / Java / Hadoop runtime:                 PASS
selected Silver signal input:  1,416 files / 116,242,840 rows
Spark S3A reconciliation:      116,242,840/116,242,840 rows
feature transformation:        83,909 rows / 5 partial rows
Gold publication:              5 data files / 5 manifests
Gold read-back validation:     83,909/83,909 rows
full Gold rerun:               0 written / 5 skipped
Gold recovery/fail-closed smoke: PASS
```

## 15. Phase 9 feature integration

Phase 9 keeps the Phase 8 feature grain:

```text
recording_id + channel_id + epoch_number
```

The signal side comes from the five compact Phase 8 Gold Parquet files rather
than from the 116,242,840 Silver sample rows. Warehouse recording/channel
context is resolved by concrete Silver identity. Sleep-stage context is
left-joined by `recording_id + epoch_number`.

Current integrated output:

```text
83,909 total rows
83,384 rows with sleep-stage context
525 rows without sleep-stage context
5 Parquet data files
```

All 525 unlabeled rows belong to the real signal tail of `ST7011J`. No
sleep-stage value is fabricated.

Integrated publication identity includes:

```text
schema_version
feature_version
integration_version
input_recording_id
warehouse_context_sha256
```

The Warehouse fingerprint makes the integrated representation immutable with
respect to both the selected signal representation and the relational context.
Completed exact outputs are skipped. Incomplete prefixes without
`_SUCCESS.json` can be rebuilt; completed invalid prefixes fail closed.

See [`feature_integration.md`](feature_integration.md).

## 16. Next architectural scope

Still outside Phase 9:

- orchestration with Airflow;
- device-event streaming with Kafka;
- broader quality hardening and operational observability;
- dashboards and broader BI access;
- full-source processing.

Those layers should be added only when their upstream datasets, grain, and use
are explicit.
