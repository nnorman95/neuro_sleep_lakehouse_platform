# Architecture

This document describes the platform as implemented through **Phase 7: Analytics
Marts**. Bronze, Silver, PostgreSQL staging, and the Warehouse Core are already in
place; Phase 7 adds a small relational analytics layer on top of the trusted
epoch fact.

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
Gold        future curated signal/ML features
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

## 12. Scale boundary

PostgreSQL is used for:

- operational metadata and lineage;
- quality and quarantine metadata;
- subject/recording/channel/interval/epoch staging data;
- dimensional models;
- low-volume relational marts.

PostgreSQL is **not** used as row-by-row storage for raw signal samples. The
current 116M+ signal rows remain in Parquet in MinIO. Later signal features can
be computed with a distributed/high-volume engine and only loaded into
PostgreSQL when their grain and use are clear.

## 13. Current validation baseline

```text
Core smoke tests:         15/15
Reliability smoke tests:  17/17
Silver smoke tests:       26/26
Python smoke total:       58/58
dbt models:               14
dbt data tests:           249
Full dbt build:           257/257 PASS
```

Phase 7 validation also confirmed 18 recording summaries, 126 recording-stage
rows, 6 coverage rows, preservation of the non-zero epoch starts in `ST7091J`
and `ST7161J`, and identical recording-summary content across two consecutive
full rebuilds.

## 14. Next architectural scope

Still outside the current implemented analytical path:

- distributed signal-feature processing;
- curated Gold signal/ML features;
- device-event/Kafka models;
- window-level analytical signal-quality facts;
- dashboards and broader BI access;
- full-source processing.

Those layers should be added only when their upstream datasets, grain, and use
are explicit.
