# Data Flow

This document describes the implemented Bronze, Silver, staging, and Phase 6
Warehouse analytical flow.

## 1. Extract and Bronze

```text
Sleep-EDF / PhysioNet
  -> RECORDS + SHA256SUMS.txt
  -> manifest parsing
  -> sample/full source selection
  -> streaming HTTP download
  -> official SHA-256 verification
  -> MinIO Bronze
  -> raw.file_registry
  -> ops.pipeline_run
  -> ops.file_attempt
```

Existing verified objects are skipped or recovered instead of being downloaded
blindly. Bronze reconciliation compares MinIO object state with PostgreSQL
registry state.

On failure or user interruption, the pipeline finalizes run and attempt status,
stops the heartbeat, releases the advisory lock, closes network/storage
resources, and removes unfinished `.part` files.

## 2. Bronze to Silver Recording Flow

```text
Bronze PSG EDF + Hypnogram EDF
  -> complete-pair discovery by recording_key
  -> verified source-lineage resolution
  -> edfio parsing
  -> recording and channel metadata
  -> source annotation intervals
  -> 30-second epoch expansion
  -> chunked signal extraction
  -> Silver quality gate
  -> PyArrow tables
  -> Parquet + ZSTD
  -> verified MinIO upload
  -> _SUCCESS.json
  -> reconciliation
  -> durable quality history
```

The recording output is versioned by:

```text
schema_version
transform_version
source_pair_id
input_fingerprint
config_id
```

A completed matching output is skipped on rerun. An incomplete prefix without a
valid success manifest is cleaned and rebuilt.

## 3. Bronze to Silver Subject-Metadata Flow

```text
SC-subjects.xls + ST-subjects.xls
  -> Bronze registry lookup
  -> object download and checksum verification
  -> collection-specific parsing
  -> demographic normalization
  -> deterministic subject_key generation
  -> recording_key and subject_key reconciliation
  -> subjects.parquet
  -> recording_contexts.parquet
  -> _SUCCESS.json
```

The production publication contains:

```text
100 subjects
197 recording contexts
```

A completed matching metadata publication is skipped on rerun.

## 4. Sleep-Stage Semantics

Silver preserves source Stage 3 and Stage 4 separately:

```text
Sleep stage 3 -> N3
Sleep stage 4 -> N4
```

A later analytical layer may map both values to analytical `N3`, but the
source-preserving values remain available.

`UNKNOWN` and `MOVEMENT` remain explicit and must not silently become ordinary
sleep stages.

## 5. Cassette Coverage Semantics

For the inspected Sleep Cassette recordings, Hypnogram coverage can extend past
the PSG end.

Silver:

- preserves the source interval and overhang metric;
- emits only real 30-second epochs inside the PSG timeline;
- records out-of-range epoch counts;
- prevents out-of-range annotation rows from being joined to signal samples.

## 6. Telemetry Coverage Semantics

Sleep Telemetry may have:

- a recording duration not aligned exactly to 30 seconds;
- a PSG tail without source annotation coverage.

These are warnings when the real annotation-derived epochs remain within PSG
coverage. A real epoch extending past the PSG boundary is an error.

The complete PSG signal remains in Silver even when the final signal tail has no
annotation-derived epoch.

## 7. Current PostgreSQL Staging Flow

Physical staging DDL currently exists for:

```text
staging.silver_recordings
staging.silver_channels
staging.silver_sleep_stage_intervals
staging.silver_sleep_stage_epochs
staging.silver_subjects
staging.silver_recording_contexts
```

Migration `025` finalized version-aware recording identity and lineage.
Migration `036` added explicit `dataset_version`, `collection`, and
`recording_key` columns for canonical logical recording reconciliation.

The subject-metadata staging path is implemented:

```text
subjects.parquet + recording_contexts.parquet + _SUCCESS.json
  -> publication identity validation
  -> object-size and SHA-256 verification
  -> exact Parquet-schema validation
  -> one PostgreSQL transaction
  -> staging.silver_subjects: 100 rows
  -> staging.silver_recording_contexts: 197 rows
  -> ops.pipeline_run
```

An unchanged rerun is tracked as `skipped` and writes zero rows.

The recording staging path is also implemented:

```text
5 current Silver recording publications
  -> exclude legacy/incompatible publication versions
  -> validate _SUCCESS.json and canonical logical identity
  -> verify object sizes + SHA-256
  -> exact Parquet-schema and row-count validation
  -> PostgreSQL transaction
  -> staging.silver_recordings: 5 rows
  -> staging.silver_channels: 33 rows
  -> staging.silver_sleep_stage_intervals: 834 rows
  -> staging.silver_sleep_stage_epochs: 12,224 rows
  -> ops.pipeline_run
```

Signal Parquet objects are not downloaded by the staging loader. An unchanged
recording rerun is tracked as `skipped` and writes zero rows.

## 8. Implemented Phase 6 Analytical Flow

```text
MinIO Silver metadata and epochs
  -> manifest validation
  -> tracked staging-load pipeline run
  -> idempotent PostgreSQL staging load
  -> dbt staging source tests
  -> fail-closed metadata publication / recording representation selection
  -> deterministic Warehouse dimensions and sleep-epoch fact
  -> dbt model contracts + grain + relationship + reconciliation tests
  -> marts and Gold only after explicit downstream grains are defined
```

Both required production staging paths are implemented:

```text
staging.silver_subjects
staging.silver_recording_contexts
staging.silver_recordings
staging.silver_channels
staging.silver_sleep_stage_intervals
staging.silver_sleep_stage_epochs
```

The Warehouse Core implementation and its transformation tests are complete for
the current production baseline.

Current Warehouse Core:

```text
warehouse.dim_subject          100 rows
warehouse.dim_recording          5 rows
warehouse.dim_channel           33 rows
warehouse.dim_sleep_stage        8 rows
warehouse.fact_sleep_epoch  12,224 rows
```

The full dbt build currently passes 201/201 models/tests combined. High-volume
signal samples never enter this relational path.

## 9. Scale Boundary

Low-volume metadata and epochs may be loaded into PostgreSQL. The
116,242,840 production signal rows remain in MinIO/Parquet.

Future signal features may be written to Gold and loaded selectively into
PostgreSQL only when their grain and analytical use are defined.

## 10. Lineage Boundary

Warehouse surrogate keys do not replace lineage. Warehouse rows must retain or
be traceable to:

```text
subject_key
recording_key
recording_id
source_pair_id
input_fingerprint
schema_version
transform_version
config_id
source file IDs
Silver bucket and output prefix
pipeline run IDs
```
