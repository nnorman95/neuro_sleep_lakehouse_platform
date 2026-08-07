# Architecture

This document describes the implemented platform architecture and the boundary
of **Phase 6: Warehouse Modeling**.

## 1. Active Source

```text
Dataset: Sleep-EDF Database Expanded
Version: 1.0.0
Source system: physionet_sleep_edf
Collections: sleep-cassette, sleep-telemetry
External access model: open
Internal patient-level access policy: restricted
```

The dataset is publicly downloadable, but subject-level sleep data and
quasi-identifying metadata are not treated as unrestricted inside the platform.

## 2. Architectural Layers

The project uses two related vocabularies.

### Object-storage layers

```text
Bronze      immutable source objects
Silver      validated and versioned Parquet datasets
Gold        future curated analytical and ML-ready Parquet
Quarantine  rejected or diagnostic payloads
Logs        persisted log artifacts when needed
```

### PostgreSQL schemas

```text
raw         source-object registry and ingestion metadata
staging     relational landing area for selected Silver datasets
warehouse   dimensional analytical models
mart        consumption-ready relational models
ops         pipeline execution and file-attempt history
quality     quarantine and durable quality history
governance  source registry, contracts, and classifications
```

MinIO `bronze`, `silver`, and `gold` are not synonyms for PostgreSQL `raw`,
`staging`, `warehouse`, and `mart`.

## 3. Implemented End-to-End Flow

```text
PhysioNet
  -> RECORDS and SHA256SUMS.txt
  -> safe manifest parsing and source selection
  -> streaming Python Extract
  -> official SHA-256 verification
  -> MinIO Bronze
  -> raw.file_registry
  -> Python / edfio / NumPy / PyArrow
  -> Silver quality gate
  -> versioned Parquet in MinIO Silver
  -> _SUCCESS.json
  -> reconciliation and durable quality history
```

Operational execution is tracked through:

```text
ops.pipeline_run
ops.file_attempt
quality.quality_check_results
```

## 4. Bronze Architecture

Bronze preserves source-relative object keys under:

```text
bronze/physionet/sleep-edfx/1.0.0/
```

Implemented reliability behavior includes:

- retryable HTTP and object-storage operations;
- official checksum validation;
- verified-object recovery;
- idempotent file registration;
- PostgreSQL advisory locks;
- pipeline heartbeats;
- safe cleanup after failures and user interruption;
- removal of unfinished `.part` files;
- MinIO/PostgreSQL reconciliation.

Bronze objects are never rewritten to “clean” source data.

## 5. Silver Recording Architecture

Each PSG/Hypnogram pair is parsed into:

```text
recordings
channels
sleep_stage_intervals
sleep_stage_epochs
signals
```

The first four are low-volume metadata or analytical rows. `signals` contains
high-volume sample-level data and remains in object storage.

Silver publication provides:

- explicit Arrow schemas;
- Parquet with Zstandard compression;
- source-preserving stage labels;
- normalized stage values;
- quality errors that block publication;
- warnings that remain visible but can permit publication;
- payload checksums;
- atomic local writes and verified uploads;
- versioned output prefixes;
- `_SUCCESS.json` manifests;
- partial-output recovery;
- idempotent reruns;
- reconciliation.

## 6. Silver Subject-Metadata Architecture

The metadata pipeline reads:

```text
SC-subjects.xls
ST-subjects.xls
```

It publishes:

```text
subjects.parquet
recording_contexts.parquet
_SUCCESS.json
```

`subjects` contains person-level attributes and deterministic `subject_key`
values. `recording_contexts` contains recording-level night, treatment, and
lights-off information.

The two concerns remain separate so recording-specific context is not duplicated
on every subject row.

## 7. Identity and Lineage

The platform separates logical identity from materialized Silver identity.

```text
recording_key
  logical Sleep-EDF recording/night

source_pair_id
  SHA-256 identity of PSG/Hypnogram object locations

input_fingerprint
  SHA-256 identity of verified input bytes

config_id
  SHA-256 identity of the Silver transform configuration

recording_id
  UUIDv7 of one concrete materialized Silver representation
```

A logical `recording_key` can have more than one valid `recording_id` over time
when input bytes, schema, transform version, or configuration changes.

The accepted decision is recorded in
[`decisions/001_silver_identity_and_lineage.md`](decisions/001_silver_identity_and_lineage.md).

## 8. Current PostgreSQL Analytical Path

Implemented staging tables:

```text
staging.silver_recordings
staging.silver_channels
staging.silver_sleep_stage_intervals
staging.silver_sleep_stage_epochs
staging.silver_subjects
staging.silver_recording_contexts
```

Migration `025_correct_staging_silver_identity_and_lineage.sql` has already
applied the accepted version-aware identity and lineage design.

The production subject-metadata staging loader validates the current
`_SUCCESS.json`, object inventory, file sizes, SHA-256 checksums, Parquet
schemas, publication identity, and subject relationships. It loads both
subject datasets in one PostgreSQL transaction and records the execution in
`ops.pipeline_run`.

Current staged subject metadata:

```text
staging.silver_subjects: 100 rows
staging.silver_recording_contexts: 197 rows
orphan recording contexts: 0
```

An unchanged subject-metadata rerun is tracked as `skipped` and writes no
duplicate rows.

The recording staging loader selects only current compatible Silver
publications, validates manifests, object sizes, SHA-256 checksums, exact
Parquet schemas, logical recording identity, and parent/child relationships,
and writes the four relational datasets transactionally.

Current staged recording metadata:

```text
staging.silver_recordings: 5 rows
staging.silver_channels: 33 rows
staging.silver_sleep_stage_intervals: 834 rows
staging.silver_sleep_stage_epochs: 12,224 rows
orphan recording rows: 0
unresolved recording contexts: 0
legacy Silver recording versions loaded: 0
```

An unchanged recording rerun is also tracked as `skipped`. The relational
staging path required by the initial Warehouse Core is now complete.

The Warehouse Core can now be built:

```text
warehouse.dim_subject
warehouse.dim_recording
warehouse.dim_channel
warehouse.dim_sleep_stage
warehouse.fact_sleep_epoch
```

A dbt project may be introduced after the physical staging grain and loaders are
stable. It must implement actual transformation and testing needs, not exist only
as a decorative tool.

## 9. Scale Boundary

PostgreSQL should store:

- subjects and recording contexts;
- recording, channel, interval, and epoch metadata;
- dimensional models and analytical facts;
- quality results and summaries;
- source and object pointers.

PostgreSQL should not store every raw signal sample. The current five production
recordings already contain 116,255,936 Silver signal rows, which remain better
suited to Parquet in MinIO.

## 10. Current Production Baseline

```text
Sleep Cassette recordings: 4
Sleep Telemetry recordings: 1
Production signal rows: 116,255,936
Subjects: 100
Recording contexts: 197
Smoke tests: 56/56
```

Completed Bronze and Silver behavior must not be rebuilt during Warehouse
Modeling unless a regression is found.

## 11. Future Scope

The following remain outside the initial Warehouse Core:

- window-level analytical signal-quality facts;
- device-event and Kafka models;
- curated Gold features;
- final marts;
- dashboards;
- full-source production processing.

These may be added only after their upstream datasets and grains are defined.
