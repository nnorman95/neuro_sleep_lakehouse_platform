# NeuroSleep Lakehouse Platform

NeuroSleep is a local data engineering platform for processing and analyzing
sleep-study data from PhysioNet.

It takes raw EDF/XLS source files through a complete lakehouse-style pipeline:
immutable ingestion, validated Parquet datasets, PostgreSQL staging and
Warehouse models, dbt analytics, Spark feature engineering, Airflow
orchestration, Kafka streaming, data-quality controls, and operational recovery.

The project is designed to be reproducible on a fresh machine and safe to rerun.
Large signal data stays in object storage, relational metadata stays in
PostgreSQL, and every major processing boundary is validated before data moves
forward.

## What the platform does

```text
PhysioNet Sleep-EDF
        |
        v
   Bronze / MinIO
 immutable source files
        |
        v
   Silver / Parquet
        |
        +---------------------------+
        | metadata + epochs         | signal samples
        v                           v
PostgreSQL staging               Spark
        |                           |
        v                           v
   dbt Warehouse             Gold signal features
        |                           |
        +--> Analytics marts        |
        |                           |
        +------ Warehouse context --+
                                    |
                                    v
                             Integrated Gold


Simulated BCI events
        |
        v
      Kafka
        |
        v
 validated consumer
   |              |
invalid          valid
   |              |
   v              v
quarantine   PostgreSQL inbox
                  |
                  v
          Warehouse event fact
```

Airflow orchestrates the batch/lakehouse path by calling the same project
commands that can be run manually. Kafka is a separate streaming path.

## Data flow in plain language

1. **Download and verify the source data.**
   Sleep-EDF files are fetched from PhysioNet, checked against the official
   manifest and SHA-256 checksums, and stored unchanged in MinIO.

2. **Turn raw files into structured Silver datasets.**
   Python parses subject metadata, PSG recordings, channels, hypnograms, and
   sleep-stage epochs. The results are written as versioned Parquet
   publications with explicit schemas and success manifests.

3. **Load relational data into PostgreSQL.**
   Current compatible Silver publications are loaded into staging tables with
   lineage and idempotency checks.

4. **Build the Warehouse and analytical marts with dbt.**
   dbt creates dimensions, facts, and sleep-analysis marts while enforcing
   grain, relationships, accepted values, reconciliation, and current-version
   rules.

5. **Process high-volume signals with Spark.**
   Signal samples stay in Parquet rather than being copied into PostgreSQL.
   Spark creates compact Gold signal features for selected recordings.

6. **Add Warehouse context to Gold features.**
   Signal features are joined with recording, channel, subject, and optional
   sleep-stage context without recomputing the underlying signals.

7. **Orchestrate the batch pipeline with Airflow.**
   The DAG coordinates the existing extraction, Silver, staging, dbt, Spark,
   and integration entrypoints instead of duplicating business logic inside
   Airflow.

8. **Process simulated device events with Kafka.**
   Valid events are durably ingested into PostgreSQL and modeled in the
   Warehouse. Invalid events are quarantined. Offset handling is restart-safe
   and event IDs are deduplicated.

## Current verified data

| Area | Verified state |
| --- | ---: |
| Source dataset | Sleep-EDF Database Expanded v1.0.0 |
| Subjects | 100 |
| Recording contexts | 197 |
| Analytical cohort | 18 recordings / 9 represented subjects |
| Staged channels | 110 |
| Sleep-stage intervals | 3,263 |
| Sleep epochs | 35,710 |
| Silver signal subset | 5 recordings / 116,242,840 rows |
| Gold signal features | 83,909 rows |
| Integrated Gold features | 83,909 rows |
| Integrated rows with sleep-stage labels | 83,384 |
| Integrated rows without a source label | 525 |
| dbt project | 15 models / 292 data tests |
| Airflow pipeline | 8 tasks |
| Kafka device-event topic | 3 partitions |

The analytical cohort is intentionally larger than the full-signal subset.
Metadata and sleep-stage analytics can be expanded without generating expensive
signal Parquet for recordings that current signal models do not use.

## Core engineering decisions

### Keep raw data immutable

Bronze objects are checksum-verified and never rewritten to hide source
problems. If source data is wrong, the pipeline records the failure instead of
silently changing the input.

### Make reruns safe

Completed Bronze, Silver, staging, Gold, and integrated publications can be
detected and skipped. Re-running the same work does not create duplicate
outputs.

### Fail closed when identity is ambiguous

If more than one compatible Silver representation exists for the same logical
recording, the Warehouse build does not guess which one is current. The build
stops until the ambiguity is resolved.

### Separate bad data from broken infrastructure

Schema, manifest, and publication-quality failures can create quarantine
incidents. Network, database, object-storage, or code failures remain
operational failures rather than being mislabeled as data-quality problems.

### Keep large signals out of PostgreSQL

The current signal subset contains more than 116 million Silver rows. Those
samples remain columnar Parquet in MinIO, while PostgreSQL holds relational
metadata, lineage, operational state, quality records, Warehouse models, and
analytical marts.

### Preserve source meaning

Source `N3` and `N4` remain distinct in Silver/Warehouse lineage. Analytical
marts may group them as `N3`, but the original meaning is still recoverable.
`UNKNOWN` and `MOVEMENT` also remain explicit.

### Do not invent scientific rules

The pipeline reports descriptive metrics and coverage. It does not label a
recording “good”, “bad”, or “usable” using an arbitrary scientific threshold.

## Main components

| Component | Role |
| --- | --- |
| **Python** | extraction, parsing, validation, staging loaders, operational tooling |
| **MinIO** | Bronze, Silver, and Gold object storage |
| **Parquet / PyArrow** | versioned columnar datasets |
| **PostgreSQL** | staging, Warehouse, marts, lineage, quality and operational state |
| **dbt** | dimensional models, marts, contracts and data tests |
| **Spark** | high-volume signal processing and Gold feature integration |
| **Airflow** | batch/lakehouse orchestration |
| **Kafka** | simulated device-event streaming path |
| **Docker Compose** | reproducible local platform runtime |
| **GitHub Actions** | lightweight repository-contract CI |

## Storage and modeling boundaries

```text
Bronze
  raw EDF/XLS objects
  immutable and checksum-verified

Silver
  versioned Parquet publications
  metadata, epochs and optional signal samples

PostgreSQL staging
  current compatible Silver metadata

Warehouse
  dimensions and facts with deterministic keys and lineage

Marts
  recording summaries, stage distributions and dataset coverage

Gold
  compact Spark signal features

Integrated Gold
  Gold features enriched with Warehouse context
```

This split is deliberate: object storage handles high-volume files and signals,
while PostgreSQL handles relational modeling and operational state.

## Quick start

### 1. Clone the repository

```bash
git clone https://github.com/nnorman95/neuro_sleep_lakehouse_platform.git
cd neuro_sleep_lakehouse_platform
```

### 2. Check local prerequisites

```bash
make doctor
```

`make doctor` is read-only. It verifies the required host tools and project
contracts before anything is created.

### 3. Bootstrap the local platform

```bash
make bootstrap
```

Bootstrap safely initializes the local environment, Python virtual environment,
PostgreSQL, MinIO, Kafka, and Airflow, then verifies platform readiness.

It is designed to be rerunnable.

### 4. Run the compact demo

```bash
make demo
```

The demo runs one recording through the main local path:

```text
source
  -> Bronze
  -> Silver
  -> PostgreSQL staging
  -> dbt Warehouse + marts
  -> Gold signal features
```

### 5. Check platform health

```bash
make platform-status
```

## Normal local operation

```bash
make platform-up
make platform-status

# work with the platform

make platform-down
```

`make platform-down` stops the services without deleting persistent Docker
volumes.

## Recovery and operations

### Operational health

```bash
make ops-status
```

The health report separates historical failures from current incidents and
checks for stale pipeline runs, orphaned file attempts, active quarantine
records, and recent failures.

### Targeted recording backfill

```bash
make backfill RECORDING_KEY=SC4001E
```

The backfill path reprocesses one existing Silver recording through the
relational and Gold layers instead of rebuilding the entire dataset.

### SQL migration history

```bash
make migration-history-check
```

Applied SQL migrations and seeds are tracked with SHA-256 checksums. Editing an
already-applied migration is blocked; schema changes must be added as a new
numbered SQL file.

## Validation

### Fast repository checks

```bash
make ci-check
```

This is the same lightweight contract suite used by GitHub Actions. It checks:

- Python and dependency contracts;
- environment/configuration contracts;
- SQL migration manifest consistency;
- Python compilation;
- shell syntax;
- recording-scope regression behavior;
- repository hygiene.

It intentionally avoids requiring the full Docker/Spark/Airflow runtime.

### Full release-boundary audit

```bash
make phase13-check
```

This is the broad local operational gate. It validates prerequisites, CI
contracts, platform readiness, migration history, a compact end-to-end demo,
targeted recovery, operational health, and predecessor regressions.

Current verified status:

```text
Python smoke tests:                    58/58
dbt build:                            301/301 PASS
dbt project:                           15 models / 292 data tests
Gold publication validation:           5/5 recordings
Integrated Gold validation:            5/5 recordings
Full Airflow DAG runs:                  2/2 successful
Kafka audit:                            PASS
Data-quality hardening audit:           PASS
SQL migration history baseline:        46/46 registered
Lightweight GitHub Actions:             PASS
Operational hardening audit:            PASS
```

## Useful commands

For normal use, the main commands are:

```bash
make help
make doctor
make bootstrap
make demo
make platform-up
make platform-status
make platform-down
make ops-status
make backfill RECORDING_KEY=SC4001E
make ci-check
make phase13-check
```

Lower-level development and validation commands remain available through:

```bash
make help
```

This keeps the README focused on the normal workflow instead of duplicating the
entire Makefile interface.

## Repository documentation

Start here:

- [`docs/architecture.md`](docs/architecture.md) — components, boundaries, and system design;
- [`docs/data_flow.md`](docs/data_flow.md) — end-to-end batch, Gold, Kafka, and recovery flows;
- [`docs/local_setup.md`](docs/local_setup.md) — setup, bootstrap, daily operation, and troubleshooting;
- [`docs/process_optimization.md`](docs/process_optimization.md) — how repeated work and operational complexity are reduced.

Data model and governance:

- [`docs/data_model.md`](docs/data_model.md)
- [`docs/database_schemas.md`](docs/database_schemas.md)
- [`docs/data_contracts.md`](docs/data_contracts.md)
- [`docs/quality_rules.md`](docs/quality_rules.md)
- [`docs/access_model.md`](docs/access_model.md)

Pipeline details:

- [`docs/data_sources.md`](docs/data_sources.md)
- [`docs/extract_runbook.md`](docs/extract_runbook.md)
- [`docs/edf_inspection.md`](docs/edf_inspection.md)
- [`docs/storage_layout.md`](docs/storage_layout.md)
- [`docs/analytics_marts.md`](docs/analytics_marts.md)
- [`docs/spark_signal_features.md`](docs/spark_signal_features.md)
- [`docs/feature_integration.md`](docs/feature_integration.md)
- [`docs/airflow_orchestration.md`](docs/airflow_orchestration.md)
- [`docs/kafka_device_events.md`](docs/kafka_device_events.md)
- [`docs/data_quality_hardening.md`](docs/data_quality_hardening.md)

Architecture decisions:

- [`docs/decisions/001_silver_identity_and_lineage.md`](docs/decisions/001_silver_identity_and_lineage.md)
- [`docs/decisions/002_warehouse_grain_and_version_selection.md`](docs/decisions/002_warehouse_grain_and_version_selection.md)
- [`docs/decisions/003_warehouse_physical_model_and_build_semantics.md`](docs/decisions/003_warehouse_physical_model_and_build_semantics.md)

## Release

Latest published release: **v1.0.0**.

The README describes the current platform itself rather than the order in which
it was developed. Release history remains available through GitHub tags and
releases.

## Repository hygiene

Real EDF/XLS source files, generated Parquet data, credentials, local
environment files, and runtime logs are not committed to Git.
