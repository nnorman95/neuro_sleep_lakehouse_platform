# NeuroSleep Lakehouse Platform

NeuroSleep is a local lakehouse-style data engineering platform built around
sleep-neuroscience data. It ingests source files from PhysioNet, keeps the raw
objects immutable, creates validated Silver Parquet datasets, loads relational
metadata into PostgreSQL, and builds analytical models with dbt.

Phase 7 is complete and released as `v0.4.0-analytics`. The current development
branch starts **Phase 8: Spark Signal Features**. Spark is introduced for the
high-volume Silver signal path while PostgreSQL and dbt remain responsible for
relational analytics.

## Current state

```text
Source dataset:                 Sleep-EDF Database Expanded v1.0.0
Source system:                  physionet_sleep_edf
Collections:                    sleep-cassette, sleep-telemetry
Subject metadata:               100 subjects / 197 recording contexts
Analytical cohort:              18 recordings / 9 represented subjects
Staged recording metadata:      110 channels / 3,263 intervals / 35,710 epochs
Warehouse:                      100 subjects / 18 recordings / 110 channels / 35,710 epochs
Analytical marts:               18 summary rows / 126 stage rows / 6 coverage rows
Full-signal subset:             5 recordings / 116,242,840 Silver signal rows
```

The analytical cohort is larger than the full-signal subset on purpose. Phase 7
only needs recording metadata and sleep-stage epochs, so 13 additional recordings
were processed without generating signal Parquet. This expands analytical
coverage without doing expensive work that the current models do not use.

## Architecture

```text
PhysioNet Sleep-EDF
        |
        v
Python Extract
streaming HTTP + source manifest + SHA-256 verification
        |
        v
MinIO Bronze
immutable EDF/XLS source objects
        |
        +--> raw.file_registry
        +--> ops.pipeline_run / ops.file_attempt
        |
        v
Python / edfio / NumPy / PyArrow
parsing + normalization + quality gates
        |
        v
MinIO Silver
versioned Parquet + ZSTD + _SUCCESS.json
        |
        +--> recordings / channels / intervals / epochs
        +--> subjects / recording_contexts
        +--> signal samples for the full-signal subset
        |
        v
PostgreSQL staging
verified relational landing for current Silver publications
        |
        v
dbt Warehouse Core
fail-closed selection + deterministic dimensions + epoch fact
        |
        v
dbt Analytics Marts
recording summary + stage distribution + dataset coverage
```

High-volume signal samples stay in MinIO/Parquet. PostgreSQL is used for
operational metadata, lineage, quality history, staging data, dimensional models,
and relational analytics.

## Engineering decisions

A few design choices are deliberate:

- **Raw data stays immutable.** Bronze objects are checksum-verified and never
  rewritten to hide source problems.
- **Processing is idempotent.** Completed Bronze/Silver objects and unchanged
  staging publications are skipped safely instead of being duplicated.
- **Version selection fails closed.** If more than one compatible current Silver
  representation exists for a logical recording, dbt does not guess which one is
  “latest”. The build is blocked until the ambiguity is resolved.
- **Data-quality failures and runtime failures are separated.** Silver quality-gate
  failures create or refresh a quarantine incident. Network, database, storage,
  and code failures remain operational failures rather than being mislabeled as
  bad data.
- **Large signal data is kept out of PostgreSQL.** The 116M+ signal rows in the
  current full-signal subset remain columnar Parquet in MinIO.
- **Analytics preserve source meaning.** Source `N3` and `N4` stay distinct in
  Silver/Warehouse lineage; analytical marts group both as `N3`. `UNKNOWN` and
  `MOVEMENT` remain explicit.
- **No scientific threshold is invented by the pipeline.** Marts report coverage
  and descriptive sleep metrics, but they do not mark a recording “good”, “bad”,
  or “usable” using an arbitrary cutoff.

## Implemented layers

### Bronze

- official PhysioNet manifest parsing and source selection;
- streaming downloads with retry and interruption cleanup;
- official SHA-256 verification before publication;
- verified-object recovery and idempotent registration;
- advisory locks, heartbeats, per-file attempt history, and reconciliation.

### Silver

- PSG metadata and channel extraction;
- Hypnogram interval parsing and 30-second epoch expansion;
- source-preserving sleep-stage labels and normalized values;
- optional chunked signal extraction;
- explicit PyArrow schemas and Zstandard-compressed Parquet;
- version-aware identity (`source_pair_id`, `input_fingerprint`, `config_id`,
  schema version, transform version);
- atomic publication with payload checksums and `_SUCCESS.json`;
- durable quality history and active quarantine routing for quality-gate errors;
- metadata-only processing mode for analytical cohort expansion.

### PostgreSQL staging

```text
staging.silver_subjects                 100
staging.silver_recording_contexts       197
staging.silver_recordings                18
staging.silver_channels                 110
staging.silver_sleep_stage_intervals   3,263
staging.silver_sleep_stage_epochs     35,710
```

The recording staging loader processes current compatible Silver publications
only. On the cohort expansion run it wrote 13 new publications and skipped the
5 already loaded ones. The immediate rerun skipped all 18 and wrote zero rows.

### Warehouse Core

```text
warehouse.dim_subject          100
warehouse.dim_recording         18
warehouse.dim_channel          110
warehouse.dim_sleep_stage        8
warehouse.fact_sleep_epoch  35,710
```

The Warehouse uses deterministic surrogate keys, explicit model contracts,
relationship tests, grain tests, reconciliation, and fail-closed current-version
selection.

### Analytics Marts

```text
mart.mart_recording_sleep_summary       18 rows
mart.mart_recording_stage_distribution 126 rows
mart.mart_dataset_coverage                6 rows
```

The stage-distribution mart always emits seven analytical stages per recording,
including zero-duration stages. More detail is in
[`docs/analytics_marts.md`](docs/analytics_marts.md).

## Validation

Current verified regression status:

```text
Core smoke tests:         15/15
Reliability smoke tests:  17/17
Silver smoke tests:       26/26
Python smoke total:       58/58
dbt project:              14 models / 249 data tests
Full dbt build:           257/257 PASS, 0 WARN, 0 ERROR
```

The Phase 7 validation also confirmed:

```text
recording summary rows:        18
stage distribution rows:      126
coverage rows:                   6
ST7091J first annotated epoch:   1
ST7161J first annotated epoch:  14
```

Two consecutive full dbt rebuilds produced the same recording-summary content
checksum, providing a direct regression check for deterministic rebuild behavior.

## Local setup

```bash
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make bootstrap
```

The project supports Python 3.11+. The current local development environment uses
Python 3.13.5 and PostgreSQL 18.4 on host port 5433.

## Common commands

```bash
make up
make down
make ps
make buckets
make migrate
make smoke
make reliability-smoke
make silver-smoke
make spark-smoke
make test
make source-check
make psql
./scripts/run_dbt.sh build
```

Main data-flow commands:

```bash
PYTHONPATH=src python scripts/plan_silver_batch.py
PYTHONPATH=src python scripts/run_silver_batch.py
PYTHONPATH=src python scripts/run_silver_subject_metadata.py
PYTHONPATH=src python scripts/load_subject_metadata_staging.py
PYTHONPATH=src python scripts/load_recording_staging.py
```

## Milestones

```text
v0.1.0-bronze
v0.2.0-silver
v0.3.0-warehouse
v0.4.0-analytics
```

Phase 8 development continues on `phase/8-spark-signal-features`.

## Documentation

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/data_flow.md`](docs/data_flow.md)
- [`docs/analytics_marts.md`](docs/analytics_marts.md)
- [`docs/data_model.md`](docs/data_model.md)
- [`docs/database_schemas.md`](docs/database_schemas.md)
- [`docs/data_contracts.md`](docs/data_contracts.md)
- [`docs/quality_rules.md`](docs/quality_rules.md)
- [`docs/access_model.md`](docs/access_model.md)
- [`docs/data_sources.md`](docs/data_sources.md)
- [`docs/storage_layout.md`](docs/storage_layout.md)
- [`docs/local_setup.md`](docs/local_setup.md)
- [`docs/extract_runbook.md`](docs/extract_runbook.md)
- [`docs/edf_inspection.md`](docs/edf_inspection.md)
- [`docs/decisions/001_silver_identity_and_lineage.md`](docs/decisions/001_silver_identity_and_lineage.md)
- [`docs/decisions/002_warehouse_grain_and_version_selection.md`](docs/decisions/002_warehouse_grain_and_version_selection.md)
- [`docs/decisions/003_warehouse_physical_model_and_build_semantics.md`](docs/decisions/003_warehouse_physical_model_and_build_semantics.md)

Real EDF/XLS files, generated Parquet, credentials, and runtime logs are not
committed to Git.
