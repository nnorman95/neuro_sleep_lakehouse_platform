# NeuroSleep Lakehouse Platform

NeuroSleep is a local lakehouse-style data engineering platform built around
sleep-neuroscience data. It ingests source files from PhysioNet, keeps the raw
objects immutable, creates validated Silver Parquet datasets, loads relational
metadata into PostgreSQL, and builds analytical models with dbt.

Phase 8 is complete and released as `v0.5.0-features`. Phase 9 adds
**Feature Integration**: compact Gold signal features are joined to Warehouse
subject, recording, channel, and optional sleep-stage context without
recomputing sample-level signals. Phase 10 adds **Airflow orchestration** around
the existing project entrypoints without moving data-processing logic into DAGs.

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
Gold signal features:           5 recordings / 83,909 rows / 5 Parquet files
Integrated Gold features:       5 recordings / 83,909 rows / 83,384 labeled / 525 unlabeled
Orchestration:                  Airflow 3.3.1 / LocalExecutor / 8-task pipeline DAG
```

The analytical cohort is larger than the full-signal subset on purpose. Phase 7
only needs recording metadata and sleep-stage epochs, so 13 additional recordings
were processed without generating signal Parquet. This expands analytical
coverage without doing expensive work that the current models do not use.

## Architecture

```text
Airflow 3.3.1 / LocalExecutor
neurosleep_lakehouse_pipeline
        |
        | invokes existing project entrypoints
        v
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
        v
Python / edfio / NumPy / PyArrow
parsing + normalization + quality gates
        |
        v
MinIO Silver
versioned Parquet + _SUCCESS.json
        |
        +----------------------------+
        |                            |
        v                            v
PostgreSQL staging              Spark 4.2 + S3A
metadata + epochs              selected signal Parquet
        |                            |
        v                            v
dbt Warehouse Core              MinIO Gold signal_features
        |                       30-second signal features
        |                            |
        +-------------+--------------+
                      |
                      v
              Spark Feature Integration
                      |
                      v
              MinIO Gold integrated_signal_features
              features + Warehouse context

dbt Warehouse Core
        |
        v
dbt Analytics Marts
```

High-volume signal samples stay in MinIO/Parquet. PostgreSQL remains the
relational path for operational metadata, lineage, quality, dimensional models,
and marts. Spark handles the high-volume signal path and the compact
Gold-to-Warehouse feature integration.
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

### Spark / Gold signal features

```text
selected Silver signal files:  1,416
selected Silver signal rows:   116,242,840
Gold feature rows:                  83,909
Gold Parquet data files:                 5
Gold success manifests:                  5
Gold other objects:                      0
```

Spark preserves final partial windows, validates sample counts and timing, and
publishes one compact Gold Parquet file per selected recording. Completed exact
Gold representations are skipped on rerun.

More detail is in
[`docs/spark_signal_features.md`](docs/spark_signal_features.md).

### Phase 9 integrated signal features

```text
Integrated feature rows:        83,909
Rows with sleep-stage label:    83,384
Rows without sleep-stage label:    525
Integrated Parquet data files:       5
```

Every Gold feature row resolves Warehouse recording/channel context. Sleep-stage
context is a left join, so real signal windows without a source hypnogram label
are preserved rather than dropped or assigned an invented stage.

The integrated publication is immutable, includes a deterministic Warehouse
context fingerprint, validates source Gold lineage, and skips completed exact
representations on rerun.

More detail is in
[`docs/feature_integration.md`](docs/feature_integration.md).
### Phase 10 Airflow orchestration

Airflow is a thin control plane over the existing pipeline commands. The DAG does
not reimplement Bronze, Silver, staging, dbt, Spark, or Gold business logic.

```text
extract_bronze
      |
      +----------------------+
      v                      v
build_subject_metadata_    build_recording_silver
silver                      |
      |                     +--------------------+
      v                     v                    v
load_subject_metadata_   load_recording_      build_gold_signal_
staging                  staging              features
      |                     |                    |
      +----------+----------+                    |
                 v                               |
       build_warehouse_and_marts                 |
                 |                               |
                 +---------------+---------------+
                                 v
                 build_integrated_signal_features
```

The DAG uses `schedule=None`, `catchup=False`, `max_active_runs=1`, one retry per
task, and the existing project commands inside the custom
`neurosleep-airflow:phase10` execution image. The local Airflow runtime uses
`LocalExecutor` with project-level parallelism limited to two tasks.

Two consecutive full scheduler-driven runs completed with all 8 tasks successful.
The second run reused existing idempotent outputs instead of duplicating them.

More detail is in
[`docs/airflow_orchestration.md`](docs/airflow_orchestration.md).

## Validation

Current verified regression status:

```text
Core smoke tests:                         15/15
Reliability smoke tests:                  17/17
Silver smoke tests:                       26/26
Python smoke total:                       58/58
dbt project:                              14 models / 249 data tests
Full dbt build:                           257/257 PASS, 0 WARN, 0 ERROR
Spark selected-input reconciliation:      116,242,840/116,242,840 rows
Spark feature validation:                 83,909 rows / 5 partial rows
Gold publication validation:              5/5 recordings / 83,909 rows
Gold full rerun:                           0 written / 5 skipped
Gold recovery/fail-closed smoke:           PASS
Feature integration validation:           83,909 rows / 83,384 labeled / 525 unlabeled
Integrated Gold publication:              5/5 recordings / 83,909 rows
Integrated Gold full rerun:               0 written / 5 skipped
Integrated Gold recovery/fail-closed:     PASS
Airflow runtime image validation:          PASS
Airflow Compose/runtime connectivity:      PASS
Airflow foundation smoke:                  PASS
Pipeline DAG contract:                     8/8 tasks
Full Airflow DAG runs:                      2/2 success
Phase 10 regression:                       PASS
```

The Phase 7 relational baseline also confirms:

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
make airflow-bootstrap
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
make spark-feature-check
make gold-signal-features
make gold-signal-features-check
make gold-reliability-smoke
make feature-integration-check
make integrated-signal-features
make integrated-signal-features-check
make integrated-gold-reliability-smoke
make phase8-check
make phase9-check
make phase10-check
make airflow-bootstrap
make airflow-up
make airflow-down
make airflow-ps
make airflow-smoke
make airflow-password
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
v0.5.0-features
```

Phase 9 extends the released feature layer with Warehouse-aware integrated Gold
data while preserving the reusable Phase 8 signal-feature dataset. Phase 10 is
the current orchestration milestone; it adds a reproducible Airflow runtime and a
thin end-to-end DAG. No Phase 10 release tag has been created yet.
## Documentation

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/data_flow.md`](docs/data_flow.md)
- [`docs/analytics_marts.md`](docs/analytics_marts.md)
- [`docs/spark_signal_features.md`](docs/spark_signal_features.md)
- [`docs/feature_integration.md`](docs/feature_integration.md)
- [`docs/airflow_orchestration.md`](docs/airflow_orchestration.md)
- [`docs/process_optimization.md`](docs/process_optimization.md)
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
