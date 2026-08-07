# NeuroSleep Lakehouse Platform

NeuroSleep is a local data engineering platform for ingesting, validating,
storing, transforming, and analyzing sleep-neuroscience data.

The active source is **Sleep-EDF Database Expanded v1.0.0** from PhysioNet.
The project contains completed Bronze ingestion and Silver transformation scopes.
The active development stage is **Phase 6: Warehouse Modeling**. The initial
Warehouse Core, dbt transformation/test layer, and Warehouse governance metadata
are implemented for the current production baseline.

## Architecture

```text
PhysioNet Sleep-EDF
        |
        v
Python Extract
manifest parsing + streaming HTTP + SHA-256 verification
        |
        v
MinIO Bronze
immutable source objects
        |
        +--> raw.file_registry
        +--> ops.pipeline_run
        +--> ops.file_attempt
        +--> quality.quarantine_records
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
        +--> high-volume signal samples
        |
        v
PostgreSQL staging
selected Silver relational landing
        |
        v
dbt Warehouse Core
fail-closed selection + dimensions + epoch fact
        |
        v
Mart and Gold outputs
future downstream scope
```

PostgreSQL stores operational metadata, lineage, quality history, staging
records, and the current analytical dimensions and epoch fact. High-volume signal
samples remain in MinIO/Parquet instead of being loaded into PostgreSQL row by row.

## Implemented

### Bronze

- Streaming Sleep-EDF extraction from official PhysioNet manifests.
- Official SHA-256 verification before publication.
- Recoverable downloads, verified-object recovery, retry policies, pipeline
  locks, heartbeats, and per-file attempt history.
- Safe interruption cleanup for `KeyboardInterrupt` and other
  `BaseException` subclasses.
- Bronze reconciliation between MinIO and `raw.file_registry`.
- Structured UTC logging and live download progress.

### Silver recordings

- PSG metadata parsing and Hypnogram interval parsing.
- Source-preserving sleep-stage labels and normalized stage values.
- 30-second epoch expansion.
- Channel metadata and chunked signal extraction.
- Explicit PyArrow schemas and Zstandard-compressed Parquet.
- Version-aware identity using `source_pair_id`, `input_fingerprint`,
  `schema_version`, `transform_version`, and `config_id`.
- Atomic writes, payload checksums, `_SUCCESS.json`, reconciliation,
  partial-output recovery, and idempotent reruns.
- Batch discovery, batch execution, progress reporting, pipeline tracking, and
  durable quality-check history.

### Silver subject metadata

- Normalized `subjects.parquet` and `recording_contexts.parquet` datasets.
- Deterministic `subject_key` values.
- Collection-specific sex-code normalization.
- Recording-level night, treatment, and lights-off context.
- Source-object lineage and idempotent publication.

### PostgreSQL

Implemented schemas:

```text
raw
staging
warehouse
mart
ops
quality
governance
```

Implemented staging tables:

```text
staging.silver_recordings
staging.silver_channels
staging.silver_sleep_stage_intervals
staging.silver_sleep_stage_epochs
staging.silver_subjects
staging.silver_recording_contexts
```

Both production staging paths are implemented. Current PostgreSQL staging
contains 100 subjects, 197 recording contexts, 5 recordings, 33 channels,
834 source annotation intervals, and 12,224 emitted sleep-stage epochs.
High-volume signal samples remain in MinIO. The dbt project builds and validates
five Warehouse Core tables from the selected staging publications.

## Current production coverage

```text
Sleep Cassette recordings: 4
Sleep Telemetry recordings: 1
Total Silver signal rows: 116,242,840
Subjects: 100
Recording contexts: 197
```

The five production recordings are:

```text
SC4001E
SC4002E
SC4011E
SC4012E
ST7011J
```

## Data profiles

Sample mode:

```env
DATA_PROFILE=sample
SLEEP_EDF_MAX_RECORDINGS=4
SLEEP_EDF_INCLUDE_CASSETTE=true
SLEEP_EDF_INCLUDE_TELEMETRY=true
SLEEP_EDF_INCLUDE_METADATA=true
```

Full-source mode:

```env
DATA_PROFILE=full
```

`SLEEP_EDF_MAX_RECORDINGS=0` removes the sample recording limit.

## Local setup

```bash
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make bootstrap
```

The package supports Python 3.11 or newer. The current development environment
uses Python 3.13.5.

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
make test
make source-check
make psql
./scripts/run_dbt.sh build
```

Production-oriented Silver commands:

```bash
PYTHONPATH=src python scripts/plan_silver_batch.py
PYTHONPATH=src python scripts/run_silver_batch.py
PYTHONPATH=src python scripts/run_silver_subject_metadata.py
PYTHONPATH=src python scripts/load_subject_metadata_staging.py
PYTHONPATH=src python scripts/load_recording_staging.py
```

## Validation status

```text
Core smoke tests:         15/15
Reliability smoke tests:  17/17
Silver smoke tests:       24/24
Python smoke total:        56/56
Warehouse dbt build:      201/201
```

## Completed milestones

```text
v0.1.0-bronze
v0.2.0-silver
```

## Current Phase 6 scope

The initial Warehouse Core is implemented:

```text
warehouse.dim_subject          100 rows
warehouse.dim_recording          5 rows
warehouse.dim_channel           33 rows
warehouse.dim_sleep_stage        8 rows
warehouse.fact_sleep_epoch  12,224 rows
```

The dbt layer includes staging source tests, fail-closed metadata/recording
selection, deterministic Warehouse surrogate keys, model contracts,
relationship tests, and source-to-Warehouse reconciliation. Warehouse governance
is also registered through five active v1 YAML contracts and column-level
classification for all 81 Warehouse columns.

`warehouse.fact_signal_quality`, device-event models, marts, and Gold outputs
remain future scope and require explicit upstream datasets and grains.

## Documentation

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/data_sources.md`](docs/data_sources.md)
- [`docs/data_flow.md`](docs/data_flow.md)
- [`docs/storage_layout.md`](docs/storage_layout.md)
- [`docs/database_schemas.md`](docs/database_schemas.md)
- [`docs/data_model.md`](docs/data_model.md)
- [`docs/data_contracts.md`](docs/data_contracts.md)
- [`docs/quality_rules.md`](docs/quality_rules.md)
- [`docs/access_model.md`](docs/access_model.md)
- [`docs/local_setup.md`](docs/local_setup.md)
- [`docs/extract_runbook.md`](docs/extract_runbook.md)
- [`docs/edf_inspection.md`](docs/edf_inspection.md)
- [`docs/decisions/001_silver_identity_and_lineage.md`](docs/decisions/001_silver_identity_and_lineage.md)
- [`docs/decisions/002_warehouse_grain_and_version_selection.md`](docs/decisions/002_warehouse_grain_and_version_selection.md)

Real EDF files, generated Parquet objects, credentials, and runtime logs are not
committed to Git.
