# NeuroSleep Lakehouse Platform

NeuroSleep is a local data engineering platform for ingesting, validating,
storing, transforming, and analyzing sleep neuroscience data.

The active source is **Sleep-EDF Database Expanded v1.0.0** from PhysioNet.

## Current architecture

```text
PhysioNet Sleep-EDF
        |
        v
Python Extract
streaming HTTP + SHA-256 verification
        |
        v
MinIO Bronze
        |
        +--> raw.file_registry
        +--> ops.pipeline_run
        +--> ops.file_attempt
        +--> quality.quarantine_records
        |
        v
Python / edfio / NumPy / PyArrow
quality gate + chunked signal extraction
        |
        v
MinIO Silver
Parquet + versioned prefixes + _SUCCESS.json
        |
        +--> metadata/epochs -> PostgreSQL staging -> dbt/warehouse later
        |
        +--> high-volume signals -> feature processing/Gold later
```

## Implemented

- PostgreSQL 18 and MinIO through Docker Compose.
- UUIDv7 identifiers.
- SQL migrations and idempotent governance seeds.
- Streaming Sleep-EDF Extract with official SHA-256 verification.
- Recoverable Bronze ingestion, locking, heartbeat, per-file attempt history,
  structured logs, and Bronze reconciliation.
- EDF schema inspection for four PSG/Hypnogram sample pairs.
- Silver recordings, channels, source annotation intervals, 30-second epochs,
  and chunked signal samples.
- Explicit PyArrow schemas, Parquet output, quality gates, idempotent writes,
  `_SUCCESS.json`, payload checksums, and reconciliation.
- A successful full persistent Silver run for `SC4001E0` with more than
  24 million rows.
- Initial PostgreSQL `staging.silver_*` landing tables.

Real EDF and generated Parquet files are never committed to Git.

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

## Local setup

```bash
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make bootstrap
```

## Common commands

```bash
make up
make ps
make migrate
make smoke
make reliability-smoke
make silver-smoke
make test
make source-check
make psql
```

`make smoke` is the core platform suite. `make reliability-smoke` covers
retry/failure/recovery behavior. `make silver-smoke` covers the Silver layer.
`make test` runs all three suites.

## Current milestone

Completed milestones:

```text
v0.1.0-bronze
v0.2.0-silver
```

The current branch is stabilizing Silver identity, lineage, operational
reliability, sample coverage, and PostgreSQL staging before warehouse/dbt
models are built.

The `staging.silver_*` DDL exists, but the production Silver-to-staging loader
is intentionally not enabled until the version/lineage design is finalized.
