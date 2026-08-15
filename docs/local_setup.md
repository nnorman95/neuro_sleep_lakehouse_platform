# Local Setup

## 1. Requirements

- macOS or another Docker-supported development system;
- Docker Desktop;
- Python 3.11 or newer;
- Java 21;
- Make;
- Git.

The current verified environment uses:

```text
Python 3.13.5
Java 21.0.12
PySpark 4.2.0
Spark 4.2.0
Hadoop 3.5.0
PostgreSQL 18.4
MinIO API port 9000
MinIO console port 9001
PostgreSQL host port 5433
```

## 2. Project Location

Current local path:

```text
/Users/norman/Documents/S/Data Engineering/neuro_sleep_lakehouse_platform
```

Enter the project:

```bash
cd "/Users/norman/Documents/S/Data Engineering/neuro_sleep_lakehouse_platform"
```

## 3. Python Environment

```bash
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Most direct Python commands use:

```bash
PYTHONPATH=src
```

## 4. Start Local Services

```bash
make up
make ps
```

Equivalent Docker commands:

```bash
docker compose up -d postgres minio
docker compose ps
```

Required MinIO buckets:

```text
bronze
silver
gold
quarantine
logs
```

Initialize them with:

```bash
make buckets
```

## 5. Database Initialization

Run all migrations and idempotent seeds registered in the manifest:

```bash
make migrate
```

Open PostgreSQL:

```bash
make psql
```

Exit with:

```text
\q
```

## 6. Bootstrap

For a new local environment:

```bash
make bootstrap
```

Bootstrap starts PostgreSQL and MinIO, initializes buckets, runs SQL migrations
and seeds, and runs the core smoke suite.

## 7. Source Configuration

Sample profile:

```env
ACTIVE_SOURCE=sleep_edf
DATA_PROFILE=sample
SLEEP_EDF_VERSION=1.0.0
SLEEP_EDF_MAX_RECORDINGS=4
SLEEP_EDF_INCLUDE_CASSETTE=true
SLEEP_EDF_INCLUDE_TELEMETRY=true
SLEEP_EDF_INCLUDE_METADATA=true
```

Full profile:

```env
DATA_PROFILE=full
```

## 8. Validation

Common suites:

```bash
make smoke
make reliability-smoke
make silver-smoke
make spark-smoke
make gold-reliability-smoke
make integrated-gold-reliability-smoke
make test
```

High-volume feature and Gold checks are explicit:

```bash
make spark-feature-check
make gold-signal-features-check
make feature-integration-check
make integrated-signal-features-check
```

Complete milestone regressions:

```bash
make phase8-check
make phase9-check
```

`phase9-check` runs the normal smoke suites, full Spark feature validation,
source Gold validation, feature integration validation, integrated Gold
validation, and a full dbt build.
## 9. Run Extract

Example one-recording Extract:

```bash
SLEEP_EDF_MAX_RECORDINGS=1 PYTHONPATH=src python -m neuro_sleep.ingestion.sleep_edf_extract
```

See [`extract_runbook.md`](extract_runbook.md) before production-like reruns or
manual recovery.

## 10. Run Silver

Plan current recording batch:

```bash
PYTHONPATH=src python scripts/plan_silver_batch.py
```

Run current recording batch:

```bash
PYTHONPATH=src python scripts/run_silver_batch.py
```

Run subject metadata:

```bash
PYTHONPATH=src python scripts/run_silver_subject_metadata.py
```

Load the current subject metadata publication into PostgreSQL staging:

```bash
PYTHONPATH=src python scripts/load_subject_metadata_staging.py
```

The first subject-metadata staging load writes 100 subjects and 197 recording
contexts. An unchanged rerun returns `skipped` with zero rows written.

Load current recording metadata into PostgreSQL staging:

```bash
PYTHONPATH=src python scripts/load_recording_staging.py
```

The current recording staging state contains 18 recordings, 110 channels,
3,263 annotation intervals, and 35,710 epochs. It processes only the four
metadata Parquet datasets for each current publication; signal samples remain
in MinIO. An unchanged rerun returns `skipped`.

Completed Silver production outputs should also return `skipped` on an
unchanged rerun.

## 11. Run Spark and Gold

Spark uses the local Java runtime plus PySpark from the project virtual
environment. The S3A dependency is resolved through the Spark package argument
embedded in the project run scripts.

Check runtime and exact Silver input reconciliation:

```bash
make spark-smoke
```

Validate feature transformation over the current selected signal set:

```bash
make spark-feature-check
```

Build missing Phase 8 Gold signal-feature publications or skip completed ones:

```bash
make gold-signal-features
```

Validate Phase 8 Gold outputs:

```bash
make gold-signal-features-check
```

Test Phase 8 Gold recovery and fail-closed behavior:

```bash
make gold-reliability-smoke
```

Validate Gold-to-Warehouse integration without publishing:

```bash
make feature-integration-check
```

Build or safely skip integrated Gold publications:

```bash
make integrated-signal-features
```

Validate completed integrated Gold publications:

```bash
make integrated-signal-features-check
```

Test integrated Gold recovery and fail-closed behavior:

```bash
make integrated-gold-reliability-smoke
```

See [`spark_signal_features.md`](spark_signal_features.md) for the Phase 8
feature path and [`feature_integration.md`](feature_integration.md) for Phase 9
join semantics, lineage, and publication behavior.
## 12. Stop Services


```bash
make down
```

## 13. Git Safety

Before every commit:

```bash
git status --short --branch
git diff --check
```

For staged changes:

```bash
git diff --cached --check
git --no-pager diff --cached --stat
```

Do not commit `.env`, source EDF/XLS files, generated Parquet, logs, or temporary
runtime files.
