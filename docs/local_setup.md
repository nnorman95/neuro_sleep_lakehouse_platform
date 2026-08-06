# Local Setup

## 1. Requirements

- macOS or another Docker-supported development system;
- Docker Desktop;
- Python 3.11 or newer;
- Make;
- Git.

The current verified environment uses:

```text
Python 3.13.5
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

```bash
make smoke
make reliability-smoke
make silver-smoke
make test
```

`make test` runs all three suites. Current verified result:

```text
12/12 core
17/17 reliability
24/24 Silver
53/53 total
```

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

Completed production outputs should return `skipped` on an unchanged rerun.

## 11. Stop Services

```bash
make down
```

## 12. Git Safety

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
