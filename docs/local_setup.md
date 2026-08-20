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
Airflow 3.3.1 (custom local execution image)
Airflow API/UI port 8080
Kafka 4.3.1 / KRaft
Kafka host port 9092
confluent-kafka 2.15.0
```

## 2. Fresh Checkout

Clone the repository and enter it:

```bash
git clone https://github.com/nnorman95/neuro_sleep_lakehouse_platform.git
cd neuro_sleep_lakehouse_platform
```

Do not copy `.env.example` manually and do not create `.venv` manually for the
normal setup path. The project bootstrap owns those steps so that the same setup
logic is used on every machine.

## 3. Check Host Prerequisites

Run the read-only doctor before bootstrap:

```bash
make doctor
```

It checks Git, Make, curl, Docker, Docker Compose v2, Python 3.11+, Java 21,
required repository files, Compose resolution, and the Python dependency
contract.

The command does not create containers, files, databases, or buckets.

## 4. Complete First-Time Bootstrap

Run:

```bash
make bootstrap
```

The complete bootstrap is rerunnable and performs the local initialization in a
controlled order:

```text
host prerequisite checks
        |
        v
safe .env initialization
        |
        v
reproducible .venv + project dependencies
        |
        v
PostgreSQL + MinIO + Kafka
        |
        v
MinIO buckets + SQL migrations/seeds + core smoke tests
        |
        v
Kafka topic initialization
        |
        v
Airflow metadata DB + runtime image + migrations
        |
        v
Airflow scheduler + DAG processor + API server
        |
        v
full platform readiness check
```

On an existing environment, completed setup work is reused where safe. For
example, an existing Airflow runtime image is not rebuilt on every bootstrap.

The bootstrap creates `.env` safely from `.env.example` when `.env` does not
exist. Existing configured `.env` files are preserved. Existing files with
missing, empty, or placeholder credentials fail closed instead of being silently
rewritten.

The Python environment is also managed by bootstrap. It creates `.venv` when
needed, installs the project in editable mode, validates the dependency contract,
and skips dependency installation while the dependency fingerprint is unchanged.

## 5. Run the Compact End-to-End Demo

After bootstrap:

```bash
make demo
```

The demo uses one deterministic Sleep-EDF recording (`SC4001E`) and runs:

```text
PhysioNet
  -> Bronze
  -> Silver metadata + signals
  -> PostgreSQL staging
  -> dbt Warehouse + marts
  -> Spark Gold signal features
  -> Gold validation
```

The demo intentionally avoids the full high-volume dataset. Existing immutable
Bronze/Silver/Gold publications are reused on rerun.

Override the default recording only when a different compatible recording is
already intended:

```bash
DEMO_RECORDING_KEY=SC4002E make demo
```

## 6. Daily Local Lifecycle

After the machine has been bootstrapped, normal local operation uses:

```bash
make platform-up
make platform-status
make platform-down
```

`make platform-up` starts PostgreSQL, MinIO, Kafka, and the three Airflow
services, ensures the Kafka topic contract, and waits for readiness.

`make platform-status` is read-only and reports the readiness of PostgreSQL,
MinIO, Kafka, the Airflow scheduler, Airflow API server, and Airflow DAG
processor.

`make platform-down` stops those services without deleting persistent Docker
volumes.

Operational state can be summarized separately with:

```bash
make ops-status
```

## 7. Focused Component Commands

The unified lifecycle is the normal path. Lower-level commands remain available
for focused development and recovery.

### PostgreSQL and MinIO

```bash
make up
make ps
make buckets
make migrate
make psql
```

Required MinIO buckets:

```text
bronze
silver
gold
quarantine
logs
```

### Kafka

```bash
make kafka-up
make kafka-init
make kafka-topic-check
```

The local host bootstrap is `localhost:9092`; the Docker-network listener is
`kafka:19092`. Automatic topic creation is disabled. The application topic is
created and validated from its version-controlled contract.

### Airflow

```bash
make airflow-bootstrap
make airflow-up
make airflow-down
make airflow-ps
make airflow-smoke
make airflow-password
```

Airflow is available on port `8080`. Containers use `postgres:5432` and
`http://minio:9000` inside the Compose network, while host commands continue to
use `localhost:5433` and `localhost:9000`.

`make airflow-bootstrap` remains available for focused Airflow initialization or
repair. Normal fresh-machine setup should use `make bootstrap`, which includes
the Airflow bootstrap.

See [`airflow_orchestration.md`](airflow_orchestration.md).

## 8. Source Configuration

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

## 9. Validation

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
make phase10-check
make phase11-check
make phase12-check
```

`phase9-check` runs the normal smoke suites, full Spark feature validation,
source Gold validation, feature integration validation, integrated Gold
validation, and a full dbt build.

`phase10-check` runs the complete Phase 9 regression and then validates the
Python dependency contract, Airflow execution image, Compose runtime connectivity,
Airflow foundation smoke DAG, and the main eight-task pipeline DAG contract.
### Phase 11 Kafka validation

Focused streaming checks:

```bash
make kafka-smoke
make kafka-topic-check
make kafka-producer-check
make kafka-consumer-check
make kafka-inbox-check
make kafka-ingestion-check
make kafka-invalid-check
make kafka-arrival-check
make kafka-warehouse-check
```

Complete Phase 11 audit:

```bash
make phase11-check
```

See [`kafka_device_events.md`](kafka_device_events.md).

### Phase 12 data-quality validation

Run only the controlled broken-data fixture groups:

```bash
make phase12-quality-smoke
```

Run the complete Phase 12 audit:

```bash
make phase12-check
```

The complete audit performs source compilation, all four Phase 12 fixture groups,
the existing 26-test Silver regression, and repository diff hygiene. The fixtures
use temporary local data and do not modify trusted Bronze/Silver datasets.

See [`data_quality_hardening.md`](data_quality_hardening.md).

## 10. Run Extract

Example one-recording Extract:

```bash
SLEEP_EDF_MAX_RECORDINGS=1 PYTHONPATH=src python -m neuro_sleep.ingestion.sleep_edf_extract
```

See [`extract_runbook.md`](extract_runbook.md) before production-like reruns or
manual recovery.

## 11. Run Silver

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

## 12. Run Spark and Gold

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
## 13. Stop Services


```bash
make airflow-down
make kafka-down
make down
```

Stop Airflow first when Airflow is running, then stop Kafka if it was started,
then stop the base PostgreSQL/MinIO stack.

## 14. Git Safety

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
