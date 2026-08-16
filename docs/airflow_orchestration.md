# Airflow Orchestration

## 1. Scope

Phase 10 adds Apache Airflow as a thin orchestration layer around the existing
NeuroSleep pipeline. Airflow owns task ordering, retries, bounded concurrency,
and run state. Bronze, Silver, staging, dbt, Spark, Gold, quality, lineage, and
reconciliation logic remain in their existing modules and scripts.

The main DAG is:

```text
neurosleep_lakehouse_pipeline
```

It is intentionally manual at this stage:

```text
schedule=None
catchup=False
max_active_runs=1
task retries=1
retry delay=2 minutes
```

## 2. Runtime

The local runtime uses:

```text
Airflow:       3.3.1
Executor:      LocalExecutor
Parallelism:   2
Runtime image: neurosleep-airflow:phase10
Java in image: 17
PySpark:       4.2.0
dbt-core:      1.12.0
dbt-postgres:  1.11.0
```

The image is built from the pinned upstream Airflow base image in
`Dockerfile.airflow`. Project source is copied to `/opt/neurosleep` and
`PYTHONPATH=/opt/neurosleep/src` is set in the image.

`.env`, local data, logs, Airflow state, generated dbt artifacts, and large data
files are excluded from the image build context. Runtime configuration is
injected by Compose.

## 3. Network topology

Host commands and Airflow containers use different addresses for the same local
services:

```text
Host                          Airflow containers
PostgreSQL localhost:5433     postgres:5432
MinIO      localhost:9000     http://minio:9000
```

`src/neuro_sleep/config.py` does not override existing environment values, so the
container environment wins even though the same project code also supports the
host workflow.

## 4. Services

The local Airflow stack contains:

```text
airflow-scheduler
airflow-dag-processor
airflow-api-server
airflow-init
```

PostgreSQL and MinIO remain the shared project services. Airflow has its own
metadata database inside PostgreSQL.

Bootstrap:

```bash
make airflow-bootstrap
```

The bootstrap sequence is:

```text
ensure Airflow env
-> start PostgreSQL
-> initialize Airflow metadata DB
-> build project runtime image
-> validate runtime image
-> prepare Airflow state
-> run Airflow migrations
-> start scheduler / DAG processor / API server
```

## 5. Pipeline DAG

The DAG has exactly eight tasks:

```text
extract_bronze
build_subject_metadata_silver
build_recording_silver
load_subject_metadata_staging
load_recording_staging
build_warehouse_and_marts
build_gold_signal_features
build_integrated_signal_features
```

Dependency graph:

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

The DAG uses a generic TaskFlow task only to execute existing project commands
from `/opt/neurosleep`. It does not contain data-transformation logic.

## 6. Existing entrypoints reused

```text
Bronze:
python -m neuro_sleep.ingestion.sleep_edf_extract

Subject Silver:
python scripts/run_silver_subject_metadata.py

Recording Silver:
python scripts/run_silver_batch.py

Subject staging:
python scripts/load_subject_metadata_staging.py

Recording staging:
python scripts/load_recording_staging.py

Warehouse + marts:
bash scripts/run_dbt.sh build

Gold signal features:
bash scripts/run_gold_signal_features.sh

Integrated Gold:
bash scripts/run_integrated_signal_features.sh
```

This keeps Airflow replaceable as an orchestrator and keeps the host command path
usable for debugging and direct operation.

## 7. Validation

Airflow-specific validation is split by boundary:

```text
validate_airflow_runtime_image.sh
  runtime image, project import, Java, PySpark, dbt, dependency contract, Spark JVM

validate_airflow_compose_runtime.sh
  Compose config, service health, container env, PostgreSQL, dbt, MinIO

run_airflow_foundation_smoke.sh
  Airflow version/config/metadata DB/components/imports and a real smoke DAG run

validate_airflow_pipeline_dag.sh
  syntax, import errors, DAG discovery, exact 8-task contract, DAG details
```

The complete milestone regression is:

```bash
make phase10-check
```

It runs the complete Phase 9 regression first and then the Airflow-specific
contracts above.

## 8. End-to-end evidence

Two real scheduler-driven executions of `neurosleep_lakehouse_pipeline` were run
consecutively. Both finished with:

```text
DAG state: success
8/8 task instances: success
```

The second unchanged run completed the Gold signal-feature task in about 6.5
seconds versus about 135 seconds on the first run, consistent with the existing
idempotent publication skip path.

After the repeated Airflow runs, the full Phase 9 regression passed again,
including:

```text
Silver idempotency:              PASS
duplicate Silver object keys:    false
Spark row reconciliation:        116,242,840 / 116,242,840
Gold validation:                 83,909 rows
Integrated Gold validation:      83,909 rows
dbt build:                       257/257 PASS, 0 WARN, 0 ERROR
```

The final Phase 10 regression also completed with:

```text
phase10_regression_status=success
```

## 9. Operational commands

```bash
make airflow-bootstrap
make airflow-up
make airflow-down
make airflow-ps
make airflow-smoke
make airflow-password
make phase10-check
```

The main DAG is not scheduled automatically yet. This is deliberate: scheduling
policy should be introduced only when the desired cadence and operational need
are explicit.
