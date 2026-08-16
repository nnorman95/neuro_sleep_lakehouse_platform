# Airflow Orchestration

## 1. Phase 10 scope

Phase 10 introduces Airflow as a thin orchestration layer over the already
validated NeuroSleep pipeline. Existing Python, dbt, Spark, PostgreSQL, MinIO,
quality, and publication logic remain the execution units.

The first block establishes only the local Airflow runtime and a smoke DAG.

## 2. Runtime

```text
Apache Airflow 3.3.1
LocalExecutor
existing PostgreSQL 18 service
separate Airflow metadata database and role
Airflow API server
Airflow scheduler
Airflow DAG processor
```

Redis, Celery workers, Flower, Kubernetes, and a triggerer are intentionally not
added in this block.

## 3. Metadata isolation

```text
application database: neuro_sleep
Airflow metadata DB:  airflow
Airflow DB role:      airflow
```

Airflow secrets are generated into the local gitignored `.env` file when absent.

## 4. Initial scheduling policy

```text
schedule=None
catchup=False
max_active_runs=1
```

Sleep-EDF is not a continuously arriving source, so Phase 10 does not invent an
artificial daily schedule.

## 5. Retry boundary

Airflow task retries are not enabled by default. Existing project code already
owns retry logic for transient HTTP, PostgreSQL, and object-storage failures and
owns idempotency/fail-closed behavior for Silver and Gold publication.

## 6. Foundation commands

```bash
make airflow-bootstrap
make airflow-ps
make airflow-smoke
make airflow-password
make airflow-down
```

The next block adds the real NeuroSleep dependency DAG after this foundation is
validated.

## Local state and Execution API routing

The Airflow services run as the unprivileged Airflow UID. Docker creates a new
named volume as `root:root`, so `prepare_airflow_state.sh` initializes ownership
of `/opt/airflow/state` to `${AIRFLOW_UID}:0`, applies mode `0770`, and verifies
write access before the API server starts.

Airflow 3 tasks communicate execution state through the Execution API. Because
the API server runs in a separate Compose service, worker processes inside the
scheduler container use the internal Docker DNS address instead of `localhost`:

```text
http://airflow-api-server:8080/execution/
```

The local runtime also sets:

```text
parallelism=2
```

This keeps LocalExecutor resource usage bounded on the development machine.

## Image reproducibility

The local Airflow runtime uses Apache Airflow 3.3.1 pinned to the verified
linux/arm64 image digest:

```text
apache/airflow@sha256:0c4bcc0370e526de1b7892a3bf4343d260c6c82359c66f77155b53cd773d6339
```

The human-readable release remains 3.3.1, while the digest prevents a mutable
tag from silently resolving to different image contents on a later bootstrap.
