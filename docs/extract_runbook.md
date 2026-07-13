# Sleep-EDF Extract Runbook

## Purpose

This runbook describes how to run, inspect, recover, and validate the Sleep-EDF Extract pipeline.

The Extract pipeline:

- prevents concurrent execution with a PostgreSQL advisory lock;
- records pipeline runs in `ops.pipeline_run`;
- records per-object outcomes in `ops.file_attempt`;
- updates pipeline liveness through `heartbeat_at`;
- stores verified source files in the MinIO `bronze` bucket;
- reconciles MinIO objects with `raw.file_registry`.

## Important terminal rule

Commands beginning with `PYTHONPATH=`, `make`, `docker compose`, or `ps aux` must be run in the normal shell prompt:

```text
(.venv) norman@Mac neuro_sleep_lakehouse_platform %
```

Do not paste those commands into the interactive PostgreSQL prompt:

```text
neuro_sleep=#
```

To cancel unfinished SQL, press `Ctrl + C`. To leave `psql`, enter:

```text
\q
```

## Prerequisites

Start PostgreSQL and MinIO:

```bash
make up
```

Apply database migrations and seeds:

```bash
make migrate
```

Run platform smoke tests:

```bash
make smoke
```

Run reliability and failure tests:

```bash
make reliability-smoke
```

## Normal Extract run

Run Extract for one Sleep-EDF recording:

```bash
SLEEP_EDF_MAX_RECORDINGS=1 \
PYTHONPATH=src \
python -m neuro_sleep.ingestion.sleep_edf_extract
```

A successful run should end with messages similar to:

```text
✓ Completed
🔓 Pipeline lock released
```

Existing valid objects are skipped instead of downloaded again.

## Inspect recent pipeline runs

```bash
docker compose exec -T postgres \
psql -P pager=off \
-U neuro_sleep \
-d neuro_sleep \
-c "
select
    run_id,
    pipeline_name,
    status,
    started_at,
    heartbeat_at,
    finished_at,
    files_processed,
    error_message
from ops.pipeline_run
where pipeline_name = 'sleep_edf_extract'
order by started_at desc
limit 10;
"
```

## Inspect the latest Extract file-attempt history

This query automatically selects the latest `sleep_edf_extract` run. No UUID needs to be copied or replaced.

```bash
docker compose exec -T postgres \
psql -P pager=off \
-U neuro_sleep \
-d neuro_sleep \
-c "
with latest_run as (
    select run_id
    from ops.pipeline_run
    where pipeline_name = 'sleep_edf_extract'
    order by started_at desc
    limit 1
)
select
    attempt.object_key,
    attempt.status,
    attempt.resolution,
    attempt.file_size_bytes,
    attempt.error_type,
    attempt.error_message,
    attempt.started_at,
    attempt.finished_at
from ops.file_attempt as attempt
join latest_run
  on latest_run.run_id = attempt.pipeline_run_id
order by attempt.started_at;
"
```

Possible file-attempt results:

| Status | Resolution | Meaning |
|---|---|---|
| `uploaded` | `downloaded_and_uploaded` | Object was downloaded, verified, uploaded, and registered |
| `skipped` | `existing_valid` | MinIO and PostgreSQL already contained a valid object |
| `skipped` | `recovered_existing` | Valid MinIO object existed and its registry state was recovered |
| `failed` | `null` | Object processing failed |

## Failed Extract run

A failed pipeline should have:

```text
ops.pipeline_run.status = failed
ops.pipeline_run.error_message is not null
```

The failed object should have:

```text
ops.file_attempt.status = failed
error_type is not null
error_message is not null
finished_at is not null
```

Inspect the pipeline run and its file-attempt rows before deleting or modifying anything.

After fixing a temporary source, PostgreSQL, or MinIO problem, rerun Extract. Valid completed objects will be skipped.

## Stale heartbeat check

List pipeline runs that are still marked `started`:

```bash
docker compose exec -T postgres \
psql -P pager=off \
-U neuro_sleep \
-d neuro_sleep \
-c "
select
    run_id,
    pipeline_name,
    status,
    started_at,
    heartbeat_at,
    now() - heartbeat_at as heartbeat_age
from ops.pipeline_run
where status = 'started'
order by heartbeat_at;
"
```

Check whether an Extract process is active:

```bash
ps aux \
| grep \
'[n]euro_sleep.ingestion.sleep_edf_extract'
```

Do not manually close a run while its process is active.

## Safely close a confirmed stale run

Use this non-interactive shell block only after confirming that the process no longer exists. It validates the UUID before sending it to PostgreSQL.

```bash
read -r -p "Paste the confirmed stale run UUID: " RUN_ID

if [[ ! "$RUN_ID" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]; then
    echo "Invalid UUID format"
    exit 1
fi

docker compose exec -T postgres \
psql -v ON_ERROR_STOP=1 \
-P pager=off \
-U neuro_sleep \
-d neuro_sleep \
-v run_id="$RUN_ID" \
-c "
with updated_run as (
    update ops.pipeline_run
    set
        status = 'failed',
        finished_at = now(),
        error_message =
            'Manually closed as stale after operator verification.'
    where run_id = :'run_id'::uuid
      and status = 'started'
    returning run_id
)
select count(*) as updated_rows
from updated_run;
"
```

Expected result for one confirmed stale run:

```text
updated_rows
------------
1
```

If `updated_rows` is `0`, PostgreSQL changed nothing. Recheck the UUID and current run status instead of forcing an update.

The PostgreSQL advisory lock is connection-scoped. PostgreSQL releases it automatically when the crashed process connection disappears.

## Concurrent Extract blocked

A second Extract process should print:

```text
⛔ Extract blocked  another run is already active
```

It exits with status code `2`.

Do not bypass the lock. Wait for the active Extract to finish.

When no active process exists but a pipeline row remains `started`, follow the stale-heartbeat procedure.

## Reconciliation check

The reconciliation service compares:

- MinIO object existence;
- PostgreSQL registry existence;
- registry status;
- file size;
- SHA256 metadata.

Run this command only from the normal shell prompt, not from `psql`:

```bash
PYTHONPATH=src python - <<'PY'
from collections import Counter

from neuro_sleep.reconciliation.bronze_reconciliation import (
    reconcile_bronze_prefix,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
)


client = get_object_storage_client()

try:
    results = reconcile_bronze_prefix(
        bucket="bronze",
        prefix="physionet/sleep-edfx/1.0.0/",
        client=client,
    )

finally:
    client.close()


counts = Counter(
    result.status
    for result in results
)

print(f"reconciled_object_count={len(results)}")

for status in (
    "healthy",
    "missing_in_storage",
    "missing_in_registry",
    "metadata_mismatch",
):
    print(f"{status}={counts[status]}")

for result in results:
    if not result.healthy:
        print(
            f"{result.status} | "
            f"{result.object_key} | "
            f"{result.reason}"
        )
PY
```

Possible reconciliation statuses:

| Status | Meaning |
|---|---|
| `healthy` | MinIO and PostgreSQL metadata match |
| `missing_in_storage` | Registry row exists but the MinIO object is missing |
| `missing_in_registry` | MinIO object exists but the registry row is missing |
| `metadata_mismatch` | Registry status, size, or SHA256 differs |

## Reconciliation recovery

### `missing_in_storage`

Rerun Extract. The source object should be downloaded, verified, uploaded, and registered again.

### `missing_in_registry`

Rerun Extract. When the existing MinIO object has the correct official SHA256 metadata, the pipeline should recover its registry row without downloading the object again.

### `metadata_mismatch`

Do not manually trust either MinIO or PostgreSQL. Rerun Extract so the object can be checked against the official source manifest and checksum.

### Verified object with database finalization failure

The terminal may report:

```text
verified_object_preserved
registry_finalization_pending=true
```

Do not delete this object. Rerun Extract so verified-object recovery can finish the PostgreSQL registry state.

## Dedicated reliability checks

Parallel-run protection:

```bash
PYTHONPATH=src \
python -m neuro_sleep.ops.pipeline_lock_smoke
```

File-attempt history:

```bash
PYTHONPATH=src \
python -m neuro_sleep.ops.file_attempt_smoke
```

Bronze reconciliation:

```bash
PYTHONPATH=src \
python -m neuro_sleep.reconciliation.bronze_reconciliation_smoke
```

## Final validation

```bash
make smoke
```

```bash
make reliability-smoke
```

Both commands must finish successfully before Extract reliability changes are considered complete.
