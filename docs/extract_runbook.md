# Sleep-EDF Extract Runbook

## 1. Purpose

This runbook describes how to run, inspect, recover, and validate the Sleep-EDF
Extract pipeline.

The pipeline:

- prevents concurrent execution with a PostgreSQL advisory lock;
- records runs in `ops.pipeline_run`;
- records per-object outcomes in `ops.file_attempt`;
- updates liveness through `heartbeat_at`;
- downloads through streaming HTTP;
- verifies official SHA-256 checksums;
- stores source objects in MinIO `bronze`;
- registers verified objects in `raw.file_registry`;
- reconciles MinIO and PostgreSQL state;
- safely handles failure and user interruption.

## 2. Terminal Rule

Run shell commands at the normal shell prompt:

```text
(.venv) norman@Mac neuro_sleep_lakehouse_platform %
```

Do not paste `make`, `docker compose`, or `PYTHONPATH=...` commands into the
interactive PostgreSQL prompt:

```text
neuro_sleep=#
```

Exit `psql` with:

```text
\q
```

## 3. Prerequisites

```bash
cd "/Users/norman/Documents/S/Data Engineering/neuro_sleep_lakehouse_platform"
source .venv/bin/activate
make up
make ps
make migrate
```

Run platform checks before production-like work:

```bash
make smoke
make reliability-smoke
```

## 4. Source Configuration

Typical sample settings:

```env
ACTIVE_SOURCE=sleep_edf
DATA_PROFILE=sample
SLEEP_EDF_VERSION=1.0.0
SLEEP_EDF_MAX_RECORDINGS=4
SLEEP_EDF_INCLUDE_CASSETTE=true
SLEEP_EDF_INCLUDE_TELEMETRY=true
SLEEP_EDF_INCLUDE_METADATA=true
```

For an unrestricted source selection:

```env
DATA_PROFILE=full
```

## 5. Normal Extract Run

Example one-recording run:

```bash
SLEEP_EDF_MAX_RECORDINGS=1 PYTHONPATH=src python -m neuro_sleep.ingestion.sleep_edf_extract
```

Existing valid objects are skipped. A verified object missing only its registry
finalization can be recovered without redownloading.

## 6. Interruption Safety

Pressing `Ctrl + C` during an active download raises `KeyboardInterrupt`.
Implemented cleanup behavior:

```text
active file attempt -> failed
pipeline run        -> failed
heartbeat           -> stopped
pipeline lock       -> released
HTTP response       -> closed
Requests session    -> closed
MinIO client        -> closed
unfinished .part    -> removed
interruption        -> re-raised to terminal
```

Do not use `kill -9` for normal cancellation because it prevents application
cleanup handlers from running.

Console timestamps and download-progress timestamps use UTC.

## 7. Inspect Recent Extract Runs

```bash
docker compose exec -T postgres psql -P pager=off -U neuro_sleep -d neuro_sleep -c "
select
    run_id,
    pipeline_name,
    task_name,
    status,
    started_at,
    heartbeat_at,
    finished_at,
    files_processed,
    rows_read,
    rows_written,
    error_message
from ops.pipeline_run
where pipeline_name = 'sleep_edf_extract'
order by started_at desc
limit 10;
"
```

## 8. Inspect Latest File Attempts

```bash
docker compose exec -T postgres psql -P pager=off -U neuro_sleep -d neuro_sleep -c "
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
    attempt.checksum_sha256,
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

Expected terminal attempt statuses include:

```text
uploaded
skipped
failed
```

Terminal attempt rows are immutable.

## 9. Inspect Registered Bronze Objects

```bash
docker compose exec -T postgres psql -P pager=off -U neuro_sleep -d neuro_sleep -c "
select
    file_id,
    bucket,
    object_key,
    file_size_bytes,
    checksum_sha256,
    status,
    ingested_at
from raw.file_registry
where source_system = 'physionet_sleep_edf'
order by object_key;
"
```

## 10. Reconciliation

Run from the shell:

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

counts = Counter(result.status for result in results)

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

## 11. Recovery Guidance

### `missing_in_storage`

Rerun Extract. The object should be downloaded, verified, uploaded, and
registered again.

### `missing_in_registry`

Rerun Extract. A valid MinIO object with matching official checksum metadata can
recover its registry row without a source download.

### `metadata_mismatch`

Do not manually trust either side. Rerun Extract so the object is checked
against the official source manifest.

### Verified object with database finalization failure

Preserve the object and rerun Extract. Verified-object recovery should complete
the PostgreSQL state.

### Leftover `.part` file

A normal handled failure or interruption removes unfinished `.part` files. If a
process was forcibly terminated, first confirm no Extract process is active and
then remove only the clearly unfinished local temporary file. Do not delete
verified MinIO objects as generic cleanup.

## 12. Concurrent-Run Protection

Only one Extract run for the protected pipeline scope should hold the advisory
lock. A blocked concurrent invocation should not be registered as a normal
active run.

Do not bypass the lock manually.

## 13. Dedicated Reliability Checks

```bash
PYTHONPATH=src python -m neuro_sleep.ops.pipeline_lock_smoke
PYTHONPATH=src python -m neuro_sleep.ops.file_attempt_smoke
PYTHONPATH=src python -m neuro_sleep.reconciliation.bronze_reconciliation_smoke
PYTHONPATH=src python -m neuro_sleep.ingestion.sleep_edf_interrupt_cleanup_smoke
```

The registered suite is preferred:

```bash
make reliability-smoke
```

## 14. Final Validation

```bash
make test
git diff --check
```

Current verified result:

```text
Core:        12/12
Reliability: 17/17
Silver:      24/24
Total:       53/53
```

Extract changes are not complete until failure handling, interruption cleanup,
resource closure, lock release, heartbeat termination, and reconciliation remain
verified.
