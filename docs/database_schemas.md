# Database Schemas

## Current PostgreSQL Version

The current local database uses PostgreSQL 18.

Metadata IDs use PostgreSQL 18 `uuidv7()` defaults.

## Schemas

| Schema | Purpose |
|---|---|
| `raw` | Raw ingestion metadata and file registry |
| `staging` | Temporary cleaned/intermediate structures |
| `warehouse` | Analytical warehouse models |
| `mart` | Final consumption-ready analytical/ML tables |
| `ops` | Pipeline run logs and operational metadata |
| `quality` | Quality checks and quarantine metadata |
| `governance` | Data contracts and column-level classification |

## Current Tables

### `ops.pipeline_run`

Tracks pipeline and task execution.

Main columns:

```text
run_id uuid default uuidv7()
pipeline_name
 task_name
source_system
status
started_at
finished_at
rows_read
rows_written
files_processed
records_quarantined
error_message
created_at
```

Allowed statuses:

```text
started
success
failed
skipped
warning
```

### `ops.file_attempt`

Stores immutable per-run history for individual object-processing attempts.

Main columns:

```text
attempt_id uuid default uuidv7()
pipeline_run_id
source_system
source_url
bucket
object_key
file_name
file_type
status
resolution
file_size_bytes
checksum_sha256
error_type
error_message
started_at
finished_at
created_at
```

### `raw.file_registry`

Registers files from external sources and their object-storage locations.

Main columns:

```text
file_id uuid default uuidv7()
source_system
source_url
bucket
object_key
file_name
file_type
file_size_bytes
checksum_sha256
ingested_at
ingestion_run_id
status
created_at
```

Important constraints:

```text
unique(bucket, object_key)
file_size_bytes >= 0 when present
status must be one of the allowed ingestion statuses
```

### `quality.quarantine_records`

Stores rejected-record metadata and optional pointers to large quarantined payloads.

Main columns:

```text
quarantine_id uuid default uuidv7()
source_system
source_file_id
record_key
raw_payload
payload_bucket
payload_object_key
payload_size_bytes
payload_checksum_sha256
error_code
error_message
severity
detected_at
pipeline_run_id
status
created_at
```

Design rule:

```text
Small rejected fragments may be stored in raw_payload.
Large rejected payloads should be stored in MinIO and referenced through payload_bucket + payload_object_key.
```

### `governance.data_contract_registry`

Registers data contract files for known tables.

Main columns:

```text
contract_id uuid default uuidv7()
table_schema
table_name
contract_name
contract_version
contract_path
owner_role
data_layer
status
created_at
updated_at
```

`updated_at` is maintained by a trigger.

### `governance.column_classification`

Stores column-level sensitivity and access metadata.

Main columns:

```text
classification_id uuid default uuidv7()
table_schema
table_name
column_name
data_layer
classification_level
contains_personal_data
contains_health_data
contains_direct_identifier
sensitivity_reason
access_policy
masking_policy
created_at
updated_at
```

`updated_at` is maintained by a trigger.

### `governance.source_system_registry`

Registers external source identity, dataset version, source access model,
sensitivity flags, internal platform access policy, and operational status.

Current Sleep-EDF policy:

```text
access_model = open
credential_required = false
access_policy = restricted
status = active
```

External open access and internal patient-level access are intentionally separate concepts.

### Current Silver staging tables

```text
staging.silver_recordings
staging.silver_channels
staging.silver_sleep_stage_intervals
staging.silver_sleep_stage_epochs
```

These are current landing tables for low-volume Silver metadata and epochs.
A corrective identity/version-lineage migration is planned before the production
Silver-to-staging loader is enabled.

## Current Governance Coverage

| Table | Classified Columns |
|---|---:|
| `ops.pipeline_run` | 14 |
| `ops.file_attempt` | 17 |
| `quality.quarantine_records` | 16 |
| `raw.file_registry` | 13 |
| `governance.source_system_registry` | 18 |
| `staging.silver_recordings` | 13 |
| `staging.silver_channels` | 13 |
| `staging.silver_sleep_stage_intervals` | 9 |
| `staging.silver_sleep_stage_epochs` | 10 |

## SQL Execution

SQL files are split into:

```text
scripts/sql/migrations
scripts/sql/seeds
scripts/sql/manual
```

Only files listed in this manifest are executed by the runner:

```text
scripts/sql/migrations_manifest.txt
```

Run migrations:

```bash
./scripts/run_sql_migrations.sh
```

## Data Layer Vocabulary

In governance tables, `data_layer` refers to database/data-product layers:

```text
raw
staging
warehouse
mart
ops
quality
governance
```

It is not the same thing as MinIO bucket names such as `bronze`, `silver`, or `gold`.
