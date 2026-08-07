# Database Schemas

## 1. Current Database

```text
PostgreSQL: 18.4 locally
Host port: 5433
Database: neuro_sleep
Metadata identifiers: UUIDv7
```

The package supports Python 3.11 or newer; the current development environment
uses Python 3.13.5.

## 2. Schema Purposes

| Schema | Purpose | Current state |
|---|---|---|
| `raw` | source-object registry and ingestion metadata | implemented |
| `staging` | relational landing area for selected Silver datasets | subject loader implemented; recording loaders pending |
| `warehouse` | dimensional analytical models | schema exists; tables not implemented |
| `mart` | consumption-ready relational models | schema exists; tables not implemented |
| `ops` | pipeline execution and file-attempt history | implemented |
| `quality` | quarantine and durable quality history | implemented |
| `governance` | source registry, contracts, and classification | implemented |

## 3. Implemented Operational Tables

### `ops.pipeline_run`

Grain: one tracked pipeline or task execution.

Important columns:

```text
run_id
pipeline_name
task_name
source_system
status
started_at
heartbeat_at
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

Grain: one immutable attempt to process one source object in one pipeline run.

Important columns:

```text
attempt_id
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

## 4. Implemented Raw Table

### `raw.file_registry`

Grain: one registered source object location.

Important columns:

```text
file_id
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
status limited to allowed ingestion states
```

## 5. Implemented Quality Tables

### `quality.quarantine_records`

Grain: one rejected or suspicious record/payload reference.

Small fragments may be stored in `raw_payload`. Large payloads are stored in
MinIO `quarantine` and referenced through:

```text
payload_bucket
payload_object_key
payload_size_bytes
payload_checksum_sha256
```

### `quality.quality_check_results`

Grain: one durable quality-check result for one pipeline run and dataset scope.

Important columns:

```text
quality_result_id
pipeline_run_id
source_system
data_layer
dataset_name
recording_id
record_key
check_name
severity
status
rows_checked
rows_failed
error_code
message
details
checked_at
created_at
```

Allowed severity values:

```text
info
warning
error
critical
```

Allowed status values:

```text
passed
warning
failed
skipped
```

`data_layer` intentionally accepts both lakehouse scopes (`bronze`, `silver`,
`gold`) and PostgreSQL/data-product scopes (`raw`, `staging`, `warehouse`,
`mart`, `ops`, `quality`, `governance`) because quality results can describe
both systems.

## 6. Implemented Governance Tables

```text
governance.source_system_registry
governance.data_contract_registry
governance.column_classification
```

`source_system_registry` separates external access from internal policy:

```text
access_model = open
credential_required = false
access_policy = restricted
status = active
```

`data_contract_registry` stores versioned contract metadata.
`column_classification` stores personal/health-data flags, access policy, and
masking guidance.

In governance tables, `data_layer` uses PostgreSQL/data-product vocabulary:

```text
raw
staging
warehouse
mart
ops
quality
governance
```

## 7. Current Silver Staging Tables

```text
staging.silver_recordings
staging.silver_channels
staging.silver_sleep_stage_intervals
staging.silver_sleep_stage_epochs
staging.silver_subjects
staging.silver_recording_contexts
```

The four recording-related staging tables currently contain zero rows.
The subject metadata tables contain the current verified production
publication: 100 subjects and 197 recording contexts.

### `staging.silver_recordings`

Grain: one concrete versioned Silver recording representation.

Primary key:

```text
recording_id
```

Version-aware unique identity:

```text
source_system
+ source_pair_id
+ input_fingerprint
+ schema_version
+ transform_version
+ config_id
```

Unique output location:

```text
silver_bucket
+ silver_output_prefix
```

Lineage foreign keys:

```text
psg_file_id          -> raw.file_registry.file_id
hypnogram_file_id    -> raw.file_registry.file_id
staging_load_run_id  -> ops.pipeline_run.run_id
```

Migration `025_correct_staging_silver_identity_and_lineage.sql` has already
implemented this design. It also removed the obsolete source-path-only
uniqueness rule and permits negative source interval onset values.

### `staging.silver_channels`

Grain: one channel in one concrete Silver recording representation.

### `staging.silver_sleep_stage_intervals`

Grain: one source annotation interval in one concrete Silver representation.
Negative `onset_seconds` is allowed because a source annotation may begin before
the PSG boundary.

### `staging.silver_sleep_stage_epochs`

Grain: one emitted 30-second epoch in one concrete Silver representation.
Epoch timeline positions remain non-negative and limited to the emitted PSG
range.

## 8. Subject Metadata Staging Tables

Migration `033_create_staging_silver_subject_metadata_tables.sql` implements:

```text
staging.silver_subjects
staging.silver_recording_contexts
```

`staging.silver_subjects` grain:

```text
one logical subject row
+ one concrete Silver metadata publication
```

Primary key:

```text
subject_key
+ metadata_input_fingerprint
```

`staging.silver_recording_contexts` grain:

```text
one logical recording-context row
+ one concrete Silver metadata publication
```

Primary key:

```text
source_system
+ dataset_version
+ collection
+ recording_key
+ metadata_input_fingerprint
```

The context table has a composite foreign key to the matching staged subject
publication. Their v1 contracts, column classifications, schema smoke test,
and production staging loader are implemented.

The loader verifies the completed Silver publication, downloads and validates
both Parquet files, and writes 100 subjects plus 197 recording contexts in one
transaction. All rows retain the metadata fingerprint, versions, Silver
location, staging run ID, and load timestamp. An unchanged rerun is tracked as
`skipped` and writes no duplicates.

## 9. Planned Warehouse Core

```text
warehouse.dim_subject
warehouse.dim_recording
warehouse.dim_channel
warehouse.dim_sleep_stage
warehouse.fact_sleep_epoch
```

The exact physical columns and constraints are defined in
[`data_model.md`](data_model.md) and must be approved through a Warehouse identity
ADR before implementation.

`warehouse.fact_signal_quality` and device-event tables remain future scope.

## 10. SQL Execution

SQL files are stored in:

```text
scripts/sql/migrations
scripts/sql/seeds
scripts/sql/manual
```

Only files listed in this manifest are executed:

```text
scripts/sql/migrations_manifest.txt
```

Run the registered migrations and seeds:

```bash
make migrate
```

Equivalent script:

```bash
./scripts/run_sql_migrations.sh
```

All migrations and seeds must be idempotent under the project runner.

## 11. Current Migration Baseline

The current manifest includes:

```text
024_create_staging_silver_tables.sql
025_correct_staging_silver_identity_and_lineage.sql
026_create_quality_check_results.sql
033_create_staging_silver_subject_metadata_tables.sql
```

Related governance seeds register the active contracts and column
classifications.

## 12. Phase 6 Migration Rules

New Phase 6 migrations must:

- preserve completed Bronze and Silver behavior;
- define grain and keys before columns are finalized;
- use foreign keys where parent rows are guaranteed;
- retain source and Silver lineage alongside Warehouse surrogate keys;
- support idempotent loads;
- include contracts and column classification;
- add focused smoke tests;
- pass `git diff --check` and the full regression suite before commit.
