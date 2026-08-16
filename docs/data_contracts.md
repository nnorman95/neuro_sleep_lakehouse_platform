# Data Contracts

Data contracts document expected table structure, ownership, access policy, and
version. PostgreSQL contract metadata is registered in
`governance.data_contract_registry`; YAML files remain version-controlled in
`contracts/`.

## 1. Current PostgreSQL Contract Files

```text
contracts/raw_file_registry.yml
contracts/ops_pipeline_run.yml
contracts/ops_file_attempt.yml
contracts/quality_quarantine_records.yml
contracts/quality_quarantine_records_v2.yml
contracts/quality_check_results.yml
contracts/governance_source_system_registry.yml
contracts/staging_silver_recordings.yml
contracts/staging_silver_recordings_v2.yml
contracts/staging_silver_recordings_v3.yml
contracts/staging_silver_channels.yml
contracts/staging_silver_sleep_stage_intervals.yml
contracts/staging_silver_sleep_stage_intervals_v2.yml
contracts/staging_silver_sleep_stage_epochs.yml
contracts/staging_silver_subjects.yml
contracts/staging_silver_recording_contexts.yml
contracts/warehouse_dim_subject.yml
contracts/warehouse_dim_recording.yml
contracts/warehouse_dim_channel.yml
contracts/warehouse_dim_sleep_stage.yml
contracts/warehouse_fact_sleep_epoch.yml
contracts/warehouse_fact_device_event.yml
```

## 2. Active and Historical Staging Contracts

Migration `024` introduced the first staging schema. Migration `025` corrected
recording identity and lineage.

The repository intentionally preserves both contract generations:

```text
staging_silver_recordings.yml
staging_silver_sleep_stage_intervals.yml
    historical v1 contracts; registry status = deprecated

staging_silver_recordings_v2.yml
    historical recording contract after migration 025; registry status = deprecated

staging_silver_sleep_stage_intervals_v2.yml
    active interval contract after migration 025

staging_silver_recordings_v3.yml
    active recording contract after migration 036; adds explicit
    dataset_version + collection + recording_key logical identity
```

The channel and epoch contracts did not require a separate v2 file because their
physical contract did not receive the same identity correction.

Historical contracts must not be deleted merely because a newer version is
active. They document schema evolution and match the governance registry.

Quality quarantine contracts follow the same lifecycle:

```text
quality_quarantine_records.yml
    historical v1 contract; registry status = deprecated

quality_quarantine_records_v2.yml
    active v2 contract; documents the active-incident identity used by
    Silver quality-gate quarantine routing
```

## Kafka Device-Event Contracts

Phase 11 adds two version-controlled streaming contracts outside the PostgreSQL
table-contract registry:

```text
contracts/simulated_bci_device_event_v1.schema.json
contracts/kafka/simulated_bci_device_events_v1.topic.json
```

The event contract defines the payload-level identity and schema, including
`event_id`, `device_id`, `session_id`, `event_type`, UTC `event_time`,
non-negative `sequence_number`, and event-specific `payload`.

The topic contract defines:

```text
topic: neurosleep.simulated-bci.device-events.v1
key_field: device_id
partitions: 3
replication_factor: 1
cleanup.policy: delete
retention.ms: 604800000
min.insync.replicas: 1
```

Topic initialization is explicit and idempotent; configuration drift fails
closed.

The downstream PostgreSQL Warehouse contract is:

```text
contracts/warehouse_fact_device_event.yml
```

It is registered as an active v1 governance contract by seed `045`, while seed
`046` classifies all 27 physical fact columns.

## 3. Silver Parquet Contracts

Silver Parquet schemas are enforced in code rather than through the PostgreSQL
contract registry.

Core recording schemas are defined in:

```text
src/neuro_sleep/silver/parquet_schemas.py
```

Subject metadata schemas are defined in:

```text
src/neuro_sleep/silver/subject_parquet.py
```

Implemented Silver datasets:

```text
recordings
channels
sleep_stage_intervals
sleep_stage_epochs
signals
subjects
recording_contexts
```

Silver publication additionally validates object size, SHA-256 metadata,
manifest content, row counts, and expected object inventory.

## 4. Gold Signal-Feature Contract

The Phase 8 Gold feature dataset is object-storage data, so its contract is
enforced in Spark/Python code and the Gold `_SUCCESS.json` manifest rather than
the PostgreSQL governance registry.

Current versions:

```text
Gold schema_version:   1.0.0
feature_version:       1.0.0
window:                30 seconds
grain:                 recording_id + channel_id + epoch_number
```

The feature frame requires identity, channel context, window boundaries, sample
coverage, descriptive statistics, and feature version fields. Publication also
validates exact Silver lineage, expected row counts, partial-window counts,
physical object inventory, file size, and ETag.

A completed valid representation is immutable and skipped on rerun.

See [`spark_signal_features.md`](spark_signal_features.md) for the full field
groups and publication layout.

## 5. Integrated Gold Feature Contract

Phase 9 adds `integrated_signal_features`, enforced in Spark/Python code and its
Gold `_SUCCESS.json` manifest.

```text
schema_version:       1.0.0
feature_version:      1.0.0
integration_version:  1.0.0
grain:                recording_id + channel_id + epoch_number
```

Recording/channel Warehouse context is required on every row. Sleep-stage fields
are nullable only for real signal windows without a corresponding emitted
Warehouse epoch.

The immutable publication identity includes `input_recording_id` and
`warehouse_context_sha256`. The manifest additionally records validated
source-Gold object lineage, Warehouse context counts, integrated row counts,
label coverage, data-object size/ETag, and Spark version.

See [`feature_integration.md`](feature_integration.md).

## 6. Contract Lifecycle

Use these statuses:

```text
draft
active
deprecated
```

A contract change should follow this sequence:

1. define the new grain and compatibility impact;
2. create or update the physical migration;
3. add a new YAML contract version when compatibility changes;
4. register the new version through an idempotent seed;
5. deprecate the superseded registry version without deleting its file;
6. update column classification;
7. add focused smoke tests;
8. run the full regression suite.

## 7. Contract Requirements

Every important relational contract should state:

- schema and table name;
- contract version;
- row grain;
- required columns and data types;
- nullability;
- primary and unique keys;
- foreign keys;
- accepted values or check constraints;
- lineage fields;
- owner role;
- data layer;
- access policy;
- compatibility notes.

## 8. Warehouse and Mart Contract Work

The subject-aware staging contracts are implemented and registered as active
v1 contracts:

```text
staging.silver_subjects
staging.silver_recording_contexts
```

The five Warehouse Core YAML contracts are also implemented and registered as
active v1 contracts:

```text
warehouse.dim_subject
warehouse.dim_recording
warehouse.dim_channel
warehouse.dim_sleep_stage
warehouse.fact_sleep_epoch
```

All 81 physical Warehouse columns have matching governance classifications.
The dbt Warehouse models separately use enforced model contracts and schema/data
tests; the YAML registry contracts remain the version-controlled governance
record.

Phase 7 marts also use enforced dbt model contracts in:

```text
dbt/models/marts/marts.yml
```

Those contracts cover the physical mart columns and types used during dbt builds.
Separate registry-backed mart governance contracts/classifications are not added
yet; they belong with the later access/BI rollout before broader consumption is
enabled.

Do not create contracts for `fact_signal_quality` or device-event models until
trusted upstream datasets and exact grains exist.

## 9. Privacy Boundary

Contracts for subject-aware tables must mark the following as restricted or
sensitive as appropriate:

```text
source_subject_id
source_subject_number
age_years
sex
subject_key
recording_key
treatment
source object lineage
```

`subject_key` is pseudonymous, not guaranteed anonymous.

## 10. Current Status

Implemented:

- registry-backed contracts for core operational tables;
- active quality-check-results contract;
- versioned staging recording and interval contracts;
- active Silver recording v3 contract with explicit logical identity;
- active subject/context staging contracts;
- five active Warehouse Core v1 governance contracts;
- Warehouse column classification for all 81 physical columns;
- enforced dbt model contracts plus Warehouse relationship/reconciliation tests;
- enforced dbt contracts for the three Phase 7 marts;
- explicit Silver Parquet schemas;
- code- and manifest-enforced Gold signal-feature v1 contract;
- code- and manifest-enforced integrated Gold feature v1 contract.

Not implemented yet:

- contracts for deferred signal-quality/device-event facts;
- registry-backed mart governance contracts/classifications for broader access;
