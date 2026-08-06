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
contracts/quality_check_results.yml
contracts/governance_source_system_registry.yml
contracts/staging_silver_recordings.yml
contracts/staging_silver_recordings_v2.yml
contracts/staging_silver_channels.yml
contracts/staging_silver_sleep_stage_intervals.yml
contracts/staging_silver_sleep_stage_intervals_v2.yml
contracts/staging_silver_sleep_stage_epochs.yml
contracts/staging_silver_subjects.yml
contracts/staging_silver_recording_contexts.yml
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
staging_silver_sleep_stage_intervals_v2.yml
    active contracts after migration 025
```

The channel and epoch contracts did not require a separate v2 file because their
physical contract did not receive the same identity correction.

Historical contracts must not be deleted merely because a newer version is
active. They document schema evolution and match the governance registry.

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

## 4. Contract Lifecycle

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

## 5. Contract Requirements

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

## 6. Phase 6 Contract Work

The subject-aware staging contracts are implemented and registered as active
v1 contracts:

```text
staging.silver_subjects
staging.silver_recording_contexts
```

After Warehouse DDL is approved, add contracts for:

```text
warehouse.dim_subject
warehouse.dim_recording
warehouse.dim_channel
warehouse.dim_sleep_stage
warehouse.fact_sleep_epoch
```

Do not create contracts for `fact_signal_quality` or device-event models until
trusted upstream datasets and exact grains exist.

## 7. Privacy Boundary

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

## 8. Current Status

Implemented:

- registry-backed contracts for core operational tables;
- active quality-check-results contract;
- versioned staging recording and interval contracts;
- active subject/context staging contracts;
- explicit Silver Parquet schemas.

Not implemented yet:

- Warehouse contracts;
- dbt schema tests derived from Warehouse contracts.
