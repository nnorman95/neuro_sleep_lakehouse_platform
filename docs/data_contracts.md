# Data Contracts

The platform registers YAML contracts for important PostgreSQL tables in
`governance.data_contract_registry`.

Current contract files:

```text
contracts/raw_file_registry.yml
contracts/ops_pipeline_run.yml
contracts/ops_file_attempt.yml
contracts/quality_quarantine_records.yml
contracts/governance_source_system_registry.yml
contracts/staging_silver_recordings.yml
contracts/staging_silver_channels.yml
contracts/staging_silver_sleep_stage_intervals.yml
contracts/staging_silver_sleep_stage_epochs.yml
```

The staging contracts describe the current migration-024 landing schema and
will be versioned when the staging lineage model changes.

Silver Parquet schemas are explicitly defined in:

```text
src/neuro_sleep/silver/parquet_schemas.py
```

Sleep recordings and sleep-stage data are patient-level health-related data.
External source access is open, while internal patient-level data access is
restricted. Real Bronze/Silver data remains local and is never committed.
