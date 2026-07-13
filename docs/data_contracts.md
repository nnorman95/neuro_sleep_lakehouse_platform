# Data Contracts

The platform currently registers contracts for:

```text
raw.file_registry
ops.pipeline_run
quality.quarantine_records
governance.source_system_registry
```

## Active source identifier

```text
physionet_sleep_edf
```

## Raw file contract

Every successfully ingested source object should record:

- source system;
- original source URL;
- MinIO bucket;
- MinIO object key;
- file name;
- file type;
- file size;
- SHA-256 checksum;
- ingestion run identifier;
- ingestion status;
- ingestion timestamp.

## Source registry contract

The Sleep-EDF source registry row describes:

```text
dataset_name = Sleep-EDF Database Expanded
dataset_version = 1.0.0
access_model = open
credential_required = false
access_policy = open
```

## Sensitive-data handling

Sleep recordings remain patient-level health-related data even
though the source files are openly distributed and anonymized.

Raw files are stored locally in MinIO and are not committed to Git.
