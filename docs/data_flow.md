# Data Flow

## Extract

```text
Sleep-EDF Database Expanded
         |
         v
SHA256SUMS.txt
         |
         v
manifest parsing
         |
         v
sample/full file selection
         |
         v
streaming HTTP download
```

## Bronze load

```text
downloaded source file
         |
         v
SHA-256 verification
         |
         v
MinIO bronze object
         |
         v
raw.file_registry
```

## Operational metadata

Each pipeline execution is written to:

```text
ops.pipeline_run
```

Each source file is written to:

```text
raw.file_registry
```

Invalid files or records are written to:

```text
quality.quarantine_records
```

Large quarantine payloads are stored in MinIO, while PostgreSQL
stores the pointer and checksum.

## Profiles

```text
DATA_PROFILE=sample
    -> apply user-configured limits

DATA_PROFILE=full
    -> select all discovered source files
```

The real downloader is the next implementation phase.
