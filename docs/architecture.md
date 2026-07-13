# Architecture

## Source

```text
Sleep-EDF Database Expanded v1.0.0
source_system = physionet_sleep_edf
access_model = open
```

## Platform flow

```text
PhysioNet
    |
    v
Source manifest and file selection
    |
    v
Streaming Python Extract
    |
    v
MinIO Bronze
    |
    +--> raw.file_registry
    +--> ops.pipeline_run
    +--> quality.quarantine_records
    |
    v
Silver normalized data
    |
    v
Warehouse and marts
```

## Current implemented components

- Docker Compose
- PostgreSQL
- MinIO
- SQL migrations and seeds
- source-system registry
- data-contract registry
- column classification
- pipeline-run audit log
- raw-file registry
- quarantine records
- quarantine payload pointers
- reusable Bronze writer
- Sleep-EDF source definition
- checksum manifest parsing and sample/full selection

## Source modules

```text
src/neuro_sleep/sources/sleep_edf.py
src/neuro_sleep/sources/sleep_edf_manifest.py
```

## Planned Extract properties

- stream large EDF files without loading them fully into RAM;
- verify SHA-256 against the official manifest;
- support sample and full profiles;
- resume interrupted downloads;
- skip already uploaded files;
- register every source object;
- quarantine invalid or mismatched files.
