# NeuroSleep Lakehouse Platform

NeuroSleep is a local data engineering platform for ingesting,
validating, storing, transforming, and analyzing sleep neuroscience
data.

The project currently uses **Sleep-EDF Database Expanded v1.0.0**
as its source.

## Source

Source system:

```text
physionet_sleep_edf
```

Dataset:

```text
Sleep-EDF Database Expanded v1.0.0
```

Access model:

```text
open access
credentials required: no
```

The source contains polysomnography recordings in EDF format,
matching EDF+ hypnograms, and descriptive metadata.

Real source files are never committed to the repository.

## Data profiles

Limited development mode:

```env
DATA_PROFILE=sample
SLEEP_EDF_MAX_RECORDINGS=4
SLEEP_EDF_INCLUDE_CASSETTE=true
SLEEP_EDF_INCLUDE_TELEMETRY=true
SLEEP_EDF_INCLUDE_METADATA=true
```

A GitHub user can change the recording limit without modifying
Python code:

```env
SLEEP_EDF_MAX_RECORDINGS=20
```

Full-source mode:

```env
DATA_PROFILE=full
```

Full mode ignores sample limits and selects every file discovered
in the official checksum manifest.

## Local architecture

```text
Sleep-EDF / PhysioNet
          |
          v
Python Extract and validation
          |
          v
MinIO Bronze
          |
          +----> raw.file_registry
          |
          +----> ops.pipeline_run
          |
          +----> quality.quarantine_records
```

Current infrastructure:

- PostgreSQL
- MinIO
- Docker Compose
- Python
- SQL migrations and seeds
- governance metadata
- raw file registry
- pipeline audit log
- quarantine layer
- reusable Bronze writer
- Sleep-EDF checksum manifest selection

## Local setup

```bash
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make bootstrap
```

## Common commands

```bash
make help
make up
make ps
make migrate
make smoke
make source-check
make psql
```

## Source modules

```text
src/neuro_sleep/sources/sleep_edf.py
src/neuro_sleep/sources/sleep_edf_manifest.py
```

Check the source definition:

```bash
PYTHONPATH=src python -m neuro_sleep.sources.sleep_edf
```

Check manifest selection:

```bash
PYTHONPATH=src python -m neuro_sleep.sources.sleep_edf_manifest_smoke
```

## Current stage

The local platform, metadata layer, Bronze writer, and source
manifest are implemented.

The next phase is the real open-access HTTP Extract:

```text
fetch SHA256SUMS.txt
select sample or full manifest
stream files from PhysioNet
verify SHA-256
write files to MinIO Bronze
register files in PostgreSQL
resume interrupted downloads
skip already completed files
```

No real Sleep-EDF data is stored in this repository.
