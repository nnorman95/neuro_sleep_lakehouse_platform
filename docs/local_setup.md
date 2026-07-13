# Local Setup

## Requirements

- Docker Desktop
- Python 3.11 or newer
- Make

## Environment

Create the local environment file:

```bash
cp .env.example .env
```

Create and activate the Python environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Start the platform

```bash
make bootstrap
```

This starts PostgreSQL and MinIO, initializes buckets, applies SQL
migrations and seeds, and runs smoke tests.

## Check services

```bash
make ps
```

## Check source configuration

```bash
make source-check
```

## Run all smoke tests

```bash
make smoke
```

## Sample source configuration

```env
ACTIVE_SOURCE=sleep_edf
DATA_PROFILE=sample
SLEEP_EDF_VERSION=1.0.0
SLEEP_EDF_MAX_RECORDINGS=4
SLEEP_EDF_INCLUDE_CASSETTE=true
SLEEP_EDF_INCLUDE_TELEMETRY=true
SLEEP_EDF_INCLUDE_METADATA=true
```

## Full-source configuration

```env
ACTIVE_SOURCE=sleep_edf
DATA_PROFILE=full
```

Full source data is downloaded only when the real Extract command
is implemented and explicitly executed.
