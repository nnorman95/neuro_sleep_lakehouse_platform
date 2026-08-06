# Local Setup

## Requirements

- Docker Desktop
- Python 3.11 or newer
- Make

## Environment

```bash
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Bootstrap

```bash
make bootstrap
```

Bootstrap starts PostgreSQL/MinIO, initializes buckets, runs SQL migrations and
seeds, and runs the core smoke suite.

## Validation

```bash
make smoke
make reliability-smoke
make silver-smoke
make test
```

`make test` runs all registered suites.

## Source configuration

```env
ACTIVE_SOURCE=sleep_edf
DATA_PROFILE=sample
SLEEP_EDF_VERSION=1.0.0
SLEEP_EDF_MAX_RECORDINGS=4
SLEEP_EDF_INCLUDE_CASSETTE=true
SLEEP_EDF_INCLUDE_TELEMETRY=true
SLEEP_EDF_INCLUDE_METADATA=true
```

The real HTTP Extract pipeline is implemented. Example one-recording run:

```bash
SLEEP_EDF_MAX_RECORDINGS=1 PYTHONPATH=src python -m neuro_sleep.ingestion.sleep_edf_extract
```
