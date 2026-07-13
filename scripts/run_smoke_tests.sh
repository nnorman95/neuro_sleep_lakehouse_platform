#!/usr/bin/env bash

set -euo pipefail

echo "Running NeuroSleep smoke tests..."
echo

echo "1/10 Check Python configuration"
PYTHONPATH=src python -m neuro_sleep.config
echo

echo "2/10 Check PostgreSQL connection"
PYTHONPATH=src python -m neuro_sleep.db.postgres
echo

echo "3/10 Check MinIO object storage"
PYTHONPATH=src python -m neuro_sleep.storage.object_storage
echo

echo "4/10 Check ops.pipeline_run"
PYTHONPATH=src python -m neuro_sleep.ops.pipeline_run
echo

echo "5/10 Check raw.file_registry"
PYTHONPATH=src python -m neuro_sleep.raw.file_registry
echo

echo "6/10 Check quality.quarantine_records"
PYTHONPATH=src python -m neuro_sleep.quality.quarantine
echo

echo "7/10 Check quarantine payload pointer"
PYTHONPATH=src python -m neuro_sleep.quality.quarantine_payload_smoke
echo

echo "8/10 Check reusable bronze writer"
PYTHONPATH=src python -m neuro_sleep.ingestion.bronze_writer_smoke
echo

echo "9/10 Check Sleep-EDF checksum manifest"
PYTHONPATH=src python -m neuro_sleep.sources.sleep_edf_manifest_smoke
echo

echo "10/10 Check Sleep-EDF open source configuration"
PYTHONPATH=src python -m neuro_sleep.sources.sleep_edf
echo

echo "All smoke tests completed."
