#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

current=0
total=13


run_test() {
    local title="$1"
    local module="$2"

    current=$((current + 1))

    echo "${current}/${total} ${title}"
    python -m "${module}"
    echo
}


echo "Running NeuroSleep core smoke tests..."
echo

run_test \
    "Check Python configuration" \
    "neuro_sleep.config"

run_test \
    "Check PostgreSQL connection" \
    "neuro_sleep.db.postgres"
run_test \
    "Check MinIO object storage" \
    "neuro_sleep.storage.object_storage"

run_test \
    "Check ops.pipeline_run" \
    "neuro_sleep.ops.pipeline_run"

run_test \
    "Check raw.file_registry" \
    "neuro_sleep.raw.file_registry"

run_test \
    "Check quality.quarantine_records" \
    "neuro_sleep.quality.quarantine"

run_test \
    "Check quarantine payload pointer" \
    "neuro_sleep.quality.quarantine_payload_smoke"
run_test \
    "Check quality-check result history" \
    "neuro_sleep.quality.check_results_smoke"

run_test \
    "Check Silver staging identity schema" \
    "neuro_sleep.staging.identity_schema_smoke"

run_test \
    "Check subject metadata staging schema" \
    "neuro_sleep.staging.subject_metadata_schema_smoke"

run_test \
    "Check production Bronze file writer" \
    "neuro_sleep.ingestion.bronze_file_writer_success_smoke"

run_test \
    "Check Sleep-EDF checksum manifest" \
    "neuro_sleep.sources.sleep_edf_manifest_smoke"
run_test \
    "Check Sleep-EDF source configuration" \
    "neuro_sleep.sources.sleep_edf"

echo "All core smoke tests completed."
