#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

current=0
total=16


run_test() {
    local title="$1"
    local module="$2"

    current=$((current + 1))

    echo "${current}/${total} ${title}"
    python -m "${module}"
    echo
}


echo "Running NeuroSleep reliability smoke tests..."
echo

echo "Compile Python source"
python -m compileall -q src
echo "python_compilation_status=success"
echo


run_test \
    "Check generic retry engine" \
    "neuro_sleep.reliability.retry_smoke"

run_test \
    "Check source HTTP retry" \
    "neuro_sleep.reliability.source_http_smoke"

run_test \
    "Check PostgreSQL connection retry" \
    "neuro_sleep.reliability.database_retry_smoke"

run_test \
    "Check object-storage retry" \
    "neuro_sleep.reliability.object_storage_retry_smoke"

run_test \
    "Check Sleep-EDF download retry" \
    "neuro_sleep.ingestion.sleep_edf_http_downloader_smoke"

run_test \
    "Check bronze writer failures" \
    "neuro_sleep.ingestion.bronze_file_writer_failure_smoke"

run_test \
    "Check verified-object recovery" \
    "neuro_sleep.ingestion.sleep_edf_object_recovery_smoke"

run_test \
    "Check file-task recovery" \
    "neuro_sleep.ingestion.sleep_edf_file_task_recovery_smoke"

run_test \
    "Check structured logging" \
    "neuro_sleep.observability.structured_logging_smoke"

run_test \
    "Check pipeline heartbeat" \
    "neuro_sleep.observability.pipeline_heartbeat_smoke"

run_test \
    "Check download progress" \
    "neuro_sleep.observability.download_progress_smoke"

run_test \
    "Check parallel-run protection" \
    "neuro_sleep.ops.pipeline_lock_smoke"

run_test \
    "Check file-attempt database history" \
    "neuro_sleep.ops.file_attempt_smoke"

run_test \
    "Check file-attempt integration" \
    "neuro_sleep.ingestion.sleep_edf_file_attempt_integration_smoke"

run_test \
    "Check Extract failure observability" \
    "neuro_sleep.ingestion.sleep_edf_extract_observability_smoke"

run_test \
    "Check Bronze reconciliation" \
    "neuro_sleep.reconciliation.bronze_reconciliation_smoke"


echo "All reliability smoke tests completed."
