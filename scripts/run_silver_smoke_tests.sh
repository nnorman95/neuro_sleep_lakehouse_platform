#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

current=0
total=17


run_test() {
    local title="$1"
    local module="$2"

    current=$((current + 1))

    echo "${current}/${total} ${title}"
    python -m "${module}"
    echo
}


echo "Running NeuroSleep Silver smoke tests..."
echo

echo "Compile Python source"
python -m compileall -q src
echo "python_compilation_status=success"
echo
run_test \
    "Check Bronze EDF reader" \
    "neuro_sleep.silver.bronze_edf_reader_smoke"

run_test \
    "Check Silver domain models" \
    "neuro_sleep.silver.models_smoke"

run_test \
    "Check Hypnogram parser" \
    "neuro_sleep.silver.hypnogram_parser_smoke"

run_test \
    "Check epoch expansion" \
    "neuro_sleep.silver.epoch_expander_smoke"

run_test \
    "Check PSG metadata parser" \
    "neuro_sleep.silver.psg_metadata_parser_smoke"

run_test \
    "Check recording bundle builder" \
    "neuro_sleep.silver.recording_builder_smoke"

run_test \
    "Check signal chunk extraction" \
    "neuro_sleep.silver.signal_extractor_smoke"

run_test \
    "Check Parquet schemas" \
    "neuro_sleep.silver.parquet_schemas_smoke"

run_test \
    "Check Arrow table builders" \
    "neuro_sleep.silver.parquet_tables_smoke"

run_test \
    "Check Silver object writer" \
    "neuro_sleep.silver.silver_object_writer_smoke"

run_test \
    "Check Silver recording writer" \
    "neuro_sleep.silver.silver_recording_writer_smoke"

run_test \
    "Check Silver quality gate" \
    "neuro_sleep.silver.quality_checks_smoke"

run_test \
    "Check Silver idempotency" \
    "neuro_sleep.silver.idempotency_smoke"

run_test \
    "Check Silver reconciliation" \
    "neuro_sleep.silver.reconciliation_smoke"

run_test \
    "Check end-to-end Silver pipeline" \
    "neuro_sleep.silver.silver_pipeline_smoke"

run_test \
    "Check Silver partial-output recovery" \
    "neuro_sleep.silver.partial_recovery_smoke"

run_test \
    "Check tracked Silver job observability" \
    "neuro_sleep.silver.silver_job_observability_smoke"

echo "All Silver smoke tests completed."
