#!/usr/bin/env bash
set -euo pipefail

echo "Running NeuroSleep Phase 10 regression..."
echo

echo "1/6 Phase 9 regression"
./scripts/validate_phase9.sh
echo

echo "2/6 Python dependency contract"
python scripts/validate_dependency_contract.py
echo

echo "3/6 Airflow execution image"
./scripts/validate_airflow_runtime_image.sh
echo

echo "4/6 Airflow Compose runtime"
./scripts/validate_airflow_compose_runtime.sh
echo

echo "5/6 Airflow foundation"
./scripts/run_airflow_foundation_smoke.sh
echo

echo "6/6 NeuroSleep pipeline DAG contract"
./scripts/validate_airflow_pipeline_dag.sh
echo

echo "phase10_regression_status=success"
