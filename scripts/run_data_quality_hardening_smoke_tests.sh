#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

echo "Running Phase 12 data-quality hardening smoke tests..."
echo

echo "Compile Phase 12 quality fixture"
python -m py_compile \
  src/neuro_sleep/quality/schema_drift_smoke.py
echo "phase12_quality_python_compilation=success"
echo

echo "1/1 Check fail-closed Silver schema drift"
python -m neuro_sleep.quality.schema_drift_smoke
echo

echo "phase12_quality_smoke_status=success"
