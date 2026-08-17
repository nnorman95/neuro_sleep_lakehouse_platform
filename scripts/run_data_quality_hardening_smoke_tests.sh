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

echo "Compile Phase 12 quality fixtures"
python -m py_compile \
  src/neuro_sleep/quality/schema_drift_smoke.py \
  src/neuro_sleep/quality/manifest_integrity_smoke.py \
  src/neuro_sleep/quality/publication_consistency_smoke.py \
  src/neuro_sleep/quality/subject_metadata_identity_smoke.py
echo "phase12_quality_python_compilation=success"
echo

echo "1/4 Check fail-closed Silver schema drift"
python -m neuro_sleep.quality.schema_drift_smoke
echo

echo "2/4 Check fail-closed Silver manifest integrity"
python -m neuro_sleep.quality.manifest_integrity_smoke
echo

echo "3/4 Check fail-closed Silver publication consistency"
python -m neuro_sleep.quality.publication_consistency_smoke
echo

echo "4/4 Check fail-closed subject metadata identity"
python -m neuro_sleep.quality.subject_metadata_identity_smoke
echo

echo "phase12_quality_smoke_status=success"
