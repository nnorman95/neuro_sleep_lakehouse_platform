#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

echo "Running Phase 12 data-quality hardening validation..."
echo

echo "=== 1/4 Source compilation ==="
python -m compileall -q src
echo "phase12_source_compilation=success"
echo

echo "=== 2/4 Broken-data fixture suite ==="
make phase12-quality-smoke
echo "phase12_broken_data_fixtures=success"
echo

echo "=== 3/4 Existing Silver quality regression ==="
make silver-smoke
echo "phase12_silver_quality_regression=success"
echo

echo "=== 4/4 Repository diff hygiene ==="
git diff --check
echo "phase12_diff_hygiene=success"
echo

echo "phase12_validation_status=success"
