#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

echo "Running Gold publication reliability smoke tests..."
echo

python -m neuro_sleep.gold.publication_recovery_smoke

echo
echo "Gold publication reliability smoke tests completed."
