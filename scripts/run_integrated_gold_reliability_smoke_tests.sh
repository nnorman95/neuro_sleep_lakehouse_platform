#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

echo "Running integrated Gold publication reliability smoke tests..."
echo

python -m neuro_sleep.gold.integrated_publication_recovery_smoke

echo
echo "Integrated Gold publication reliability smoke tests completed."
