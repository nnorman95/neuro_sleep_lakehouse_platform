#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

echo "Running NeuroSleep Spark smoke tests..."
echo

echo "Compile Python source"
python -m compileall -q src
echo "python_compilation_status=success"
echo

echo "1/2 Check Spark runtime"
python -m neuro_sleep.spark.runtime_smoke
echo

echo "2/2 Check fail-closed Silver signal input selection"
python -m neuro_sleep.spark.signal_input_smoke
echo

echo "All Spark smoke tests completed."
