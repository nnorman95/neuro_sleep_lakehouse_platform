#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

HADOOP_AWS_PACKAGE="${HADOOP_AWS_PACKAGE:-org.apache.hadoop:hadoop-aws:3.5.0}"
export PYSPARK_SUBMIT_ARGS="--packages ${HADOOP_AWS_PACKAGE} pyspark-shell"

echo "Running NeuroSleep Phase 9 feature integration validation..."
echo

echo "1/2 Check left-join semantics on synthetic data"
python -m neuro_sleep.spark.feature_integration_smoke

echo
echo "2/2 Validate integration on current Gold + Warehouse data"
python -m neuro_sleep.spark.feature_integration_validation

echo
echo "Feature integration validation completed."
