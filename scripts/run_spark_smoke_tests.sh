#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

HADOOP_AWS_PACKAGE="${HADOOP_AWS_PACKAGE:-org.apache.hadoop:hadoop-aws:3.5.0}"
export PYSPARK_SUBMIT_ARGS="--packages ${HADOOP_AWS_PACKAGE} pyspark-shell"

echo "Running NeuroSleep Spark smoke tests..."
echo

echo "Compile Python source"
python -m compileall -q src
echo "python_compilation_status=success"
echo

echo "1/3 Check Spark runtime"
python -m neuro_sleep.spark.runtime_smoke
echo

echo "2/3 Check fail-closed Silver signal input selection"
python -m neuro_sleep.spark.signal_input_smoke
echo

echo "3/3 Reconcile selected Silver signals through Spark + S3A"
python -m neuro_sleep.spark.signal_read_reconciliation
echo

echo "All Spark smoke tests completed."
