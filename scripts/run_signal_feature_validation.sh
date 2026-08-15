#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

HADOOP_AWS_PACKAGE="${HADOOP_AWS_PACKAGE:-org.apache.hadoop:hadoop-aws:3.5.0}"
export PYSPARK_SUBMIT_ARGS="--packages ${HADOOP_AWS_PACKAGE} pyspark-shell"

echo "Running NeuroSleep Spark signal feature validation..."
echo

echo "1/2 Check signal feature math on synthetic data"
python -m neuro_sleep.spark.signal_features_smoke
echo

echo "2/2 Validate features on selected Silver signals"
python -m neuro_sleep.spark.signal_feature_validation
echo

echo "Spark signal feature validation completed."
