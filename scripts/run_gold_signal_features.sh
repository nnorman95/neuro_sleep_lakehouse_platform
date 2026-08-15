#!/usr/bin/env bash

set -euo pipefail

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

HADOOP_AWS_PACKAGE="${HADOOP_AWS_PACKAGE:-org.apache.hadoop:hadoop-aws:3.5.0}"
export PYSPARK_SUBMIT_ARGS="--packages ${HADOOP_AWS_PACKAGE} pyspark-shell"

python -m neuro_sleep.spark.signal_feature_job
