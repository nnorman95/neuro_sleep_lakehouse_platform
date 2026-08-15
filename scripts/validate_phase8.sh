#!/usr/bin/env bash
set -euo pipefail

echo "Running NeuroSleep Phase 8 regression..."
echo

echo "1/4 Core + reliability + Silver + Spark smoke suites"
make test
echo

echo "2/4 Full Spark signal feature validation"
make spark-feature-check
echo

echo "3/4 Gold publication validation"
make gold-signal-features-check
echo

echo "4/4 Relational regression"
./scripts/run_dbt.sh build
echo

echo "phase8_regression_status=success"
