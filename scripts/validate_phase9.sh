#!/usr/bin/env bash
set -euo pipefail

echo "Running NeuroSleep Phase 9 regression..."
echo

echo "1/6 Core + reliability + Silver + Spark smoke suites"
make test
echo

echo "2/6 Full Spark signal feature validation"
make spark-feature-check
echo

echo "3/6 Source Gold publication validation"
make gold-signal-features-check
echo

echo "4/6 Gold + Warehouse feature integration validation"
make feature-integration-check
echo

echo "5/6 Integrated Gold publication validation"
make integrated-signal-features-check
echo

echo "6/6 Relational regression"
./scripts/run_dbt.sh build
echo

echo "phase9_regression_status=success"
