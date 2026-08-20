#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

echo "Stopping NeuroSleep full local platform..."

./scripts/airflow_compose.sh stop \
  airflow-api-server \
  airflow-dag-processor \
  airflow-scheduler \
  kafka \
  minio \
  postgres

echo "local_platform_stop_status=success"
