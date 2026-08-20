#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

echo "Starting NeuroSleep full local platform..."
echo

echo "1/6 Preparing local configuration"
./scripts/initialize_local_env.sh
./scripts/ensure_python_environment.sh

export VIRTUAL_ENV="$PROJECT_ROOT/.venv"
export PATH="$VIRTUAL_ENV/bin:$PATH"
hash -r

set -a
source .env
set +a

runtime_image="${AIRFLOW_RUNTIME_IMAGE:-neurosleep-airflow:phase10}"

if ! docker image inspect "$runtime_image" >/dev/null 2>&1; then
  echo "ERROR: Airflow runtime image is missing: $runtime_image" >&2
  echo "Run 'make airflow-bootstrap' for first-time Airflow initialization." >&2
  exit 1
fi
echo "airflow_runtime_image=${runtime_image}"
echo

echo "2/6 Starting PostgreSQL, MinIO, and Kafka"
./scripts/airflow_compose.sh up -d postgres minio kafka
echo

echo "3/6 Waiting for core services"
./scripts/check_local_platform_status.sh \
  --core-only \
  --wait-seconds 120
echo

echo "4/6 Initializing Kafka topics"
python scripts/init_kafka_topics.py
echo

echo "5/6 Starting Airflow services"
./scripts/airflow_compose.sh up -d --no-deps \
  airflow-scheduler \
  airflow-dag-processor \
  airflow-api-server
echo

echo "6/6 Waiting for full platform readiness"
./scripts/check_local_platform_status.sh \
  --wait-seconds 180

echo
echo "local_platform_start_status=success"
