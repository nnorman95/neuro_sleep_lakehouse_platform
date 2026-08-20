#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

echo "Bootstrapping NeuroSleep complete local platform..."
echo

echo "1/11 Checking host prerequisites"
./scripts/check_local_prerequisites.sh
echo

echo "2/11 Preparing local environment"
./scripts/initialize_local_env.sh
echo

echo "3/11 Preparing Python environment"
./scripts/ensure_python_environment.sh
export VIRTUAL_ENV="$PROJECT_ROOT/.venv"
export PATH="$VIRTUAL_ENV/bin:$PATH"
hash -r
echo "bootstrap_python=$(command -v python)"
echo

set -a
source .env
set +a

echo "4/11 Starting PostgreSQL, MinIO, and Kafka"
./scripts/airflow_compose.sh up -d postgres minio kafka
echo

echo "5/11 Waiting for core services"
./scripts/check_local_platform_status.sh \
  --core-only \
  --wait-seconds 120
echo

echo "6/11 Initializing MinIO buckets"
./scripts/init_minio_buckets.sh
echo

echo "7/11 Running SQL migrations and seeds"
./scripts/run_sql_migrations.sh
echo

echo "8/11 Running core platform smoke tests"
./scripts/run_smoke_tests.sh
echo

echo "9/11 Initializing Kafka topics"
python scripts/init_kafka_topics.py
echo

echo "10/11 Initializing Airflow"
./scripts/bootstrap_airflow_local.sh
echo

echo "11/11 Verifying complete platform readiness"
./scripts/check_local_platform_status.sh \
  --wait-seconds 180
echo

echo "local_bootstrap_status=success"
