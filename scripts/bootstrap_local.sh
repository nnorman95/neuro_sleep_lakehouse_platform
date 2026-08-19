#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

echo "Bootstrapping NeuroSleep local environment..."
echo

echo "1/7 Checking host prerequisites"
./scripts/check_local_prerequisites.sh
echo

echo "2/7 Preparing local environment"
./scripts/initialize_local_env.sh
echo

set -a
source .env
set +a

echo "3/7 Starting core Docker services"
docker compose up -d postgres minio
echo

echo "4/7 Waiting for core services"
postgres_ready=false
for _ in $(seq 1 60); do
  if docker compose exec -T postgres \
      pg_isready \
      -U "${POSTGRES_USER}" \
      -d "${POSTGRES_DB}" \
      >/dev/null 2>&1
  then
    postgres_ready=true
    break
  fi
  sleep 2
done

if [[ "$postgres_ready" != "true" ]]; then
  echo "ERROR: PostgreSQL did not become ready." >&2
  docker compose ps postgres >&2 || true
  exit 1
fi
echo "postgres_ready=true"

minio_ready=false
for _ in $(seq 1 60); do
  if curl -fsS \
      "${MINIO_ENDPOINT%/}/minio/health/live" \
      >/dev/null 2>&1
  then
    minio_ready=true
    break
  fi
  sleep 2
done

if [[ "$minio_ready" != "true" ]]; then
  echo "ERROR: MinIO did not become ready." >&2
  docker compose ps minio >&2 || true
  exit 1
fi
echo "minio_ready=true"
echo

echo "5/7 Initializing MinIO buckets"
./scripts/init_minio_buckets.sh
echo

echo "6/7 Running SQL migrations and seeds"
./scripts/run_sql_migrations.sh
echo

echo "7/7 Running core platform smoke tests"
./scripts/run_smoke_tests.sh
echo

echo "local_bootstrap_status=success"
