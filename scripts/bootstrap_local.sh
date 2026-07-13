#!/usr/bin/env bash

set -euo pipefail

echo "Bootstrapping NeuroSleep local environment..."
echo

if [ ! -f ".env" ]; then
  echo ".env file not found."
  echo "Creating .env from .env.example"
  cp .env.example .env
fi

echo "1/5 Starting Docker services"
docker compose up -d postgres minio
echo

echo "2/5 Checking Docker services"
docker compose ps
echo

echo "3/5 Initializing MinIO buckets"
./scripts/init_minio_buckets.sh
echo

echo "4/5 Running SQL migrations and seeds"
./scripts/run_sql_migrations.sh
echo

echo "5/5 Running smoke tests"
./scripts/run_smoke_tests.sh
echo

echo "Local bootstrap completed successfully."
