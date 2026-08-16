#!/bin/bash
set -euo pipefail

exec docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  "$@"
