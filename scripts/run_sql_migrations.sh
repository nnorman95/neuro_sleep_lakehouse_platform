#!/usr/bin/env bash

set -euo pipefail

MANIFEST_FILE="${1:-scripts/sql/migrations_manifest.txt}"

if [ ! -f "${MANIFEST_FILE}" ]; then
  echo "Manifest file not found: ${MANIFEST_FILE}" >&2
  exit 1
fi

if [ -f ".env" ]; then
  set -a
  source ".env"
  set +a
fi

POSTGRES_USER="${POSTGRES_USER:-neuro_sleep}"
POSTGRES_DB="${POSTGRES_DB:-neuro_sleep}"

echo "Using manifest: ${MANIFEST_FILE}"
echo "Running SQL files against database: ${POSTGRES_DB}"

while IFS= read -r sql_file || [ -n "${sql_file}" ]; do
  if [[ -z "${sql_file}" || "${sql_file}" == \#* ]]; then
    continue
  fi

  if [ ! -f "${sql_file}" ]; then
    echo "SQL file not found: ${sql_file}" >&2
    exit 1
  fi

  echo "Running ${sql_file}"

  docker compose exec -T postgres psql \
    -v ON_ERROR_STOP=1 \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    < "${sql_file}"

done < "${MANIFEST_FILE}"

echo "All SQL files from manifest completed successfully."
