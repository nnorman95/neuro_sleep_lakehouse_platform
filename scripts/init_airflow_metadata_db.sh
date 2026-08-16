#!/bin/bash
set -euo pipefail

./scripts/ensure_airflow_env.sh
set -a
source .env
set +a

for name in POSTGRES_USER AIRFLOW_DB_USER AIRFLOW_DB_PASSWORD AIRFLOW_DB_NAME; do
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required environment variable: $name" >&2
    exit 1
  fi
done

ready=false
for attempt in $(seq 1 30); do
  if docker compose exec -T postgres pg_isready -U "${POSTGRES_USER}" -d postgres >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
[[ "$ready" == "true" ]] || { echo "PostgreSQL did not become ready." >&2; exit 1; }

docker compose exec -T postgres psql \
  -v ON_ERROR_STOP=1 \
  -v airflow_user="${AIRFLOW_DB_USER}" \
  -v airflow_password="${AIRFLOW_DB_PASSWORD}" \
  -v airflow_db="${AIRFLOW_DB_NAME}" \
  -U "${POSTGRES_USER}" -d postgres <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'airflow_user', :'airflow_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'airflow_user')
\gexec
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'airflow_user', :'airflow_password')
\gexec
SELECT format('CREATE DATABASE %I OWNER %I ENCODING ''UTF8''', :'airflow_db', :'airflow_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'airflow_db')
\gexec
SELECT format('ALTER DATABASE %I OWNER TO %I', :'airflow_db', :'airflow_user')
\gexec
SQL

docker compose exec -T postgres psql \
  -v ON_ERROR_STOP=1 \
  -v airflow_user="${AIRFLOW_DB_USER}" \
  -U "${POSTGRES_USER}" -d "${AIRFLOW_DB_NAME}" <<'SQL'
SELECT format('GRANT ALL ON SCHEMA public TO %I', :'airflow_user')
\gexec
SQL

owner="$(docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d postgres -tAc "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='${AIRFLOW_DB_NAME}'" | tr -d '[:space:]')"
[[ "$owner" == "${AIRFLOW_DB_USER}" ]] || { echo "Unexpected Airflow DB owner: $owner" >&2; exit 1; }

echo "airflow_metadata_db=${AIRFLOW_DB_NAME}"
echo "airflow_metadata_db_owner=${owner}"
echo "airflow_metadata_db_status=success"
