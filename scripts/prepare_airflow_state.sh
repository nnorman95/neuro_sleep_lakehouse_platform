#!/bin/bash
set -euo pipefail

if [[ ! -f ".env" ]]; then
  echo ".env is required before Airflow state initialization." >&2
  exit 1
fi

set -a
source .env
set +a

if [[ -z "${AIRFLOW_UID:-}" ]]; then
  echo "AIRFLOW_UID is required." >&2
  exit 1
fi

echo "Preparing Airflow state volume for uid=${AIRFLOW_UID}..."

./scripts/airflow_compose.sh run \
  --rm \
  --no-deps \
  --user 0:0 \
  --entrypoint /bin/bash \
  airflow-init \
  -c "set -euo pipefail
      chown -R ${AIRFLOW_UID}:0 /opt/airflow/state
      chmod 0770 /opt/airflow/state
      stat -c 'airflow_state_owner=%u:%g mode=%a' /opt/airflow/state"

./scripts/airflow_compose.sh run \
  --rm \
  --no-deps \
  --entrypoint /bin/bash \
  airflow-init \
  -c 'set -euo pipefail
      test -w /opt/airflow/state
      touch /opt/airflow/state/.permission_smoke
      rm /opt/airflow/state/.permission_smoke
      echo airflow_state_write_test=success'

echo "airflow_state_permission_status=success"
