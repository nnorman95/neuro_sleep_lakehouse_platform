#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

echo "Bootstrapping NeuroSleep Airflow runtime..."
echo

./scripts/ensure_airflow_env.sh
set -a
source .env
set +a

runtime_image="${AIRFLOW_RUNTIME_IMAGE:-neurosleep-airflow:phase10}"

echo "1/7 Start PostgreSQL"
./scripts/airflow_compose.sh up -d postgres

echo
echo "2/7 Initialize Airflow metadata DB"
./scripts/init_airflow_metadata_db.sh

echo
echo "3/7 Ensure NeuroSleep Airflow runtime image"
if docker image inspect "$runtime_image" >/dev/null 2>&1; then
  echo "airflow_runtime_image=${runtime_image}"
  echo "airflow_runtime_image_status=existing"
  image_built=false
else
  ./scripts/build_airflow_runtime_image.sh "$runtime_image"
  echo "airflow_runtime_image=${runtime_image}"
  echo "airflow_runtime_image_status=built"
  image_built=true
fi

echo
echo "4/7 Validate newly built Airflow runtime image"
if [[ "$image_built" == "true" ]]; then
  ./scripts/validate_airflow_runtime_image.sh "$runtime_image"
  echo "airflow_runtime_image_validation=success"
else
  echo "airflow_runtime_image_validation=skipped_existing"
fi

echo
echo "5/7 Prepare Airflow state volume"
./scripts/prepare_airflow_state.sh

echo
echo "6/7 Run Airflow migrations"
./scripts/stop_airflow_local.sh
./scripts/airflow_compose.sh up \
  --no-deps \
  --abort-on-container-exit \
  airflow-init

echo
echo "7/7 Start Airflow services"
mkdir -p logs/airflow
./scripts/airflow_compose.sh up -d --no-deps \
  airflow-scheduler \
  airflow-dag-processor \
  airflow-api-server

api_ready=false
for _ in $(seq 1 60); do
  if curl -fsS http://localhost:8080/api/v2/version >/dev/null 2>&1; then
    api_ready=true
    break
  fi

  sleep 2
done

if [[ "$api_ready" != "true" ]]; then
  echo "Airflow API did not become ready." >&2
  ./scripts/airflow_compose.sh ps
  exit 1
fi

version="$(
  ./scripts/airflow_compose.sh exec -T airflow-scheduler \
    airflow version | tail -n 1 | tr -d '\r'
)"
executor="$(
  ./scripts/airflow_compose.sh exec -T airflow-scheduler \
    airflow config get-value core executor | tail -n 1 | tr -d '\r'
)"

echo "airflow_version=${version}"
echo "airflow_executor=${executor}"
echo "airflow_api=http://localhost:8080"
echo "airflow_bootstrap_status=success"
