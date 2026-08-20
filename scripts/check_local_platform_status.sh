#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

wait_seconds=0
core_only=false

while (($#)); do
  case "$1" in
    --wait-seconds)
      if (($# < 2)); then
        echo "ERROR: --wait-seconds requires a value." >&2
        exit 2
      fi
      wait_seconds="$2"
      shift 2
      ;;
    --core-only)
      core_only=true
      shift
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if ! [[ "$wait_seconds" =~ ^[0-9]+$ ]]; then
  echo "ERROR: --wait-seconds must be a non-negative integer." >&2
  exit 2
fi

if [[ ! -f .env ]]; then
  echo "ERROR: .env is missing. Run make env-init first." >&2
  exit 1
fi

set -a
source .env
set +a

service_container_id() {
  ./scripts/airflow_compose.sh ps -q "$1" 2>/dev/null || true
}

container_state() {
  local service="$1"
  local container_id

  container_id="$(service_container_id "$service")"
  if [[ -z "$container_id" ]]; then
    printf '%s\n' "absent"
    return 0
  fi

  docker inspect \
    --format '{{.State.Status}}' \
    "$container_id" \
    2>/dev/null || printf '%s\n' "unknown"
}

container_health() {
  local service="$1"
  local container_id

  container_id="$(service_container_id "$service")"
  if [[ -z "$container_id" ]]; then
    printf '%s\n' "absent"
    return 0
  fi

  docker inspect \
    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
    "$container_id" \
    2>/dev/null || printf '%s\n' "unknown"
}

postgres_ready() {
  [[ "$(container_state postgres)" == "running" ]] \
    && ./scripts/airflow_compose.sh exec -T postgres \
      pg_isready \
      -U "${POSTGRES_USER}" \
      -d "${POSTGRES_DB}" \
      >/dev/null 2>&1
}

minio_ready() {
  [[ "$(container_state minio)" == "running" ]] \
    && curl -fsS \
      "${MINIO_ENDPOINT%/}/minio/health/live" \
      >/dev/null 2>&1
}

kafka_ready() {
  [[ "$(container_health kafka)" == "healthy" ]]
}

airflow_scheduler_ready() {
  [[ "$(container_health airflow-scheduler)" == "healthy" ]]
}

airflow_api_ready() {
  [[ "$(container_health airflow-api-server)" == "healthy" ]]
}

airflow_dag_processor_ready() {
  [[ "$(container_state airflow-dag-processor)" == "running" ]]
}

all_ready() {
  postgres_ready \
    && minio_ready \
    && kafka_ready \
    || return 1

  if [[ "$core_only" == "true" ]]; then
    return 0
  fi

  airflow_scheduler_ready \
    && airflow_api_ready \
    && airflow_dag_processor_ready
}

deadline=$((SECONDS + wait_seconds))

while ! all_ready; do
  if (( SECONDS >= deadline )); then
    break
  fi
  sleep 2
done

postgres_status="not_ready"
if postgres_ready; then
  postgres_status="ready"
fi

minio_status="not_ready"
if minio_ready; then
  minio_status="ready"
fi

kafka_status="$(container_health kafka)"

echo "platform_postgres=${postgres_status}"
echo "platform_minio=${minio_status}"
echo "platform_kafka=${kafka_status}"

ready=true
if [[ "$postgres_status" != "ready" ]] \
  || [[ "$minio_status" != "ready" ]] \
  || [[ "$kafka_status" != "healthy" ]]
then
  ready=false
fi

if [[ "$core_only" != "true" ]]; then
  scheduler_status="$(container_health airflow-scheduler)"
  api_status="$(container_health airflow-api-server)"
  dag_processor_status="$(container_state airflow-dag-processor)"

  echo "platform_airflow_scheduler=${scheduler_status}"
  echo "platform_airflow_api_server=${api_status}"
  echo "platform_airflow_dag_processor=${dag_processor_status}"

  if [[ "$scheduler_status" != "healthy" ]] \
    || [[ "$api_status" != "healthy" ]] \
    || [[ "$dag_processor_status" != "running" ]]
  then
    ready=false
  fi
fi

if [[ "$ready" != "true" ]]; then
  echo "local_platform_status=not_ready"
  exit 1
fi

if [[ "$core_only" == "true" ]]; then
  echo "local_platform_scope=core"
else
  echo "local_platform_scope=full"
fi

echo "local_platform_status=ready"
