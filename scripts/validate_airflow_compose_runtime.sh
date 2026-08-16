#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
    pwd
)"

cd "$PROJECT_ROOT"

resolve_runtime_image() {
    local file value

    if [[ -n "${AIRFLOW_RUNTIME_IMAGE:-}" ]]; then
        printf '%s\n' "$AIRFLOW_RUNTIME_IMAGE"
        return 0
    fi

    for file in ".env" ".env.example"; do
        [[ -f "$file" ]] || continue

        value="$(
            awk -F= '
                /^AIRFLOW_RUNTIME_IMAGE=/ {
                    sub(/^[^=]*=/, "")
                    print
                }
            ' "$file" | tail -n 1
        )"

        if [[ -n "$value" ]]; then
            printf '%s\n' "$value"
            return 0
        fi
    done

    printf '%s\n' "neurosleep-airflow:phase10"
}

wait_for_service() {
    local service="$1"
    local expected="$2"
    local attempts="${3:-30}"
    local container_id status

    for ((i = 1; i <= attempts; i++)); do
        container_id="$(./scripts/airflow_compose.sh ps -q "$service" 2>/dev/null || true)"

        if [[ -n "$container_id" ]]; then
            status="$(
                docker inspect \
                    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
                    "$container_id" 2>/dev/null || true
            )"

            if [[ "$status" == "$expected" ]]; then
                printf '%s: %s\n' "$service" "$status"
                return 0
            fi
        fi

        sleep 2
    done

    echo "ERROR: ${service} did not reach ${expected} state." >&2
    ./scripts/airflow_compose.sh ps >&2 || true
    return 1
}

RUNTIME_IMAGE="$(resolve_runtime_image)"

echo "=== COMPOSE CONFIG ==="
./scripts/airflow_compose.sh config --quiet
echo "Compose config: OK"

echo
echo "=== RUNTIME IMAGE ==="
docker image inspect "$RUNTIME_IMAGE" >/dev/null
echo "$RUNTIME_IMAGE: present locally"

echo
echo "=== AIRFLOW SERVICE READINESS ==="
wait_for_service airflow-scheduler healthy
wait_for_service airflow-api-server healthy
wait_for_service airflow-dag-processor running

echo
echo "=== AIRFLOW SERVICES ==="
./scripts/airflow_compose.sh ps

echo
echo "=== SCHEDULER EXECUTION ENV ==="
./scripts/airflow_compose.sh exec -T airflow-scheduler bash -lc '
set -euo pipefail

cd /opt/neurosleep

test ! -e .env

[[ "${POSTGRES_HOST}" == "postgres" ]]
[[ "${POSTGRES_PORT}" == "5432" ]]
[[ "${MINIO_ENDPOINT}" == "http://minio:9000" ]]

echo "POSTGRES_HOST=${POSTGRES_HOST}"
echo "POSTGRES_PORT=${POSTGRES_PORT}"
echo "MINIO_ENDPOINT=${MINIO_ENDPOINT}"
echo ".env: absent"

echo
echo "=== SETTINGS ==="
python - <<'"'"'PY'"'"'
from neuro_sleep.config import get_settings

settings = get_settings()
safe = settings.safe_dict()

assert safe["postgres_host"] == "postgres"
assert safe["postgres_port"] == 5432
assert safe["minio_endpoint"] == "http://minio:9000"

print("Settings container topology: OK")
print("postgres_host:", safe["postgres_host"])
print("postgres_port:", safe["postgres_port"])
print("minio_endpoint:", safe["minio_endpoint"])
PY

echo
echo "=== POSTGRES TCP ==="
python - <<'"'"'PY'"'"'
import socket
import time

last_error = None

for _ in range(20):
    try:
        with socket.create_connection(("postgres", 5432), timeout=2):
            print("postgres:5432 reachable")
            break
    except OSError as exc:
        last_error = exc
        time.sleep(1)
else:
    raise SystemExit(f"postgres:5432 unreachable: {last_error}")
PY

echo
echo "=== DBT DEBUG ==="
bash scripts/run_dbt.sh debug

echo
echo "=== MINIO API ==="
python - <<'"'"'PY'"'"'
import os
import time

import boto3

client = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    region_name="us-east-1",
)

last_error = None

for _ in range(20):
    try:
        response = client.list_buckets()
        print("MinIO API: OK")
        print("bucket_count:", len(response.get("Buckets", [])))
        break
    except Exception as exc:
        last_error = exc
        time.sleep(1)
else:
    raise SystemExit(f"MinIO API unavailable: {last_error}")
PY
'
