#!/bin/bash
set -euo pipefail

set -a
source .env
set +a

echo "Running NeuroSleep Airflow foundation smoke tests..."
version="$(./scripts/airflow_compose.sh exec -T airflow-scheduler airflow version | tail -n 1 | tr -d '\r')"
executor="$(./scripts/airflow_compose.sh exec -T airflow-scheduler airflow config get-value core executor | tail -n 1 | tr -d '\r')"
[[ "$version" == "3.3.1" ]] || { echo "Unexpected Airflow version: $version" >&2; exit 1; }
[[ "$executor" == "LocalExecutor" ]] || { echo "Unexpected executor: $executor" >&2; exit 1; }
echo "airflow_version_check=success"
echo "airflow_executor_check=success"

execution_api_url="$(
  ./scripts/airflow_compose.sh exec -T airflow-scheduler     airflow config get-value core execution_api_server_url | tail -n 1 | tr -d ''
)"
parallelism="$(
  ./scripts/airflow_compose.sh exec -T airflow-scheduler     airflow config get-value core parallelism | tail -n 1 | tr -d ''
)"

if [[ "$execution_api_url" != "http://airflow-api-server:8080/execution/" ]]; then
  echo "Unexpected execution API URL: $execution_api_url" >&2
  exit 1
fi
if [[ "$parallelism" != "2" ]]; then
  echo "Unexpected Airflow parallelism: $parallelism" >&2
  exit 1
fi

echo "airflow_execution_api_url_check=success"
echo "airflow_parallelism_check=success"

./scripts/airflow_compose.sh exec -T airflow-scheduler airflow db check
echo "airflow_metadata_db_check=success"

health_file="$(mktemp)"
trap 'rm -f "$health_file"' EXIT
curl -fsS http://localhost:8080/api/v2/monitor/health > "$health_file"
python - "$health_file" <<'PY'
import json, sys
from pathlib import Path
payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
for component in ("metadatabase", "scheduler", "dag_processor"):
    value = payload.get(component)
    if not isinstance(value, dict) or value.get("status") != "healthy":
        raise SystemExit(f"Unhealthy Airflow component: {component}={value!r}")
print("airflow_component_health_check=success")
PY

import_errors="$(./scripts/airflow_compose.sh exec -T airflow-scheduler airflow dags list-import-errors -l -o json)"
python - "$import_errors" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
if payload:
    raise SystemExit(f"Airflow DAG import errors: {payload!r}")
print("airflow_dag_import_check=success")
PY

dag_list="$(./scripts/airflow_compose.sh exec -T airflow-scheduler airflow dags list -l -o json)"
python - "$dag_list" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
dag_ids = {row.get("dag_id") for row in payload if isinstance(row, dict)}
if "neurosleep_airflow_smoke" not in dag_ids:
    raise SystemExit("neurosleep_airflow_smoke was not discovered")
print("airflow_smoke_dag_discovery=success")
PY

./scripts/airflow_compose.sh exec -T airflow-scheduler airflow dags test --use-executor neurosleep_airflow_smoke

echo "airflow_foundation_smoke_status=success"
