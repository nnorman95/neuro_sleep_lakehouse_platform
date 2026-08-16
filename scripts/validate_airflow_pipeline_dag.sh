#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
    pwd
)"

cd "$PROJECT_ROOT"

DAG_ID="neurosleep_lakehouse_pipeline"
DAG_FILE="airflow/dags/neurosleep_lakehouse_pipeline.py"

echo "=== DAG FILE ==="
test -f "$DAG_FILE"
python -m py_compile "$DAG_FILE"
echo "Python syntax: OK"

echo
echo "=== AIRFLOW IMPORT ERRORS ==="
import_errors="$(
    ./scripts/airflow_compose.sh exec -T airflow-scheduler \
        airflow dags list-import-errors -l -o json
)"

python - "$import_errors" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])

if payload:
    raise SystemExit(
        f"Airflow DAG import errors: {payload!r}"
    )

print("Airflow DAG import check: OK")
PY

echo
echo "=== DAG DISCOVERY ==="
dag_list="$(
    ./scripts/airflow_compose.sh exec -T airflow-scheduler \
        airflow dags list -l -o json
)"

python - "$dag_list" "$DAG_ID" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
dag_id = sys.argv[2]

dag_ids = {
    row.get("dag_id")
    for row in payload
    if isinstance(row, dict)
}

if dag_id not in dag_ids:
    raise SystemExit(
        f"{dag_id} was not discovered"
    )

print(f"{dag_id}: discovered")
PY

echo
echo "=== TASK CONTRACT ==="
task_list="$(
    ./scripts/airflow_compose.sh exec -T airflow-scheduler \
        airflow tasks list "$DAG_ID"
)"

printf '%s\n' "$task_list"

python - "$task_list" <<'PY'
import sys

expected = {
    "extract_bronze",
    "build_subject_metadata_silver",
    "build_recording_silver",
    "load_subject_metadata_staging",
    "load_recording_staging",
    "build_warehouse_and_marts",
    "build_gold_signal_features",
    "build_integrated_signal_features",
}

actual = {
    line.strip()
    for line in sys.argv[1].splitlines()
    if line.strip()
}

if actual != expected:
    raise SystemExit(
        "Unexpected DAG task set.\n"
        f"Expected: {sorted(expected)}\n"
        f"Actual:   {sorted(actual)}"
    )

print(
    "Task contract: OK "
    f"({len(actual)} tasks)"
)
PY

echo
echo "=== DAG POLICY ==="
./scripts/airflow_compose.sh exec -T airflow-scheduler \
    airflow dags details "$DAG_ID" -o json >/dev/null
echo "Airflow DAG details: OK"

echo
echo "=== DIFF CHECK ==="
git diff --check
echo "git diff --check: OK"

echo
echo "Airflow pipeline DAG validation: OK"
