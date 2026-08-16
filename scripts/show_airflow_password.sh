#!/bin/bash
set -euo pipefail
set -a
source .env
set +a
./scripts/airflow_compose.sh exec -T airflow-api-server python - "/opt/airflow/state/simple_auth_manager_passwords.json.generated" "${AIRFLOW_ADMIN_USERNAME:-admin}" <<'PY'
import json, sys
from pathlib import Path
path = Path(sys.argv[1]); username = sys.argv[2]
if not path.exists():
    raise SystemExit("Airflow password file has not been generated yet")
payload = json.loads(path.read_text(encoding="utf-8"))
password = payload.get(username)
if not password:
    raise SystemExit(f"No password found for {username!r}")
print(f"airflow_username={username}")
print(f"airflow_password={password}")
PY
