#!/bin/bash
set -euo pipefail

if [[ ! -f ".env" ]]; then
  echo ".env is required before Airflow bootstrap." >&2
  exit 1
fi

python - <<'PY'
from pathlib import Path
import os
import platform
import secrets

path = Path(".env")
lines = path.read_text(encoding="utf-8").splitlines()
existing = {}

for index, line in enumerate(lines):
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in line:
        continue

    key, value = line.split("=", 1)
    existing[key.strip()] = (index, value)

defaults = {
    "AIRFLOW_IMAGE": (
        "apache/airflow@sha256:"
        "0c4bcc0370e526de1b7892a3bf4343d260c6c82359c66f77155b53cd773d6339"
    ),
    "AIRFLOW_RUNTIME_IMAGE": "neurosleep-airflow:phase10",
    "AIRFLOW_UID": "50000" if platform.system() == "Darwin" else str(os.getuid()),
    "AIRFLOW_DB_USER": "airflow",
    "AIRFLOW_DB_NAME": "airflow",
    "AIRFLOW_DB_PASSWORD": secrets.token_hex(24),
    "AIRFLOW_ADMIN_USERNAME": "admin",
    "AIRFLOW_JWT_SECRET": secrets.token_hex(32),
}

changed = []

for key, value in defaults.items():
    if key in existing and existing[key][1].strip():
        continue

    if key in existing:
        index, _ = existing[key]
        lines[index] = f"{key}={value}"
    else:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(f"{key}={value}")

    changed.append(key)

path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

print(f"airflow_env_generated_keys={len(changed)}")
print("airflow_env_status=success")
PY
