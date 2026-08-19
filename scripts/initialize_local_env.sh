#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

ENV_FILE="${1:-.env}"
ENV_TEMPLATE="${2:-.env.example}"

if [[ ! -f "$ENV_TEMPLATE" ]]; then
  echo "ERROR: environment template not found: $ENV_TEMPLATE" >&2
  exit 1
fi

python_command=""
if command -v python3 >/dev/null 2>&1; then
  python_command="python3"
elif command -v python >/dev/null 2>&1; then
  python_command="python"
else
  echo "ERROR: python3/python is required to initialize the local environment." >&2
  exit 1
fi

"$python_command" - "$ENV_FILE" "$ENV_TEMPLATE" <<'PY'
from __future__ import annotations

import os
import platform
import secrets
import stat
import sys
from pathlib import Path


env_path = Path(sys.argv[1])
template_path = Path(sys.argv[2])

secret_factories = {
    "POSTGRES_PASSWORD": lambda: secrets.token_hex(24),
    "MINIO_ACCESS_KEY": lambda: "neurosleep" + secrets.token_hex(8),
    "MINIO_SECRET_KEY": lambda: secrets.token_hex(24),
    "AIRFLOW_DB_PASSWORD": lambda: secrets.token_hex(24),
    "AIRFLOW_JWT_SECRET": lambda: secrets.token_hex(32),
}

placeholder_prefix = "change_me_"


def parse(lines: list[str]) -> dict[str, tuple[int, str]]:
    values: dict[str, tuple[int, str]] = {}

    for index, line in enumerate(lines):
        stripped = line.strip()
        if (
            not stripped
            or stripped.startswith("#")
            or "=" not in line
        ):
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = (index, value.strip())

    return values


def is_placeholder(value: str) -> bool:
    return value.startswith(placeholder_prefix)


created = not env_path.exists()
generated_keys: list[str] = []

if created:
    lines = template_path.read_text(
        encoding="utf-8"
    ).splitlines()
    existing = parse(lines)

    for key, factory in secret_factories.items():
        if key not in existing:
            raise SystemExit(
                f"Environment template is missing required secret key: {key}"
            )

        index, current_value = existing[key]
        if current_value and not is_placeholder(current_value):
            raise SystemExit(
                "Environment template must not contain a real secret "
                f"for {key}"
            )

        lines[index] = f"{key}={factory()}"
        generated_keys.append(key)

    existing = parse(lines)
    airflow_uid = (
        "50000"
        if platform.system() == "Darwin"
        else str(os.getuid())
    )

    if "AIRFLOW_UID" not in existing:
        raise SystemExit(
            "Environment template is missing AIRFLOW_UID"
        )

    airflow_uid_index, _ = existing["AIRFLOW_UID"]
    lines[airflow_uid_index] = (
        f"AIRFLOW_UID={airflow_uid}"
    )

    env_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    env_path.write_text(
        "\n".join(lines).rstrip() + "\n",
        encoding="utf-8",
    )
else:
    lines = env_path.read_text(
        encoding="utf-8"
    ).splitlines()
    existing = parse(lines)
    invalid: list[str] = []

    for key in secret_factories:
        item = existing.get(key)
        if item is None:
            invalid.append(f"{key}=missing")
            continue

        value = item[1]
        if not value:
            invalid.append(f"{key}=empty")
        elif is_placeholder(value):
            invalid.append(f"{key}=placeholder")

    if invalid:
        print(
            "ERROR: existing .env contains unsafe or incomplete "
            "credential values:",
            file=sys.stderr,
        )
        for item in invalid:
            print(f"  {item}", file=sys.stderr)
        print(
            "Refusing to rewrite credentials in an existing "
            "environment because Docker volumes may already depend "
            "on them.",
            file=sys.stderr,
        )
        print(
            "For a fresh, unused checkout remove the existing .env "
            "and run this command again. Otherwise configure the "
            "reported values manually.",
            file=sys.stderr,
        )
        raise SystemExit(1)

env_path.chmod(
    stat.S_IRUSR | stat.S_IWUSR
)

print(f"local_env_file={env_path}")
print(
    "local_env_created="
    + str(created).lower()
)
print(
    "local_env_generated_secrets="
    f"{len(generated_keys)}"
)
print("local_env_status=success")
PY
