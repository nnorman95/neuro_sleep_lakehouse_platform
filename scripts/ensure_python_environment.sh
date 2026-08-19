#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

VENV_DIR="${NEUROSLEEP_VENV_DIR:-$PROJECT_ROOT/.venv}"
FINGERPRINT_FILE="$VENV_DIR/.neurosleep_dependency_fingerprint"

bootstrap_python=""
if command -v python3 >/dev/null 2>&1; then
  bootstrap_python="python3"
elif command -v python >/dev/null 2>&1; then
  bootstrap_python="python"
else
  echo "ERROR: python3/python is required." >&2
  exit 1
fi

created=false

if [[ ! -d "$VENV_DIR" ]]; then
  echo "Creating virtual environment: $VENV_DIR"
  "$bootstrap_python" -m venv "$VENV_DIR"
  created=true
fi

VENV_PYTHON="$VENV_DIR/bin/python"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "ERROR: virtual environment is incomplete: $VENV_DIR" >&2
  echo "Remove it and run make python-env again." >&2
  exit 1
fi

python_version="$(
  "$VENV_PYTHON" -c \
    'import sys; print(".".join(map(str, sys.version_info[:3])))'
)"

if ! "$VENV_PYTHON" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
then
  echo "ERROR: .venv must use Python >= 3.11; found $python_version" >&2
  echo "Remove .venv and recreate it with a supported Python interpreter." >&2
  exit 1
fi

if ! "$VENV_PYTHON" -m pip --version >/dev/null 2>&1; then
  echo "Installing pip into the virtual environment..."
  "$VENV_PYTHON" -m ensurepip --upgrade
fi

"$VENV_PYTHON" scripts/validate_dependency_contract.py

current_fingerprint="$(
  "$VENV_PYTHON" - <<'PY'
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

digest = hashlib.sha256()

for path in (
    Path("pyproject.toml"),
    Path("requirements.txt"),
):
    digest.update(path.name.encode("utf-8"))
    digest.update(b"\0")
    digest.update(path.read_bytes())
    digest.update(b"\0")

digest.update(
    f"python={sys.version_info.major}.{sys.version_info.minor}".encode(
        "utf-8"
    )
)

print(digest.hexdigest())
PY
)"

stored_fingerprint=""
if [[ -f "$FINGERPRINT_FILE" ]]; then
  stored_fingerprint="$(
    tr -d '[:space:]' < "$FINGERPRINT_FILE"
  )"
fi

project_import_ok=false
if "$VENV_PYTHON" - <<'PY' >/dev/null 2>&1
from pathlib import Path
import neuro_sleep

project_root = Path.cwd().resolve()
package_path = Path(neuro_sleep.__file__).resolve()

raise SystemExit(
    0
    if package_path.is_relative_to(project_root / "src")
    else 1
)
PY
then
  project_import_ok=true
fi

dependencies_installed=false

if [[ "$stored_fingerprint" != "$current_fingerprint" ]] \
  || [[ "$project_import_ok" != "true" ]]
then
  echo "Installing NeuroSleep Python dependencies..."
  "$VENV_PYTHON" -m pip install \
    --disable-pip-version-check \
    -e .

  printf '%s\n' "$current_fingerprint" \
    > "$FINGERPRINT_FILE"

  dependencies_installed=true
else
  echo "Python dependency fingerprint unchanged; install skipped."
fi

"$VENV_PYTHON" -m pip check

"$VENV_PYTHON" - <<'PY'
from pathlib import Path
import neuro_sleep

project_root = Path.cwd().resolve()
package_path = Path(neuro_sleep.__file__).resolve()

if not package_path.is_relative_to(project_root / "src"):
    raise SystemExit(
        "Editable neuro_sleep package does not point at this checkout"
    )

print(f"python_project_import={package_path}")
PY

echo "python_env_path=$VENV_DIR"
echo "python_env_version=$python_version"
echo "python_env_created=$created"
echo "python_env_dependencies_installed=$dependencies_installed"
echo "python_env_status=success"
