#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo "Running NeuroSleep lightweight CI checks..."
echo

echo "1/8 Check Python runtime"
python - <<'PY'
import sys

minimum = (3, 11)
current = sys.version_info[:2]

if current < minimum:
    raise SystemExit(
        "Python 3.11+ is required; "
        f"found {sys.version.split()[0]}"
    )

print(f"ci_python_version={sys.version.split()[0]}")
print("ci_python_runtime=success")
PY
echo

echo "2/8 Validate dependency contract"
python scripts/validate_dependency_contract.py
echo "ci_dependency_contract=success"
echo
echo "3/8 Validate command/config contract"
python scripts/validate_command_config_contract.py
echo "ci_command_config_contract=success"
echo

echo "4/8 Validate SQL manifest contract"
python scripts/validate_sql_manifest_contract.py
echo "ci_sql_manifest_contract=success"
echo

echo "5/8 Compile Python sources"
PYTHONPYCACHEPREFIX="$tmp_dir/pycache" \
  python -m compileall -q \
    src \
    scripts \
    airflow/dags
echo "ci_python_compile=success"
echo

echo "6/8 Validate shell syntax"
shell_count=0
while IFS= read -r -d '' script; do
  bash -n "$script"
  shell_count=$((shell_count + 1))
done < <(
  find scripts \
    -type f \
    -name '*.sh' \
    -print0
)

if [[ "$shell_count" -eq 0 ]]; then
  echo "ERROR: no shell scripts were found." >&2
  exit 1
fi

echo "ci_shell_scripts_checked=$shell_count"
echo "ci_shell_syntax=success"
echo

echo "7/8 Run pure recording-scope regression"
PYTHONPATH=src \
  python -m neuro_sleep.spark.recording_scope_smoke
echo "ci_recording_scope_regression=success"
echo

echo "8/8 Check repository hygiene"
if git ls-files --error-unmatch .env >/dev/null 2>&1; then
  echo "ERROR: .env must not be tracked." >&2
  exit 1
fi

tracked_generated="$(
  git ls-files \
    | grep -E \
      '(^|/)(__pycache__/|[^/]+[.]py[co]$|[.]DS_Store$)' \
    || true
)"

if [[ -n "$tracked_generated" ]]; then
  echo "ERROR: generated files are tracked:" >&2
  printf '%s\n' "$tracked_generated" >&2
  exit 1
fi

echo "ci_env_not_tracked=true"
echo "ci_generated_files_not_tracked=true"
echo "ci_repository_hygiene=success"
echo

echo "lightweight_ci_status=success"
