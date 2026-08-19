#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

failures=0

pass() {
  printf 'PASS  %s\n' "$1"
}

fail() {
  printf 'FAIL  %s\n' "$1" >&2
  failures=$((failures + 1))
}

require_command() {
  local command_name="$1"
  local label="$2"

  if command -v "$command_name" >/dev/null 2>&1; then
    pass "$label: $(command -v "$command_name")"
    return 0
  fi

  fail "$label: command '$command_name' not found"
  return 1
}

echo "=== NEUROSLEEP LOCAL DOCTOR ==="
echo "project_root=$PROJECT_ROOT"
echo "host_os=$(uname -s)"
echo "host_arch=$(uname -m)"
echo

echo "=== REQUIRED COMMANDS ==="
require_command git "Git" || true
require_command make "Make" || true
require_command curl "curl" || true
require_command docker "Docker CLI" || true

python_command=""
if command -v python3 >/dev/null 2>&1; then
  python_command="python3"
elif command -v python >/dev/null 2>&1; then
  python_command="python"
fi

if [[ -z "$python_command" ]]; then
  fail "Python: python3/python not found"
else
  python_version="$(
    "$python_command" -c \
      'import sys; print(".".join(map(str, sys.version_info[:3])))'
  )"
  if "$python_command" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
  then
    pass "Python >= 3.11: $python_version ($python_command)"
  else
    fail "Python >= 3.11 required; found $python_version"
  fi
fi

if command -v java >/dev/null 2>&1; then
  java_spec="$(
    java -XshowSettings:properties -version 2>&1 \
      | awk -F'= ' '/^[[:space:]]*java.specification.version = / {print $2; exit}'
  )"

  if [[ "$java_spec" == "21" ]]; then
    pass "Java 21: specification version $java_spec"
  else
    fail "Java 21 required for local Spark; found '${java_spec:-unknown}'"
  fi
else
  fail "Java 21: command 'java' not found"
fi

echo
echo "=== DOCKER RUNTIME ==="
if command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then
    pass "Docker daemon reachable"
  else
    fail "Docker daemon is not reachable; start Docker Desktop/Engine"
  fi

  if docker compose version >/dev/null 2>&1; then
    compose_version="$(docker compose version --short 2>/dev/null || true)"
    if [[ -n "$compose_version" ]]; then
      pass "Docker Compose v2: $compose_version"
    else
      pass "Docker Compose v2 available"
    fi
  else
    fail "Docker Compose v2 ('docker compose') is unavailable"
  fi
fi

echo
echo "=== REPOSITORY CONTRACT ==="
required_files=(
  ".env.example"
  "docker-compose.yml"
  "pyproject.toml"
  "requirements.txt"
  "Makefile"
  "scripts/bootstrap_local.sh"
  "scripts/run_sql_migrations.sh"
  "scripts/run_smoke_tests.sh"
)

for path in "${required_files[@]}"; do
  if [[ -f "$path" ]]; then
    pass "Required file: $path"
  else
    fail "Required file missing: $path"
  fi
done

if [[ -f ".env.example" ]] \
  && command -v docker >/dev/null 2>&1 \
  && docker compose version >/dev/null 2>&1
then
  if docker compose \
      --env-file .env.example \
      config --quiet >/dev/null 2>&1
  then
    pass "Compose configuration resolves with .env.example"
  else
    fail "Compose configuration does not resolve with .env.example"
  fi
fi

echo
echo "=== PYTHON DEPENDENCY CONTRACT ==="
if [[ -n "$python_command" ]]; then
  if "$python_command" scripts/validate_dependency_contract.py; then
    pass "pyproject.toml and requirements.txt are aligned"
  else
    fail "Python dependency contract validation failed"
  fi
fi

echo
echo "=== RESULT ==="
if (( failures > 0 )); then
  echo "local_prerequisite_failures=$failures"
  echo "local_prerequisite_status=failed"
  exit 1
fi

echo "local_prerequisite_failures=0"
echo "local_prerequisite_status=success"
