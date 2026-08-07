#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
    pwd
)"

DBT_PROJECT_DIR="${PROJECT_ROOT}/dbt"
DBT_PROFILES_DIR="${PROJECT_ROOT}/dbt"

cd "$PROJECT_ROOT"

if [[ -f ".env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source ".env"
    set +a
fi

if [[ $# -eq 0 ]]; then
    exec dbt
fi

case "$1" in
    --help|-h|--version|-v)
        exec dbt "$@"
        ;;
esac

dbt_command="$1"
shift

exec dbt \
    "${dbt_command}" \
    --project-dir "${DBT_PROJECT_DIR}" \
    --profiles-dir "${DBT_PROFILES_DIR}" \
    "$@"
