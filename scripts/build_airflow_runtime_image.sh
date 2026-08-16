#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
    pwd
)"

cd "$PROJECT_ROOT"

resolve_env_value() {
    local name="$1"
    local current_value="${!name:-}"
    local file value

    if [[ -n "$current_value" ]]; then
        printf '%s\n' "$current_value"
        return 0
    fi

    for file in ".env" ".env.example"; do
        [[ -f "$file" ]] || continue

        value="$(
            awk -F= -v key="$name" '
                $1 == key {
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

    return 1
}

AIRFLOW_BASE_IMAGE="$(resolve_env_value AIRFLOW_IMAGE || true)"
IMAGE_TAG="${1:-$(resolve_env_value AIRFLOW_RUNTIME_IMAGE || true)}"
IMAGE_TAG="${IMAGE_TAG:-neurosleep-airflow:phase10}"

if [[ -z "$AIRFLOW_BASE_IMAGE" ]]; then
    echo "ERROR: AIRFLOW_IMAGE is not set in the environment, .env, or .env.example." >&2
    exit 1
fi

echo "=== AIRFLOW BASE IMAGE ==="
echo "$AIRFLOW_BASE_IMAGE"

echo
echo "=== BUILD TARGET ==="
echo "$IMAGE_TAG"

docker build \
    --build-arg "AIRFLOW_IMAGE=${AIRFLOW_BASE_IMAGE}" \
    --file Dockerfile.airflow \
    --tag "$IMAGE_TAG" \
    "$PROJECT_ROOT"
