#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
    pwd
)"
IMAGE_TAG="${1:-neurosleep-airflow:phase10}"

cd "$PROJECT_ROOT"

resolve_airflow_image() {
    if [[ -n "${AIRFLOW_IMAGE:-}" ]]; then
        printf '%s\n' "$AIRFLOW_IMAGE"
        return 0
    fi

    local file value

    for file in ".env" ".env.example"; do
        [[ -f "$file" ]] || continue

        value="$(
            awk -F= '
                /^AIRFLOW_IMAGE=/ {
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

AIRFLOW_BASE_IMAGE="$(resolve_airflow_image || true)"

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
