#!/usr/bin/env bash

set -euo pipefail

if [ -f ".env" ]; then
  set -a
  source ".env"
  set +a
fi

: "${MINIO_ACCESS_KEY:?MINIO_ACCESS_KEY is required. Create .env from .env.example.}"
: "${MINIO_SECRET_KEY:?MINIO_SECRET_KEY is required. Create .env from .env.example.}"

MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-minio/mc:latest}"

MINIO_CONTAINER_ID="$(docker compose ps -q minio)"

if [ -z "${MINIO_CONTAINER_ID}" ]; then
  echo "MinIO container is not running. Start it with: docker compose up -d minio" >&2
  exit 1
fi

MINIO_NETWORK="$(docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "${MINIO_CONTAINER_ID}" | head -n 1)"

if [ -z "${MINIO_NETWORK}" ]; then
  echo "Could not detect Docker network for MinIO container." >&2
  exit 1
fi

echo "Using MinIO network: ${MINIO_NETWORK}"

docker run --rm \
  --network "${MINIO_NETWORK}" \
  --entrypoint /bin/sh \
  "${MINIO_MC_IMAGE}" \
  -c "
    mc alias set local http://minio:9000 '${MINIO_ACCESS_KEY}' '${MINIO_SECRET_KEY}' &&
    mc mb --ignore-existing local/bronze local/silver local/gold local/quarantine local/logs &&
    mc ls local
  "

echo "MinIO buckets are ready."
