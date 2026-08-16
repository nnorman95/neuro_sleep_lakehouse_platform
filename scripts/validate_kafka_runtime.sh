#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

container_name="neuro_sleep_kafka"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

host_port="${KAFKA_PORT:-9092}"

echo "=== COMPOSE CONFIG ==="
docker compose config --quiet
echo "Compose config: OK"

echo
echo "=== KAFKA SERVICE ==="
running="$(
  docker compose ps \
    --status running \
    --services kafka
)"

if [[ "$running" != "kafka" ]]; then
  echo "ERROR: Kafka service is not running." >&2
  exit 1
fi

echo "kafka: running"

echo
echo "=== KAFKA HEALTH ==="
healthy=false

for _ in $(seq 1 30); do
  health="$(
    docker inspect \
      --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
      "$container_name"
  )"

  if [[ "$health" == "healthy" ]]; then
    healthy=true
    break
  fi

  if [[ "$health" == "unhealthy" ]]; then
    docker compose logs --tail=80 kafka >&2
    echo "ERROR: Kafka became unhealthy." >&2
    exit 1
  fi

  sleep 2
done

if [[ "$healthy" != "true" ]]; then
  docker compose logs --tail=80 kafka >&2
  echo "ERROR: Kafka did not become healthy in time." >&2
  exit 1
fi

echo "kafka_health=healthy"

echo
echo "=== KAFKA VERSION ==="
version="$(
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --version \
    | tail -n 1 \
    | tr -d '\r'
)"
echo "kafka_version=${version}"

echo
echo "=== KRAFT CONTRACT ==="
docker compose exec -T kafka sh -lc '
  test "${KAFKA_PROCESS_ROLES}" = "broker,controller"
  test "${KAFKA_AUTO_CREATE_TOPICS_ENABLE}" = "false"
  test "${KAFKA_LOG_DIRS}" = "/var/lib/kafka/data"
'
echo "kafka_kraft_mode=combined"
echo "kafka_auto_create_topics=false"

echo
echo "=== INTERNAL BROKER API ==="
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka:19092 \
  >/dev/null
echo "kafka_internal_listener=success"

echo
echo "=== TOPIC LIST ==="
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list \
  >/dev/null
echo "kafka_topic_list_status=success"

echo
echo "=== HOST TCP ==="
python - "$host_port" <<'PY'
import socket
import sys

port = int(sys.argv[1])

with socket.create_connection(
    ("localhost", port),
    timeout=5,
):
    pass

print(f"kafka_host_port={port}")
print("kafka_host_tcp=success")
PY

echo
echo "=== PERSISTENT DATA VOLUME ==="
volume_name="$(
  docker inspect \
    --format '{{range .Mounts}}{{if eq .Destination "/var/lib/kafka/data"}}{{.Name}}{{end}}{{end}}' \
    "$container_name"
)"

if [[ -z "$volume_name" ]]; then
  echo "ERROR: Kafka data volume is not mounted." >&2
  exit 1
fi

echo "kafka_data_volume=${volume_name}"
echo "kafka_persistent_storage=success"

echo
echo "kafka_runtime_status=success"
