#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

echo "=== KAFKA RUNTIME ==="
./scripts/validate_kafka_runtime.sh

echo
echo "=== TOPIC INITIALIZATION: FIRST PASS ==="
first_output="$(
  python scripts/init_kafka_topics.py
)"
printf '%s\n' "$first_output"

echo
echo "=== TOPIC INITIALIZATION: SECOND PASS ==="
second_output="$(
  python scripts/init_kafka_topics.py
)"
printf '%s\n' "$second_output"

if ! grep -q \
  '^kafka_topic_status=existing$' \
  <<<"$second_output"; then
  echo "ERROR: second Kafka topic initialization was not idempotent." >&2
  exit 1
fi

if ! grep -q \
  '^kafka_topic_contract_status=success$' \
  <<<"$second_output"; then
  echo "ERROR: Kafka topic contract did not validate." >&2
  exit 1
fi

echo
echo "kafka_topic_idempotency=success"
echo "kafka_topic_validation_status=success"
