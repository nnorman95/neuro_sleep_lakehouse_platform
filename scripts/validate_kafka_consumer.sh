#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

echo "=== DEPENDENCY CONTRACT ==="
python scripts/validate_dependency_contract.py

echo
echo "=== KAFKA RUNTIME ==="
./scripts/validate_kafka_runtime.sh

echo
echo "=== APPLICATION TOPIC ==="
python scripts/init_kafka_topics.py

echo
echo "=== ISOLATED CONSUMER FIXTURE ==="
fixture_output="$(
  PYTHONPATH=src \
    python scripts/validate_kafka_consumer_fixture.py
)"
printf '%s\n' "$fixture_output"

for marker in \
  'kafka_consumer_fixture_produced=3' \
  'kafka_consumer_fixture_consumed=3' \
  'kafka_consumer_fixture_exact_event_ids=success' \
  'kafka_consumer_fixture_offset_isolation=success' \
  'kafka_consumer_fixture_status=success'
do
  if ! grep -q "^${marker}$" <<<"$fixture_output"; then
    echo "ERROR: missing consumer fixture marker: ${marker}" >&2
    exit 1
  fi
done

echo
echo "kafka_consumer_validation_status=success"
