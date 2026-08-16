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
echo "=== ARRIVAL CLASSIFICATION MIGRATION ==="
./scripts/run_sql_migrations.sh

echo
echo "=== ARRIVAL CLASSIFICATION FLOW ==="
output="$(
  PYTHONPATH=src \
    python scripts/validate_kafka_arrival_classification.py
)"
printf '%s\n' "$output"

for marker in \
  'kafka_arrival_fixture_events=3' \
  'kafka_arrival_valid_events=3' \
  'kafka_arrival_quarantined_messages=0' \
  'kafka_arrival_forward_gap_allowed=success' \
  'kafka_arrival_late_detection=success' \
  'kafka_arrival_out_of_order_detection=success' \
  'kafka_arrival_reason=sequence_and_event_time' \
  'kafka_arrival_classification_persisted=success' \
  'kafka_arrival_offset_commit=success' \
  'kafka_arrival_classification_status=success'
do
  if ! grep -q "^${marker}$" <<<"$output"; then
    echo "ERROR: missing arrival marker: ${marker}" >&2
    exit 1
  fi
done

echo
echo "kafka_arrival_validation_status=success"
