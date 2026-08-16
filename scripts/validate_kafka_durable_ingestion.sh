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
echo "=== DURABLE INBOX ==="
./scripts/validate_kafka_device_event_inbox.sh

echo
echo "=== DURABLE INGESTION / OFFSET COMMIT ==="
output="$(
  PYTHONPATH=src \
    python scripts/validate_kafka_durable_ingestion.py
)"
printf '%s\n' "$output"

for marker in \
  'kafka_ingestion_fixture_events=3' \
  'kafka_ingestion_first_pass_processed=2' \
  'kafka_ingestion_commit_after_durable_write=success' \
  'kafka_ingestion_failure_before_persist=observed' \
  'kafka_ingestion_offset_unchanged_on_failure=success' \
  'kafka_ingestion_restart_processed=1' \
  'kafka_ingestion_restart_resume=success' \
  'kafka_ingestion_all_events_durable=success' \
  'kafka_ingestion_at_least_once=success' \
  'kafka_ingestion_smoke_status=success'
do
  if ! grep -q "^${marker}$" <<<"$output"; then
    echo "ERROR: missing ingestion marker: ${marker}" >&2
    exit 1
  fi
done

echo
echo "kafka_ingestion_validation_status=success"
