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
echo "=== QUARANTINE BASELINE ==="
PYTHONPATH=src python - <<'PY'
from neuro_sleep.quality.quarantine import (
    upsert_active_quarantine_record,
)

print("quarantine_upsert_import=success")
PY

echo
echo "=== INVALID MESSAGE FLOW ==="
output="$(
  PYTHONPATH=src \
    python scripts/validate_kafka_invalid_message_quarantine.py
)"
printf '%s\n' "$output"

for marker in \
  'kafka_invalid_fixture_messages=1' \
  'kafka_invalid_quarantine_failure=observed' \
  'kafka_invalid_offset_unchanged_on_quarantine_failure=success' \
  'kafka_invalid_message_quarantined=success' \
  'kafka_invalid_raw_payload_preserved=success' \
  'kafka_invalid_offset_commit_after_quarantine=success' \
  'kafka_invalid_quarantine_upsert_idempotency=success' \
  'kafka_invalid_message_flow_status=success'
do
  if ! grep -q "^${marker}$" <<<"$output"; then
    echo "ERROR: missing invalid-message marker: ${marker}" >&2
    exit 1
  fi
done

echo
echo "kafka_invalid_message_validation_status=success"
