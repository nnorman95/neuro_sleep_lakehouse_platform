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
echo "=== KAFKA CLIENT ==="
python - <<'PY'
import confluent_kafka

print(
    "confluent_kafka_version="
    f"{confluent_kafka.__version__}"
)
PY

echo
echo "=== KAFKA RUNTIME ==="
./scripts/validate_kafka_runtime.sh

echo
echo "=== APPLICATION TOPIC ==="
python scripts/init_kafka_topics.py

echo
echo "=== PRODUCER DELIVERY ==="
output="$(
  PYTHONPATH=src python scripts/produce_simulated_bci_events.py \
    --devices 2 \
    --signal-quality-events 2 \
    --seed 11
)"
printf '%s\n' "$output"

for marker in \
  'simulated_bci_devices=2' \
  'simulated_bci_events=10' \
  'kafka_producer_delivery_success=10' \
  'kafka_producer_device_partition_invariant=success' \
  'kafka_producer_status=success'
do
  if ! grep -q "^${marker}$" <<<"$output"; then
    echo "ERROR: missing producer marker: ${marker}" >&2
    exit 1
  fi
done

echo
echo "kafka_producer_validation_status=success"
