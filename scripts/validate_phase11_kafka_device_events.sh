#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

run_step() {
  local marker="$1"
  local title="$2"
  shift 2

  echo
  echo "=== ${title} ==="
  "$@"
  echo "${marker}=success"
}

echo "=== PHASE 11 KAFKA DEVICE EVENT AUDIT ==="
echo "phase11_audit_scope=contract,runtime,topic,producer,consumer,inbox,offsets,quarantine,arrival,warehouse"

run_step \
  "phase11_dependency_contract" \
  "DEPENDENCY CONTRACT" \
  python scripts/validate_dependency_contract.py

run_step \
  "phase11_event_contract" \
  "DEVICE EVENT CONTRACT" \
  ./scripts/run_device_event_contract_smoke.sh

run_step \
  "phase11_topic_contract" \
  "KAFKA RUNTIME + TOPIC CONTRACT" \
  ./scripts/validate_kafka_topics.sh

run_step \
  "phase11_producer" \
  "SIMULATED BCI PRODUCER" \
  ./scripts/validate_kafka_producer.sh

run_step \
  "phase11_consumer" \
  "KAFKA CONSUMER FOUNDATION" \
  ./scripts/validate_kafka_consumer.sh

run_step \
  "phase11_durable_inbox" \
  "DURABLE EVENT INBOX + EVENT_ID DEDUP" \
  ./scripts/validate_kafka_device_event_inbox.sh

run_step \
  "phase11_durable_ingestion" \
  "DURABLE INGESTION + SAFE KAFKA OFFSET COMMIT" \
  ./scripts/validate_kafka_durable_ingestion.sh

run_step \
  "phase11_invalid_quarantine" \
  "INVALID MESSAGE QUARANTINE" \
  ./scripts/validate_kafka_invalid_message_quarantine.sh

run_step \
  "phase11_arrival_classification" \
  "LATE / OUT-OF-ORDER CLASSIFICATION" \
  ./scripts/validate_kafka_arrival_classification.sh

run_step \
  "phase11_warehouse_fact" \
  "WAREHOUSE FACT_DEVICE_EVENT" \
  ./scripts/validate_kafka_warehouse_fact.sh

echo
echo "=== REPOSITORY DIFF CHECK ==="
git diff --check
echo "phase11_git_diff_check=success"

echo
echo "=== PHASE 11 AUDIT SUMMARY ==="
echo "phase11_event_contract=success"
echo "phase11_kafka_runtime=success"
echo "phase11_topic_contract=success"
echo "phase11_producer=success"
echo "phase11_consumer=success"
echo "phase11_durable_inbox=success"
echo "phase11_event_id_dedup=success"
echo "phase11_kafka_offset_commit=success"
echo "phase11_invalid_message_quarantine=success"
echo "phase11_late_event_detection=success"
echo "phase11_out_of_order_detection=success"
echo "phase11_warehouse_fact_device_event=success"
echo
echo "phase11_validation_status=success"
