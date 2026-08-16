#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

POSTGRES_USER="${POSTGRES_USER:-neuro_sleep}"
POSTGRES_DB="${POSTGRES_DB:-neuro_sleep}"

fixture_event_id=""

cleanup() {
  status=$?
  trap - EXIT INT TERM

  if [[ -n "$fixture_event_id" ]]; then
    PYTHONPATH=src \
      python scripts/cleanup_kafka_warehouse_fixture.py \
        --event-id "$fixture_event_id" \
      >/dev/null 2>&1 || true

    ./scripts/run_dbt.sh run \
      --select fact_device_event \
      >/dev/null 2>&1 || true
  fi

  exit "$status"
}
trap cleanup EXIT INT TERM

echo "=== SQL / GOVERNANCE ==="
./scripts/run_sql_migrations.sh

echo
echo "=== DBT PARSE ==="
./scripts/run_dbt.sh parse
echo "kafka_warehouse_dbt_parse=success"

echo
echo "=== CREATE TRUSTED INBOX FIXTURE ==="
fixture_output="$(
  PYTHONPATH=src \
    python scripts/create_kafka_warehouse_fixture.py
)"
printf '%s\n' "$fixture_output"

fixture_event_id="$(
  printf '%s\n' "$fixture_output" \
    | sed -n 's/^kafka_warehouse_fixture_event_id=//p'
)"

if [[ -z "$fixture_event_id" ]]; then
  echo "ERROR: fixture event_id was not returned." >&2
  exit 1
fi

echo
echo "=== BUILD WAREHOUSE FACT ==="
./scripts/run_dbt.sh build \
  --select +fact_device_event

echo
echo "=== WAREHOUSE RECONCILIATION ==="
fact_row="$(
  docker compose exec -T postgres psql \
    -P pager=off \
    -At \
    -F '|' \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -c "
select
    event_id::text,
    source_system,
    device_id,
    event_type,
    sequence_number::text,
    delivery_count::text,
    coalesce(is_late::text, 'null'),
    coalesce(is_out_of_order::text, 'null')
from warehouse.fact_device_event
where event_id = '${fixture_event_id}'::uuid;
"
)"

if [[ -z "$fact_row" ]]; then
  echo "ERROR: fixture event is missing from warehouse fact." >&2
  exit 1
fi

IFS='|' read -r \
  fact_event_id \
  fact_source_system \
  fact_device_id \
  fact_event_type \
  fact_sequence_number \
  fact_delivery_count \
  fact_is_late \
  fact_is_out_of_order \
  <<<"$fact_row"

[[ "$fact_event_id" == "$fixture_event_id" ]]
[[ "$fact_source_system" == "simulated_bci_device" ]]
[[ "$fact_device_id" == "bci-device-warehouse-smoke" ]]
[[ "$fact_event_type" == "signal_quality" ]]
[[ "$fact_sequence_number" == "0" ]]
[[ "$fact_delivery_count" == "1" ]]
[[ "$fact_is_late" == "false" ]]
[[ "$fact_is_out_of_order" == "false" ]]

echo "kafka_warehouse_fixture_present=success"
echo "kafka_warehouse_grain_event_id=success"
echo "kafka_warehouse_arrival_classification_preserved=success"

echo
echo "=== IDEMPOTENT REBUILD ==="
before_count="$(
  docker compose exec -T postgres psql \
    -At \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -c "
select count(*)
from warehouse.fact_device_event
where event_id = '${fixture_event_id}'::uuid;
"
)"

./scripts/run_dbt.sh build \
  --select fact_device_event

after_count="$(
  docker compose exec -T postgres psql \
    -At \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -c "
select count(*)
from warehouse.fact_device_event
where event_id = '${fixture_event_id}'::uuid;
"
)"

if [[ "$before_count" != "1" || "$after_count" != "1" ]]; then
  echo "ERROR: warehouse fact rebuild changed fixture grain." >&2
  exit 1
fi

echo "kafka_warehouse_idempotent_rebuild=success"

echo
echo "=== GOVERNANCE CONTRACT ==="
contract_count="$(
  docker compose exec -T postgres psql \
    -At \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -c "
select count(*)
from governance.data_contract_registry
where table_schema = 'warehouse'
  and table_name = 'fact_device_event'
  and contract_version = 'v1'
  and status = 'active';
"
)"

classification_count="$(
  docker compose exec -T postgres psql \
    -At \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -c "
select count(*)
from governance.column_classification
where table_schema = 'warehouse'
  and table_name = 'fact_device_event';
"
)"

[[ "$contract_count" == "1" ]]
[[ "$classification_count" == "27" ]]

echo "kafka_warehouse_governance_contract=success"
echo "kafka_warehouse_column_classification=27"

echo
echo "=== CLEANUP / RECONCILE ==="
PYTHONPATH=src \
  python scripts/cleanup_kafka_warehouse_fixture.py \
    --event-id "$fixture_event_id"

./scripts/run_dbt.sh run \
  --select fact_device_event

remaining="$(
  docker compose exec -T postgres psql \
    -At \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -c "
select count(*)
from warehouse.fact_device_event
where event_id = '${fixture_event_id}'::uuid;
"
)"

if [[ "$remaining" != "0" ]]; then
  echo "ERROR: fixture remained in warehouse after reconciliation." >&2
  exit 1
fi

fixture_event_id=""

echo "kafka_warehouse_cleanup_reconciliation=success"
echo
echo "kafka_warehouse_fact_validation_status=success"
