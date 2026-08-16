#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

POSTGRES_USER="${POSTGRES_USER:-neuro_sleep}"
POSTGRES_DB="${POSTGRES_DB:-neuro_sleep}"

echo "=== POSTGRESQL ==="
docker compose ps \
  --status running \
  --services postgres \
  | grep -qx postgres
echo "postgres=running"

echo
echo "=== SQL MIGRATIONS ==="
./scripts/run_sql_migrations.sh

echo
echo "=== INBOX TABLE CONTRACT ==="
table_status="$(
  docker compose exec -T postgres psql \
    -P pager=off \
    -At \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -c "
select
    case
        when to_regclass(
            'ops.kafka_device_event_inbox'
        ) is not null
        then 'present'
        else 'missing'
    end;
"
)"

echo "kafka_inbox_table=${table_status}"

if [[ "$table_status" != "present" ]]; then
  echo "ERROR: Kafka inbox table is missing." >&2
  exit 1
fi

echo
echo "=== DURABLE DEDUP SMOKE ==="
output="$(
  PYTHONPATH=src \
    python scripts/validate_kafka_device_event_inbox.py
)"
printf '%s\n' "$output"

for marker in \
  'kafka_inbox_first_write=inserted' \
  'kafka_inbox_duplicate_write=duplicate' \
  'kafka_inbox_delivery_count=2' \
  'kafka_inbox_first_coordinate_preserved=true' \
  'kafka_inbox_last_coordinate_refreshed=true' \
  'kafka_inbox_identity_conflict_blocked=true' \
  'kafka_inbox_event_id_deduplication=success' \
  'kafka_inbox_smoke_status=success'
do
  if ! grep -q "^${marker}$" <<<"$output"; then
    echo "ERROR: missing inbox marker: ${marker}" >&2
    exit 1
  fi
done

echo
echo "kafka_inbox_validation_status=success"
