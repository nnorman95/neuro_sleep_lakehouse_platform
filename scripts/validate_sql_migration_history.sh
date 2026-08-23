#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

POSTGRES_USER="${POSTGRES_USER:-neuro_sleep}"
POSTGRES_DB="${POSTGRES_DB:-neuro_sleep}"

temp_db="neuro_sleep_migration_history_smoke_$$"
temp_dir="$(mktemp -d)"
manifest="$temp_dir/manifest.txt"
fixture_a="$temp_dir/001_fixture_table.sql"
fixture_b="$temp_dir/002_fixture_seed.sql"

cleanup() {
  status=$?
  trap - EXIT INT TERM

  docker compose exec -T postgres dropdb \
    --if-exists \
    -U "$POSTGRES_USER" \
    "$temp_db" \
    >/dev/null 2>&1 \
    || true

  rm -rf "$temp_dir"
  exit "$status"
}
trap cleanup EXIT INT TERM

cat > "$fixture_a" <<'SQL'
create table if not exists public.migration_history_fixture (
    fixture_id integer primary key,
    fixture_value text not null
);
SQL

cat > "$fixture_b" <<'SQL'
insert into public.migration_history_fixture (
    fixture_id,
    fixture_value
)
values (
    1,
    'stable'
)
on conflict (fixture_id) do nothing;
SQL

cat > "$manifest" <<EOF
scripts/sql/migrations/047_create_ops_sql_migration_history.sql
$fixture_a
$fixture_b
EOF

docker compose exec -T postgres createdb \
  -U "$POSTGRES_USER" \
  "$temp_db"

# The real project creates the ops schema in earlier migrations. This focused
# temporary database intentionally runs only the migration-history bootstrap
# migration plus two fixtures, so establish that prerequisite explicitly.
docker compose exec -T postgres psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -U "$POSTGRES_USER" \
  -d "$temp_db" \
  -c 'create schema if not exists ops;' \
  >/dev/null

first_log="$temp_dir/first.log"
second_log="$temp_dir/second.log"
drift_log="$temp_dir/drift.log"

set +e
MIGRATION_DATABASE="$temp_db" \
  ./scripts/run_sql_migrations.sh "$manifest" \
  >"$first_log" 2>&1
first_status=$?
set -e

cat "$first_log"

if [[ "$first_status" -ne 0 ]]; then
  echo "ERROR: first-run migration-history fixture failed." >&2
  exit "$first_status"
fi

grep -q '^sql_migration_history_mode=bootstrap$' "$first_log"
grep -q '^sql_migrations_applied=3$' "$first_log"
grep -q '^sql_migrations_skipped=0$' "$first_log"
grep -q '^sql_migration_history_registered=3$' "$first_log"
grep -q '^sql_migration_history_status=success$' "$first_log"

set +e
MIGRATION_DATABASE="$temp_db" \
  ./scripts/run_sql_migrations.sh "$manifest" \
  >"$second_log" 2>&1
second_status=$?
set -e

cat "$second_log"

if [[ "$second_status" -ne 0 ]]; then
  echo "ERROR: idempotent migration-history rerun failed." >&2
  exit "$second_status"
fi

grep -q '^sql_migration_history_mode=tracked$' "$second_log"
grep -q '^sql_migrations_applied=0$' "$second_log"
grep -q '^sql_migrations_skipped=3$' "$second_log"
grep -q '^sql_migration_history_registered=3$' "$second_log"
grep -q '^sql_migration_history_status=success$' "$second_log"

cat >> "$fixture_b" <<'SQL'
-- checksum drift fixture
SQL

set +e
MIGRATION_DATABASE="$temp_db" \
  ./scripts/run_sql_migrations.sh "$manifest" \
  >"$drift_log" 2>&1
drift_status=$?
set -e

cat "$drift_log"

if [[ "$drift_status" -eq 0 ]]; then
  echo "ERROR: checksum drift was accepted." >&2
  exit 1
fi

grep -q "SQL migration checksum mismatch: $fixture_b" "$drift_log"
grep -q \
  '^Do not edit an applied migration or seed; add a new numbered SQL file.$' \
  "$drift_log"

history_count="$(
  docker compose exec -T postgres psql \
    -X \
    -Atq \
    -U "$POSTGRES_USER" \
    -d "$temp_db" \
    -c 'select count(*) from ops.sql_migration_history;'
)"

fixture_count="$(
  docker compose exec -T postgres psql \
    -X \
    -Atq \
    -U "$POSTGRES_USER" \
    -d "$temp_db" \
    -c 'select count(*) from public.migration_history_fixture;'
)"

if [[ "$history_count" != "3" ]]; then
  echo "ERROR: expected 3 migration history rows, got $history_count" >&2
  exit 1
fi

if [[ "$fixture_count" != "1" ]]; then
  echo "ERROR: expected 1 fixture row, got $fixture_count" >&2
  exit 1
fi

echo "sql_migration_history_first_run=success"
echo "sql_migration_history_idempotent_rerun=success"
echo "sql_migration_history_checksum_drift=blocked"
echo "sql_migration_history_smoke_status=success"
