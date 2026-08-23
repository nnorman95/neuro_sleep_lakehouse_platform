#!/usr/bin/env bash
set -euo pipefail

MANIFEST_FILE="${1:-scripts/sql/migrations_manifest.txt}"
HISTORY_BOOTSTRAP_FILE="scripts/sql/migrations/047_create_ops_sql_migration_history.sql"

if [[ ! -f "$MANIFEST_FILE" ]]; then
  echo "Manifest file not found: $MANIFEST_FILE" >&2
  exit 1
fi

migration_database_override="${MIGRATION_DATABASE:-}"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

POSTGRES_USER="${POSTGRES_USER:-neuro_sleep}"
POSTGRES_DB="${POSTGRES_DB:-neuro_sleep}"
TARGET_DB="${migration_database_override:-$POSTGRES_DB}"

checksum_file() {
  local path="$1"

  python3 - "$path" <<'PY'
from pathlib import Path
import hashlib
import sys

path = Path(sys.argv[1])
digest = hashlib.sha256(path.read_bytes()).hexdigest()
print(digest)
PY
}

psql_query() {
  local sql="$1"

  docker compose exec -T postgres psql \
    -X \
    -v ON_ERROR_STOP=1 \
    -Atq \
    -U "$POSTGRES_USER" \
    -d "$TARGET_DB" \
    -c "$sql"
}

history_exists() {
  local result

  result="$(
    psql_query \
      "select case when to_regclass('ops.sql_migration_history') is null then 'false' else 'true' end;"
  )"

  [[ "$result" == "true" ]]
}

history_checksum() {
  local migration_path="$1"

  docker compose exec -T postgres psql \
    -X \
    -v ON_ERROR_STOP=1 \
    -Atq \
    -U "$POSTGRES_USER" \
    -d "$TARGET_DB" \
    -v migration_path="$migration_path" \
    <<'SQL'
select checksum_sha256
from ops.sql_migration_history
where migration_path = :'migration_path';
SQL
}

register_history() {
  local migration_path="$1"
  local checksum="$2"

  docker compose exec -T postgres psql \
    -X \
    -v ON_ERROR_STOP=1 \
    -q \
    -U "$POSTGRES_USER" \
    -d "$TARGET_DB" \
    -v migration_path="$migration_path" \
    -v checksum="$checksum" \
    <<'SQL'
insert into ops.sql_migration_history (
    migration_path,
    checksum_sha256
)
values (
    :'migration_path',
    :'checksum'
);
SQL
}

run_sql_file() {
  local sql_file="$1"

  echo "Running $sql_file"

  docker compose exec -T postgres psql \
    -X \
    -v ON_ERROR_STOP=1 \
    -U "$POSTGRES_USER" \
    -d "$TARGET_DB" \
    < "$sql_file"
}

sql_files=()
sql_checksums=()
seen_sql_files=""
bootstrap_file_present=false

while IFS= read -r sql_file || [[ -n "$sql_file" ]]; do
  if [[ -z "$sql_file" || "$sql_file" == \#* ]]; then
    continue
  fi

  if [[ ! -f "$sql_file" ]]; then
    echo "SQL file not found: $sql_file" >&2
    exit 1
  fi

  case $'\n'"$seen_sql_files"$'\n' in
    *$'\n'"$sql_file"$'\n'*)
      echo "Duplicate SQL manifest entry: $sql_file" >&2
      exit 1
      ;;
  esac
  seen_sql_files+="${sql_file}"$'\n'

  if [[ "$sql_file" == "$HISTORY_BOOTSTRAP_FILE" ]]; then
    bootstrap_file_present=true
  fi

  sql_files+=("$sql_file")
  sql_checksums+=("$(checksum_file "$sql_file")")
done < "$MANIFEST_FILE"

if [[ "${#sql_files[@]}" -eq 0 ]]; then
  echo "SQL manifest contains no runnable entries: $MANIFEST_FILE" >&2
  exit 1
fi

echo "Using manifest: $MANIFEST_FILE"
echo "Running SQL files against database: $TARGET_DB"
echo "sql_migration_manifest_entries=${#sql_files[@]}"

if ! history_exists; then
  if [[ "$bootstrap_file_present" != "true" ]]; then
    echo \
      "SQL migration history is missing and manifest does not contain " \
      "$HISTORY_BOOTSTRAP_FILE" >&2
    exit 1
  fi

  echo "sql_migration_history_mode=bootstrap"
  echo \
    "Migration history is not initialized; running the current manifest once " \
    "before recording its checksums."

  applied_count=0

  for index in "${!sql_files[@]}"; do
    run_sql_file "${sql_files[$index]}"
    applied_count=$((applied_count + 1))
  done

  if ! history_exists; then
    echo \
      "SQL migration history table was not created by " \
      "$HISTORY_BOOTSTRAP_FILE" >&2
    exit 1
  fi

  registered_count=0

  for index in "${!sql_files[@]}"; do
    register_history \
      "${sql_files[$index]}" \
      "${sql_checksums[$index]}"
    registered_count=$((registered_count + 1))
  done

  echo "sql_migrations_applied=$applied_count"
  echo "sql_migrations_skipped=0"
  echo "sql_migration_history_registered=$registered_count"
  echo "sql_migration_history_bootstrap=true"
  echo "sql_migration_history_status=success"
  exit 0
fi

echo "sql_migration_history_mode=tracked"

entry_states=()

# Preflight every registered file before applying any pending file. This makes
# checksum drift fail closed without partially advancing the manifest.
for index in "${!sql_files[@]}"; do
  sql_file="${sql_files[$index]}"
  expected_checksum="${sql_checksums[$index]}"
  stored_checksum="$(history_checksum "$sql_file")"

  if [[ -z "$stored_checksum" ]]; then
    entry_states+=("pending")
    continue
  fi

  if [[ "$stored_checksum" != "$expected_checksum" ]]; then
    echo "SQL migration checksum mismatch: $sql_file" >&2
    echo "registered_checksum=$stored_checksum" >&2
    echo "current_checksum=$expected_checksum" >&2
    echo \
      "Do not edit an applied migration or seed; add a new numbered SQL file." \
      >&2
    exit 1
  fi

  entry_states+=("registered")
done

applied_count=0
skipped_count=0

for index in "${!sql_files[@]}"; do
  sql_file="${sql_files[$index]}"

  if [[ "${entry_states[$index]}" == "registered" ]]; then
    echo "Skipping $sql_file (checksum verified)"
    skipped_count=$((skipped_count + 1))
    continue
  fi

  run_sql_file "$sql_file"
  register_history \
    "$sql_file" \
    "${sql_checksums[$index]}"
  applied_count=$((applied_count + 1))
done

echo "sql_migrations_applied=$applied_count"
echo "sql_migrations_skipped=$skipped_count"
echo "sql_migration_history_registered=${#sql_files[@]}"
echo "sql_migration_history_bootstrap=false"
echo "sql_migration_history_status=success"
