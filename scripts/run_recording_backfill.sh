#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

if [[ ! -f .env ]]; then
  echo "ERROR: .env is missing." >&2
  echo "Run make bootstrap first." >&2
  exit 1
fi

recording_key="${RECORDING_KEY:-}"

if [[ -z "$recording_key" ]]; then
  echo "ERROR: RECORDING_KEY is required." >&2
  echo "Usage: make backfill RECORDING_KEY=SC4001E" >&2
  exit 1
fi

if [[ ! "$recording_key" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "ERROR: invalid RECORDING_KEY: $recording_key" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

# One key is intentionally reused by both downstream selectors:
# - SLEEP_EDF_RECORDING_KEYS limits Silver -> staging discovery.
# - SIGNAL_FEATURE_RECORDING_KEYS limits Gold + integrated Gold.
export SLEEP_EDF_RECORDING_KEYS="$recording_key"
export SIGNAL_FEATURE_RECORDING_KEYS="$recording_key"

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

staging_log="$work_dir/staging.log"
gold_log="$work_dir/gold.log"
gold_validation_log="$work_dir/gold_validation.log"
integrated_log="$work_dir/integrated.log"
integrated_validation_log="$work_dir/integrated_validation.log"

value_from_log() {
  local key="$1"
  local file="$2"
  local line

  line="$(
    grep -E "^${key}=" "$file" \
      | tail -n 1 \
      || true
  )"

  if [[ -z "$line" ]]; then
    echo "ERROR: missing marker ${key}= in $file" >&2
    exit 1
  fi

  printf '%s' "${line#*=}"
}

require_equals() {
  local actual="$1"
  local expected="$2"
  local label="$3"

  if [[ "$actual" != "$expected" ]]; then
    echo "ERROR: ${label}: expected ${expected}, got ${actual}" >&2
    exit 1
  fi
}

echo "Running targeted NeuroSleep recording backfill..."
echo "backfill_recording_key=$recording_key"
echo "backfill_source_boundary=current_silver"
echo "backfill_scope=single_existing_signal_recording"
echo

echo "1/8 Verifying local platform"
./scripts/check_local_platform_status.sh
echo

echo "2/8 Preparing Python environment"
./scripts/ensure_python_environment.sh
export VIRTUAL_ENV="$PROJECT_ROOT/.venv"
export PATH="$VIRTUAL_ENV/bin:$PATH"
export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"
hash -r
echo "backfill_python=$(command -v python)"
echo

echo "3/8 Reconciling the selected Silver publication into staging"
python scripts/load_recording_staging.py \
  2>&1 | tee "$staging_log"

staging_publications="$(
  value_from_log publication_count "$staging_log"
)"
staging_written="$(
  value_from_log publications_written "$staging_log"
)"
staging_skipped="$(
  value_from_log publications_skipped "$staging_log"
)"
require_equals \
  "$staging_publications" \
  "1" \
  "targeted staging publication count"
echo

echo "4/8 Rebuilding relational Warehouse and marts"
./scripts/run_dbt.sh build
echo "backfill_dbt_status=success"
echo

echo "5/8 Reconciling and validating Gold signal features"
./scripts/run_gold_signal_features.sh \
  2>&1 | tee "$gold_log"

gold_recordings="$(
  value_from_log gold_signal_feature_recordings "$gold_log"
)"
gold_written="$(
  value_from_log gold_signal_feature_written "$gold_log"
)"
gold_skipped="$(
  value_from_log gold_signal_feature_skipped "$gold_log"
)"
gold_recovered="$(
  value_from_log gold_signal_feature_recovered_objects "$gold_log"
)"
require_equals \
  "$gold_recordings" \
  "1" \
  "targeted Gold recording count"

./scripts/validate_gold_signal_features.sh \
  2>&1 | tee "$gold_validation_log"

gold_validation_recordings="$(
  value_from_log gold_validation_recordings "$gold_validation_log"
)"
require_equals \
  "$gold_validation_recordings" \
  "1" \
  "targeted Gold validation recording count"
echo

echo "6/8 Reconciling and validating integrated Gold"
./scripts/run_integrated_signal_features.sh \
  2>&1 | tee "$integrated_log"

integrated_recordings="$(
  value_from_log integrated_gold_recordings "$integrated_log"
)"
integrated_written="$(
  value_from_log integrated_gold_written "$integrated_log"
)"
integrated_skipped="$(
  value_from_log integrated_gold_skipped "$integrated_log"
)"
integrated_recovered="$(
  value_from_log integrated_gold_recovered_objects "$integrated_log"
)"
require_equals \
  "$integrated_recordings" \
  "1" \
  "targeted integrated Gold recording count"

./scripts/validate_integrated_signal_features.sh \
  2>&1 | tee "$integrated_validation_log"

integrated_validation_recordings="$(
  value_from_log integrated_gold_validation_recordings \
    "$integrated_validation_log"
)"
require_equals \
  "$integrated_validation_recordings" \
  "1" \
  "targeted integrated Gold validation recording count"
echo

echo "7/8 Showing operational health"
make ops-status
echo

echo "8/8 Backfill summary"
echo "backfill_staging_publications=$staging_publications"
echo "backfill_staging_written=$staging_written"
echo "backfill_staging_skipped=$staging_skipped"
echo "backfill_gold_recordings=$gold_recordings"
echo "backfill_gold_written=$gold_written"
echo "backfill_gold_skipped=$gold_skipped"
echo "backfill_gold_recovered_objects=$gold_recovered"
echo "backfill_gold_validation_recordings=$gold_validation_recordings"
echo "backfill_integrated_recordings=$integrated_recordings"
echo "backfill_integrated_written=$integrated_written"
echo "backfill_integrated_skipped=$integrated_skipped"
echo "backfill_integrated_recovered_objects=$integrated_recovered"
echo "backfill_integrated_validation_recordings=$integrated_validation_recordings"
echo "recording_backfill_path=silver->staging->warehouse+marts->gold_signal_features->integrated_gold"
echo "recording_backfill_status=success"
