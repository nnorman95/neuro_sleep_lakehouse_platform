#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"
cd "$PROJECT_ROOT"

run_step() {
  local step="$1"
  local marker="$2"
  local title="$3"
  shift 3

  echo
  echo "=== ${step}/12 ${title} ==="
  "$@"
  echo "${marker}=success"
}

echo "=== PHASE 13 OPERATIONAL HARDENING AUDIT ==="
echo \
  "phase13_audit_scope="\
"prerequisites,ci,platform,operations,migrations,demo,backfill,phase10,phase11,phase12,health,diff"

run_step \
  "1" \
  "phase13_prerequisite_doctor" \
  "LOCAL PREREQUISITE DOCTOR" \
  make doctor

run_step \
  "2" \
  "phase13_lightweight_ci" \
  "LIGHTWEIGHT REPOSITORY CI" \
  make ci-check

run_step \
  "3" \
  "phase13_platform_readiness" \
  "FULL LOCAL PLATFORM READINESS" \
  make platform-status

run_step \
  "4" \
  "phase13_operational_health_smoke" \
  "OPERATIONAL HEALTH CLASSIFICATION" \
  make ops-status-smoke

run_step \
  "5" \
  "phase13_migration_history" \
  "SQL MIGRATION HISTORY + CHECKSUM DRIFT" \
  make migration-history-check

run_step \
  "6" \
  "phase13_compact_demo" \
  "COMPACT ONE-RECORDING DEMO" \
  make demo

run_step \
  "7" \
  "phase13_targeted_backfill" \
  "TARGETED RECORDING BACKFILL" \
  make backfill RECORDING_KEY=SC4001E

run_step \
  "8" \
  "phase13_phase10_regression" \
  "COMPLETE PHASE 10 REGRESSION" \
  make phase10-check

run_step \
  "9" \
  "phase13_phase11_regression" \
  "COMPLETE PHASE 11 KAFKA AUDIT" \
  make phase11-check

run_step \
  "10" \
  "phase13_phase12_regression" \
  "COMPLETE PHASE 12 DATA-QUALITY AUDIT" \
  make phase12-check

run_step \
  "11" \
  "phase13_operational_health_readout" \
  "FINAL OPERATIONAL HEALTH READOUT" \
  make ops-status

run_step \
  "12" \
  "phase13_diff_hygiene" \
  "REPOSITORY DIFF HYGIENE" \
  git diff --check

echo
echo "=== PHASE 13 AUDIT SUMMARY ==="
echo "phase13_process_optimization=success"
echo "phase13_reproducible_local_operation=success"
echo "phase13_targeted_recovery=success"
echo "phase13_migration_execution_tracking=success"
echo "phase13_repository_ci=success"
echo "phase13_predecessor_regressions=success"
echo
echo "phase13_validation_status=success"
