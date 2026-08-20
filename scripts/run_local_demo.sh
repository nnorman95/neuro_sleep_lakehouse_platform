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

demo_recording_key="${DEMO_RECORDING_KEY:-SC4001E}"

set -a
# shellcheck disable=SC1091
source .env
set +a

# Keep the demo deterministic and intentionally small.
export DATA_PROFILE=sample
export SLEEP_EDF_MAX_RECORDINGS=1
export SLEEP_EDF_INCLUDE_CASSETTE=true
export SLEEP_EDF_INCLUDE_TELEMETRY=false
export SLEEP_EDF_INCLUDE_METADATA=true
export SLEEP_EDF_RECORDING_KEYS="$demo_recording_key"
export SILVER_INCLUDE_SIGNALS=true
export SPARK_SIGNAL_RECORDING_KEYS="$demo_recording_key"

echo "Running NeuroSleep compact local demo..."
echo "demo_recording_key=$demo_recording_key"
echo "demo_scope=batch_plus_gold_single_recording"
echo

echo "1/10 Verifying local platform"
./scripts/check_local_platform_status.sh
echo

echo "2/10 Preparing Python environment"
./scripts/ensure_python_environment.sh
export VIRTUAL_ENV="$PROJECT_ROOT/.venv"
export PATH="$VIRTUAL_ENV/bin:$PATH"
hash -r
echo "demo_python=$(command -v python)"
echo

echo "3/10 Extracting one Sleep-EDF recording to Bronze"
python -m neuro_sleep.ingestion.sleep_edf_extract
echo

echo "4/10 Building Silver subject metadata"
python scripts/run_silver_subject_metadata.py
echo

echo "5/10 Building one Silver recording publication"
python scripts/run_silver_batch.py
echo

echo "6/10 Loading Silver metadata into PostgreSQL staging"
python scripts/load_subject_metadata_staging.py
python scripts/load_recording_staging.py
echo

echo "7/10 Building Warehouse and marts with dbt"
./scripts/run_dbt.sh build
echo

echo "8/10 Building Gold signal features for the demo recording"
./scripts/run_gold_signal_features.sh
echo

echo "9/10 Validating Gold signal features"
./scripts/validate_gold_signal_features.sh
echo

echo "10/10 Showing operational health"
PYTHONPATH=src python -m neuro_sleep.ops.operational_health
echo

echo "demo_path=source->bronze->silver->staging->warehouse+marts->gold_signal_features"
echo "local_demo_status=success"
