#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"

cd "$PROJECT_ROOT"

echo "Running Phase 11 device event contract smoke..."
PYTHONPATH=src python -m neuro_sleep.streaming.device_event_smoke
