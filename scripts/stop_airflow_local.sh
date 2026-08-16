#!/bin/bash
set -euo pipefail
./scripts/airflow_compose.sh stop airflow-api-server airflow-dag-processor airflow-scheduler
echo "airflow_services_stopped=success"
