#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG="${1:-neurosleep-airflow:phase10}"

echo "=== IMAGE ==="
echo "$IMAGE_TAG"

docker image inspect "$IMAGE_TAG" >/dev/null

docker run --rm \
    --entrypoint bash \
    "$IMAGE_TAG" \
    -lc '
set -euo pipefail

echo
echo "=== USER ==="
id

echo
echo "=== PROJECT ==="
test -d /opt/neurosleep
test -f /opt/neurosleep/pyproject.toml
test -f /opt/neurosleep/requirements.txt
test -f /opt/neurosleep/scripts/validate_dependency_contract.py
echo "/opt/neurosleep: OK"

echo
echo "=== AIRFLOW ==="
airflow version

echo
echo "=== JAVA ==="
java -version

echo
echo "=== PYSPARK ==="
python -c "import pyspark; print(\"PySpark:\", pyspark.__version__)"

echo
echo "=== DBT ==="
dbt --version

echo
echo "=== NEUROSLEEP IMPORT ==="
python -c "import neuro_sleep; print(\"NeuroSleep package: OK\"); print(neuro_sleep.__file__)"

echo
echo "=== DEPENDENCY CONTRACT ==="
python /opt/neurosleep/scripts/validate_dependency_contract.py

echo
echo "=== SPARK JVM SMOKE ==="
python - <<'"'"'PY'"'"'
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("neurosleep-airflow-runtime-smoke")
    .getOrCreate()
)

try:
    print("Spark:", spark.version)
    assert spark.range(3).count() == 3
    print("Spark JVM smoke: OK")
finally:
    spark.stop()
PY
'
