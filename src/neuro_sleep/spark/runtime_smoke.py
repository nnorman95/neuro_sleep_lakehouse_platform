from __future__ import annotations

import sys

import pyspark

from neuro_sleep.spark.session import (
    create_spark_session,
)


EXPECTED_PYSPARK_VERSION = "4.2.0"


def run_smoke_test() -> None:
    if pyspark.__version__ != EXPECTED_PYSPARK_VERSION:
        raise RuntimeError(
            "Unexpected PySpark version: "
            f"{pyspark.__version__}; "
            f"expected {EXPECTED_PYSPARK_VERSION}"
        )

    spark = create_spark_session(
        "neurosleep-spark-runtime-smoke",
        master="local[2]",
        ui_enabled=False,
    )

    try:
        java_version = (
            spark.sparkContext._jvm
            .java.lang.System
            .getProperty("java.version")
        )
        hadoop_version = (
            spark.sparkContext._jvm
            .org.apache.hadoop.util.VersionInfo
            .getVersion()
        )

        count = spark.range(10).count()
        if count != 10:
            raise RuntimeError(
                "Spark runtime count check failed: "
                f"expected 10, got {count}"
            )

        print(f"python={sys.version.split()[0]}")
        print(f"pyspark={pyspark.__version__}")
        print(f"spark={spark.version}")
        print(f"java={java_version}")
        print(f"hadoop={hadoop_version}")
        print(f"spark_test_count={count}")
        print("spark_runtime_smoke_status=success")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_smoke_test()
