from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
    *,
    master: str = "local[*]",
    ui_enabled: bool = False,
) -> SparkSession:
    if not app_name.strip():
        raise ValueError("app_name cannot be empty")

    return (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config(
            "spark.ui.enabled",
            str(ui_enabled).lower(),
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )
