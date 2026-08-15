from __future__ import annotations

from urllib.parse import urlparse

from pyspark.sql import SparkSession

from neuro_sleep.config import Settings


HADOOP_AWS_VERSION = "3.5.0"
HADOOP_AWS_PACKAGE = (
    "org.apache.hadoop:"
    f"hadoop-aws:{HADOOP_AWS_VERSION}"
)


def assert_s3a_runtime(
    spark: SparkSession,
) -> str:
    jvm = spark.sparkContext._jvm

    hadoop_version = (
        jvm.org.apache.hadoop.util.VersionInfo
        .getVersion()
    )

    if hadoop_version != HADOOP_AWS_VERSION:
        raise RuntimeError(
            "Spark Hadoop runtime does not match "
            "the hadoop-aws package version: "
            f"hadoop={hadoop_version} "
            f"hadoop_aws={HADOOP_AWS_VERSION}"
        )

    loader = (
        jvm.java.lang.Thread
        .currentThread()
        .getContextClassLoader()
    )

    try:
        loader.loadClass(
            "org.apache.hadoop.fs.s3a."
            "S3AFileSystem"
        )
    except Exception as error:
        raise RuntimeError(
            "S3AFileSystem is not available. "
            "Run Spark with package "
            f"{HADOOP_AWS_PACKAGE}."
        ) from error

    return hadoop_version


def configure_minio_s3a(
    spark: SparkSession,
    *,
    settings: Settings,
) -> None:
    endpoint = settings.minio_endpoint.rstrip(
        "/"
    )
    parsed = urlparse(endpoint)

    if parsed.scheme not in {
        "http",
        "https",
    }:
        raise RuntimeError(
            "MINIO_ENDPOINT must use "
            "http or https"
        )

    if not parsed.hostname:
        raise RuntimeError(
            "MINIO_ENDPOINT must include "
            "a hostname"
        )

    hadoop_conf = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    hadoop_conf.set(
        "fs.s3a.impl",
        (
            "org.apache.hadoop.fs.s3a."
            "S3AFileSystem"
        ),
    )
    hadoop_conf.set(
        "fs.s3a.endpoint",
        endpoint,
    )
    hadoop_conf.set(
        "fs.s3a.endpoint.region",
        "us-east-1",
    )
    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )
    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        str(
            parsed.scheme == "https"
        ).lower(),
    )
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        (
            "org.apache.hadoop.fs.s3a."
            "SimpleAWSCredentialsProvider"
        ),
    )
    hadoop_conf.set(
        "fs.s3a.access.key",
        settings.minio_access_key,
    )
    hadoop_conf.set(
        "fs.s3a.secret.key",
        settings.minio_secret_key,
    )


def build_s3a_path(
    *,
    bucket: str,
    object_key: str,
) -> str:
    bucket_clean = bucket.strip()
    key_clean = object_key.lstrip("/")

    if not bucket_clean:
        raise ValueError(
            "bucket cannot be empty"
        )

    if not key_clean:
        raise ValueError(
            "object_key cannot be empty"
        )

    return (
        f"s3a://{bucket_clean}/"
        f"{key_clean}"
    )
