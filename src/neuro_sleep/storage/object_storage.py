from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from neuro_sleep.config import Settings, get_settings
from neuro_sleep.reliability.object_storage_retry import (
    get_client_error_details,
    run_object_storage_operation,
)


MULTIPART_THRESHOLD_BYTES = 64 * 1024 * 1024
MULTIPART_CHUNK_SIZE_BYTES = 16 * 1024 * 1024
MAX_UPLOAD_CONCURRENCY = 4


NOT_FOUND_ERROR_CODES = {
    "404",
    "NoSuchBucket",
    "NoSuchKey",
    "NotFound",
}


@dataclass(frozen=True)
class ObjectStorageSummary:
    bucket: str
    object_key: str
    content_length: int


def get_object_storage_client(
    settings: Settings | None = None,
) -> BaseClient:
    if settings is None:
        settings = get_settings()

    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name="us-east-1",
    )


def list_bucket_names(
    client: BaseClient | None = None,
) -> list[str]:
    if client is None:
        client = get_object_storage_client()

    response = run_object_storage_operation(
        operation=client.list_buckets,
        operation_name="list_buckets",
    )

    return sorted(
        bucket["Name"]
        for bucket in response.get("Buckets", [])
    )


def bucket_exists(
    bucket: str,
    client: BaseClient | None = None,
) -> bool:
    if client is None:
        client = get_object_storage_client()

    try:
        run_object_storage_operation(
            operation=lambda: client.head_bucket(
                Bucket=bucket
            ),
            operation_name=f"head_bucket:{bucket}",
        )

        return True

    except ClientError as error:
        error_code, status_code = (
            get_client_error_details(error)
        )

        if (
            error_code in NOT_FOUND_ERROR_CODES
            or status_code == 404
        ):
            return False

        raise


def validate_required_buckets(
    required_buckets: Sequence[str],
    client: BaseClient | None = None,
) -> None:
    if client is None:
        client = get_object_storage_client()

    missing_buckets = [
        bucket
        for bucket in required_buckets
        if not bucket_exists(
            bucket=bucket,
            client=client,
        )
    ]

    if missing_buckets:
        missing = ", ".join(missing_buckets)

        raise RuntimeError(
            f"Missing required MinIO buckets: {missing}"
        )


def put_bytes_object(
    bucket: str,
    object_key: str,
    data: bytes,
    content_type: str = "application/octet-stream",
    client: BaseClient | None = None,
) -> None:
    if client is None:
        client = get_object_storage_client()

    run_object_storage_operation(
        operation=lambda: client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=data,
            ContentType=content_type,
        ),
        operation_name=(
            f"put_object:{bucket}/{object_key}"
        ),
    )


def put_text_object(
    bucket: str,
    object_key: str,
    text: str,
    client: BaseClient | None = None,
) -> None:
    put_bytes_object(
        bucket=bucket,
        object_key=object_key,
        data=text.encode("utf-8"),
        content_type="text/plain",
        client=client,
    )


def put_file_object(
    bucket: str,
    object_key: str,
    file_path: Path,
    content_type: str = "application/octet-stream",
    checksum_sha256: str | None = None,
    client: BaseClient | None = None,
) -> None:
    if client is None:
        client = get_object_storage_client()

    resolved_file_path = (
        file_path.expanduser().resolve()
    )

    if not resolved_file_path.is_file():
        raise FileNotFoundError(
            f"Local file not found: {resolved_file_path}"
        )

    if resolved_file_path.stat().st_size == 0:
        raise ValueError(
            f"Local file is empty: {resolved_file_path}"
        )

    extra_args: dict[str, object] = {
        "ContentType": content_type,
    }

    if checksum_sha256 is not None:
        extra_args["Metadata"] = {
            "sha256": checksum_sha256,
        }

    transfer_config = TransferConfig(
        multipart_threshold=(
            MULTIPART_THRESHOLD_BYTES
        ),
        multipart_chunksize=(
            MULTIPART_CHUNK_SIZE_BYTES
        ),
        max_concurrency=MAX_UPLOAD_CONCURRENCY,
        use_threads=True,
    )

    run_object_storage_operation(
        operation=lambda: client.upload_file(
            Filename=str(resolved_file_path),
            Bucket=bucket,
            Key=object_key,
            ExtraArgs=extra_args,
            Config=transfer_config,
        ),
        operation_name=(
            f"upload_file:{bucket}/{object_key}"
        ),
    )


def get_object_metadata(
    bucket: str,
    object_key: str,
    client: BaseClient | None = None,
) -> dict:
    if client is None:
        client = get_object_storage_client()

    response = run_object_storage_operation(
        operation=lambda: client.head_object(
            Bucket=bucket,
            Key=object_key,
        ),
        operation_name=(
            f"head_object:{bucket}/{object_key}"
        ),
    )

    custom_metadata = response.get(
        "Metadata",
        {},
    )

    return {
        "content_length": response.get(
            "ContentLength"
        ),
        "content_type": response.get(
            "ContentType"
        ),
        "etag": response.get("ETag"),
        "last_modified": response.get(
            "LastModified"
        ),
        "metadata": custom_metadata,
        "checksum_sha256": custom_metadata.get(
            "sha256"
        ),
    }


def list_object_summaries(
    bucket: str,
    prefix: str = "",
    client: BaseClient | None = None,
) -> list[ObjectStorageSummary]:
    if not bucket.strip():
        raise ValueError(
            "bucket cannot be empty"
        )

    if client is None:
        client = get_object_storage_client()

    summaries: list[
        ObjectStorageSummary
    ] = []

    continuation_token: str | None = None
    page_number = 1

    while True:
        request: dict[str, object] = {
            "Bucket": bucket,
            "Prefix": prefix,
        }

        if continuation_token is not None:
            request["ContinuationToken"] = (
                continuation_token
            )

        response = run_object_storage_operation(
            operation=lambda request=request: (
                client.list_objects_v2(
                    **request
                )
            ),
            operation_name=(
                "list_objects_v2:"
                f"{bucket}/{prefix}:"
                f"page={page_number}"
            ),
        )

        contents = response.get(
            "Contents",
            [],
        )

        for item in contents:
            object_key = item.get("Key")
            content_length = item.get("Size")

            if not isinstance(
                object_key,
                str,
            ):
                raise RuntimeError(
                    "MinIO object listing "
                    "returned an invalid key"
                )

            if not isinstance(
                content_length,
                int,
            ):
                raise RuntimeError(
                    "MinIO object listing "
                    "returned an invalid size"
                )

            summaries.append(
                ObjectStorageSummary(
                    bucket=bucket,
                    object_key=object_key,
                    content_length=(
                        content_length
                    ),
                )
            )

        if not response.get(
            "IsTruncated",
            False,
        ):
            break

        next_token = response.get(
            "NextContinuationToken"
        )

        if not isinstance(
            next_token,
            str,
        ):
            raise RuntimeError(
                "Truncated MinIO listing "
                "has no continuation token"
            )

        continuation_token = next_token
        page_number += 1

    return sorted(
        summaries,
        key=lambda item: item.object_key,
    )


def delete_object(
    bucket: str,
    object_key: str,
    client: BaseClient | None = None,
) -> None:
    if client is None:
        client = get_object_storage_client()

    run_object_storage_operation(
        operation=lambda: client.delete_object(
            Bucket=bucket,
            Key=object_key,
        ),
        operation_name=(
            f"delete_object:{bucket}/{object_key}"
        ),
    )


def run_smoke_test() -> None:
    client = get_object_storage_client()

    required_buckets = [
        "bronze",
        "silver",
        "gold",
        "quarantine",
        "logs",
    ]

    validate_required_buckets(
        required_buckets=required_buckets,
        client=client,
    )

    bucket_names = list_bucket_names(
        client=client
    )

    print(f"buckets={bucket_names}")

    bucket = "bronze"
    object_key = (
        "smoke-tests/object-storage/"
        "test-object.txt"
    )

    put_text_object(
        bucket=bucket,
        object_key=object_key,
        text=(
            "NeuroSleep object storage "
            "smoke test."
        ),
        client=client,
    )

    metadata = get_object_metadata(
        bucket=bucket,
        object_key=object_key,
        client=client,
    )

    print(f"bucket={bucket}")
    print(f"object_key={object_key}")
    print(
        "content_length="
        f"{metadata['content_length']}"
    )
    print(
        f"content_type={metadata['content_type']}"
    )
    print(f"etag={metadata['etag']}")

    delete_object(
        bucket=bucket,
        object_key=object_key,
        client=client,
    )

    print("smoke_test_cleanup=done")
    print("smoke_test_status=success")


if __name__ == "__main__":
    run_smoke_test()
