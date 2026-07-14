from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory

from botocore.client import BaseClient
import pyarrow as pa

from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)
from neuro_sleep.silver.parquet_tables import (
    write_silver_parquet,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
)


PARQUET_CONTENT_TYPE = (
    "application/vnd.apache.parquet"
)


@dataclass(frozen=True)
class SilverObjectWriteResult:
    bucket: str
    object_key: str
    dataset_name: str
    row_count: int
    file_size_bytes: int
    checksum_sha256: str
    etag: str


def validate_silver_object_reference(
    bucket: str,
    object_key: str,
) -> None:
    if not bucket.strip():
        raise ValueError(
            "bucket cannot be empty"
        )

    if not object_key.strip():
        raise ValueError(
            "object_key cannot be empty"
        )

    if object_key.startswith("/"):
        raise ValueError(
            "object_key must be relative"
        )

    if "\\" in object_key:
        raise ValueError(
            "object_key must use forward "
            "slashes"
        )

    object_path = PurePosixPath(
        object_key
    )

    if ".." in object_path.parts:
        raise ValueError(
            "Parent path traversal is not "
            "allowed"
        )

    if object_path.suffix.lower() != (
        ".parquet"
    ):
        raise ValueError(
            "Silver object key must end "
            "with .parquet"
        )


def get_schema_text_metadata(
    table: pa.Table,
    key: bytes,
) -> str:
    metadata = table.schema.metadata

    if metadata is None:
        raise ValueError(
            "Arrow schema metadata is "
            "missing"
        )

    value = metadata.get(key)

    if value is None:
        raise ValueError(
            "Required Arrow schema metadata "
            f"is missing: {key!r}"
        )

    return value.decode("utf-8")


def calculate_file_sha256(
    file_path: Path,
) -> str:
    digest = sha256()

    with file_path.open("rb") as stream:
        while chunk := stream.read(
            1024 * 1024
        ):
            digest.update(chunk)

    return digest.hexdigest()


def upload_silver_table(
    table: pa.Table,
    bucket: str,
    object_key: str,
    *,
    client: BaseClient | None = None,
) -> SilverObjectWriteResult:
    validate_silver_object_reference(
        bucket=bucket,
        object_key=object_key,
    )

    if table.num_rows <= 0:
        raise ValueError(
            "Silver table cannot be empty"
        )

    layer_name = (
        get_schema_text_metadata(
            table=table,
            key=b"lakehouse_layer",
        )
    )

    if layer_name != "silver":
        raise ValueError(
            "Only Silver Arrow tables can "
            "be uploaded"
        )

    dataset_name = (
        get_schema_text_metadata(
            table=table,
            key=b"dataset_name",
        )
    )

    schema_version = (
        get_schema_text_metadata(
            table=table,
            key=b"schema_version",
        )
    )

    owns_client = client is None

    if client is None:
        client = get_object_storage_client()

    try:
        with TemporaryDirectory(
            prefix=(
                "neuro_sleep_silver_upload_"
            )
        ) as temporary_directory:
            local_path = (
                Path(temporary_directory)
                / Path(object_key).name
            )

            write_silver_parquet(
                table=table,
                output_path=local_path,
            )

            file_size_bytes = (
                local_path.stat().st_size
            )

            checksum_sha256 = (
                calculate_file_sha256(
                    local_path
                )
            )

            metadata = {
                "lakehouse_layer": "silver",
                "dataset_name": dataset_name,
                "schema_version": schema_version,
                "row_count": str(
                    table.num_rows
                ),
                "checksum_sha256": (
                    checksum_sha256
                ),
            }

            run_object_storage_operation(
                operation=lambda: (
                    client.upload_file(
                        Filename=str(
                            local_path
                        ),
                        Bucket=bucket,
                        Key=object_key,
                        ExtraArgs={
                            "ContentType": (
                                PARQUET_CONTENT_TYPE
                            ),
                            "Metadata": metadata,
                        },
                    )
                ),
                operation_name=(
                    f"upload_file:{bucket}/"
                    f"{object_key}"
                ),
            )

            head = (
                run_object_storage_operation(
                    operation=lambda: (
                        client.head_object(
                            Bucket=bucket,
                            Key=object_key,
                        )
                    ),
                    operation_name=(
                        f"head_object:{bucket}/"
                        f"{object_key}"
                    ),
                )
            )

            actual_size = int(
                head["ContentLength"]
            )

            if (
                actual_size
                != file_size_bytes
            ):
                raise RuntimeError(
                    "Uploaded Silver object "
                    "size mismatch: "
                    f"expected="
                    f"{file_size_bytes}, "
                    f"actual={actual_size}"
                )

            actual_metadata = (
                head.get("Metadata", {})
            )

            for key, expected_value in (
                metadata.items()
            ):
                actual_value = (
                    actual_metadata.get(key)
                )

                if (
                    actual_value
                    != expected_value
                ):
                    raise RuntimeError(
                        "Uploaded Silver object "
                        "metadata mismatch: "
                        f"key={key}, "
                        f"expected="
                        f"{expected_value!r}, "
                        f"actual="
                        f"{actual_value!r}"
                    )

            content_type = head.get(
                "ContentType"
            )

            if (
                content_type
                != PARQUET_CONTENT_TYPE
            ):
                raise RuntimeError(
                    "Uploaded Silver object "
                    "content type mismatch: "
                    f"{content_type!r}"
                )

            return SilverObjectWriteResult(
                bucket=bucket,
                object_key=object_key,
                dataset_name=dataset_name,
                row_count=table.num_rows,
                file_size_bytes=(
                    file_size_bytes
                ),
                checksum_sha256=(
                    checksum_sha256
                ),
                etag=str(
                    head.get("ETag", "")
                ),
            )

    finally:
        if owns_client:
            client.close()
