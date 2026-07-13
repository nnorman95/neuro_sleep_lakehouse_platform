import hashlib
from dataclasses import dataclass
from uuid import UUID

from neuro_sleep.raw.file_registry import (
    mark_raw_file_uploaded,
    register_raw_file,
)
from neuro_sleep.storage.object_storage import (
    get_object_metadata,
    get_object_storage_client,
    put_bytes_object,
)


RunId = UUID | str


@dataclass(frozen=True)
class BronzeWriteResult:
    file_id: UUID
    bucket: str
    object_key: str
    file_name: str
    file_type: str
    file_size_bytes: int
    checksum_sha256: str


def calculate_sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def infer_file_name(object_key: str) -> str:
    return object_key.rstrip("/").rsplit("/", 1)[-1]


def infer_file_type(file_name: str) -> str:
    if "." not in file_name:
        return "unknown"

    return file_name.rsplit(".", 1)[-1].lower()


def write_bytes_to_bronze_and_register(
    source_system: str,
    bucket: str,
    object_key: str,
    data: bytes,
    source_url: str | None = None,
    ingestion_run_id: RunId | None = None,
    content_type: str = "application/octet-stream",
    file_name: str | None = None,
    file_type: str | None = None,
) -> BronzeWriteResult:
    if not data:
        raise ValueError("data must not be empty")

    if file_name is None:
        file_name = infer_file_name(object_key)

    if file_type is None:
        file_type = infer_file_type(file_name)

    checksum_sha256 = calculate_sha256_bytes(data)

    client = get_object_storage_client()

    put_bytes_object(
        bucket=bucket,
        object_key=object_key,
        data=data,
        content_type=content_type,
        client=client,
    )

    metadata = get_object_metadata(
        bucket=bucket,
        object_key=object_key,
        client=client,
    )

    file_size_bytes = metadata["content_length"]

    if file_size_bytes != len(data):
        raise RuntimeError(
            f"Object size mismatch for {bucket}/{object_key}: "
            f"metadata={file_size_bytes}, local={len(data)}"
        )

    file_id = register_raw_file(
        source_system=source_system,
        source_url=source_url,
        bucket=bucket,
        object_key=object_key,
        file_name=file_name,
        file_type=file_type,
        ingestion_run_id=ingestion_run_id,
    )

    mark_raw_file_uploaded(
        file_id=file_id,
        file_size_bytes=file_size_bytes,
        checksum_sha256=checksum_sha256,
        ingestion_run_id=ingestion_run_id,
    )

    return BronzeWriteResult(
        file_id=file_id,
        bucket=bucket,
        object_key=object_key,
        file_name=file_name,
        file_type=file_type,
        file_size_bytes=file_size_bytes,
        checksum_sha256=checksum_sha256,
    )
