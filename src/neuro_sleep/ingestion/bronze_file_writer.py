import hashlib
import mimetypes
from dataclasses import dataclass
from pathlib import Path
from string import hexdigits
from uuid import UUID

from botocore.client import BaseClient

from neuro_sleep.raw.file_registry import (
    mark_raw_file_failed,
    mark_raw_file_uploaded,
    register_raw_file,
)
from neuro_sleep.storage.object_storage import (
    delete_object,
    get_object_metadata,
    get_object_storage_client,
    put_file_object,
)


RunId = UUID | str


@dataclass(frozen=True)
class BronzeFileWriteResult:
    file_id: UUID
    local_file_path: Path
    bucket: str
    object_key: str
    file_name: str
    file_type: str
    content_type: str
    file_size_bytes: int
    checksum_sha256: str


def calculate_local_file_sha256(
    file_path: Path,
    chunk_size_bytes: int = 1024 * 1024,
) -> str:
    if chunk_size_bytes <= 0:
        raise ValueError(
            "chunk_size_bytes must be positive"
        )

    checksum = hashlib.sha256()

    with file_path.open("rb") as source_file:
        while True:
            chunk = source_file.read(
                chunk_size_bytes
            )

            if not chunk:
                break

            checksum.update(chunk)

    return checksum.hexdigest()


def validate_sha256(
    checksum_sha256: str,
) -> str:
    normalized_checksum = (
        checksum_sha256.strip().lower()
    )

    if len(normalized_checksum) != 64:
        raise ValueError(
            "SHA-256 must contain exactly "
            "64 hexadecimal characters"
        )

    if any(
        character not in hexdigits
        for character in normalized_checksum
    ):
        raise ValueError(
            "SHA-256 contains non-hexadecimal "
            "characters"
        )

    return normalized_checksum


def infer_file_name(
    object_key: str,
) -> str:
    file_name = (
        object_key.rstrip("/")
        .rsplit("/", 1)[-1]
    )

    if not file_name:
        raise ValueError(
            "Could not infer file name "
            "from object_key"
        )

    return file_name


def infer_file_type(
    file_name: str,
) -> str:
    if "." not in file_name:
        return "unknown"

    return (
        file_name.rsplit(".", 1)[-1]
        .lower()
    )


def infer_content_type(
    file_name: str,
) -> str:
    if file_name in {
        "RECORDS",
        "RECORDS-v1",
    }:
        return "text/plain"

    guessed_type, _ = mimetypes.guess_type(
        file_name
    )

    return (
        guessed_type
        or "application/octet-stream"
    )


def write_local_file_to_bronze_and_register(
    source_system: str,
    source_url: str,
    bucket: str,
    object_key: str,
    local_file_path: Path,
    expected_checksum_sha256: str | None = None,
    ingestion_run_id: RunId | None = None,
    file_name: str | None = None,
    file_type: str | None = None,
    content_type: str | None = None,
    client: BaseClient | None = None,
) -> BronzeFileWriteResult:
    resolved_file_path = (
        local_file_path.expanduser().resolve()
    )

    if not resolved_file_path.is_file():
        raise FileNotFoundError(
            "Local file not found: "
            f"{resolved_file_path}"
        )

    file_size_bytes = (
        resolved_file_path.stat().st_size
    )

    if file_size_bytes == 0:
        raise ValueError(
            "Local file is empty: "
            f"{resolved_file_path}"
        )

    checksum_sha256 = (
        calculate_local_file_sha256(
            resolved_file_path
        )
    )

    if expected_checksum_sha256 is not None:
        expected_checksum_sha256 = validate_sha256(
            expected_checksum_sha256
        )

        if (
            checksum_sha256
            != expected_checksum_sha256
        ):
            raise RuntimeError(
                "Local file SHA-256 mismatch: "
                f"expected={expected_checksum_sha256}, "
                f"actual={checksum_sha256}, "
                f"file={resolved_file_path}"
            )

    if file_name is None:
        file_name = infer_file_name(
            object_key
        )

    if file_type is None:
        file_type = infer_file_type(
            file_name
        )

    if content_type is None:
        content_type = infer_content_type(
            file_name
        )

    if client is None:
        client = get_object_storage_client()

    file_id = register_raw_file(
        source_system=source_system,
        source_url=source_url,
        bucket=bucket,
        object_key=object_key,
        file_name=file_name,
        file_type=file_type,
        ingestion_run_id=ingestion_run_id,
    )

    object_verified = False

    try:
        put_file_object(
            bucket=bucket,
            object_key=object_key,
            file_path=resolved_file_path,
            content_type=content_type,
            checksum_sha256=checksum_sha256,
            client=client,
        )

        object_metadata = get_object_metadata(
            bucket=bucket,
            object_key=object_key,
            client=client,
        )

        remote_size = object_metadata[
            "content_length"
        ]

        remote_checksum = object_metadata[
            "checksum_sha256"
        ]

        if remote_size != file_size_bytes:
            raise RuntimeError(
                "MinIO object size mismatch: "
                f"local={file_size_bytes}, "
                f"remote={remote_size}"
            )

        if remote_checksum != checksum_sha256:
            raise RuntimeError(
                "MinIO SHA-256 metadata mismatch: "
                f"local={checksum_sha256}, "
                f"remote={remote_checksum}"
            )

        object_verified = True

        mark_raw_file_uploaded(
            file_id=file_id,
            file_size_bytes=file_size_bytes,
            checksum_sha256=checksum_sha256,
            ingestion_run_id=ingestion_run_id,
        )

    except Exception as original_error:
        if object_verified:
            print(
                "verified_object_preserved="
                f"{bucket}/{object_key}"
            )
            print(
                "registry_finalization_pending=true"
            )

            raise

        cleanup_errors: list[str] = []

        try:
            delete_object(
                bucket=bucket,
                object_key=object_key,
                client=client,
            )

        except Exception as cleanup_error:
            cleanup_errors.append(
                "object cleanup failed: "
                f"{cleanup_error}"
            )

        try:
            mark_raw_file_failed(
                file_id=file_id,
                ingestion_run_id=ingestion_run_id,
            )

        except Exception as status_error:
            cleanup_errors.append(
                "registry status update failed: "
                f"{status_error}"
            )

        if cleanup_errors:
            details = "; ".join(
                cleanup_errors
            )

            raise RuntimeError(
                f"{original_error}; {details}"
            ) from original_error

        raise

    return BronzeFileWriteResult(
        file_id=file_id,
        local_file_path=resolved_file_path,
        bucket=bucket,
        object_key=object_key,
        file_name=file_name,
        file_type=file_type,
        content_type=content_type,
        file_size_bytes=file_size_bytes,
        checksum_sha256=checksum_sha256,
    )
