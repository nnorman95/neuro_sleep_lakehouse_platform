from dataclasses import dataclass
from string import hexdigits
from uuid import UUID

from botocore.client import BaseClient

from neuro_sleep.ingestion.sleep_edf_object_state import (
    get_object_metadata_or_none,
)
from neuro_sleep.raw.file_registry import (
    get_raw_file_by_object_key,
    mark_raw_file_uploaded,
    register_raw_file,
)


RunId = UUID | str


@dataclass(frozen=True)
class RecoveredObjectResult:
    file_id: UUID
    bucket: str
    object_key: str
    file_size_bytes: int
    checksum_sha256: str


def normalize_sha256(
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


def recover_existing_verified_object(
    source_system: str,
    source_url: str,
    bucket: str,
    object_key: str,
    file_name: str,
    file_type: str,
    expected_checksum_sha256: str,
    ingestion_run_id: RunId | None,
    client: BaseClient,
) -> RecoveredObjectResult | None:
    expected_checksum = normalize_sha256(
        expected_checksum_sha256
    )

    object_metadata = get_object_metadata_or_none(
        bucket=bucket,
        object_key=object_key,
        client=client,
    )

    if object_metadata is None:
        return None

    remote_size = object_metadata.get(
        "content_length"
    )

    remote_checksum = object_metadata.get(
        "checksum_sha256"
    )

    if (
        not isinstance(remote_size, int)
        or remote_size <= 0
    ):
        return None

    if remote_checksum != expected_checksum:
        return None

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
        file_size_bytes=remote_size,
        checksum_sha256=expected_checksum,
        ingestion_run_id=ingestion_run_id,
    )

    registry_record = (
        get_raw_file_by_object_key(
            bucket=bucket,
            object_key=object_key,
        )
    )

    if registry_record is None:
        raise RuntimeError(
            "Recovered registry record "
            "was not found"
        )

    if registry_record.status != "uploaded":
        raise RuntimeError(
            "Recovered registry status "
            f"is not uploaded: {registry_record.status}"
        )

    if (
        registry_record.file_size_bytes
        != remote_size
    ):
        raise RuntimeError(
            "Recovered registry file size "
            "does not match MinIO"
        )

    if (
        registry_record.checksum_sha256
        != expected_checksum
    ):
        raise RuntimeError(
            "Recovered registry checksum "
            "does not match MinIO"
        )

    return RecoveredObjectResult(
        file_id=file_id,
        bucket=bucket,
        object_key=object_key,
        file_size_bytes=remote_size,
        checksum_sha256=expected_checksum,
    )
