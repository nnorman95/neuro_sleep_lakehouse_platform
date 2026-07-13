from botocore.client import BaseClient
from botocore.exceptions import ClientError

from neuro_sleep.raw.file_registry import (
    get_raw_file_by_object_key,
)
from neuro_sleep.storage.object_storage import (
    get_object_metadata,
)


OBJECT_NOT_FOUND_ERROR_CODES = {
    "404",
    "NoSuchKey",
    "NotFound",
}


def get_object_metadata_or_none(
    bucket: str,
    object_key: str,
    client: BaseClient,
) -> dict | None:
    try:
        return get_object_metadata(
            bucket=bucket,
            object_key=object_key,
            client=client,
        )

    except ClientError as exc:
        error = exc.response.get(
            "Error",
            {},
        )

        response_metadata = exc.response.get(
            "ResponseMetadata",
            {},
        )

        error_code = str(
            error.get("Code", "")
        )

        status_code = response_metadata.get(
            "HTTPStatusCode"
        )

        if (
            error_code
            in OBJECT_NOT_FOUND_ERROR_CODES
            or status_code == 404
        ):
            return None

        raise


def existing_upload_is_valid(
    bucket: str,
    object_key: str,
    expected_checksum_sha256: str,
    client: BaseClient,
) -> bool:
    registry_record = (
        get_raw_file_by_object_key(
            bucket=bucket,
            object_key=object_key,
        )
    )

    if registry_record is None:
        return False

    if registry_record.status != "uploaded":
        return False

    if (
        registry_record.checksum_sha256
        != expected_checksum_sha256
    ):
        return False

    if registry_record.file_size_bytes is None:
        return False

    object_metadata = (
        get_object_metadata_or_none(
            bucket=bucket,
            object_key=object_key,
            client=client,
        )
    )

    if object_metadata is None:
        return False

    if (
        object_metadata["content_length"]
        != registry_record.file_size_bytes
    ):
        return False

    if (
        object_metadata["checksum_sha256"]
        != expected_checksum_sha256
    ):
        return False

    return True
