from unittest.mock import Mock, patch
from neuro_sleep.identifiers import new_uuid7

import neuro_sleep.ingestion.sleep_edf_object_recovery as recovery
from neuro_sleep.raw.models import RawFileRecord
from neuro_sleep.reliability.errors import (
    DatabaseTransientError,
)


SOURCE_SYSTEM = "physionet_sleep_edf"
SOURCE_URL = "https://example.test/test.edf"
BUCKET = "bronze"
OBJECT_KEY = "recovery-smoke/test.edf"
FILE_NAME = "test.edf"
FILE_TYPE = "edf"

CHECKSUM_SHA256 = "a" * 64
FILE_SIZE_BYTES = 1024


def build_uploaded_record(
    file_id,
) -> RawFileRecord:
    return RawFileRecord(
        file_id=file_id,
        source_system=SOURCE_SYSTEM,
        source_url=SOURCE_URL,
        bucket=BUCKET,
        object_key=OBJECT_KEY,
        file_name=FILE_NAME,
        file_type=FILE_TYPE,
        file_size_bytes=FILE_SIZE_BYTES,
        checksum_sha256=CHECKSUM_SHA256,
        ingestion_run_id=None,
        status="uploaded",
        ingested_at=None,
    )


def run_smoke_test() -> None:
    file_id = new_uuid7()

    register_mock = Mock(
        return_value=file_id
    )

    mark_uploaded_mock = Mock()

    with (
        patch.object(
            recovery,
            "get_object_metadata_or_none",
            return_value={
                "content_length": FILE_SIZE_BYTES,
                "checksum_sha256": CHECKSUM_SHA256,
            },
        ),
        patch.object(
            recovery,
            "register_raw_file",
            register_mock,
        ),
        patch.object(
            recovery,
            "mark_raw_file_uploaded",
            mark_uploaded_mock,
        ),
        patch.object(
            recovery,
            "get_raw_file_by_object_key",
            return_value=build_uploaded_record(
                file_id
            ),
        ),
    ):
        result = (
            recovery
            .recover_existing_verified_object(
                source_system=SOURCE_SYSTEM,
                source_url=SOURCE_URL,
                bucket=BUCKET,
                object_key=OBJECT_KEY,
                file_name=FILE_NAME,
                file_type=FILE_TYPE,
                expected_checksum_sha256=(
                    CHECKSUM_SHA256
                ),
                ingestion_run_id=None,
                client=Mock(),
            )
        )

    if result is None:
        raise RuntimeError(
            "Verified object was not recovered"
        )

    if result.file_id != file_id:
        raise RuntimeError(
            "Unexpected recovered file_id"
        )

    if register_mock.call_count != 1:
        raise RuntimeError(
            "Registry row was not registered"
        )

    if mark_uploaded_mock.call_count != 1:
        raise RuntimeError(
            "Registry row was not marked uploaded"
        )

    print(
        "verified_object_registry_recovered=true"
    )

    missing_register_mock = Mock()
    missing_mark_mock = Mock()

    with (
        patch.object(
            recovery,
            "get_object_metadata_or_none",
            return_value=None,
        ),
        patch.object(
            recovery,
            "register_raw_file",
            missing_register_mock,
        ),
        patch.object(
            recovery,
            "mark_raw_file_uploaded",
            missing_mark_mock,
        ),
    ):
        missing_result = (
            recovery
            .recover_existing_verified_object(
                source_system=SOURCE_SYSTEM,
                source_url=SOURCE_URL,
                bucket=BUCKET,
                object_key=OBJECT_KEY,
                file_name=FILE_NAME,
                file_type=FILE_TYPE,
                expected_checksum_sha256=(
                    CHECKSUM_SHA256
                ),
                ingestion_run_id=None,
                client=Mock(),
            )
        )

    if missing_result is not None:
        raise RuntimeError(
            "Missing object was incorrectly recovered"
        )

    if (
        missing_register_mock.call_count != 0
        or missing_mark_mock.call_count != 0
    ):
        raise RuntimeError(
            "Database was changed for missing object"
        )

    print("missing_object_not_recovered=true")

    mismatch_register_mock = Mock()
    mismatch_mark_mock = Mock()

    with (
        patch.object(
            recovery,
            "get_object_metadata_or_none",
            return_value={
                "content_length": FILE_SIZE_BYTES,
                "checksum_sha256": "b" * 64,
            },
        ),
        patch.object(
            recovery,
            "register_raw_file",
            mismatch_register_mock,
        ),
        patch.object(
            recovery,
            "mark_raw_file_uploaded",
            mismatch_mark_mock,
        ),
    ):
        mismatch_result = (
            recovery
            .recover_existing_verified_object(
                source_system=SOURCE_SYSTEM,
                source_url=SOURCE_URL,
                bucket=BUCKET,
                object_key=OBJECT_KEY,
                file_name=FILE_NAME,
                file_type=FILE_TYPE,
                expected_checksum_sha256=(
                    CHECKSUM_SHA256
                ),
                ingestion_run_id=None,
                client=Mock(),
            )
        )

    if mismatch_result is not None:
        raise RuntimeError(
            "Checksum mismatch was incorrectly recovered"
        )

    if (
        mismatch_register_mock.call_count != 0
        or mismatch_mark_mock.call_count != 0
    ):
        raise RuntimeError(
            "Database was changed for invalid object"
        )

    print(
        "checksum_mismatch_not_recovered=true"
    )

    with (
        patch.object(
            recovery,
            "get_object_metadata_or_none",
            return_value={
                "content_length": FILE_SIZE_BYTES,
                "checksum_sha256": CHECKSUM_SHA256,
            },
        ),
        patch.object(
            recovery,
            "register_raw_file",
            return_value=file_id,
        ),
        patch.object(
            recovery,
            "mark_raw_file_uploaded",
            side_effect=DatabaseTransientError(
                "PostgreSQL temporarily unavailable"
            ),
        ),
    ):
        try:
            recovery.recover_existing_verified_object(
                source_system=SOURCE_SYSTEM,
                source_url=SOURCE_URL,
                bucket=BUCKET,
                object_key=OBJECT_KEY,
                file_name=FILE_NAME,
                file_type=FILE_TYPE,
                expected_checksum_sha256=(
                    CHECKSUM_SHA256
                ),
                ingestion_run_id=None,
                client=Mock(),
            )

        except DatabaseTransientError:
            print(
                "recovery_db_error_propagated=true"
            )

        else:
            raise RuntimeError(
                "Recovery database error "
                "was not propagated"
            )

    print(
        "verified_object_remains_in_storage=true"
    )
    print(
        "object_recovery_smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
