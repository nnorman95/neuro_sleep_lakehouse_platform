import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
from neuro_sleep.identifiers import new_uuid7

import neuro_sleep.ingestion.bronze_file_writer as writer
from neuro_sleep.reliability.errors import (
    DatabaseTransientError,
    ObjectStorageTransientError,
)


def run_smoke_test() -> None:
    payload = (
        b"NeuroSleep Bronze failure smoke test."
    )

    checksum_sha256 = hashlib.sha256(
        payload
    ).hexdigest()

    file_id = new_uuid7()

    with TemporaryDirectory() as temp_dir:
        local_path = (
            Path(temp_dir)
            / "test-object.txt"
        )

        local_path.write_bytes(payload)

        delete_mock = Mock()
        failed_mock = Mock()

        with (
            patch.object(
                writer,
                "register_raw_file",
                return_value=file_id,
            ),
            patch.object(
                writer,
                "put_file_object",
            ),
            patch.object(
                writer,
                "get_object_metadata",
                return_value={
                    "content_length": len(payload),
                    "checksum_sha256": (
                        checksum_sha256
                    ),
                },
            ),
            patch.object(
                writer,
                "mark_raw_file_uploaded",
                side_effect=DatabaseTransientError(
                    "PostgreSQL temporarily unavailable"
                ),
            ),
            patch.object(
                writer,
                "delete_object",
                delete_mock,
            ),
            patch.object(
                writer,
                "mark_raw_file_failed",
                failed_mock,
            ),
        ):
            try:
                writer.write_local_file_to_bronze_and_register(
                    source_system=(
                        "physionet_sleep_edf"
                    ),
                    source_url=(
                        "https://example.test/"
                        "test-object.txt"
                    ),
                    bucket="bronze",
                    object_key=(
                        "failure-smoke/"
                        "verified-object.txt"
                    ),
                    local_file_path=local_path,
                    expected_checksum_sha256=(
                        checksum_sha256
                    ),
                    client=Mock(),
                )

            except DatabaseTransientError:
                print(
                    "verified_object_db_error="
                    "propagated"
                )

            else:
                raise RuntimeError(
                    "Database finalization error "
                    "was not propagated"
                )

        if delete_mock.call_count != 0:
            raise RuntimeError(
                "Verified MinIO object was deleted"
            )

        if failed_mock.call_count != 0:
            raise RuntimeError(
                "Verified object was incorrectly "
                "marked as failed"
            )

        print(
            "verified_object_preserved_after_db_error=true"
        )

        delete_mock = Mock()
        failed_mock = Mock()

        with (
            patch.object(
                writer,
                "register_raw_file",
                return_value=file_id,
            ),
            patch.object(
                writer,
                "put_file_object",
                side_effect=(
                    ObjectStorageTransientError(
                        "MinIO upload failed"
                    )
                ),
            ),
            patch.object(
                writer,
                "delete_object",
                delete_mock,
            ),
            patch.object(
                writer,
                "mark_raw_file_failed",
                failed_mock,
            ),
        ):
            try:
                writer.write_local_file_to_bronze_and_register(
                    source_system=(
                        "physionet_sleep_edf"
                    ),
                    source_url=(
                        "https://example.test/"
                        "upload-failure.txt"
                    ),
                    bucket="bronze",
                    object_key=(
                        "failure-smoke/"
                        "upload-failure.txt"
                    ),
                    local_file_path=local_path,
                    expected_checksum_sha256=(
                        checksum_sha256
                    ),
                    client=Mock(),
                )

            except ObjectStorageTransientError:
                print(
                    "failed_upload_error=propagated"
                )

            else:
                raise RuntimeError(
                    "Upload failure was not propagated"
                )

        if delete_mock.call_count != 1:
            raise RuntimeError(
                "Failed upload cleanup did not run"
            )

        if failed_mock.call_count != 1:
            raise RuntimeError(
                "Failed upload was not marked failed"
            )

        print(
            "failed_upload_cleanup=true"
        )

        delete_mock = Mock()
        failed_mock = Mock()

        with (
            patch.object(
                writer,
                "register_raw_file",
                return_value=file_id,
            ),
            patch.object(
                writer,
                "put_file_object",
            ),
            patch.object(
                writer,
                "get_object_metadata",
                return_value={
                    "content_length": len(payload),
                    "checksum_sha256": "0" * 64,
                },
            ),
            patch.object(
                writer,
                "delete_object",
                delete_mock,
            ),
            patch.object(
                writer,
                "mark_raw_file_failed",
                failed_mock,
            ),
        ):
            try:
                writer.write_local_file_to_bronze_and_register(
                    source_system=(
                        "physionet_sleep_edf"
                    ),
                    source_url=(
                        "https://example.test/"
                        "bad-metadata.txt"
                    ),
                    bucket="bronze",
                    object_key=(
                        "failure-smoke/"
                        "bad-metadata.txt"
                    ),
                    local_file_path=local_path,
                    expected_checksum_sha256=(
                        checksum_sha256
                    ),
                    client=Mock(),
                )

            except RuntimeError:
                print(
                    "invalid_object_error=propagated"
                )

            else:
                raise RuntimeError(
                    "Invalid MinIO metadata "
                    "was not rejected"
                )

        if delete_mock.call_count != 1:
            raise RuntimeError(
                "Invalid object was not deleted"
            )

        if failed_mock.call_count != 1:
            raise RuntimeError(
                "Invalid object was not marked failed"
            )

        print(
            "invalid_object_cleanup=true"
        )

    print(
        "bronze_file_writer_failure_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
