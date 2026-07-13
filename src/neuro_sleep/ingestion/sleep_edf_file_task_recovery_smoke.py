from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch
from neuro_sleep.identifiers import new_uuid7

import neuro_sleep.ingestion.sleep_edf_file_task as file_task
from neuro_sleep.ingestion.sleep_edf_object_recovery import (
    RecoveredObjectResult,
)


CHECKSUM_SHA256 = "a" * 64
FILE_SIZE_BYTES = 2048
BUCKET = "bronze"


def build_recovery_result(
    object_key: str,
) -> RecoveredObjectResult:
    return RecoveredObjectResult(
        file_id=new_uuid7(),
        bucket=BUCKET,
        object_key=object_key,
        file_size_bytes=FILE_SIZE_BYTES,
        checksum_sha256=CHECKSUM_SHA256,
    )


def run_smoke_test() -> None:
    source_file = SimpleNamespace(
        bucket=BUCKET,
        object_key=(
            "recovery-smoke/source-file.edf"
        ),
        checksum_sha256=CHECKSUM_SHA256,
        source_url=(
            "https://example.test/source-file.edf"
        ),
        file_name="source-file.edf",
        file_type="edf",
    )

    downloader_mock = Mock()
    writer_mock = Mock()

    with TemporaryDirectory() as temp_dir:
        with (
            patch.object(
                file_task,
                "existing_upload_is_valid",
                return_value=False,
            ),
            patch.object(
                file_task,
                "recover_existing_verified_object",
                return_value=build_recovery_result(
                    source_file.object_key
                ),
            ),
            patch.object(
                file_task,
                "download_sleep_edf_source_file",
                downloader_mock,
            ),
            patch.object(
                file_task,
                "write_local_file_to_bronze_and_register",
                writer_mock,
            ),
        ):
            result = file_task.run_source_file_task(
                source_file=source_file,
                destination_root=Path(temp_dir),
                settings=Mock(),
                download_session=Mock(),
                storage_client=Mock(),
                run_id=new_uuid7(),
            )

    if not result.skipped:
        raise RuntimeError(
            "Recovered source file was not skipped"
        )

    if downloader_mock.call_count != 0:
        raise RuntimeError(
            "Recovered source file was downloaded"
        )

    if writer_mock.call_count != 0:
        raise RuntimeError(
            "Recovered source file was re-uploaded"
        )

    print(
        "recovered_source_download_skipped=true"
    )
    print(
        "recovered_source_upload_skipped=true"
    )

    artifact = SimpleNamespace(
        bucket=BUCKET,
        object_key=(
            "recovery-smoke/SHA256SUMS.txt"
        ),
        source_url=(
            "https://example.test/SHA256SUMS.txt"
        ),
        file_name="SHA256SUMS.txt",
        file_type="txt",
    )

    manifest = SimpleNamespace(
        checksum_text=(
            f"{CHECKSUM_SHA256}  source-file.edf\n"
        )
    )

    control_writer_mock = Mock()

    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        with (
            patch.object(
                file_task,
                "existing_upload_is_valid",
                return_value=False,
            ),
            patch.object(
                file_task,
                "recover_existing_verified_object",
                return_value=build_recovery_result(
                    artifact.object_key
                ),
            ),
            patch.object(
                file_task,
                "write_local_file_to_bronze_and_register",
                control_writer_mock,
            ),
        ):
            control_result = (
                file_task.run_control_artifact_task(
                    artifact=artifact,
                    manifest=manifest,
                    destination_root=temp_path,
                    storage_client=Mock(),
                    run_id=new_uuid7(),
                )
            )

        remaining_files = list(
            temp_path.iterdir()
        )

    if not control_result.skipped:
        raise RuntimeError(
            "Recovered control artifact "
            "was not skipped"
        )

    if control_writer_mock.call_count != 0:
        raise RuntimeError(
            "Recovered control artifact "
            "was re-uploaded"
        )

    if remaining_files:
        raise RuntimeError(
            "Temporary control artifact "
            "was not deleted"
        )

    print(
        "recovered_control_upload_skipped=true"
    )
    print(
        "control_temporary_file_cleanup=true"
    )

    recovery_mock = Mock()
    existing_downloader_mock = Mock()

    with TemporaryDirectory() as temp_dir:
        with (
            patch.object(
                file_task,
                "existing_upload_is_valid",
                return_value=True,
            ),
            patch.object(
                file_task,
                "recover_existing_verified_object",
                recovery_mock,
            ),
            patch.object(
                file_task,
                "download_sleep_edf_source_file",
                existing_downloader_mock,
            ),
        ):
            existing_result = (
                file_task.run_source_file_task(
                    source_file=source_file,
                    destination_root=Path(temp_dir),
                    settings=Mock(),
                    download_session=Mock(),
                    storage_client=Mock(),
                    run_id=new_uuid7(),
                )
            )

    if not existing_result.skipped:
        raise RuntimeError(
            "Valid existing upload was not skipped"
        )

    if recovery_mock.call_count != 0:
        raise RuntimeError(
            "Recovery ran for an already valid upload"
        )

    if existing_downloader_mock.call_count != 0:
        raise RuntimeError(
            "Valid existing upload was downloaded"
        )

    print(
        "valid_existing_upload_skips_recovery=true"
    )
    print(
        "file_task_recovery_smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
