from unittest.mock import Mock, patch

import neuro_sleep.ingestion.sleep_edf_extract as extract
from neuro_sleep.identifiers import new_uuid7
from neuro_sleep.ingestion.sleep_edf_file_task import (
    SleepEdfFileTaskResult,
)


def run_smoke_test() -> None:
    run_id = new_uuid7()

    uploaded_attempt_id = new_uuid7()
    skipped_attempt_id = new_uuid7()
    failed_attempt_id = new_uuid7()

    uploaded_result = SleepEdfFileTaskResult(
        status="uploaded",
        resolution="downloaded_and_uploaded",
        object_key="test/uploaded.edf",
        file_size_bytes=2048,
        checksum_sha256="a" * 64,
    )

    skipped_result = SleepEdfFileTaskResult(
        status="skipped",
        resolution="existing_valid",
        object_key="test/skipped.edf",
        file_size_bytes=0,
    )

    uploaded_task = Mock(
        return_value=uploaded_result
    )

    skipped_task = Mock(
        return_value=skipped_result
    )

    simulated_error = RuntimeError(
        "Simulated tracked file failure"
    )

    failed_task = Mock(
        side_effect=simulated_error
    )

    with (
        patch.object(
            extract,
            "start_file_attempt",
            side_effect=[
                uploaded_attempt_id,
                skipped_attempt_id,
                failed_attempt_id,
            ],
        ) as start_mock,
        patch.object(
            extract,
            "finish_file_attempt_uploaded",
        ) as uploaded_mock,
        patch.object(
            extract,
            "finish_file_attempt_skipped",
        ) as skipped_mock,
        patch.object(
            extract,
            "finish_file_attempt_failed",
        ) as failed_mock,
    ):
        returned_uploaded = (
            extract.run_tracked_file_task(
                run_id=run_id,
                source_system=(
                    "physionet_sleep_edf"
                ),
                source_url=(
                    "https://example.local/"
                    "uploaded.edf"
                ),
                bucket="bronze",
                object_key="test/uploaded.edf",
                file_name="uploaded.edf",
                file_type="edf",
                task=uploaded_task,
            )
        )

        returned_skipped = (
            extract.run_tracked_file_task(
                run_id=run_id,
                source_system=(
                    "physionet_sleep_edf"
                ),
                source_url=(
                    "https://example.local/"
                    "skipped.edf"
                ),
                bucket="bronze",
                object_key="test/skipped.edf",
                file_name="skipped.edf",
                file_type="edf",
                task=skipped_task,
            )
        )

        try:
            extract.run_tracked_file_task(
                run_id=run_id,
                source_system=(
                    "physionet_sleep_edf"
                ),
                source_url=(
                    "https://example.local/"
                    "failed.edf"
                ),
                bucket="bronze",
                object_key="test/failed.edf",
                file_name="failed.edf",
                file_type="edf",
                task=failed_task,
            )

        except RuntimeError as error:
            if error is not simulated_error:
                raise RuntimeError(
                    "Original tracked task "
                    "error was replaced"
                ) from error

        else:
            raise RuntimeError(
                "Tracked task failure "
                "was not propagated"
            )

    if returned_uploaded != uploaded_result:
        raise RuntimeError(
            "Uploaded task result changed"
        )

    if returned_skipped != skipped_result:
        raise RuntimeError(
            "Skipped task result changed"
        )

    if start_mock.call_count != 3:
        raise RuntimeError(
            "Unexpected attempt start count"
        )

    uploaded_mock.assert_called_once_with(
        attempt_id=uploaded_attempt_id,
        file_size_bytes=2048,
        checksum_sha256="a" * 64,
    )

    skipped_mock.assert_called_once_with(
        attempt_id=skipped_attempt_id,
        resolution="existing_valid",
    )

    failed_mock.assert_called_once_with(
        attempt_id=failed_attempt_id,
        error=simulated_error,
    )

    print("uploaded_attempt_finished=true")
    print("skipped_attempt_finished=true")
    print("failed_attempt_finished=true")
    print("original_task_error_preserved=true")
    print(
        "file_attempt_integration_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
