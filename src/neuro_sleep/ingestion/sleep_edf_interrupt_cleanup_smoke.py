from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

import neuro_sleep.ingestion.sleep_edf_extract as extract
import neuro_sleep.ingestion.sleep_edf_http_downloader as downloader
from neuro_sleep.identifiers import new_uuid7


class FakeResource:
    def __init__(self) -> None:
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1


class FakePipelineLock:
    created_instances: list[
        "FakePipelineLock"
    ] = []

    def __init__(
        self,
        pipeline_name: str,
        settings=None,
    ) -> None:
        self.pipeline_name = pipeline_name
        self.settings = settings
        self.acquire_count = 0
        self.release_count = 0
        self.created_instances.append(self)

    def acquire(self) -> None:
        self.acquire_count += 1

    def release(self) -> None:
        self.release_count += 1


class FakeHeartbeat:
    created_instances: list[
        "FakeHeartbeat"
    ] = []

    def __init__(
        self,
        run_id,
        pipeline_name: str,
        interval_seconds: float,
        **kwargs,
    ) -> None:
        self.run_id = run_id
        self.pipeline_name = pipeline_name
        self.interval_seconds = interval_seconds
        self.start_count = 0
        self.stop_count = 0
        self.created_instances.append(self)

    def start(self) -> None:
        self.start_count += 1

    def stop(self) -> None:
        self.stop_count += 1


class FakeProgressReporter:
    def __init__(self) -> None:
        self.start_count = 0
        self.update_count = 0
        self.fail_count = 0
        self.failed_error: BaseException | None = None

    def start(
        self,
        total_bytes: int | None,
    ) -> None:
        self.start_count += 1

    def update(
        self,
        downloaded_bytes: int,
        total_bytes: int | None = None,
    ) -> None:
        self.update_count += 1

    def complete(
        self,
        downloaded_bytes: int,
        total_bytes: int | None = None,
    ) -> None:
        raise RuntimeError(
            "Interrupted download cannot "
            "complete"
        )

    def fail(
        self,
        error: BaseException,
        downloaded_bytes: int,
        total_bytes: int | None = None,
    ) -> None:
        self.fail_count += 1
        self.failed_error = error


class InterruptingResponse:
    status_code = 200
    reason = "OK"

    def __init__(
        self,
        interrupt: KeyboardInterrupt,
    ) -> None:
        self.interrupt = interrupt
        self.headers = {
            "Content-Length": "6",
        }
        self.close_count = 0

    def iter_content(
        self,
        chunk_size: int,
    ):
        yield b"abc"
        raise self.interrupt

    def close(self) -> None:
        self.close_count += 1


class FakeDownloadSession:
    def __init__(
        self,
        response: InterruptingResponse,
    ) -> None:
        self.response = response

    def get(
        self,
        source_url: str,
        stream: bool,
        timeout,
    ) -> InterruptingResponse:
        return self.response


def run_pipeline_interrupt_check() -> None:
    run_id = new_uuid7()
    attempt_id = new_uuid7()
    interrupt = KeyboardInterrupt()

    settings = SimpleNamespace(
        data_profile="sample",
        sleep_edf_version="1.0.0",
        sleep_edf_max_recordings=1,
    )

    source_file = SimpleNamespace(
        source_url=(
            "https://example.local/"
            "sleep-cassette/"
            "interrupted.edf"
        ),
        bucket="bronze",
        object_key=(
            "physionet/sleep-edfx/1.0.0/"
            "sleep-cassette/"
            "interrupted.edf"
        ),
        file_name="interrupted.edf",
        file_type="edf",
        relative_path=(
            "sleep-cassette/"
            "interrupted.edf"
        ),
    )

    manifest = SimpleNamespace(
        dataset_version="1.0.0",
        selected_recording_count=1,
        selected_extract_object_count=1,
        selected_source_file_count=1,
        selected_control_artifact_count=0,
        selected_files=(source_file,),
        control_artifacts=(),
    )

    storage_client = FakeResource()
    download_session = FakeResource()

    finish_success = Mock()
    finish_failed = Mock()
    finish_attempt_failed = Mock()

    regular_events: list[str] = []
    exception_events: list[str] = []

    def record_event(
        event: str,
        **fields,
    ) -> dict:
        regular_events.append(event)
        return {
            "event": event,
            **fields,
        }

    def record_exception(
        event: str,
        error: BaseException,
        **fields,
    ) -> dict:
        exception_events.append(event)
        return {
            "event": event,
            "error_type": (
                type(error).__name__
            ),
            "error_message": str(error),
            **fields,
        }

    FakePipelineLock.created_instances.clear()
    FakeHeartbeat.created_instances.clear()

    with (
        patch.object(
            extract,
            "get_settings",
            return_value=settings,
        ),
        patch.object(
            extract,
            "start_pipeline_run",
            return_value=run_id,
        ),
        patch.object(
            extract,
            "PipelineRunLock",
            FakePipelineLock,
        ),
        patch.object(
            extract,
            "PipelineHeartbeat",
            FakeHeartbeat,
        ),
        patch.object(
            extract,
            "fetch_sleep_edf_remote_manifest",
            return_value=manifest,
        ),
        patch.object(
            extract,
            "get_object_storage_client",
            return_value=storage_client,
        ),
        patch.object(
            extract,
            "create_download_session",
            return_value=download_session,
        ),
        patch.object(
            extract,
            "run_source_file_task",
            side_effect=interrupt,
        ),
        patch.object(
            extract,
            "start_file_attempt",
            return_value=attempt_id,
        ),
        patch.object(
            extract,
            "finish_file_attempt_uploaded",
        ),
        patch.object(
            extract,
            "finish_file_attempt_skipped",
        ),
        patch.object(
            extract,
            "finish_file_attempt_failed",
            finish_attempt_failed,
        ),
        patch.object(
            extract,
            "finish_pipeline_run_success",
            finish_success,
        ),
        patch.object(
            extract,
            "finish_pipeline_run_failed",
            finish_failed,
        ),
        patch.object(
            extract,
            "emit_event",
            side_effect=record_event,
        ),
        patch.object(
            extract,
            "emit_exception",
            side_effect=record_exception,
        ),
    ):
        try:
            extract.run_sleep_edf_extract()

        except KeyboardInterrupt as error:
            if error is not interrupt:
                raise RuntimeError(
                    "Original KeyboardInterrupt "
                    "was replaced"
                ) from error

        else:
            raise RuntimeError(
                "KeyboardInterrupt was not "
                "propagated"
            )

    finish_attempt_failed.assert_called_once_with(
        attempt_id=attempt_id,
        error=interrupt,
    )

    if finish_success.call_count != 0:
        raise RuntimeError(
            "Interrupted pipeline was marked "
            "successful"
        )

    if finish_failed.call_count != 1:
        raise RuntimeError(
            "Interrupted pipeline was not "
            "marked failed"
        )

    failure_kwargs = (
        finish_failed.call_args.kwargs
    )

    if (
        failure_kwargs.get(
            "error_message"
        )
        != "KeyboardInterrupt"
    ):
        raise RuntimeError(
            "KeyboardInterrupt failure message "
            "was not persisted"
        )

    if (
        failure_kwargs.get(
            "files_processed"
        )
        != 0
    ):
        raise RuntimeError(
            "Interrupted file was counted as "
            "processed"
        )

    if len(
        FakePipelineLock.created_instances
    ) != 1:
        raise RuntimeError(
            "Unexpected pipeline lock count"
        )

    pipeline_lock = (
        FakePipelineLock.created_instances[0]
    )

    if (
        pipeline_lock.acquire_count != 1
        or pipeline_lock.release_count != 1
    ):
        raise RuntimeError(
            "Pipeline lock was not released "
            "after interruption"
        )

    if len(
        FakeHeartbeat.created_instances
    ) != 1:
        raise RuntimeError(
            "Unexpected heartbeat count"
        )

    heartbeat = (
        FakeHeartbeat.created_instances[0]
    )

    if (
        heartbeat.start_count != 1
        or heartbeat.stop_count != 1
    ):
        raise RuntimeError(
            "Heartbeat was not stopped once "
            "after interruption"
        )

    if storage_client.close_count != 1:
        raise RuntimeError(
            "Storage client was not closed"
        )

    if download_session.close_count != 1:
        raise RuntimeError(
            "Download session was not closed"
        )

    if "pipeline_failed" not in (
        exception_events
    ):
        raise RuntimeError(
            "Interrupted pipeline failure "
            "event was not emitted"
        )

    if (
        "resource_cleanup_completed"
        not in regular_events
    ):
        raise RuntimeError(
            "Interrupted pipeline cleanup "
            "event was not emitted"
        )


def run_download_interrupt_check() -> None:
    interrupt = KeyboardInterrupt()
    response = InterruptingResponse(
        interrupt
    )
    session = FakeDownloadSession(
        response
    )
    progress = FakeProgressReporter()

    source_file = SimpleNamespace(
        source_url=(
            "https://example.local/"
            "interrupted.edf"
        ),
        relative_path=(
            "sleep-cassette/"
            "interrupted.edf"
        ),
        checksum_sha256="0" * 64,
    )

    with TemporaryDirectory(
        prefix=(
            "neuro_sleep_interrupt_"
            "download_"
        )
    ) as temporary_directory:
        root = Path(
            temporary_directory
        )
        destination_path = (
            root / "interrupted.edf"
        )
        partial_path = (
            root / "interrupted.edf.part"
        )

        try:
            downloader.download_once(
                source_file=source_file,
                destination_path=(
                    destination_path
                ),
                partial_path=partial_path,
                session=session,
                chunk_size_bytes=3,
                connect_timeout_seconds=1,
                read_timeout_seconds=1,
                progress_reporter=progress,
            )

        except KeyboardInterrupt as error:
            if error is not interrupt:
                raise RuntimeError(
                    "Download interruption "
                    "was replaced"
                ) from error

        else:
            raise RuntimeError(
                "Download interruption was "
                "not propagated"
            )

        if partial_path.exists():
            raise RuntimeError(
                "Interrupted partial download "
                "was not deleted"
            )

        if destination_path.exists():
            raise RuntimeError(
                "Interrupted download created "
                "a final file"
            )

    if progress.fail_count != 1:
        raise RuntimeError(
            "Interrupted download was not "
            "reported as failed"
        )

    if progress.failed_error is not (
        interrupt
    ):
        raise RuntimeError(
            "Progress reporter received the "
            "wrong interruption"
        )

    if response.close_count != 1:
        raise RuntimeError(
            "Interrupted HTTP response was "
            "not closed"
        )


def run_smoke_test() -> None:
    run_pipeline_interrupt_check()
    run_download_interrupt_check()

    print(
        "file_attempt_failed_after_interrupt="
        "true"
    )
    print(
        "pipeline_failed_after_interrupt=true"
    )
    print(
        "pipeline_lock_released_after_interrupt="
        "true"
    )
    print(
        "heartbeat_stopped_after_interrupt=true"
    )
    print(
        "resources_closed_after_interrupt=true"
    )
    print(
        "partial_download_removed_after_interrupt="
        "true"
    )
    print(
        "download_response_closed_after_interrupt="
        "true"
    )
    print(
        "interrupt_cleanup_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
