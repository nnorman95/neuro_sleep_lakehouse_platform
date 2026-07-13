from types import SimpleNamespace
from unittest.mock import Mock, patch

import neuro_sleep.ingestion.sleep_edf_extract as extract
from neuro_sleep.identifiers import new_uuid7


class FakeResource:
    def __init__(
        self,
        close_error: BaseException | None = None,
    ) -> None:
        self.close_error = close_error
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1

        if self.close_error is not None:
            raise self.close_error


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
        self.is_acquired = False

        self.created_instances.append(self)

    def acquire(self) -> None:
        self.acquire_count += 1
        self.is_acquired = True

    def release(self) -> None:
        self.release_count += 1
        self.is_acquired = False


class FakeHeartbeat:
    created_instances: list["FakeHeartbeat"] = []

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

    def stop(
        self,
        *args,
        **kwargs,
    ) -> None:
        self.stop_count += 1


def find_storage_factory_name() -> str:
    candidates = (
        "get_object_storage_client",
        "create_object_storage_client",
    )

    for candidate in candidates:
        if hasattr(extract, candidate):
            return candidate

    raise RuntimeError(
        "Object-storage factory was not found "
        "inside sleep_edf_extract"
    )


def run_smoke_test() -> None:
    run_id = new_uuid7()

    regular_events: list[str] = []

    exception_events: list[
        tuple[str, str, str]
    ] = []

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
        exception_events.append(
            (
                event,
                type(error).__name__,
                str(error),
            )
        )

        return {
            "event": event,
            "error_type": type(error).__name__,
            "error_message": str(error),
            **fields,
        }

    settings = SimpleNamespace(
        data_profile="sample",
        sleep_edf_version="1.0.0",
        sleep_edf_max_recordings=1,
    )

    source_file = SimpleNamespace(
        source_url=(
            "https://example.local/"
            "sleep-cassette/"
            "observability-failure.edf"
        ),
        bucket="bronze",
        object_key=(
            "physionet/sleep-edfx/1.0.0/"
            "sleep-cassette/"
            "observability-failure.edf"
        ),
        file_name=(
            "observability-failure.edf"
        ),
        file_type="edf",
        relative_path=(
            "sleep-cassette/"
            "observability-failure.edf"
        ),
    )

    manifest = SimpleNamespace(
        dataset_version="1.0.0",
        selected_recording_count=1,
        selected_extract_object_count=1,
        selected_source_file_count=1,
        selected_control_artifact_count=0,
        selected_files=(
            source_file,
        ),
        control_artifacts=(),
    )

    storage_client = FakeResource()

    download_session = FakeResource(
        close_error=RuntimeError(
            "Simulated session cleanup failure"
        )
    )

    finish_success_mock = Mock()
    finish_failed_mock = Mock()

    storage_factory_name = (
        find_storage_factory_name()
    )

    FakeHeartbeat.created_instances.clear()
    FakePipelineLock.created_instances.clear()

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
            "PipelineHeartbeat",
            FakeHeartbeat,
        ),
        patch.object(
            extract,
            "PipelineRunLock",
            FakePipelineLock,
        ),
        patch.object(
            extract,
            "fetch_sleep_edf_remote_manifest",
            return_value=manifest,
        ),
        patch.object(
            extract,
            storage_factory_name,
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
            side_effect=RuntimeError(
                "Simulated file task failure"
            ),
        ),
        patch.object(
            extract,
            "finish_pipeline_run_success",
            finish_success_mock,
        ),
        patch.object(
            extract,
            "finish_pipeline_run_failed",
            finish_failed_mock,
        ),
        patch.object(
            extract,
            "start_file_attempt",
            return_value=new_uuid7(),
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

        except RuntimeError as error:
            if str(error) != (
                "Simulated file task failure"
            ):
                raise RuntimeError(
                    "Original pipeline error "
                    "was replaced"
                ) from error

        else:
            raise RuntimeError(
                "Pipeline failure was not propagated"
            )

    if len(FakePipelineLock.created_instances) != 1:
        raise RuntimeError(
            "Unexpected pipeline lock "
            "instance count"
        )

    pipeline_lock = (
        FakePipelineLock.created_instances[0]
    )

    if pipeline_lock.acquire_count != 1:
        raise RuntimeError(
            "Pipeline lock was not "
            "acquired once"
        )

    if pipeline_lock.release_count != 1:
        raise RuntimeError(
            "Pipeline lock was not "
            "released once"
        )

    if pipeline_lock.is_acquired:
        raise RuntimeError(
            "Pipeline lock remains acquired"
        )

    if len(FakeHeartbeat.created_instances) != 1:
        raise RuntimeError(
            "Unexpected heartbeat instance count"
        )

    heartbeat = (
        FakeHeartbeat.created_instances[0]
    )

    if heartbeat.start_count != 1:
        raise RuntimeError(
            "Heartbeat was not started once"
        )

    if heartbeat.stop_count < 1:
        raise RuntimeError(
            "Heartbeat was not stopped"
        )

    if finish_success_mock.call_count != 0:
        raise RuntimeError(
            "Failed pipeline was marked successful"
        )

    if finish_failed_mock.call_count != 1:
        raise RuntimeError(
            "Pipeline failed status "
            "was not written once"
        )

    failure_call = (
        finish_failed_mock.call_args
    )

    failure_args = failure_call.args
    failure_kwargs = failure_call.kwargs

    stored_run_id = failure_kwargs.get(
        "run_id",
        failure_args[0]
        if failure_args
        else None,
    )

    stored_error_message = failure_kwargs.get(
        "error_message",
        failure_args[1]
        if len(failure_args) > 1
        else None,
    )

    if stored_run_id != run_id:
        raise RuntimeError(
            "Wrong run_id was used "
            "during failure finalization"
        )

    if stored_error_message != (
        "Simulated file task failure"
    ):
        raise RuntimeError(
            "Wrong pipeline error was stored"
        )

    files_processed = failure_kwargs.get(
        "files_processed"
    )

    if (
        files_processed is not None
        and files_processed != 0
    ):
        raise RuntimeError(
            "Failed file was incorrectly "
            "counted as processed"
        )

    if storage_client.close_count != 1:
        raise RuntimeError(
            "Storage client was not closed"
        )

    if download_session.close_count != 1:
        raise RuntimeError(
            "Download session cleanup "
            "was not attempted"
        )

    required_regular_events = {
        "pipeline_started",
        "manifest_loaded",
        "resource_cleanup_completed",
    }

    missing_regular_events = (
        required_regular_events
        - set(regular_events)
    )

    if missing_regular_events:
        raise RuntimeError(
            "Missing regular events: "
            f"{sorted(missing_regular_events)}"
        )

    exception_event_names = {
        event_name
        for (
            event_name,
            error_type,
            error_message,
        ) in exception_events
    }

    if (
        "pipeline_failed"
        not in exception_event_names
    ):
        raise RuntimeError(
            "pipeline_failed event missing"
        )

    if (
        "resource_cleanup_failed"
        not in exception_event_names
    ):
        raise RuntimeError(
            "resource_cleanup_failed "
            "event missing"
        )

    pipeline_failure = next(
        event
        for event in exception_events
        if event[0] == "pipeline_failed"
    )

    if pipeline_failure != (
        "pipeline_failed",
        "RuntimeError",
        "Simulated file task failure",
    ):
        raise RuntimeError(
            "Incorrect pipeline failure event"
        )

    cleanup_failure = next(
        event
        for event in exception_events
        if event[0]
        == "resource_cleanup_failed"
    )

    if cleanup_failure != (
        "resource_cleanup_failed",
        "RuntimeError",
        "Simulated session cleanup failure",
    ):
        raise RuntimeError(
            "Incorrect cleanup failure event"
        )

    print(
        "pipeline_lock_acquired_once=true"
    )
    print(
        "pipeline_lock_released_after_failure=true"
    )
    print(
        "uuid7_run_id=true"
    )
    print(
        "original_pipeline_error_preserved=true"
    )
    print(
        "heartbeat_started_once=true"
    )
    print(
        "heartbeat_stopped_after_failure=true"
    )
    print(
        "pipeline_marked_failed=true"
    )
    print(
        "pipeline_not_marked_success=true"
    )
    print(
        "pipeline_failure_event_recorded=true"
    )
    print(
        "cleanup_failure_event_recorded=true"
    )
    print(
        "resources_cleanup_attempted=true"
    )
    print(
        "extract_observability_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
