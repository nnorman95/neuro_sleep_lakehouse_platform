from types import SimpleNamespace
from unittest.mock import Mock, patch

import neuro_sleep.silver.silver_job as silver_job
from neuro_sleep.identifiers import new_uuid7
from neuro_sleep.reliability.errors import (
    ConcurrentPipelineRunError,
)


class FakeLock:
    created_instances: list["FakeLock"] = []

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
    created_instances: list["FakeHeartbeat"] = []

    def __init__(
        self,
        run_id,
        pipeline_name: str,
        interval_seconds: float,
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


def build_pipeline_result(
    status: str,
):
    return SimpleNamespace(
        status=status,
        source_pair_id="smoke-source-pair-id",
        input_fingerprint="smoke-input-fingerprint",
        recording_id=new_uuid7(),
        output_prefix=(
            "smoke-tests/silver-job/"
            "schema_version=1.0.0"
        ),
        row_count=123,
        data_object_count=4,
        total_object_count=5,
    )


def run_written_case() -> None:
    run_id = new_uuid7()
    result = build_pipeline_result(
        status="written"
    )

    finish_success = Mock()
    finish_skipped = Mock()
    finish_failed = Mock()

    FakeLock.created_instances.clear()
    FakeHeartbeat.created_instances.clear()

    with (
        patch.object(
            silver_job,
            "get_settings",
            return_value=SimpleNamespace(),
        ),
        patch.object(
            silver_job,
            "PipelineRunLock",
            FakeLock,
        ),
        patch.object(
            silver_job,
            "PipelineHeartbeat",
            FakeHeartbeat,
        ),
        patch.object(
            silver_job,
            "start_pipeline_run",
            return_value=run_id,
        ),
        patch.object(
            silver_job,
            "run_silver_pipeline",
            return_value=result,
        ),
        patch.object(
            silver_job,
            "finish_pipeline_run_success",
            finish_success,
        ),
        patch.object(
            silver_job,
            "finish_pipeline_run_skipped",
            finish_skipped,
        ),
        patch.object(
            silver_job,
            "finish_pipeline_run_failed",
            finish_failed,
        ),
        patch.object(
            silver_job,
            "emit_event",
        ),
        patch.object(
            silver_job,
            "emit_exception",
        ),
    ):
        tracked = (
            silver_job.run_tracked_silver_job(
                psg_bucket="bronze",
                psg_object_key="test-PSG.edf",
                hypnogram_bucket="bronze",
                hypnogram_object_key=(
                    "test-Hypnogram.edf"
                ),
                silver_bucket="silver",
                root_prefix=(
                    "smoke-tests/silver-job"
                ),
            )
        )

    if tracked.run_id != run_id:
        raise RuntimeError(
            "Tracked Silver run_id mismatch"
        )

    if tracked.pipeline_result is not result:
        raise RuntimeError(
            "Silver pipeline result changed"
        )

    lock = FakeLock.created_instances[0]
    heartbeat = (
        FakeHeartbeat.created_instances[0]
    )

    if (
        lock.acquire_count != 1
        or lock.release_count != 1
    ):
        raise RuntimeError(
            "Silver pipeline lock lifecycle "
            "is incorrect"
        )

    if (
        heartbeat.start_count != 1
        or heartbeat.stop_count != 1
    ):
        raise RuntimeError(
            "Silver heartbeat lifecycle "
            "is incorrect"
        )

    finish_success.assert_called_once_with(
        run_id=run_id,
        rows_read=0,
        rows_written=123,
        files_processed=2,
        records_quarantined=0,
    )

    if finish_skipped.call_count != 0:
        raise RuntimeError(
            "Written run was marked skipped"
        )

    if finish_failed.call_count != 0:
        raise RuntimeError(
            "Written run was marked failed"
        )

    print(
        "silver_written_run_tracked=true"
    )
    print(
        "silver_lock_acquired_and_released=true"
    )
    print(
        "silver_heartbeat_started_and_stopped=true"
    )


def run_failure_case() -> None:
    run_id = new_uuid7()
    simulated_error = RuntimeError(
        "Simulated Silver transformation failure"
    )

    finish_success = Mock()
    finish_skipped = Mock()
    finish_failed = Mock()

    FakeLock.created_instances.clear()
    FakeHeartbeat.created_instances.clear()

    with (
        patch.object(
            silver_job,
            "get_settings",
            return_value=SimpleNamespace(),
        ),
        patch.object(
            silver_job,
            "PipelineRunLock",
            FakeLock,
        ),
        patch.object(
            silver_job,
            "PipelineHeartbeat",
            FakeHeartbeat,
        ),
        patch.object(
            silver_job,
            "start_pipeline_run",
            return_value=run_id,
        ),
        patch.object(
            silver_job,
            "run_silver_pipeline",
            side_effect=simulated_error,
        ),
        patch.object(
            silver_job,
            "finish_pipeline_run_success",
            finish_success,
        ),
        patch.object(
            silver_job,
            "finish_pipeline_run_skipped",
            finish_skipped,
        ),
        patch.object(
            silver_job,
            "finish_pipeline_run_failed",
            finish_failed,
        ),
        patch.object(
            silver_job,
            "emit_event",
        ),
        patch.object(
            silver_job,
            "emit_exception",
        ),
    ):
        try:
            silver_job.run_tracked_silver_job(
                psg_bucket="bronze",
                psg_object_key="test-PSG.edf",
                hypnogram_bucket="bronze",
                hypnogram_object_key=(
                    "test-Hypnogram.edf"
                ),
                silver_bucket="silver",
                root_prefix=(
                    "smoke-tests/silver-job"
                ),
            )

        except RuntimeError as error:
            if error is not simulated_error:
                raise RuntimeError(
                    "Original Silver error changed"
                ) from error

        else:
            raise RuntimeError(
                "Silver failure was not propagated"
            )

    lock = FakeLock.created_instances[0]
    heartbeat = (
        FakeHeartbeat.created_instances[0]
    )

    if lock.release_count != 1:
        raise RuntimeError(
            "Silver lock was not released "
            "after failure"
        )

    if heartbeat.stop_count != 1:
        raise RuntimeError(
            "Silver heartbeat was not stopped "
            "after failure"
        )

    finish_failed.assert_called_once_with(
        run_id=run_id,
        error_message=str(simulated_error),
        rows_read=0,
        rows_written=0,
        files_processed=0,
        records_quarantined=0,
    )

    if finish_success.call_count != 0:
        raise RuntimeError(
            "Failed run was marked successful"
        )

    if finish_skipped.call_count != 0:
        raise RuntimeError(
            "Failed run was marked skipped"
        )

    print(
        "silver_failure_tracked=true"
    )
    print(
        "silver_failure_lock_released=true"
    )
    print(
        "silver_failure_heartbeat_stopped=true"
    )


def run_concurrent_case() -> None:
    start_run = Mock()

    blocked_lock = Mock()
    blocked_lock.acquire.side_effect = (
        ConcurrentPipelineRunError(
            "Simulated concurrent Silver run"
        )
    )

    with (
        patch.object(
            silver_job,
            "get_settings",
            return_value=SimpleNamespace(),
        ),
        patch.object(
            silver_job,
            "PipelineRunLock",
            return_value=blocked_lock,
        ),
        patch.object(
            silver_job,
            "start_pipeline_run",
            start_run,
        ),
        patch.object(
            silver_job,
            "emit_event",
        ),
        patch.object(
            silver_job,
            "emit_exception",
        ),
    ):
        try:
            silver_job.run_tracked_silver_job(
                psg_bucket="bronze",
                psg_object_key="test-PSG.edf",
                hypnogram_bucket="bronze",
                hypnogram_object_key=(
                    "test-Hypnogram.edf"
                ),
                silver_bucket="silver",
                root_prefix=(
                    "smoke-tests/silver-job"
                ),
            )

        except ConcurrentPipelineRunError:
            pass

        else:
            raise RuntimeError(
                "Concurrent Silver run "
                "was not blocked"
            )

    if start_run.call_count != 0:
        raise RuntimeError(
            "Blocked concurrent run created "
            "an ops.pipeline_run row"
        )

    print(
        "silver_concurrent_run_blocked=true"
    )
    print(
        "blocked_run_not_registered=true"
    )


def run_smoke_test() -> None:
    run_written_case()
    run_failure_case()
    run_concurrent_case()

    print(
        "silver_job_observability_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
