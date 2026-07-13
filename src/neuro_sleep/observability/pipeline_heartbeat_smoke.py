from datetime import datetime, timezone
from threading import Event
from neuro_sleep.identifiers import new_uuid7

from neuro_sleep.observability.pipeline_heartbeat import (
    PipelineHeartbeat,
)


def run_smoke_test() -> None:
    run_id = new_uuid7()

    second_update_received = Event()
    update_count = 0

    def successful_update(
        received_run_id,
    ) -> datetime:
        nonlocal update_count

        if received_run_id != run_id:
            raise RuntimeError(
                "Unexpected heartbeat run_id"
            )

        update_count += 1

        if update_count >= 2:
            second_update_received.set()

        return datetime.now(
            timezone.utc
        )

    heartbeat = PipelineHeartbeat(
        run_id=run_id,
        pipeline_name="heartbeat_smoke_test",
        interval_seconds=0.05,
        update_function=successful_update,
    )

    heartbeat.start()

    if not second_update_received.wait(
        timeout=2.0
    ):
        raise RuntimeError(
            "Periodic heartbeat update "
            "was not received"
        )

    heartbeat.stop(
        join_timeout_seconds=2.0
    )

    if heartbeat.is_running:
        raise RuntimeError(
            "Heartbeat thread did not stop"
        )

    if update_count < 2:
        raise RuntimeError(
            "Heartbeat did not run periodically"
        )

    if heartbeat.update_count < 2:
        raise RuntimeError(
            "Heartbeat success count is incorrect"
        )

    if heartbeat.failure_count != 0:
        raise RuntimeError(
            "Unexpected heartbeat failure"
        )

    print("heartbeat_periodic_update=true")
    print("heartbeat_thread_stopped=true")

    failure_received = Event()
    failure_attempt_count = 0

    def failing_update(
        received_run_id,
    ) -> datetime:
        nonlocal failure_attempt_count

        failure_attempt_count += 1

        if failure_attempt_count == 1:
            return datetime.now(
                timezone.utc
            )

        failure_received.set()

        raise RuntimeError(
            "Smoke test database failure"
        )

    failing_heartbeat = PipelineHeartbeat(
        run_id=run_id,
        pipeline_name=(
            "heartbeat_failure_smoke_test"
        ),
        interval_seconds=0.05,
        update_function=failing_update,
    )

    failing_heartbeat.start()

    if not failure_received.wait(
        timeout=2.0
    ):
        raise RuntimeError(
            "Heartbeat failure was not triggered"
        )

    failing_heartbeat.stop(
        join_timeout_seconds=2.0
    )

    if failing_heartbeat.failure_count < 1:
        raise RuntimeError(
            "Heartbeat failure was not recorded"
        )

    if failing_heartbeat.last_error is None:
        raise RuntimeError(
            "Heartbeat last error was not stored"
        )

    print("heartbeat_failure_recorded=true")
    print(
        "pipeline_heartbeat_smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
