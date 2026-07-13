from collections.abc import Callable
from datetime import datetime
from threading import Event, Lock, Thread
from uuid import UUID

from neuro_sleep.observability.structured_logging import (
    emit_event,
    emit_exception,
)
from neuro_sleep.ops.pipeline_run import (
    update_pipeline_run_heartbeat,
)


RunId = UUID | str
HeartbeatUpdateFunction = Callable[
    [RunId],
    datetime | None,
]


class PipelineHeartbeat:
    def __init__(
        self,
        run_id: RunId,
        pipeline_name: str,
        interval_seconds: float = 30.0,
        update_function: HeartbeatUpdateFunction = (
            update_pipeline_run_heartbeat
        ),
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError(
                "Heartbeat interval must be positive"
            )

        self.run_id = run_id
        self.pipeline_name = pipeline_name
        self.interval_seconds = interval_seconds
        self.update_function = update_function

        self._stop_event = Event()
        self._state_lock = Lock()
        self._thread: Thread | None = None
        self._last_error: BaseException | None = None
        self._update_count = 0
        self._failure_count = 0

    @property
    def update_count(self) -> int:
        with self._state_lock:
            return self._update_count

    @property
    def failure_count(self) -> int:
        with self._state_lock:
            return self._failure_count

    @property
    def last_error(
        self,
    ) -> BaseException | None:
        with self._state_lock:
            return self._last_error

    @property
    def is_running(self) -> bool:
        thread = self._thread

        return (
            thread is not None
            and thread.is_alive()
        )

    def _record_success(self) -> None:
        with self._state_lock:
            self._update_count += 1
            self._last_error = None

    def _record_failure(
        self,
        error: BaseException,
    ) -> None:
        with self._state_lock:
            self._failure_count += 1
            self._last_error = error

    def _update_once(self) -> bool:
        heartbeat_at = self.update_function(
            self.run_id
        )

        if heartbeat_at is None:
            emit_event(
                event="heartbeat_inactive",
                level="WARNING",
                run_id=self.run_id,
                pipeline_name=self.pipeline_name,
            )

            return False

        self._record_success()

        emit_event(
            event="heartbeat_updated",
            level="DEBUG",
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            heartbeat_at=heartbeat_at,
            heartbeat_update_count=(
                self.update_count
            ),
        )

        return True

    def _run_loop(self) -> None:
        while not self._stop_event.wait(
            self.interval_seconds
        ):
            try:
                run_is_active = self._update_once()

                if not run_is_active:
                    return

            except Exception as error:
                self._record_failure(error)

                emit_exception(
                    event="heartbeat_update_failed",
                    error=error,
                    run_id=self.run_id,
                    pipeline_name=self.pipeline_name,
                    heartbeat_failure_count=(
                        self.failure_count
                    ),
                )

    def start(self) -> None:
        if self.is_running:
            raise RuntimeError(
                "Pipeline heartbeat is already running"
            )

        self._stop_event.clear()

        heartbeat_at = self.update_function(
            self.run_id
        )

        if heartbeat_at is None:
            raise RuntimeError(
                "Cannot start heartbeat for "
                "an inactive pipeline run"
            )

        self._record_success()

        emit_event(
            event="heartbeat_started",
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            heartbeat_at=heartbeat_at,
            interval_seconds=(
                self.interval_seconds
            ),
        )

        self._thread = Thread(
            target=self._run_loop,
            name=(
                "pipeline-heartbeat-"
                f"{self.run_id}"
            ),
            daemon=True,
        )

        self._thread.start()

    def stop(
        self,
        join_timeout_seconds: float = 45.0,
    ) -> None:
        if join_timeout_seconds <= 0:
            raise ValueError(
                "Heartbeat join timeout "
                "must be positive"
            )

        thread = self._thread

        if thread is None:
            return

        self._stop_event.set()

        thread.join(
            timeout=join_timeout_seconds
        )

        if thread.is_alive():
            emit_event(
                event="heartbeat_stop_timeout",
                level="WARNING",
                run_id=self.run_id,
                pipeline_name=self.pipeline_name,
                join_timeout_seconds=(
                    join_timeout_seconds
                ),
            )

            return

        self._thread = None

        emit_event(
            event="heartbeat_stopped",
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            heartbeat_update_count=(
                self.update_count
            ),
            heartbeat_failure_count=(
                self.failure_count
            ),
        )

    def __enter__(
        self,
    ) -> "PipelineHeartbeat":
        self.start()
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        self.stop()
