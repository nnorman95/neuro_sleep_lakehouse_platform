from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from botocore.client import BaseClient

from neuro_sleep.config import (
    Settings,
    get_settings,
)
from neuro_sleep.observability.pipeline_heartbeat import (
    PipelineHeartbeat,
)
from neuro_sleep.observability.structured_logging import (
    emit_event,
    emit_exception,
)
from neuro_sleep.ops.pipeline_lock import (
    PipelineRunLock,
)
from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_failed,
    finish_pipeline_run_skipped,
    finish_pipeline_run_success,
    start_pipeline_run,
)
from neuro_sleep.reliability.errors import (
    ConcurrentPipelineRunError,
)
from neuro_sleep.sources.sleep_edf import (
    SOURCE_SYSTEM,
)
from neuro_sleep.staging.subject_metadata_loader import (
    SubjectMetadataStagingLoadResult,
    load_subject_metadata_to_staging,
)


PIPELINE_NAME = (
    "sleep_edf_subject_metadata_staging"
)
TASK_NAME = (
    "load_subject_metadata_to_staging"
)
HEARTBEAT_INTERVAL_SECONDS = 30.0


@dataclass(frozen=True)
class TrackedSubjectMetadataStagingResult:
    run_id: UUID
    load_result: SubjectMetadataStagingLoadResult


def _stop_heartbeat_safely(
    *,
    heartbeat: PipelineHeartbeat | None,
    run_id: UUID,
) -> None:
    if heartbeat is None:
        return

    try:
        heartbeat.stop()
    except Exception as error:
        emit_exception(
            event=(
                "subject_metadata_staging_"
                "heartbeat_stop_failed"
            ),
            error=error,
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
        )


def _release_lock_safely(
    pipeline_lock: PipelineRunLock,
) -> None:
    try:
        pipeline_lock.release()
    except Exception as error:
        emit_exception(
            event=(
                "subject_metadata_staging_"
                "lock_release_failed"
            ),
            error=error,
            pipeline_name=PIPELINE_NAME,
        )
    else:
        emit_event(
            event=(
                "subject_metadata_staging_"
                "lock_released"
            ),
            pipeline_name=PIPELINE_NAME,
        )


def run_tracked_subject_metadata_staging_job(
    *,
    settings: Settings | None = None,
    client: BaseClient | None = None,
) -> TrackedSubjectMetadataStagingResult:
    if settings is None:
        settings = get_settings()

    pipeline_lock = PipelineRunLock(
        pipeline_name=PIPELINE_NAME,
        settings=settings,
    )

    try:
        pipeline_lock.acquire()
    except ConcurrentPipelineRunError:
        emit_event(
            event=(
                "subject_metadata_staging_"
                "concurrent_blocked"
            ),
            pipeline_name=PIPELINE_NAME,
            source_system=SOURCE_SYSTEM,
        )
        raise

    emit_event(
        event=(
            "subject_metadata_staging_"
            "lock_acquired"
        ),
        pipeline_name=PIPELINE_NAME,
        source_system=SOURCE_SYSTEM,
    )

    run_id: UUID | None = None
    heartbeat: PipelineHeartbeat | None = None

    try:
        run_id = start_pipeline_run(
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
        )
        emit_event(
            event=(
                "subject_metadata_staging_"
                "started"
            ),
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
        )

        heartbeat = PipelineHeartbeat(
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            interval_seconds=(
                HEARTBEAT_INTERVAL_SECONDS
            ),
        )
        heartbeat.start()

        load_result = (
            load_subject_metadata_to_staging(
                run_id=run_id,
                settings=settings,
                client=client,
            )
        )

        _stop_heartbeat_safely(
            heartbeat=heartbeat,
            run_id=run_id,
        )
        heartbeat = None

        if load_result.status == "skipped":
            finish_pipeline_run_skipped(
                run_id=run_id,
                reason=(
                    "Current Silver subject "
                    "metadata publication is "
                    "already complete in staging."
                ),
                rows_read=0,
                rows_written=0,
                files_processed=0,
                records_quarantined=0,
            )
        else:
            total_rows = (
                load_result.subject_count
                + load_result
                .recording_context_count
            )
            finish_pipeline_run_success(
                run_id=run_id,
                rows_read=total_rows,
                rows_written=(
                    load_result.rows_written
                ),
                files_processed=(
                    load_result.files_processed
                ),
                records_quarantined=0,
            )

        emit_event(
            event=(
                "subject_metadata_staging_"
                "completed"
            ),
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
            status=load_result.status,
            output_prefix=(
                load_result.output_prefix
            ),
            input_fingerprint=(
                load_result.input_fingerprint
            ),
            subject_count=(
                load_result.subject_count
            ),
            recording_context_count=(
                load_result
                .recording_context_count
            ),
            rows_written=(
                load_result.rows_written
            ),
        )

        return (
            TrackedSubjectMetadataStagingResult(
                run_id=run_id,
                load_result=load_result,
            )
        )

    except BaseException as error:
        if run_id is not None:
            _stop_heartbeat_safely(
                heartbeat=heartbeat,
                run_id=run_id,
            )
            heartbeat = None

            emit_exception(
                event=(
                    "subject_metadata_staging_"
                    "failed"
                ),
                error=error,
                run_id=run_id,
                pipeline_name=PIPELINE_NAME,
                task_name=TASK_NAME,
                source_system=SOURCE_SYSTEM,
            )

            try:
                finish_pipeline_run_failed(
                    run_id=run_id,
                    error_message=(
                        f"{type(error).__name__}: "
                        f"{error}"
                    ),
                    rows_read=0,
                    rows_written=0,
                    files_processed=0,
                    records_quarantined=0,
                )
            except Exception as status_error:
                emit_exception(
                    event=(
                        "subject_metadata_staging_"
                        "status_update_failed"
                    ),
                    error=status_error,
                    run_id=run_id,
                    pipeline_name=PIPELINE_NAME,
                )

        raise
    finally:
        if (
            run_id is not None
            and heartbeat is not None
        ):
            _stop_heartbeat_safely(
                heartbeat=heartbeat,
                run_id=run_id,
            )

        _release_lock_safely(
            pipeline_lock
        )
