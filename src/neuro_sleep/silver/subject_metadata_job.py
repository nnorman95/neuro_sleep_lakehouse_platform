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
from neuro_sleep.silver.subject_metadata_pipeline import (
    SubjectMetadataPipelineResult,
    run_subject_metadata_pipeline,
)
from neuro_sleep.sources.sleep_edf import (
    SOURCE_SYSTEM,
)


PIPELINE_NAME = (
    "sleep_edf_subject_metadata_silver"
)
TASK_NAME = (
    "transform_subject_metadata_to_silver"
)
HEARTBEAT_INTERVAL_SECONDS = 30.0


@dataclass(frozen=True)
class TrackedSubjectMetadataResult:
    run_id: UUID
    pipeline_result: (
        SubjectMetadataPipelineResult
    )


def stop_heartbeat_safely(
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
                "subject_metadata_heartbeat_"
                "stop_failed"
            ),
            error=error,
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
        )


def release_lock_safely(
    pipeline_lock: PipelineRunLock,
) -> None:
    try:
        pipeline_lock.release()

    except Exception as error:
        emit_exception(
            event=(
                "subject_metadata_lock_"
                "release_failed"
            ),
            error=error,
            pipeline_name=PIPELINE_NAME,
        )

    else:
        emit_event(
            event=(
                "subject_metadata_lock_"
                "released"
            ),
            pipeline_name=PIPELINE_NAME,
        )


def run_tracked_subject_metadata_job(
    *,
    silver_bucket: str,
    root_prefix: str,
    settings: Settings | None = None,
    client: BaseClient | None = None,
) -> TrackedSubjectMetadataResult:
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
                "subject_metadata_concurrent_"
                "blocked"
            ),
            pipeline_name=PIPELINE_NAME,
            source_system=SOURCE_SYSTEM,
        )
        raise

    emit_event(
        event=(
            "subject_metadata_lock_acquired"
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
                "subject_metadata_started"
            ),
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
            silver_bucket=silver_bucket,
            root_prefix=root_prefix,
        )

        heartbeat = PipelineHeartbeat(
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            interval_seconds=(
                HEARTBEAT_INTERVAL_SECONDS
            ),
        )
        heartbeat.start()

        pipeline_result = (
            run_subject_metadata_pipeline(
                silver_bucket=(
                    silver_bucket
                ),
                root_prefix=root_prefix,
                settings=settings,
                client=client,
            )
        )

        stop_heartbeat_safely(
            heartbeat=heartbeat,
            run_id=run_id,
        )
        heartbeat = None

        if (
            pipeline_result.status
            == "skipped"
        ):
            finish_pipeline_run_skipped(
                run_id=run_id,
                reason=(
                    "Silver subject metadata "
                    "output already complete."
                ),
                rows_read=0,
                rows_written=0,
                files_processed=2,
                records_quarantined=0,
            )

        else:
            finish_pipeline_run_success(
                run_id=run_id,
                rows_read=(
                    pipeline_result
                    .subject_count
                    + pipeline_result
                    .recording_context_count
                ),
                rows_written=(
                    pipeline_result
                    .subject_count
                    + pipeline_result
                    .recording_context_count
                ),
                files_processed=2,
                records_quarantined=0,
            )

        emit_event(
            event=(
                "subject_metadata_completed"
            ),
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
            status=pipeline_result.status,
            output_prefix=(
                pipeline_result.output_prefix
            ),
            input_fingerprint=(
                pipeline_result
                .input_fingerprint
            ),
            subject_count=(
                pipeline_result.subject_count
            ),
            recording_context_count=(
                pipeline_result
                .recording_context_count
            ),
            recovered_partial_output=(
                pipeline_result
                .recovered_partial_output
            ),
        )

        return TrackedSubjectMetadataResult(
            run_id=run_id,
            pipeline_result=(
                pipeline_result
            ),
        )

    except BaseException as error:
        if run_id is not None:
            stop_heartbeat_safely(
                heartbeat=heartbeat,
                run_id=run_id,
            )
            heartbeat = None

            emit_exception(
                event=(
                    "subject_metadata_failed"
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
                        "subject_metadata_status_"
                        "update_failed"
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
            stop_heartbeat_safely(
                heartbeat=heartbeat,
                run_id=run_id,
            )

        release_lock_safely(
            pipeline_lock
        )
