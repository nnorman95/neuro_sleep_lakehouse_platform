from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from botocore.client import BaseClient

from neuro_sleep.config import Settings, get_settings
from neuro_sleep.observability.pipeline_heartbeat import (
    PipelineHeartbeat,
)
from neuro_sleep.observability.structured_logging import (
    emit_event,
    emit_exception,
)
from neuro_sleep.ops.pipeline_lock import PipelineRunLock
from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_failed,
    finish_pipeline_run_skipped,
    finish_pipeline_run_success,
    start_pipeline_run,
)
from neuro_sleep.quality.silver_quality_history import (
    persist_silver_quality_report,
    persist_skipped_silver_quality_result,
)
from neuro_sleep.reliability.errors import (
    ConcurrentPipelineRunError,
)
from neuro_sleep.silver.silver_pipeline import (
    SilverPipelineResult,
    run_silver_pipeline,
)
from neuro_sleep.silver.signal_extractor import (
    DEFAULT_CHUNK_DURATION_SECONDS,
)
from neuro_sleep.sources.sleep_edf import SOURCE_SYSTEM


PIPELINE_NAME = "sleep_edf_silver"
TASK_NAME = "transform_recording_to_silver"
HEARTBEAT_INTERVAL_SECONDS = 30.0


@dataclass(frozen=True)
class TrackedSilverJobResult:
    run_id: UUID
    pipeline_result: SilverPipelineResult

    @property
    def status(self):
        return self.pipeline_result.status

    @property
    def recording_id(self):
        return self.pipeline_result.recording_id

    @property
    def output_prefix(self) -> str:
        return self.pipeline_result.output_prefix

    @property
    def row_count(self) -> int:
        return self.pipeline_result.row_count


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
            event="silver_heartbeat_stop_failed",
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
            event="silver_pipeline_lock_release_failed",
            error=error,
            pipeline_name=PIPELINE_NAME,
        )

    else:
        emit_event(
            event="silver_pipeline_lock_released",
            pipeline_name=PIPELINE_NAME,
        )


def run_tracked_silver_job(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
    silver_bucket: str,
    root_prefix: str,
    *,
    signal_chunk_duration_seconds: float = (
        DEFAULT_CHUNK_DURATION_SECONDS
    ),
    signal_start_seconds: float = 0.0,
    signal_stop_seconds: float | None = None,
    include_signals: bool = True,
    verify_payload_checksums: bool = True,
    settings: Settings | None = None,
    client: BaseClient | None = None,
) -> TrackedSilverJobResult:
    if settings is None:
        settings = get_settings()

    pipeline_lock = PipelineRunLock(
        pipeline_name=PIPELINE_NAME,
        settings=settings,
    )

    try:
        pipeline_lock.acquire()

    except ConcurrentPipelineRunError as error:
        emit_exception(
            event="silver_pipeline_concurrent_blocked",
            error=error,
            pipeline_name=PIPELINE_NAME,
            source_system=SOURCE_SYSTEM,
        )
        raise

    except Exception as error:
        emit_exception(
            event="silver_pipeline_lock_acquire_failed",
            error=error,
            pipeline_name=PIPELINE_NAME,
            source_system=SOURCE_SYSTEM,
        )
        raise

    emit_event(
        event="silver_pipeline_lock_acquired",
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
            event="silver_pipeline_started",
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
            psg_bucket=psg_bucket,
            psg_object_key=psg_object_key,
            hypnogram_bucket=hypnogram_bucket,
            hypnogram_object_key=(
                hypnogram_object_key
            ),
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

        def quality_report_handler(
            bundle,
            report,
            output_prefix: str,
        ) -> None:
            persist_silver_quality_report(
                pipeline_run_id=run_id,
                source_system=SOURCE_SYSTEM,
                bundle=bundle,
                report=report,
                output_prefix=output_prefix,
            )

        pipeline_result = run_silver_pipeline(
            psg_bucket=psg_bucket,
            psg_object_key=psg_object_key,
            hypnogram_bucket=hypnogram_bucket,
            hypnogram_object_key=(
                hypnogram_object_key
            ),
            silver_bucket=silver_bucket,
            root_prefix=root_prefix,
            signal_chunk_duration_seconds=(
                signal_chunk_duration_seconds
            ),
            signal_start_seconds=(
                signal_start_seconds
            ),
            signal_stop_seconds=(
                signal_stop_seconds
            ),
            include_signals=include_signals,
            verify_payload_checksums=(
                verify_payload_checksums
            ),
            quality_report_handler=(
                quality_report_handler
            ),
            client=client,
        )

        stop_heartbeat_safely(
            heartbeat=heartbeat,
            run_id=run_id,
        )
        heartbeat = None

        if pipeline_result.status == "skipped":
            persist_skipped_silver_quality_result(
                pipeline_run_id=run_id,
                source_system=SOURCE_SYSTEM,
                recording_id=(
                    pipeline_result.recording_id
                ),
                output_prefix=(
                    pipeline_result.output_prefix
                ),
                reconciliation_passed=(
                    pipeline_result
                    .reconciliation_report
                    .passed
                ),
                data_object_count=(
                    pipeline_result
                    .data_object_count
                ),
                total_object_count=(
                    pipeline_result
                    .total_object_count
                ),
            )

            finish_pipeline_run_skipped(
                run_id=run_id,
                reason=(
                    "Silver output already complete "
                    "and reconciliation passed."
                ),
                rows_read=0,
                rows_written=0,
                files_processed=2,
                records_quarantined=0,
            )

        else:
            finish_pipeline_run_success(
                run_id=run_id,
                rows_read=0,
                rows_written=(
                    pipeline_result.row_count
                ),
                files_processed=2,
                records_quarantined=0,
            )

        emit_event(
            event="silver_pipeline_completed",
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
            status=pipeline_result.status,
            recording_id=(
                pipeline_result.recording_id
            ),
            output_prefix=(
                pipeline_result.output_prefix
            ),
            source_pair_id=(
                pipeline_result.source_pair_id
            ),
            input_fingerprint=(
                pipeline_result
                .input_fingerprint
            ),
            rows_written=(
                pipeline_result.row_count
                if pipeline_result.status
                == "written"
                else 0
            ),
            data_object_count=(
                pipeline_result.data_object_count
            ),
            total_object_count=(
                pipeline_result.total_object_count
            ),
        )

        return TrackedSilverJobResult(
            run_id=run_id,
            pipeline_result=pipeline_result,
        )

    except Exception as error:
        if run_id is not None:
            stop_heartbeat_safely(
                heartbeat=heartbeat,
                run_id=run_id,
            )
            heartbeat = None

            emit_exception(
                event="silver_pipeline_failed",
                error=error,
                run_id=run_id,
                pipeline_name=PIPELINE_NAME,
                task_name=TASK_NAME,
                source_system=SOURCE_SYSTEM,
                psg_object_key=psg_object_key,
                hypnogram_object_key=(
                    hypnogram_object_key
                ),
            )

            try:
                finish_pipeline_run_failed(
                    run_id=run_id,
                    error_message=str(error),
                    rows_read=0,
                    rows_written=0,
                    files_processed=0,
                    records_quarantined=0,
                )

            except Exception as status_error:
                emit_exception(
                    event=(
                        "silver_pipeline_status_"
                        "update_failed"
                    ),
                    error=status_error,
                    run_id=run_id,
                    pipeline_name=PIPELINE_NAME,
                    original_error_type=(
                        type(error).__name__
                    ),
                    original_error_message=(
                        str(error)
                    ),
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
            pipeline_lock=pipeline_lock
        )
