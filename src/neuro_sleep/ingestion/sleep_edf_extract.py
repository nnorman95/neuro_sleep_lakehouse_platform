from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

from neuro_sleep.config import Settings, get_settings
from neuro_sleep.ingestion.sleep_edf_file_task import (
    SleepEdfFileTaskResult,
    run_control_artifact_task,
    run_source_file_task,
)
from neuro_sleep.ingestion.sleep_edf_http_downloader import (
    create_download_session,
)
from neuro_sleep.ingestion.sleep_edf_remote_manifest import (
    fetch_sleep_edf_remote_manifest,
)
from neuro_sleep.observability.pipeline_heartbeat import (
    PipelineHeartbeat,
)
from neuro_sleep.observability.structured_logging import (
    emit_event,
    emit_exception,
)
from neuro_sleep.ops.file_attempt import (
    finish_file_attempt_failed,
    finish_file_attempt_skipped,
    finish_file_attempt_uploaded,
    start_file_attempt,
)
from neuro_sleep.ops.pipeline_lock import (
    PipelineRunLock,
)
from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_failed,
    finish_pipeline_run_success,
    start_pipeline_run,
)
from neuro_sleep.reliability.errors import (
    ConcurrentPipelineRunError,
)
from neuro_sleep.sources.sleep_edf import (
    SOURCE_SYSTEM,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
)


PIPELINE_NAME = "sleep_edf_extract"
TASK_NAME = "download_verify_and_load_bronze"

HEARTBEAT_INTERVAL_SECONDS = 30.0


def apply_file_task_result(
    result: SleepEdfFileTaskResult,
    uploaded_count: int,
    skipped_count: int,
    uploaded_bytes: int,
) -> tuple[int, int, int]:
    if result.uploaded:
        uploaded_count += 1
        uploaded_bytes += result.file_size_bytes

    elif result.skipped:
        skipped_count += 1

    else:
        raise RuntimeError(
            "Unexpected file task status: "
            f"{result.status}"
        )

    return (
        uploaded_count,
        skipped_count,
        uploaded_bytes,
    )


def calculate_progress_percent(
    processed_count: int,
    total_count: int,
) -> float:
    if processed_count < 0:
        raise ValueError(
            "processed_count cannot be negative"
        )

    if total_count <= 0:
        raise ValueError(
            "total_count must be positive"
        )

    if processed_count > total_count:
        raise ValueError(
            "processed_count cannot exceed "
            "total_count"
        )

    return round(
        processed_count / total_count * 100,
        2,
    )


def emit_file_progress(
    run_id: UUID,
    result: SleepEdfFileTaskResult,
    processed_count: int,
    total_count: int,
    uploaded_count: int,
    skipped_count: int,
    uploaded_bytes: int,
) -> None:
    emit_event(
        event="file_progress",
        run_id=run_id,
        pipeline_name=PIPELINE_NAME,
        task_name=TASK_NAME,
        source_system=SOURCE_SYSTEM,
        object_key=result.object_key,
        object_status=result.status,
        file_size_bytes=result.file_size_bytes,
        processed_object_count=processed_count,
        total_object_count=total_count,
        uploaded_object_count=uploaded_count,
        skipped_object_count=skipped_count,
        uploaded_bytes_total=uploaded_bytes,
        progress_percent=calculate_progress_percent(
            processed_count=processed_count,
            total_count=total_count,
        ),
    )


def stop_pipeline_heartbeat_safely(
    heartbeat: PipelineHeartbeat | None,
    run_id: UUID,
) -> None:
    if heartbeat is None:
        return

    try:
        heartbeat.stop()

    except Exception as error:
        emit_exception(
            event="heartbeat_stop_failed",
            error=error,
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
        )


def run_tracked_file_task(
    run_id: UUID,
    source_system: str,
    source_url: str | None,
    bucket: str,
    object_key: str,
    file_name: str,
    file_type: str,
    task: Callable[
        [],
        SleepEdfFileTaskResult,
    ],
) -> SleepEdfFileTaskResult:
    attempt_id = start_file_attempt(
        pipeline_run_id=run_id,
        source_system=source_system,
        source_url=source_url,
        bucket=bucket,
        object_key=object_key,
        file_name=file_name,
        file_type=file_type,
    )

    try:
        result = task()

        if result.uploaded:
            if result.checksum_sha256 is None:
                raise RuntimeError(
                    "Uploaded file task result "
                    "has no checksum"
                )

            finish_file_attempt_uploaded(
                attempt_id=attempt_id,
                file_size_bytes=(
                    result.file_size_bytes
                ),
                checksum_sha256=(
                    result.checksum_sha256
                ),
            )

        elif result.skipped:
            if result.resolution not in {
                "existing_valid",
                "recovered_existing",
            }:
                raise RuntimeError(
                    "Unexpected skipped "
                    "resolution: "
                    f"{result.resolution}"
                )

            finish_file_attempt_skipped(
                attempt_id=attempt_id,
                resolution=result.resolution,
            )

        else:
            raise RuntimeError(
                "Unexpected file task result: "
                f"{result.status}"
            )

        return result

    except Exception as error:
        try:
            finish_file_attempt_failed(
                attempt_id=attempt_id,
                error=error,
            )

        except Exception as history_error:
            emit_exception(
                event=(
                    "file_attempt_status_"
                    "update_failed"
                ),
                error=history_error,
                run_id=run_id,
                pipeline_name=PIPELINE_NAME,
                attempt_id=attempt_id,
                object_key=object_key,
                original_error_type=(
                    type(error).__name__
                ),
                original_error_message=(
                    str(error)
                ),
            )

        raise


def run_sleep_edf_extract_locked(
    settings: Settings,
) -> None:
    run_id = start_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        task_name=TASK_NAME,
        source_system=SOURCE_SYSTEM,
    )

    selected_object_count = 0
    processed_count = 0
    uploaded_count = 0
    skipped_count = 0
    uploaded_bytes = 0

    storage_client = None
    download_session = None
    heartbeat: PipelineHeartbeat | None = None

    emit_event(
        event="pipeline_started",
        run_id=run_id,
        pipeline_name=PIPELINE_NAME,
        task_name=TASK_NAME,
        source_system=SOURCE_SYSTEM,
        data_profile=settings.data_profile,
    )

    try:
        heartbeat = PipelineHeartbeat(
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            interval_seconds=(
                HEARTBEAT_INTERVAL_SECONDS
            ),
        )

        heartbeat.start()

        manifest = fetch_sleep_edf_remote_manifest(
            settings=settings
        )

        selected_object_count = (
            manifest.selected_extract_object_count
        )

        emit_event(
            event="manifest_loaded",
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
            data_profile=settings.data_profile,
            dataset_version=(
                manifest.dataset_version
            ),
            selected_recording_count=(
                manifest.selected_recording_count
            ),
            selected_source_file_count=len(
                manifest.selected_files
            ),
            selected_control_artifact_count=len(
                manifest.control_artifacts
            ),
            selected_extract_object_count=(
                selected_object_count
            ),
        )

        storage_client = (
            get_object_storage_client(
                settings=settings
            )
        )

        download_session = (
            create_download_session(settings)
        )

        with TemporaryDirectory(
            prefix="neuro_sleep_extract_"
        ) as temporary_directory:
            destination_root = Path(
                temporary_directory
            )

            for source_file in (
                manifest.selected_files
            ):
                result = run_tracked_file_task(
                    run_id=run_id,
                    source_system=SOURCE_SYSTEM,
                    source_url=(
                        source_file.source_url
                    ),
                    bucket=source_file.bucket,
                    object_key=(
                        source_file.object_key
                    ),
                    file_name=(
                        source_file.file_name
                    ),
                    file_type=(
                        source_file.file_type
                    ),
                    task=lambda: (
                        run_source_file_task(
                            source_file=source_file,
                            destination_root=(
                                destination_root
                            ),
                            settings=settings,
                            download_session=(
                                download_session
                            ),
                            storage_client=(
                                storage_client
                            ),
                            run_id=run_id,
                        )
                    ),
                )

                processed_count += 1

                (
                    uploaded_count,
                    skipped_count,
                    uploaded_bytes,
                ) = apply_file_task_result(
                    result=result,
                    uploaded_count=uploaded_count,
                    skipped_count=skipped_count,
                    uploaded_bytes=uploaded_bytes,
                )

                emit_file_progress(
                    run_id=run_id,
                    result=result,
                    processed_count=processed_count,
                    total_count=selected_object_count,
                    uploaded_count=uploaded_count,
                    skipped_count=skipped_count,
                    uploaded_bytes=uploaded_bytes,
                )

            for artifact in (
                manifest.control_artifacts
            ):
                result = run_tracked_file_task(
                    run_id=run_id,
                    source_system=SOURCE_SYSTEM,
                    source_url=(
                        artifact.source_url
                    ),
                    bucket=artifact.bucket,
                    object_key=(
                        artifact.object_key
                    ),
                    file_name=artifact.file_name,
                    file_type=artifact.file_type,
                    task=lambda: (
                        run_control_artifact_task(
                            artifact=artifact,
                            manifest=manifest,
                            destination_root=(
                                destination_root
                            ),
                            storage_client=(
                                storage_client
                            ),
                            run_id=run_id,
                        )
                    ),
                )

                processed_count += 1

                (
                    uploaded_count,
                    skipped_count,
                    uploaded_bytes,
                ) = apply_file_task_result(
                    result=result,
                    uploaded_count=uploaded_count,
                    skipped_count=skipped_count,
                    uploaded_bytes=uploaded_bytes,
                )

                emit_file_progress(
                    run_id=run_id,
                    result=result,
                    processed_count=processed_count,
                    total_count=selected_object_count,
                    uploaded_count=uploaded_count,
                    skipped_count=skipped_count,
                    uploaded_bytes=uploaded_bytes,
                )

        if processed_count != selected_object_count:
            raise RuntimeError(
                "Processed object count mismatch: "
                f"expected={selected_object_count}, "
                f"actual={processed_count}"
            )

        stop_pipeline_heartbeat_safely(
            heartbeat=heartbeat,
            run_id=run_id,
        )

        finish_pipeline_run_success(
            run_id=run_id,
            rows_read=0,
            rows_written=0,
            files_processed=processed_count,
            records_quarantined=0,
        )

        emit_event(
            event="pipeline_completed",
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
            status="success",
            processed_object_count=processed_count,
            uploaded_object_count=uploaded_count,
            skipped_object_count=skipped_count,
            uploaded_bytes_total=uploaded_bytes,
            progress_percent=100.0,
        )

    except Exception as error:
        emit_exception(
            event="pipeline_failed",
            error=error,
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            task_name=TASK_NAME,
            source_system=SOURCE_SYSTEM,
            processed_object_count=processed_count,
            total_object_count=selected_object_count,
            uploaded_object_count=uploaded_count,
            skipped_object_count=skipped_count,
            uploaded_bytes_total=uploaded_bytes,
        )

        try:
            finish_pipeline_run_failed(
                run_id=run_id,
                error_message=str(error),
                rows_read=0,
                rows_written=0,
                files_processed=processed_count,
                records_quarantined=0,
            )

        except Exception as status_error:
            emit_exception(
                event=(
                    "pipeline_status_update_failed"
                ),
                error=status_error,
                run_id=run_id,
                pipeline_name=PIPELINE_NAME,
                original_error_type=(
                    type(error).__name__
                ),
                original_error_message=str(error),
            )

        raise

    finally:
        stop_pipeline_heartbeat_safely(
            heartbeat=heartbeat,
            run_id=run_id,
        )

        cleanup_error_count = 0

        if download_session is not None:
            try:
                download_session.close()

            except Exception as cleanup_error:
                cleanup_error_count += 1

                emit_exception(
                    event="resource_cleanup_failed",
                    error=cleanup_error,
                    run_id=run_id,
                    resource_type=(
                        "requests_session"
                    ),
                )

        if storage_client is not None:
            try:
                storage_client.close()

            except Exception as cleanup_error:
                cleanup_error_count += 1

                emit_exception(
                    event="resource_cleanup_failed",
                    error=cleanup_error,
                    run_id=run_id,
                    resource_type=(
                        "object_storage_client"
                    ),
                )

        emit_event(
            event="resource_cleanup_completed",
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            cleanup_error_count=(
                cleanup_error_count
            ),
        )


def release_pipeline_lock_safely(
    pipeline_lock: PipelineRunLock,
) -> None:
    try:
        pipeline_lock.release()

    except Exception as error:
        emit_exception(
            event="pipeline_lock_release_failed",
            error=error,
            pipeline_name=PIPELINE_NAME,
        )

    else:
        emit_event(
            event="pipeline_lock_released",
            pipeline_name=PIPELINE_NAME,
        )


def run_sleep_edf_extract() -> None:
    settings = get_settings()

    pipeline_lock = PipelineRunLock(
        pipeline_name=PIPELINE_NAME,
        settings=settings,
    )

    try:
        pipeline_lock.acquire()

    except ConcurrentPipelineRunError as error:
        emit_exception(
            event="pipeline_concurrent_blocked",
            error=error,
            pipeline_name=PIPELINE_NAME,
        )

        raise

    except Exception as error:
        emit_exception(
            event="pipeline_lock_acquire_failed",
            error=error,
            pipeline_name=PIPELINE_NAME,
        )

        raise

    emit_event(
        event="pipeline_lock_acquired",
        pipeline_name=PIPELINE_NAME,
    )

    try:
        run_sleep_edf_extract_locked(
            settings=settings
        )

    finally:
        release_pipeline_lock_safely(
            pipeline_lock=pipeline_lock
        )


if __name__ == "__main__":
    try:
        run_sleep_edf_extract()

    except ConcurrentPipelineRunError:
        raise SystemExit(2) from None
