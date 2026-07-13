from psycopg.errors import UniqueViolation

from neuro_sleep.ops.file_attempt import (
    delete_file_attempts_for_smoke_test,
    finish_file_attempt_failed,
    finish_file_attempt_skipped,
    finish_file_attempt_uploaded,
    get_file_attempt,
    start_file_attempt,
)
from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_failed,
    finish_pipeline_run_success,
    start_pipeline_run,
)


SOURCE_SYSTEM = "physionet_sleep_edf"
BUCKET = "bronze"


def run_smoke_test() -> None:
    run_id = start_pipeline_run(
        pipeline_name="file_attempt_smoke_test",
        task_name="test_attempt_history",
        source_system=SOURCE_SYSTEM,
    )

    try:
        uploaded_attempt_id = start_file_attempt(
            pipeline_run_id=run_id,
            source_system=SOURCE_SYSTEM,
            source_url=(
                "https://example.local/"
                "uploaded.edf"
            ),
            bucket=BUCKET,
            object_key=(
                "smoke-tests/file-attempt/"
                "uploaded.edf"
            ),
            file_name="uploaded.edf",
            file_type="edf",
        )

        if uploaded_attempt_id.version != 7:
            raise RuntimeError(
                "attempt_id is not UUIDv7"
            )

        started_record = get_file_attempt(
            uploaded_attempt_id
        )

        if started_record is None:
            raise RuntimeError(
                "Started attempt was not found"
            )

        if started_record.status != "started":
            raise RuntimeError(
                "New attempt is not started"
            )

        print("file_attempt_started=true")
        print("file_attempt_uuid7=true")

        try:
            start_file_attempt(
                pipeline_run_id=run_id,
                source_system=SOURCE_SYSTEM,
                source_url=(
                    "https://example.local/"
                    "uploaded.edf"
                ),
                bucket=BUCKET,
                object_key=(
                    "smoke-tests/file-attempt/"
                    "uploaded.edf"
                ),
                file_name="uploaded.edf",
                file_type="edf",
            )

        except UniqueViolation:
            print(
                "duplicate_run_object_blocked=true"
            )

        else:
            raise RuntimeError(
                "Duplicate file attempt "
                "was not blocked"
            )

        checksum = "a" * 64

        finish_file_attempt_uploaded(
            attempt_id=uploaded_attempt_id,
            file_size_bytes=1024,
            checksum_sha256=checksum,
        )

        uploaded_record = get_file_attempt(
            uploaded_attempt_id
        )

        if uploaded_record is None:
            raise RuntimeError(
                "Uploaded attempt was not found"
            )

        if (
            uploaded_record.status != "uploaded"
            or uploaded_record.resolution
            != "downloaded_and_uploaded"
            or uploaded_record.file_size_bytes
            != 1024
            or uploaded_record.checksum_sha256
            != checksum
            or uploaded_record.finished_at is None
        ):
            raise RuntimeError(
                "Uploaded attempt state "
                "is incorrect"
            )

        print(
            "file_attempt_uploaded=true"
        )

        skipped_attempt_id = start_file_attempt(
            pipeline_run_id=run_id,
            source_system=SOURCE_SYSTEM,
            source_url=(
                "https://example.local/"
                "skipped.edf"
            ),
            bucket=BUCKET,
            object_key=(
                "smoke-tests/file-attempt/"
                "skipped.edf"
            ),
            file_name="skipped.edf",
            file_type="edf",
        )

        finish_file_attempt_skipped(
            attempt_id=skipped_attempt_id,
            resolution="recovered_existing",
        )

        skipped_record = get_file_attempt(
            skipped_attempt_id
        )

        if (
            skipped_record is None
            or skipped_record.status
            != "skipped"
            or skipped_record.resolution
            != "recovered_existing"
            or skipped_record.finished_at is None
        ):
            raise RuntimeError(
                "Skipped attempt state "
                "is incorrect"
            )

        print(
            "file_attempt_skipped=true"
        )

        failed_attempt_id = start_file_attempt(
            pipeline_run_id=run_id,
            source_system=SOURCE_SYSTEM,
            source_url=(
                "https://example.local/"
                "failed.edf"
            ),
            bucket=BUCKET,
            object_key=(
                "smoke-tests/file-attempt/"
                "failed.edf"
            ),
            file_name="failed.edf",
            file_type="edf",
        )

        simulated_error = RuntimeError(
            "Simulated file attempt failure"
        )

        finish_file_attempt_failed(
            attempt_id=failed_attempt_id,
            error=simulated_error,
        )

        failed_record = get_file_attempt(
            failed_attempt_id
        )

        if (
            failed_record is None
            or failed_record.status != "failed"
            or failed_record.resolution is not None
            or failed_record.error_type
            != "RuntimeError"
            or failed_record.error_message
            != "Simulated file attempt failure"
            or failed_record.finished_at is None
        ):
            raise RuntimeError(
                "Failed attempt state "
                "is incorrect"
            )

        print(
            "file_attempt_failed=true"
        )

        try:
            finish_file_attempt_failed(
                attempt_id=failed_attempt_id,
                error=simulated_error,
            )

        except RuntimeError:
            print(
                "terminal_attempt_immutable=true"
            )

        else:
            raise RuntimeError(
                "Finished attempt was modified"
            )

    except Exception as error:
        try:
            finish_pipeline_run_failed(
                run_id=run_id,
                error_message=str(error),
                rows_read=0,
                rows_written=0,
                files_processed=0,
                records_quarantined=0,
            )

        except Exception:
            pass

        raise

    else:
        finish_pipeline_run_success(
            run_id=run_id,
            rows_read=3,
            rows_written=3,
            files_processed=3,
            records_quarantined=0,
        )

    finally:
        delete_file_attempts_for_smoke_test(
            pipeline_run_id=run_id
        )

    print("file_attempt_cleanup=true")
    print(
        "file_attempt_smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
