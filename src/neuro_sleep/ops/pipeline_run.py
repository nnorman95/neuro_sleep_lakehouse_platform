from datetime import datetime
from uuid import UUID

from neuro_sleep.ops.models import PipelineRunRecord
from neuro_sleep.db.postgres import get_postgres_connection


RunId = UUID | str


def _validate_non_negative(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be 0 or a positive integer")


def start_pipeline_run(
    pipeline_name: str,
    task_name: str | None = None,
    source_system: str | None = None,
) -> UUID:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into ops.pipeline_run (
                    pipeline_name,
                    task_name,
                    source_system,
                    status
                )
                values (
                    %s,
                    %s,
                    %s,
                    'started'
                )
                returning run_id;
                """,
                (
                    pipeline_name,
                    task_name,
                    source_system,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError("Failed to create pipeline run record")

            return row[0]

def update_pipeline_run_heartbeat(
    run_id: RunId,
) -> datetime | None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                update ops.pipeline_run
                set heartbeat_at = now()
                where run_id = %s
                  and status = 'started'
                returning heartbeat_at;
                """,
                (run_id,),
            )

            row = cursor.fetchone()

            if row is not None:
                return row[0]

            cursor.execute(
                """
                select status
                from ops.pipeline_run
                where run_id = %s;
                """,
                (run_id,),
            )

            status_row = cursor.fetchone()

            if status_row is None:
                raise ValueError(
                    f"Pipeline run not found: {run_id}"
                )

            return None




def finish_pipeline_run_success(
    run_id: RunId,
    rows_read: int = 0,
    rows_written: int = 0,
    files_processed: int = 0,
    records_quarantined: int = 0,
) -> None:
    _validate_non_negative("rows_read", rows_read)
    _validate_non_negative("rows_written", rows_written)
    _validate_non_negative("files_processed", files_processed)
    _validate_non_negative("records_quarantined", records_quarantined)

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                update ops.pipeline_run
                set
                    status = 'success',
                    finished_at = now(),
                    rows_read = %s,
                    rows_written = %s,
                    files_processed = %s,
                    records_quarantined = %s,
                    error_message = null
                where run_id = %s;
                """,
                (
                    rows_read,
                    rows_written,
                    files_processed,
                    records_quarantined,
                    run_id,
                ),
            )

            if cursor.rowcount != 1:
                raise ValueError(f"Pipeline run not found: {run_id}")


def finish_pipeline_run_failed(
    run_id: RunId,
    error_message: str,
    rows_read: int = 0,
    rows_written: int = 0,
    files_processed: int = 0,
    records_quarantined: int = 0,
) -> None:
    _validate_non_negative("rows_read", rows_read)
    _validate_non_negative("rows_written", rows_written)
    _validate_non_negative("files_processed", files_processed)
    _validate_non_negative("records_quarantined", records_quarantined)

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                update ops.pipeline_run
                set
                    status = 'failed',
                    finished_at = now(),
                    rows_read = %s,
                    rows_written = %s,
                    files_processed = %s,
                    records_quarantined = %s,
                    error_message = %s
                where run_id = %s;
                """,
                (
                    rows_read,
                    rows_written,
                    files_processed,
                    records_quarantined,
                    error_message,
                    run_id,
                ),
            )

            if cursor.rowcount != 1:
                raise ValueError(f"Pipeline run not found: {run_id}")

def finish_pipeline_run_skipped(
    run_id: RunId,
    reason: str,
    rows_read: int = 0,
    rows_written: int = 0,
    files_processed: int = 0,
    records_quarantined: int = 0,
) -> None:
    _validate_non_negative("rows_read", rows_read)
    _validate_non_negative("rows_written", rows_written)
    _validate_non_negative("files_processed", files_processed)
    _validate_non_negative("records_quarantined", records_quarantined)

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                update ops.pipeline_run
                set
                    status = 'skipped',
                    finished_at = now(),
                    rows_read = %s,
                    rows_written = %s,
                    files_processed = %s,
                    records_quarantined = %s,
                    error_message = %s
                where run_id = %s;
                """,
                (
                    rows_read,
                    rows_written,
                    files_processed,
                    records_quarantined,
                    reason,
                    run_id,
                ),
            )

            if cursor.rowcount != 1:
                raise ValueError(f"Pipeline run not found: {run_id}")

def get_pipeline_run_status(
    run_id: RunId,
) -> PipelineRunRecord:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    run_id,
                    pipeline_name,
                    task_name,
                    source_system,
                    status,
                    rows_read,
                    rows_written,
                    files_processed,
                    records_quarantined,
                    error_message
                from ops.pipeline_run
                where run_id = %s;
                """,
                (run_id,),
            )

            row = cursor.fetchone()

            if row is None:
                raise ValueError(f"Pipeline run not found: {run_id}")

            return PipelineRunRecord(
                run_id=row[0],
                pipeline_name=row[1],
                task_name=row[2],
                source_system=row[3],
                status=row[4],
                rows_read=row[5],
                rows_written=row[6],
                files_processed=row[7],
                records_quarantined=row[8],
                error_message=row[9],
            )


def run_smoke_test() -> None:
    run_id = start_pipeline_run(
        pipeline_name="pipeline_run_smoke_test",
        task_name="create_and_finish_pipeline_run",
        source_system="physionet_sleep_edf",
    )

    print(f"created_run_id={run_id}")

    finish_pipeline_run_success(
        run_id=run_id,
        rows_read=10,
        rows_written=10,
        files_processed=1,
        records_quarantined=0,
    )

    row = get_pipeline_run_status(run_id)

    print(f"run_id={row.run_id}")
    print(
        f"pipeline_name={row.pipeline_name}"
    )
    print(f"task_name={row.task_name}")
    print(
        f"source_system={row.source_system}"
    )
    print(f"status={row.status}")
    print(f"rows_read={row.rows_read}")
    print(f"rows_written={row.rows_written}")
    print(
        f"files_processed={row.files_processed}"
    )
    print(
        "records_quarantined="
        f"{row.records_quarantined}"
    )
    print(
        f"error_message={row.error_message}"
    )


if __name__ == "__main__":
    run_smoke_test()
