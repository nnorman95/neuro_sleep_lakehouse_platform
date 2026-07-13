from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from neuro_sleep.db.postgres import get_postgres_connection
from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_failed,
    finish_pipeline_run_success,
    start_pipeline_run,
)


RunId = UUID | str
FileId = UUID | str
QuarantineId = UUID | str

ALLOWED_SEVERITIES = {"info", "warning", "error", "critical"}
ALLOWED_STATUSES = {"open", "reviewed", "resolved", "ignored"}


def _validate_non_negative_optional(name: str, value: int | None) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{name} must be 0 or a positive integer")


def _validate_severity(severity: str) -> None:
    if severity not in ALLOWED_SEVERITIES:
        allowed = ", ".join(sorted(ALLOWED_SEVERITIES))
        raise ValueError(f"severity must be one of: {allowed}")


def _validate_status(status: str) -> None:
    if status not in ALLOWED_STATUSES:
        allowed = ", ".join(sorted(ALLOWED_STATUSES))
        raise ValueError(f"status must be one of: {allowed}")


def create_quarantine_record(
    source_system: str,
    error_code: str,
    error_message: str,
    severity: str = "error",
    status: str = "open",
    source_file_id: FileId | None = None,
    record_key: str | None = None,
    raw_payload: dict[str, Any] | list[Any] | None = None,
    pipeline_run_id: RunId | None = None,
    payload_bucket: str | None = None,
    payload_object_key: str | None = None,
    payload_size_bytes: int | None = None,
    payload_checksum_sha256: str | None = None,
) -> UUID:
    _validate_severity(severity)
    _validate_status(status)
    _validate_non_negative_optional("payload_size_bytes", payload_size_bytes)

    payload_value = Jsonb(raw_payload) if raw_payload is not None else None

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into quality.quarantine_records (
                    source_system,
                    source_file_id,
                    record_key,
                    raw_payload,
                    error_code,
                    error_message,
                    severity,
                    pipeline_run_id,
                    status,
                    payload_bucket,
                    payload_object_key,
                    payload_size_bytes,
                    payload_checksum_sha256
                )
                values (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                returning quarantine_id;
                """,
                (
                    source_system,
                    source_file_id,
                    record_key,
                    payload_value,
                    error_code,
                    error_message,
                    severity,
                    pipeline_run_id,
                    status,
                    payload_bucket,
                    payload_object_key,
                    payload_size_bytes,
                    payload_checksum_sha256,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError("Failed to create quarantine record")

            return row[0]


def get_quarantine_record(quarantine_id: QuarantineId) -> tuple:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    quarantine_id,
                    source_system,
                    source_file_id,
                    record_key,
                    raw_payload,
                    error_code,
                    error_message,
                    severity,
                    pipeline_run_id,
                    status,
                    payload_bucket,
                    payload_object_key,
                    payload_size_bytes,
                    payload_checksum_sha256,
                    detected_at
                from quality.quarantine_records
                where quarantine_id = %s;
                """,
                (quarantine_id,),
            )

            row = cursor.fetchone()

            if row is None:
                raise ValueError(f"Quarantine record not found: {quarantine_id}")

            return row


def delete_quarantine_record_for_smoke_test(
    source_system: str,
    record_key: str,
    error_code: str,
) -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                delete from quality.quarantine_records
                where source_system = %s
                  and record_key = %s
                  and error_code = %s;
                """,
                (
                    source_system,
                    record_key,
                    error_code,
                ),
            )


def run_smoke_test() -> None:
    source_system = "physionet_sleep_edf"
    record_key = "smoke-tests/quarantine/test-record"
    error_code = "SMOKE_TEST_INVALID_RECORD"

    run_id = start_pipeline_run(
        pipeline_name="quality_quarantine_smoke_test",
        task_name="create_and_cleanup_quarantine_record",
        source_system=source_system,
    )

    try:
        delete_quarantine_record_for_smoke_test(
            source_system=source_system,
            record_key=record_key,
            error_code=error_code,
        )

        quarantine_id = create_quarantine_record(
            source_system=source_system,
            record_key=record_key,
            raw_payload={
                "example": "bad record",
                "reason": "smoke_test",
            },
            error_code=error_code,
            error_message="Smoke test quarantine record.",
            severity="warning",
            pipeline_run_id=run_id,
            status="open",
        )

        row = get_quarantine_record(quarantine_id)

        print(f"run_id={run_id}")
        print(f"quarantine_id={row[0]}")
        print(f"source_system={row[1]}")
        print(f"source_file_id={row[2]}")
        print(f"record_key={row[3]}")
        print(f"raw_payload={row[4]}")
        print(f"error_code={row[5]}")
        print(f"error_message={row[6]}")
        print(f"severity={row[7]}")
        print(f"pipeline_run_id={row[8]}")
        print(f"status={row[9]}")
        print(f"payload_bucket={row[10]}")
        print(f"payload_object_key={row[11]}")
        print(f"payload_size_bytes={row[12]}")
        print(f"payload_checksum_sha256={row[13]}")
        print(f"detected_at={row[14]}")

        delete_quarantine_record_for_smoke_test(
            source_system=source_system,
            record_key=record_key,
            error_code=error_code,
        )

        finish_pipeline_run_success(
            run_id=run_id,
            rows_read=1,
            rows_written=0,
            files_processed=0,
            records_quarantined=1,
        )

        print("smoke_test_cleanup=done")
        print("smoke_test_status=success")

    except Exception as exc:
        finish_pipeline_run_failed(
            run_id=run_id,
            error_message=str(exc),
            rows_read=1,
            rows_written=0,
            files_processed=0,
            records_quarantined=1,
        )

        raise


if __name__ == "__main__":
    run_smoke_test()
