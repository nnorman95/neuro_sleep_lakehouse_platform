from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from psycopg.types.json import Jsonb

from neuro_sleep.db.postgres import (
    get_postgres_connection,
)


RunId = UUID | str
RecordingId = UUID | str

QualitySeverity = Literal[
    "info",
    "warning",
    "error",
    "critical",
]

QualityStatus = Literal[
    "passed",
    "warning",
    "failed",
    "skipped",
]

ALLOWED_DATA_LAYERS = {
    "bronze",
    "silver",
    "gold",
    "raw",
    "staging",
    "warehouse",
    "mart",
    "ops",
    "quality",
    "governance",
}

ALLOWED_SEVERITIES = {
    "info",
    "warning",
    "error",
    "critical",
}

ALLOWED_STATUSES = {
    "passed",
    "warning",
    "failed",
    "skipped",
}


@dataclass(frozen=True)
class QualityCheckResultRecord:
    quality_result_id: UUID
    pipeline_run_id: UUID
    source_system: str | None
    data_layer: str
    dataset_name: str
    recording_id: UUID | None
    record_key: str | None
    check_name: str
    severity: str
    status: str
    rows_checked: int
    rows_failed: int
    error_code: str | None
    message: str | None
    details: dict[str, Any]
    checked_at: datetime
    created_at: datetime


def _validate_non_empty(
    name: str,
    value: str,
) -> str:
    cleaned_value = value.strip()

    if not cleaned_value:
        raise ValueError(
            f"{name} cannot be empty"
        )

    return cleaned_value


def _validate_choice(
    name: str,
    value: str,
    allowed_values: set[str],
) -> str:
    cleaned_value = _validate_non_empty(
        name=name,
        value=value,
    )

    if cleaned_value not in allowed_values:
        allowed = ", ".join(
            sorted(allowed_values)
        )

        raise ValueError(
            f"{name} must be one of: "
            f"{allowed}"
        )

    return cleaned_value


def _validate_non_negative(
    name: str,
    value: int,
) -> None:
    if value < 0:
        raise ValueError(
            f"{name} must be 0 or a "
            "positive integer"
        )


def create_quality_check_result(
    *,
    pipeline_run_id: RunId,
    data_layer: str,
    dataset_name: str,
    check_name: str,
    severity: QualitySeverity,
    status: QualityStatus,
    rows_checked: int = 0,
    rows_failed: int = 0,
    source_system: str | None = None,
    recording_id: RecordingId | None = None,
    record_key: str | None = None,
    error_code: str | None = None,
    message: str | None = None,
    details: dict[str, Any] | None = None,
) -> UUID:
    cleaned_layer = _validate_choice(
        name="data_layer",
        value=data_layer,
        allowed_values=ALLOWED_DATA_LAYERS,
    )

    cleaned_dataset_name = (
        _validate_non_empty(
            name="dataset_name",
            value=dataset_name,
        )
    )

    cleaned_check_name = _validate_non_empty(
        name="check_name",
        value=check_name,
    )

    cleaned_severity = _validate_choice(
        name="severity",
        value=severity,
        allowed_values=ALLOWED_SEVERITIES,
    )

    cleaned_status = _validate_choice(
        name="status",
        value=status,
        allowed_values=ALLOWED_STATUSES,
    )

    _validate_non_negative(
        "rows_checked",
        rows_checked,
    )
    _validate_non_negative(
        "rows_failed",
        rows_failed,
    )

    details_value = Jsonb(
        details if details is not None else {}
    )

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into quality.quality_check_results (
                    pipeline_run_id,
                    source_system,
                    data_layer,
                    dataset_name,
                    recording_id,
                    record_key,
                    check_name,
                    severity,
                    status,
                    rows_checked,
                    rows_failed,
                    error_code,
                    message,
                    details
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
                    %s,
                    %s
                )
                returning quality_result_id;
                """,
                (
                    pipeline_run_id,
                    source_system,
                    cleaned_layer,
                    cleaned_dataset_name,
                    recording_id,
                    record_key,
                    cleaned_check_name,
                    cleaned_severity,
                    cleaned_status,
                    rows_checked,
                    rows_failed,
                    error_code,
                    message,
                    details_value,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError(
                    "Failed to create quality "
                    "check result"
                )

            return row[0]


def get_quality_check_results_for_run(
    pipeline_run_id: RunId,
) -> tuple[QualityCheckResultRecord, ...]:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    quality_result_id,
                    pipeline_run_id,
                    source_system,
                    data_layer,
                    dataset_name,
                    recording_id,
                    record_key,
                    check_name,
                    severity,
                    status,
                    rows_checked,
                    rows_failed,
                    error_code,
                    message,
                    details,
                    checked_at,
                    created_at
                from quality.quality_check_results
                where pipeline_run_id = %s
                order by
                    checked_at,
                    quality_result_id;
                """,
                (pipeline_run_id,),
            )

            rows = cursor.fetchall()

    return tuple(
        QualityCheckResultRecord(
            quality_result_id=row[0],
            pipeline_run_id=row[1],
            source_system=row[2],
            data_layer=row[3],
            dataset_name=row[4],
            recording_id=row[5],
            record_key=row[6],
            check_name=row[7],
            severity=row[8],
            status=row[9],
            rows_checked=row[10],
            rows_failed=row[11],
            error_code=row[12],
            message=row[13],
            details=row[14],
            checked_at=row[15],
            created_at=row[16],
        )
        for row in rows
    )


def delete_quality_check_results_for_run(
    pipeline_run_id: RunId,
) -> int:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                delete from quality.quality_check_results
                where pipeline_run_id = %s;
                """,
                (pipeline_run_id,),
            )

            return cursor.rowcount
