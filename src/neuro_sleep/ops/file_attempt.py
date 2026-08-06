from dataclasses import dataclass
from datetime import datetime
from string import hexdigits
from typing import Literal
from uuid import UUID

from neuro_sleep.db.postgres import (
    get_postgres_connection,
)


FileAttemptId = UUID | str
RunId = UUID | str

FileAttemptStatus = Literal[
    "started",
    "uploaded",
    "skipped",
    "failed",
]

FileAttemptResolution = Literal[
    "downloaded_and_uploaded",
    "existing_valid",
    "recovered_existing",
]

SkipResolution = Literal[
    "existing_valid",
    "recovered_existing",
]


@dataclass(frozen=True)
class FileAttemptRecord:
    attempt_id: UUID
    pipeline_run_id: UUID
    source_system: str
    source_url: str | None
    bucket: str
    object_key: str
    file_name: str
    file_type: str
    status: FileAttemptStatus
    resolution: FileAttemptResolution | None
    file_size_bytes: int | None
    checksum_sha256: str | None
    error_type: str | None
    error_message: str | None
    started_at: datetime
    finished_at: datetime | None
    created_at: datetime


def _validate_non_empty(
    name: str,
    value: str,
) -> None:
    if not value.strip():
        raise ValueError(
            f"{name} cannot be empty"
        )


def _validate_non_negative(
    name: str,
    value: int,
) -> None:
    if value < 0:
        raise ValueError(
            f"{name} must be non-negative"
        )


def _validate_sha256(
    checksum_sha256: str,
) -> str:
    normalized_checksum = (
        checksum_sha256.strip().lower()
    )

    if len(normalized_checksum) != 64:
        raise ValueError(
            "SHA-256 must contain exactly "
            "64 hexadecimal characters"
        )

    if any(
        character not in hexdigits
        for character in normalized_checksum
    ):
        raise ValueError(
            "SHA-256 contains invalid "
            "characters"
        )

    return normalized_checksum


def start_file_attempt(
    pipeline_run_id: RunId,
    source_system: str,
    bucket: str,
    object_key: str,
    file_name: str,
    file_type: str,
    source_url: str | None = None,
) -> UUID:
    _validate_non_empty(
        "source_system",
        source_system,
    )
    _validate_non_empty(
        "bucket",
        bucket,
    )
    _validate_non_empty(
        "object_key",
        object_key,
    )
    _validate_non_empty(
        "file_name",
        file_name,
    )
    _validate_non_empty(
        "file_type",
        file_type,
    )

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into ops.file_attempt (
                    pipeline_run_id,
                    source_system,
                    source_url,
                    bucket,
                    object_key,
                    file_name,
                    file_type,
                    status
                )
                values (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    'started'
                )
                returning attempt_id;
                """,
                (
                    pipeline_run_id,
                    source_system,
                    source_url,
                    bucket,
                    object_key,
                    file_name,
                    file_type,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError(
                    "Failed to create file attempt"
                )

            return row[0]


def _finish_file_attempt(
    attempt_id: FileAttemptId,
    status: FileAttemptStatus,
    resolution: FileAttemptResolution | None,
    file_size_bytes: int | None,
    checksum_sha256: str | None,
    error_type: str | None,
    error_message: str | None,
) -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                update ops.file_attempt
                set
                    status = %s,
                    resolution = %s,
                    file_size_bytes = %s,
                    checksum_sha256 = %s,
                    error_type = %s,
                    error_message = %s,
                    finished_at = now()
                where attempt_id = %s
                  and status = 'started';
                """,
                (
                    status,
                    resolution,
                    file_size_bytes,
                    checksum_sha256,
                    error_type,
                    error_message,
                    attempt_id,
                ),
            )

            if cursor.rowcount == 1:
                return

            cursor.execute(
                """
                select status
                from ops.file_attempt
                where attempt_id = %s;
                """,
                (attempt_id,),
            )

            row = cursor.fetchone()

            if row is None:
                raise ValueError(
                    "File attempt not found: "
                    f"{attempt_id}"
                )

            raise RuntimeError(
                "File attempt is already "
                f"finished with status '{row[0]}': "
                f"{attempt_id}"
            )


def finish_file_attempt_uploaded(
    attempt_id: FileAttemptId,
    file_size_bytes: int,
    checksum_sha256: str,
) -> None:
    _validate_non_negative(
        "file_size_bytes",
        file_size_bytes,
    )

    normalized_checksum = _validate_sha256(
        checksum_sha256
    )

    _finish_file_attempt(
        attempt_id=attempt_id,
        status="uploaded",
        resolution="downloaded_and_uploaded",
        file_size_bytes=file_size_bytes,
        checksum_sha256=normalized_checksum,
        error_type=None,
        error_message=None,
    )


def finish_file_attempt_skipped(
    attempt_id: FileAttemptId,
    resolution: SkipResolution,
) -> None:
    if resolution not in {
        "existing_valid",
        "recovered_existing",
    }:
        raise ValueError(
            "Invalid skipped resolution: "
            f"{resolution}"
        )

    _finish_file_attempt(
        attempt_id=attempt_id,
        status="skipped",
        resolution=resolution,
        file_size_bytes=None,
        checksum_sha256=None,
        error_type=None,
        error_message=None,
    )


def finish_file_attempt_failed(
    attempt_id: FileAttemptId,
    error: BaseException,
) -> None:
    error_message = str(error).strip()

    if not error_message:
        error_message = type(error).__name__

    _finish_file_attempt(
        attempt_id=attempt_id,
        status="failed",
        resolution=None,
        file_size_bytes=None,
        checksum_sha256=None,
        error_type=type(error).__name__,
        error_message=error_message,
    )


def get_file_attempt(
    attempt_id: FileAttemptId,
) -> FileAttemptRecord | None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    attempt_id,
                    pipeline_run_id,
                    source_system,
                    source_url,
                    bucket,
                    object_key,
                    file_name,
                    file_type,
                    status,
                    resolution,
                    file_size_bytes,
                    checksum_sha256,
                    error_type,
                    error_message,
                    started_at,
                    finished_at,
                    created_at
                from ops.file_attempt
                where attempt_id = %s;
                """,
                (attempt_id,),
            )

            row = cursor.fetchone()

    if row is None:
        return None

    return FileAttemptRecord(
        attempt_id=row[0],
        pipeline_run_id=row[1],
        source_system=row[2],
        source_url=row[3],
        bucket=row[4],
        object_key=row[5],
        file_name=row[6],
        file_type=row[7],
        status=row[8],
        resolution=row[9],
        file_size_bytes=row[10],
        checksum_sha256=row[11],
        error_type=row[12],
        error_message=row[13],
        started_at=row[14],
        finished_at=row[15],
        created_at=row[16],
    )


def delete_file_attempts_for_smoke_test(
    pipeline_run_id: RunId,
) -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                delete from ops.file_attempt
                where pipeline_run_id = %s;
                """,
                (pipeline_run_id,),
            )
