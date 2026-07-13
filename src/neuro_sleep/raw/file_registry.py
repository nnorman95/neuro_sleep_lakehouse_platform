from uuid import UUID

from neuro_sleep.raw.models import RawFileRecord
from neuro_sleep.db.postgres import get_postgres_connection
from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_success,
    start_pipeline_run,
)


RunId = UUID | str
FileId = UUID | str


def _validate_non_negative(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be 0 or a positive integer")


def register_raw_file(
    source_system: str,
    bucket: str,
    object_key: str,
    file_name: str,
    file_type: str,
    source_url: str | None = None,
    ingestion_run_id: RunId | None = None,
) -> UUID:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into raw.file_registry (
                    source_system,
                    source_url,
                    bucket,
                    object_key,
                    file_name,
                    file_type,
                    ingestion_run_id,
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
                    'registered'
                )
                on conflict (bucket, object_key)
                do update set
                    source_system = excluded.source_system,
                    source_url = excluded.source_url,
                    file_name = excluded.file_name,
                    file_type = excluded.file_type,
                    ingestion_run_id = coalesce(
                        excluded.ingestion_run_id,
                        raw.file_registry.ingestion_run_id
                    ),
                    status = 'registered',
                    file_size_bytes = null,
                    checksum_sha256 = null,
                    ingested_at = null
                returning file_id;
                """,
                (
                    source_system,
                    source_url,
                    bucket,
                    object_key,
                    file_name,
                    file_type,
                    ingestion_run_id,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError("Failed to register raw file")

            return row[0]


def mark_raw_file_uploaded(
    file_id: FileId,
    file_size_bytes: int,
    checksum_sha256: str,
    ingestion_run_id: RunId | None = None,
) -> None:
    _validate_non_negative("file_size_bytes", file_size_bytes)

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                update raw.file_registry
                set
                    status = 'uploaded',
                    file_size_bytes = %s,
                    checksum_sha256 = %s,
                    ingestion_run_id = coalesce(%s, ingestion_run_id),
                    ingested_at = now()
                where file_id = %s;
                """,
                (
                    file_size_bytes,
                    checksum_sha256,
                    ingestion_run_id,
                    file_id,
                ),
            )

            if cursor.rowcount != 1:
                raise ValueError(f"Raw file not found: {file_id}")


def mark_raw_file_failed(
    file_id: FileId,
    ingestion_run_id: RunId | None = None,
) -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                update raw.file_registry
                set
                    status = 'failed',
                    ingestion_run_id = coalesce(%s, ingestion_run_id),
                    ingested_at = now()
                where file_id = %s;
                """,
                (
                    ingestion_run_id,
                    file_id,
                ),
            )

            if cursor.rowcount != 1:
                raise ValueError(f"Raw file not found: {file_id}")


def get_raw_file_by_object_key(
    bucket: str,
    object_key: str,
) -> RawFileRecord | None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    file_id,
                    source_system,
                    source_url,
                    bucket,
                    object_key,
                    file_name,
                    file_type,
                    file_size_bytes,
                    checksum_sha256,
                    ingestion_run_id,
                    status,
                    ingested_at
                from raw.file_registry
                where bucket = %s
                  and object_key = %s;
                """,
                (
                    bucket,
                    object_key,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                return None

            return RawFileRecord(
                file_id=row[0],
                source_system=row[1],
                source_url=row[2],
                bucket=row[3],
                object_key=row[4],
                file_name=row[5],
                file_type=row[6],
                file_size_bytes=row[7],
                checksum_sha256=row[8],
                ingestion_run_id=row[9],
                status=row[10],
                ingested_at=row[11],
            )


def list_raw_files_by_bucket_prefix(
    bucket: str,
    prefix: str = "",
) -> list[RawFileRecord]:
    if not bucket.strip():
        raise ValueError(
            "bucket cannot be empty"
        )

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    file_id,
                    source_system,
                    source_url,
                    bucket,
                    object_key,
                    file_name,
                    file_type,
                    file_size_bytes,
                    checksum_sha256,
                    ingestion_run_id,
                    status,
                    ingested_at
                from raw.file_registry
                where bucket = %s
                  and left(
                        object_key,
                        length(%s)
                  ) = %s
                order by object_key;
                """,
                (
                    bucket,
                    prefix,
                    prefix,
                ),
            )

            rows = cursor.fetchall()

    return [
        RawFileRecord(
            file_id=row[0],
            source_system=row[1],
            source_url=row[2],
            bucket=row[3],
            object_key=row[4],
            file_name=row[5],
            file_type=row[6],
            file_size_bytes=row[7],
            checksum_sha256=row[8],
            ingestion_run_id=row[9],
            status=row[10],
            ingested_at=row[11],
        )
        for row in rows
    ]


def delete_raw_file_for_smoke_test(
    bucket: str,
    object_key: str,
) -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                delete from raw.file_registry
                where bucket = %s
                  and object_key = %s;
                """,
                (
                    bucket,
                    object_key,
                ),
            )


def run_smoke_test() -> None:
    bucket = "bronze"
    object_key = "smoke-tests/raw-file-registry/test-file.txt"

    delete_raw_file_for_smoke_test(bucket=bucket, object_key=object_key)

    run_id = start_pipeline_run(
        pipeline_name="raw_file_registry_smoke_test",
        task_name="register_and_mark_uploaded",
        source_system="physionet_sleep_edf",
    )

    file_id = register_raw_file(
        source_system="physionet_sleep_edf",
        source_url="https://example.local/smoke-test/test-file.txt",
        bucket=bucket,
        object_key=object_key,
        file_name="test-file.txt",
        file_type="txt",
        ingestion_run_id=run_id,
    )

    print(f"created_run_id={run_id}")
    print(f"registered_file_id={file_id}")

    mark_raw_file_uploaded(
        file_id=file_id,
        file_size_bytes=128,
        checksum_sha256=(
            "0000000000000000000000000000000000000000000000000000000000000000"
        ),
        ingestion_run_id=run_id,
    )

    row = get_raw_file_by_object_key(bucket=bucket, object_key=object_key)

    if row is None:
        raise RuntimeError("Smoke test raw file record was not found")

    print(f"file_id={row.file_id}")
    print(f"source_system={row.source_system}")
    print(f"source_url={row.source_url}")
    print(f"bucket={row.bucket}")
    print(f"object_key={row.object_key}")
    print(f"file_name={row.file_name}")
    print(f"file_type={row.file_type}")
    print(
        f"file_size_bytes={row.file_size_bytes}"
    )
    print(
        f"checksum_sha256={row.checksum_sha256}"
    )
    print(
        f"ingestion_run_id={row.ingestion_run_id}"
    )
    print(f"status={row.status}")
    print(f"ingested_at={row.ingested_at}")

    delete_raw_file_for_smoke_test(bucket=bucket, object_key=object_key)

    finish_pipeline_run_success(
        run_id=run_id,
        rows_read=1,
        rows_written=1,
        files_processed=1,
        records_quarantined=0,
    )

    print("smoke_test_cleanup=done")
    print("smoke_test_status=success")


if __name__ == "__main__":
    run_smoke_test()
