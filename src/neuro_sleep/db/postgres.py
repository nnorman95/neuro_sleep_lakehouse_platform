from collections.abc import Iterator
from contextlib import contextmanager

from psycopg import Connection

from neuro_sleep.config import Settings, get_settings
from neuro_sleep.reliability.database_retry import (
    connect_postgres_with_retry,
)


POSTGRES_CONNECT_TIMEOUT_SECONDS = 10


def build_postgres_dsn(
    settings: Settings,
) -> str:
    return (
        f"host={settings.postgres_host} "
        f"port={settings.postgres_port} "
        f"dbname={settings.postgres_db} "
        f"user={settings.postgres_user} "
        f"password={settings.postgres_password} "
        "connect_timeout="
        f"{POSTGRES_CONNECT_TIMEOUT_SECONDS}"
    )


def open_postgres_connection(
    settings: Settings | None = None,
) -> Connection:
    if settings is None:
        settings = get_settings()

    dsn = build_postgres_dsn(settings)

    return connect_postgres_with_retry(
        dsn=dsn
    )


@contextmanager
def get_postgres_connection(
    settings: Settings | None = None,
) -> Iterator[Connection]:
    connection = open_postgres_connection(
        settings=settings
    )

    with connection:
        yield connection


def check_postgres_connection() -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    current_database(),
                    current_user,
                    version();
                """
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError(
                    "PostgreSQL connection check "
                    "returned no result"
                )

            database_name, user_name, version = row

    print(f"database={database_name}")
    print(f"user={user_name}")
    print(f"version={version}")


if __name__ == "__main__":
    check_postgres_connection()
