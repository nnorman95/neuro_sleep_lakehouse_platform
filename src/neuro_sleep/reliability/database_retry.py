from collections.abc import Callable
from typing import TypeVar

import psycopg
from psycopg import Connection
from psycopg.errors import InterfaceError, OperationalError

from neuro_sleep.observability.structured_logging import (
    emit_event,
)
from neuro_sleep.reliability.errors import (
    DatabaseTransientError,
)
from neuro_sleep.reliability.retry import (
    RetryEvent,
    RetryPolicy,
    run_with_retry,
)


ConnectionType = TypeVar("ConnectionType")


DEFAULT_DATABASE_CONNECT_RETRY_POLICY = RetryPolicy(
    max_attempts=4,
    initial_delay_seconds=2.0,
    multiplier=2.0,
    max_delay_seconds=30.0,
    jitter_seconds=0.5,
)


TRANSIENT_DATABASE_EXCEPTIONS = (
    OperationalError,
    InterfaceError,
)


def print_database_retry(
    operation_name: str,
    event: RetryEvent,
) -> None:
    emit_event(
        event="retry_scheduled",
        level="WARNING",
        component="database",
        operation=operation_name,
        failed_attempt=event.failed_attempt,
        next_attempt=event.next_attempt,
        delay_seconds=round(
            event.delay_seconds,
            2,
        ),
        error_type=event.error_type,
    )



def connect_postgres_with_retry(
    dsn: str,
    connector: Callable[
        [str],
        ConnectionType,
    ] = psycopg.connect,
    retry_policy: RetryPolicy = (
        DEFAULT_DATABASE_CONNECT_RETRY_POLICY
    ),
) -> ConnectionType:
    def connect_once() -> ConnectionType:
        try:
            return connector(dsn)

        except TRANSIENT_DATABASE_EXCEPTIONS as exc:
            raise DatabaseTransientError(
                "Temporary PostgreSQL connection "
                f"failure: {exc}"
            ) from exc

    return run_with_retry(
        operation=connect_once,
        policy=retry_policy,
        retry_for=(
            DatabaseTransientError,
        ),
        on_retry=lambda event: (
            print_database_retry(
                operation_name="connect",
                event=event,
            )
        ),
    )
