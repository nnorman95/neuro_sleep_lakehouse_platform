from dataclasses import dataclass

from psycopg.errors import OperationalError

from neuro_sleep.reliability.database_retry import (
    connect_postgres_with_retry,
)
from neuro_sleep.reliability.retry import (
    RetryPolicy,
)


ZERO_DELAY_POLICY = RetryPolicy(
    max_attempts=3,
    initial_delay_seconds=0.0,
    multiplier=2.0,
    max_delay_seconds=0.0,
    jitter_seconds=0.0,
)


@dataclass
class FakeConnection:
    connection_name: str


def run_smoke_test() -> None:
    transient_attempts = 0

    def transient_connector(
        dsn: str,
    ) -> FakeConnection:
        nonlocal transient_attempts

        transient_attempts += 1

        if transient_attempts == 1:
            raise OperationalError(
                "PostgreSQL is temporarily unavailable"
            )

        return FakeConnection(
            connection_name="connected"
        )

    connection = connect_postgres_with_retry(
        dsn="postgresql://smoke-test",
        connector=transient_connector,
        retry_policy=ZERO_DELAY_POLICY,
    )

    if connection.connection_name != "connected":
        raise RuntimeError(
            "Unexpected fake connection result"
        )

    if transient_attempts != 2:
        raise RuntimeError(
            "Transient database failure did not "
            "produce exactly one retry"
        )

    permanent_attempts = 0

    def permanent_connector(
        dsn: str,
    ) -> FakeConnection:
        nonlocal permanent_attempts

        permanent_attempts += 1

        raise ValueError(
            "Invalid database configuration"
        )

    try:
        connect_postgres_with_retry(
            dsn="invalid",
            connector=permanent_connector,
            retry_policy=ZERO_DELAY_POLICY,
        )

    except ValueError:
        print(
            "database_permanent_error_not_retried=true"
        )

    else:
        raise RuntimeError(
            "Permanent database error "
            "was not propagated"
        )

    if permanent_attempts != 1:
        raise RuntimeError(
            "Permanent database error "
            "was incorrectly retried"
        )

    exhausted_attempts = 0

    def exhausted_connector(
        dsn: str,
    ) -> FakeConnection:
        nonlocal exhausted_attempts

        exhausted_attempts += 1

        raise OperationalError(
            "PostgreSQL remains unavailable"
        )

    try:
        connect_postgres_with_retry(
            dsn="postgresql://unavailable",
            connector=exhausted_connector,
            retry_policy=RetryPolicy(
                max_attempts=2,
                initial_delay_seconds=0.0,
                multiplier=2.0,
                max_delay_seconds=0.0,
                jitter_seconds=0.0,
            ),
        )

    except Exception as exc:
        print(
            "database_retry_exhaustion="
            f"{type(exc).__name__}"
        )

    else:
        raise RuntimeError(
            "Exhausted database error "
            "was not propagated"
        )

    if exhausted_attempts != 2:
        raise RuntimeError(
            "Unexpected exhausted database "
            f"attempt count: {exhausted_attempts}"
        )

    print(
        "transient_database_attempt_count="
        f"{transient_attempts}"
    )
    print(
        "permanent_database_attempt_count="
        f"{permanent_attempts}"
    )
    print(
        "exhausted_database_attempt_count="
        f"{exhausted_attempts}"
    )
    print(
        "database_retry_smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
