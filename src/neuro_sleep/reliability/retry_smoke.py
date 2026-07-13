from neuro_sleep.reliability.errors import (
    ChecksumMismatchError,
    SourceNetworkError,
)
from neuro_sleep.reliability.retry import (
    RetryEvent,
    RetryPolicy,
    run_with_retry,
)


def print_retry_event(
    event: RetryEvent,
) -> None:
    print(
        "retry_event="
        f"failed_attempt:{event.failed_attempt},"
        f"next_attempt:{event.next_attempt},"
        f"delay_seconds:{event.delay_seconds},"
        f"error_type:{event.error_type}"
    )


def run_smoke_test() -> None:
    transient_attempts = 0
    recorded_delays: list[float] = []

    def transient_operation() -> str:
        nonlocal transient_attempts

        transient_attempts += 1

        if transient_attempts < 3:
            raise SourceNetworkError(
                "Temporary source timeout"
            )

        return "downloaded"

    transient_result = run_with_retry(
        operation=transient_operation,
        policy=RetryPolicy(
            max_attempts=3,
            initial_delay_seconds=1.0,
            multiplier=2.0,
            max_delay_seconds=10.0,
            jitter_seconds=0.0,
        ),
        retry_for=(SourceNetworkError,),
        on_retry=print_retry_event,
        sleep_function=recorded_delays.append,
        random_uniform=lambda start, end: 0.0,
    )

    if transient_result != "downloaded":
        raise RuntimeError(
            "Transient operation returned "
            "an unexpected result"
        )

    if transient_attempts != 3:
        raise RuntimeError(
            "Unexpected transient attempt count: "
            f"{transient_attempts}"
        )

    if recorded_delays != [1.0, 2.0]:
        raise RuntimeError(
            "Unexpected retry delays: "
            f"{recorded_delays}"
        )

    permanent_attempts = 0

    def permanent_operation() -> None:
        nonlocal permanent_attempts

        permanent_attempts += 1

        raise ChecksumMismatchError(
            "Official checksum does not match"
        )

    try:
        run_with_retry(
            operation=permanent_operation,
            policy=RetryPolicy(
                max_attempts=5,
            ),
            retry_for=(SourceNetworkError,),
            sleep_function=recorded_delays.append,
        )

    except ChecksumMismatchError as exc:
        print(
            "permanent_error_not_retried="
            f"{type(exc).__name__}"
        )

    else:
        raise RuntimeError(
            "Permanent error was not propagated"
        )

    if permanent_attempts != 1:
        raise RuntimeError(
            "Permanent error was incorrectly retried"
        )

    exhausted_attempts = 0

    def exhausted_operation() -> None:
        nonlocal exhausted_attempts

        exhausted_attempts += 1

        raise SourceNetworkError(
            "Source remains unavailable"
        )

    try:
        run_with_retry(
            operation=exhausted_operation,
            policy=RetryPolicy(
                max_attempts=2,
                initial_delay_seconds=0.0,
                jitter_seconds=0.0,
            ),
            retry_for=(SourceNetworkError,),
            sleep_function=lambda delay: None,
            random_uniform=lambda start, end: 0.0,
        )

    except SourceNetworkError:
        print(
            "retry_exhaustion_propagated=true"
        )

    else:
        raise RuntimeError(
            "Exhausted error was not propagated"
        )

    if exhausted_attempts != 2:
        raise RuntimeError(
            "Unexpected exhausted attempt count: "
            f"{exhausted_attempts}"
        )

    print(
        f"transient_attempt_count={transient_attempts}"
    )
    print(
        f"recorded_delays={recorded_delays[:2]}"
    )
    print(
        f"permanent_attempt_count={permanent_attempts}"
    )
    print(
        f"exhausted_attempt_count={exhausted_attempts}"
    )
    print("retry_smoke_status=success")


if __name__ == "__main__":
    run_smoke_test()
