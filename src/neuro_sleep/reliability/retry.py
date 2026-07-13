import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar


ResultType = TypeVar("ResultType")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_delay_seconds: float = 2.0
    multiplier: float = 2.0
    max_delay_seconds: float = 30.0
    jitter_seconds: float = 0.5

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(
                "max_attempts must be at least 1"
            )

        if self.initial_delay_seconds < 0:
            raise ValueError(
                "initial_delay_seconds must not "
                "be negative"
            )

        if self.multiplier < 1:
            raise ValueError(
                "multiplier must be at least 1"
            )

        if self.max_delay_seconds < 0:
            raise ValueError(
                "max_delay_seconds must not "
                "be negative"
            )

        if self.jitter_seconds < 0:
            raise ValueError(
                "jitter_seconds must not be negative"
            )


@dataclass(frozen=True)
class RetryEvent:
    failed_attempt: int
    max_attempts: int
    next_attempt: int
    delay_seconds: float
    error_type: str
    error_message: str


def calculate_retry_delay(
    failed_attempt: int,
    policy: RetryPolicy,
    random_uniform: Callable[
        [float, float],
        float,
    ] = random.uniform,
) -> float:
    if failed_attempt < 1:
        raise ValueError(
            "failed_attempt must be at least 1"
        )

    exponential_delay = (
        policy.initial_delay_seconds
        * (
            policy.multiplier
            ** (failed_attempt - 1)
        )
    )

    capped_delay = min(
        exponential_delay,
        policy.max_delay_seconds,
    )

    jitter = random_uniform(
        0.0,
        policy.jitter_seconds,
    )

    return capped_delay + jitter


def run_with_retry(
    operation: Callable[[], ResultType],
    policy: RetryPolicy,
    retry_for: tuple[
        type[Exception],
        ...,
    ],
    on_retry: Callable[
        [RetryEvent],
        None,
    ]
    | None = None,
    sleep_function: Callable[
        [float],
        None,
    ] = time.sleep,
    random_uniform: Callable[
        [float, float],
        float,
    ] = random.uniform,
) -> ResultType:
    if not retry_for:
        raise ValueError(
            "retry_for must contain at least "
            "one exception type"
        )

    attempt = 1

    while True:
        try:
            return operation()

        except retry_for as exc:
            if attempt >= policy.max_attempts:
                raise

            delay_seconds = calculate_retry_delay(
                failed_attempt=attempt,
                policy=policy,
                random_uniform=random_uniform,
            )

            event = RetryEvent(
                failed_attempt=attempt,
                max_attempts=policy.max_attempts,
                next_attempt=attempt + 1,
                delay_seconds=delay_seconds,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

            if on_retry is not None:
                on_retry(event)

            sleep_function(delay_seconds)

            attempt += 1
