from collections.abc import Callable
from typing import TypeVar

from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    ConnectionClosedError,
    EndpointConnectionError,
    HTTPClientError,
    ReadTimeoutError,
)

from neuro_sleep.observability.structured_logging import (
    emit_event,
)
from neuro_sleep.reliability.errors import (
    ObjectStorageTransientError,
)
from neuro_sleep.reliability.retry import (
    RetryEvent,
    RetryPolicy,
    run_with_retry,
)


ResultType = TypeVar("ResultType")


DEFAULT_OBJECT_STORAGE_RETRY_POLICY = RetryPolicy(
    max_attempts=4,
    initial_delay_seconds=2.0,
    multiplier=2.0,
    max_delay_seconds=30.0,
    jitter_seconds=0.5,
)


RETRYABLE_HTTP_STATUS_CODES = {
    408,
    425,
    429,
    500,
    502,
    503,
    504,
}


RETRYABLE_CLIENT_ERROR_CODES = {
    "InternalError",
    "InternalFailure",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
}


TRANSIENT_BOTOCORE_EXCEPTIONS = (
    ConnectTimeoutError,
    ConnectionClosedError,
    EndpointConnectionError,
    HTTPClientError,
    ReadTimeoutError,
)


def get_client_error_details(
    error: ClientError,
) -> tuple[str, int | None]:
    error_payload = error.response.get(
        "Error",
        {},
    )

    response_metadata = error.response.get(
        "ResponseMetadata",
        {},
    )

    error_code = str(
        error_payload.get("Code", "")
    )

    status_code = response_metadata.get(
        "HTTPStatusCode"
    )

    return error_code, status_code


def client_error_is_retryable(
    error: ClientError,
) -> bool:
    error_code, status_code = (
        get_client_error_details(error)
    )

    if error_code in RETRYABLE_CLIENT_ERROR_CODES:
        return True

    if (
        status_code in RETRYABLE_HTTP_STATUS_CODES
        or (
            status_code is not None
            and 500 <= status_code <= 599
        )
    ):
        return True

    return False


def exception_is_retryable(
    error: BaseException,
) -> bool:
    if isinstance(
        error,
        TRANSIENT_BOTOCORE_EXCEPTIONS,
    ):
        return True

    if isinstance(error, ClientError):
        return client_error_is_retryable(error)

    if isinstance(error, S3UploadFailedError):
        cause = error.__cause__

        if cause is not None:
            return exception_is_retryable(cause)

    return False


def print_object_storage_retry(
    operation_name: str,
    event: RetryEvent,
) -> None:
    emit_event(
        event="retry_scheduled",
        level="WARNING",
        component="object_storage",
        operation=operation_name,
        failed_attempt=event.failed_attempt,
        next_attempt=event.next_attempt,
        delay_seconds=round(
            event.delay_seconds,
            2,
        ),
        error_type=event.error_type,
    )



def run_object_storage_operation(
    operation: Callable[[], ResultType],
    operation_name: str,
    retry_policy: RetryPolicy = (
        DEFAULT_OBJECT_STORAGE_RETRY_POLICY
    ),
) -> ResultType:
    def classified_operation() -> ResultType:
        try:
            return operation()

        except Exception as error:
            if exception_is_retryable(error):
                raise ObjectStorageTransientError(
                    "Temporary object storage failure: "
                    f"operation={operation_name}, "
                    f"error={error}"
                ) from error

            raise

    return run_with_retry(
        operation=classified_operation,
        policy=retry_policy,
        retry_for=(
            ObjectStorageTransientError,
        ),
        on_retry=lambda event: (
            print_object_storage_retry(
                operation_name=operation_name,
                event=event,
            )
        ),
    )
