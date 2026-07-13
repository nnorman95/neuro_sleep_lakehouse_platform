from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
)

from neuro_sleep.reliability.retry import (
    RetryPolicy,
)
from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)


ZERO_DELAY_POLICY = RetryPolicy(
    max_attempts=3,
    initial_delay_seconds=0.0,
    multiplier=2.0,
    max_delay_seconds=0.0,
    jitter_seconds=0.0,
)


def create_client_error(
    code: str,
    status_code: int,
    operation_name: str,
) -> ClientError:
    return ClientError(
        error_response={
            "Error": {
                "Code": code,
                "Message": "Smoke test error",
            },
            "ResponseMetadata": {
                "HTTPStatusCode": status_code,
            },
        },
        operation_name=operation_name,
    )


def run_smoke_test() -> None:
    transient_attempts = 0

    def transient_operation() -> str:
        nonlocal transient_attempts

        transient_attempts += 1

        if transient_attempts == 1:
            raise create_client_error(
                code="ServiceUnavailable",
                status_code=503,
                operation_name="HeadObject",
            )

        return "metadata_received"

    transient_result = (
        run_object_storage_operation(
            operation=transient_operation,
            operation_name="head_object:test",
            retry_policy=ZERO_DELAY_POLICY,
        )
    )

    if transient_result != "metadata_received":
        raise RuntimeError(
            "Unexpected transient operation result"
        )

    if transient_attempts != 2:
        raise RuntimeError(
            "MinIO 503 did not produce one retry"
        )

    endpoint_attempts = 0

    def endpoint_operation() -> str:
        nonlocal endpoint_attempts

        endpoint_attempts += 1

        if endpoint_attempts == 1:
            raise EndpointConnectionError(
                endpoint_url=(
                    "http://localhost:9000"
                )
            )

        return "connected"

    endpoint_result = (
        run_object_storage_operation(
            operation=endpoint_operation,
            operation_name="upload_file:test",
            retry_policy=ZERO_DELAY_POLICY,
        )
    )

    if endpoint_result != "connected":
        raise RuntimeError(
            "Unexpected endpoint retry result"
        )

    if endpoint_attempts != 2:
        raise RuntimeError(
            "Endpoint failure was not retried"
        )

    not_found_attempts = 0

    def not_found_operation() -> None:
        nonlocal not_found_attempts

        not_found_attempts += 1

        raise create_client_error(
            code="NoSuchKey",
            status_code=404,
            operation_name="HeadObject",
        )

    try:
        run_object_storage_operation(
            operation=not_found_operation,
            operation_name="head_object:missing",
            retry_policy=ZERO_DELAY_POLICY,
        )

    except ClientError:
        print(
            "object_storage_404_not_retried=true"
        )

    else:
        raise RuntimeError(
            "MinIO 404 was not propagated"
        )

    if not_found_attempts != 1:
        raise RuntimeError(
            "MinIO 404 was incorrectly retried"
        )

    forbidden_attempts = 0

    def forbidden_operation() -> None:
        nonlocal forbidden_attempts

        forbidden_attempts += 1

        raise create_client_error(
            code="AccessDenied",
            status_code=403,
            operation_name="HeadBucket",
        )

    try:
        run_object_storage_operation(
            operation=forbidden_operation,
            operation_name="head_bucket:forbidden",
            retry_policy=ZERO_DELAY_POLICY,
        )

    except ClientError:
        print(
            "object_storage_403_not_retried=true"
        )

    else:
        raise RuntimeError(
            "MinIO 403 was not propagated"
        )

    if forbidden_attempts != 1:
        raise RuntimeError(
            "MinIO 403 was incorrectly retried"
        )

    print(
        "transient_storage_attempt_count="
        f"{transient_attempts}"
    )
    print(
        "endpoint_storage_attempt_count="
        f"{endpoint_attempts}"
    )
    print(
        "not_found_attempt_count="
        f"{not_found_attempts}"
    )
    print(
        "forbidden_attempt_count="
        f"{forbidden_attempts}"
    )
    print(
        "object_storage_retry_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
