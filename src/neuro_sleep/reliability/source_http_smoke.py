from neuro_sleep.reliability.errors import (
    SourceContentError,
    SourceHttpError,
)
from neuro_sleep.reliability.retry import (
    RetryPolicy,
)
from neuro_sleep.reliability.source_http import (
    fetch_text_with_retry,
)


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        text: str = "",
        reason: str = "test",
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.reason = reason
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(
        self,
        responses: list[FakeResponse],
    ) -> None:
        self.responses = list(responses)
        self.call_count = 0

    def get(
        self,
        url: str,
        timeout: tuple[int, int],
    ) -> FakeResponse:
        self.call_count += 1

        if not self.responses:
            raise RuntimeError(
                "No fake response remains"
            )

        return self.responses.pop(0)


def run_smoke_test() -> None:
    retry_policy = RetryPolicy(
        max_attempts=3,
        initial_delay_seconds=0.0,
        multiplier=2.0,
        max_delay_seconds=0.0,
        jitter_seconds=0.0,
    )

    retry_session = FakeSession(
        responses=[
            FakeResponse(
                status_code=503,
                reason="Service Unavailable",
            ),
            FakeResponse(
                status_code=200,
                text="sleep-cassette/test.edf\n",
                reason="OK",
            ),
        ]
    )

    retry_result = fetch_text_with_retry(
        session=retry_session,
        url="https://example.test/RECORDS",
        resource_name="RECORDS",
        retry_policy=retry_policy,
    )

    if retry_result != "sleep-cassette/test.edf\n":
        raise RuntimeError(
            "Unexpected successful response text"
        )

    if retry_session.call_count != 2:
        raise RuntimeError(
            "HTTP 503 did not produce one retry"
        )

    permanent_session = FakeSession(
        responses=[
            FakeResponse(
                status_code=404,
                reason="Not Found",
            ),
        ]
    )

    try:
        fetch_text_with_retry(
            session=permanent_session,
            url="https://example.test/missing.txt",
            resource_name="missing.txt",
            retry_policy=retry_policy,
        )

    except SourceHttpError:
        print("manifest_http_404_not_retried=true")

    else:
        raise RuntimeError(
            "HTTP 404 was not propagated"
        )

    if permanent_session.call_count != 1:
        raise RuntimeError(
            "HTTP 404 was incorrectly retried"
        )

    empty_session = FakeSession(
        responses=[
            FakeResponse(
                status_code=200,
                text="   \n",
                reason="OK",
            ),
        ]
    )

    try:
        fetch_text_with_retry(
            session=empty_session,
            url="https://example.test/empty.txt",
            resource_name="empty.txt",
            retry_policy=retry_policy,
        )

    except SourceContentError:
        print("empty_manifest_not_retried=true")

    else:
        raise RuntimeError(
            "Empty response was not rejected"
        )

    if empty_session.call_count != 1:
        raise RuntimeError(
            "Empty content was incorrectly retried"
        )

    print(
        "retryable_manifest_attempt_count="
        f"{retry_session.call_count}"
    )
    print(
        "permanent_manifest_attempt_count="
        f"{permanent_session.call_count}"
    )
    print(
        "empty_manifest_attempt_count="
        f"{empty_session.call_count}"
    )
    print("source_http_smoke_status=success")


if __name__ == "__main__":
    run_smoke_test()
