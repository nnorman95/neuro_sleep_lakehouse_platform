from requests import Response, Session
from requests.exceptions import (
    ConnectionError as RequestsConnectionError,
    ContentDecodingError,
    InvalidSchema,
    InvalidURL,
    MissingSchema,
    RequestException,
    Timeout,
    TooManyRedirects,
)

from neuro_sleep.observability.structured_logging import (
    emit_event,
)
from neuro_sleep.reliability.errors import (
    SourceContentError,
    SourceHttpError,
    SourceNetworkError,
)
from neuro_sleep.reliability.retry import (
    RetryEvent,
    RetryPolicy,
    run_with_retry,
)


DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_READ_TIMEOUT_SECONDS = 60

DEFAULT_SOURCE_HTTP_RETRY_POLICY = RetryPolicy(
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


def validate_source_response(
    response: Response,
    url: str,
) -> None:
    status_code = response.status_code

    if 200 <= status_code < 400:
        return

    message = (
        "Source HTTP request failed: "
        f"status={status_code}, "
        f"url={url}, "
        f"reason={response.reason}"
    )

    if (
        status_code in RETRYABLE_HTTP_STATUS_CODES
        or 500 <= status_code <= 599
    ):
        raise SourceNetworkError(message)

    raise SourceHttpError(message)


def print_source_http_retry(
    resource_name: str,
    event: RetryEvent,
) -> None:
    emit_event(
        event="retry_scheduled",
        level="WARNING",
        component="source_manifest",
        resource=resource_name,
        failed_attempt=event.failed_attempt,
        next_attempt=event.next_attempt,
        delay_seconds=round(
            event.delay_seconds,
            2,
        ),
        error_type=event.error_type,
    )



def fetch_text_once(
    session: Session,
    url: str,
    connect_timeout_seconds: int,
    read_timeout_seconds: int,
) -> str:
    response: Response | None = None

    try:
        response = session.get(
            url,
            timeout=(
                connect_timeout_seconds,
                read_timeout_seconds,
            ),
        )

        validate_source_response(
            response=response,
            url=url,
        )

        response_text = response.text

        if not response_text.strip():
            raise SourceContentError(
                f"Remote text file is empty: {url}"
            )

        return response_text

    except (
        Timeout,
        RequestsConnectionError,
        ContentDecodingError,
    ) as exc:
        raise SourceNetworkError(
            f"Temporary source network failure: {url}: {exc}"
        ) from exc

    except (
        InvalidURL,
        MissingSchema,
        InvalidSchema,
        TooManyRedirects,
    ) as exc:
        raise SourceHttpError(
            f"Permanent source URL failure: {url}: {exc}"
        ) from exc

    except RequestException as exc:
        raise SourceNetworkError(
            f"Source request failure: {url}: {exc}"
        ) from exc

    finally:
        if response is not None:
            response.close()


def fetch_text_with_retry(
    session: Session,
    url: str,
    resource_name: str | None = None,
    connect_timeout_seconds: int = (
        DEFAULT_CONNECT_TIMEOUT_SECONDS
    ),
    read_timeout_seconds: int = (
        DEFAULT_READ_TIMEOUT_SECONDS
    ),
    retry_policy: RetryPolicy = (
        DEFAULT_SOURCE_HTTP_RETRY_POLICY
    ),
) -> str:
    if resource_name is None:
        resource_name = (
            url.rstrip("/").rsplit("/", 1)[-1]
            or "remote_text"
        )

    return run_with_retry(
        operation=lambda: fetch_text_once(
            session=session,
            url=url,
            connect_timeout_seconds=(
                connect_timeout_seconds
            ),
            read_timeout_seconds=(
                read_timeout_seconds
            ),
        ),
        policy=retry_policy,
        retry_for=(SourceNetworkError,),
        on_retry=lambda event: (
            print_source_http_retry(
                resource_name=resource_name,
                event=event,
            )
        ),
    )
