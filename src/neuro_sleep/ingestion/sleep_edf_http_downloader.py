import hashlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import requests
from requests import Response, Session
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError as RequestsConnectionError,
    ContentDecodingError,
    InvalidSchema,
    InvalidURL,
    MissingSchema,
    RequestException,
    Timeout,
    TooManyRedirects,
)

from neuro_sleep.config import Settings, get_settings
from neuro_sleep.observability.download_progress import (
    DownloadProgressReporter,
)
from neuro_sleep.observability.structured_logging import (
    emit_event,
)
from neuro_sleep.reliability.errors import (
    ChecksumMismatchError,
    SourceHttpError,
    SourceNetworkError,
)
from neuro_sleep.reliability.retry import (
    RetryEvent,
    RetryPolicy,
    run_with_retry,
)
from neuro_sleep.sources.sleep_edf import (
    validate_sleep_edf_settings,
)
from neuro_sleep.sources.sleep_edf_manifest import (
    SleepEdfSourceFile,
)


DEFAULT_CHUNK_SIZE_BYTES = 128 * 1024
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_READ_TIMEOUT_SECONDS = 120

DEFAULT_DOWNLOAD_RETRY_POLICY = RetryPolicy(
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


@dataclass(frozen=True)
class SleepEdfDownloadResult:
    source_file: SleepEdfSourceFile
    destination_path: Path
    file_size_bytes: int
    checksum_sha256: str
    reused_existing_file: bool


def build_safe_destination_path(
    destination_root: Path,
    relative_path: str,
) -> Path:
    if not relative_path:
        raise ValueError(
            "relative_path must not be empty"
        )

    if "\\" in relative_path:
        raise ValueError(
            f"Backslashes are not allowed: {relative_path}"
        )

    source_path = PurePosixPath(relative_path)

    if source_path.is_absolute():
        raise ValueError(
            f"Absolute path is not allowed: {relative_path}"
        )

    if ".." in source_path.parts:
        raise ValueError(
            "Parent path traversal is not allowed: "
            f"{relative_path}"
        )

    resolved_root = (
        destination_root.expanduser().resolve()
    )

    destination_path = resolved_root.joinpath(
        *source_path.parts
    ).resolve()

    if not destination_path.is_relative_to(
        resolved_root
    ):
        raise ValueError(
            "Destination path escapes download root: "
            f"{relative_path}"
        )

    return destination_path


def calculate_file_sha256(
    file_path: Path,
    chunk_size_bytes: int = DEFAULT_CHUNK_SIZE_BYTES,
) -> str:
    if chunk_size_bytes <= 0:
        raise ValueError(
            "chunk_size_bytes must be positive"
        )

    checksum = hashlib.sha256()

    with file_path.open("rb") as source_file:
        while True:
            chunk = source_file.read(
                chunk_size_bytes
            )

            if not chunk:
                break

            checksum.update(chunk)

    return checksum.hexdigest()


def get_existing_valid_file_result(
    source_file: SleepEdfSourceFile,
    destination_path: Path,
) -> SleepEdfDownloadResult | None:
    if not destination_path.is_file():
        return None

    checksum_sha256 = calculate_file_sha256(
        destination_path
    )

    if (
        checksum_sha256
        != source_file.checksum_sha256
    ):
        destination_path.unlink(
            missing_ok=True
        )

        return None

    return SleepEdfDownloadResult(
        source_file=source_file,
        destination_path=destination_path,
        file_size_bytes=(
            destination_path.stat().st_size
        ),
        checksum_sha256=checksum_sha256,
        reused_existing_file=True,
    )


def create_download_session(
    settings: Settings,
) -> Session:
    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": (
                f"{settings.project_name}/"
                f"{settings.env}"
            ),
            "Accept": (
                "application/octet-stream,*/*"
            ),
        }
    )

    return session


def validate_http_response(
    response: Response,
    source_url: str,
) -> None:
    status_code = response.status_code

    if 200 <= status_code < 400:
        return

    message = (
        "Source HTTP request failed: "
        f"status={status_code}, "
        f"url={source_url}, "
        f"reason={response.reason}"
    )

    if (
        status_code in RETRYABLE_HTTP_STATUS_CODES
        or 500 <= status_code <= 599
    ):
        raise SourceNetworkError(message)

    raise SourceHttpError(message)


def print_download_retry_event(
    relative_path: str,
    event: RetryEvent,
) -> None:
    emit_event(
        event="retry_scheduled",
        level="WARNING",
        component="source_download",
        resource=relative_path,
        failed_attempt=event.failed_attempt,
        next_attempt=event.next_attempt,
        delay_seconds=round(
            event.delay_seconds,
            2,
        ),
        error_type=event.error_type,
    )



def download_once(
    source_file: SleepEdfSourceFile,
    destination_path: Path,
    partial_path: Path,
    session: Session,
    chunk_size_bytes: int,
    connect_timeout_seconds: int,
    read_timeout_seconds: int,
    progress_reporter: (
        DownloadProgressReporter | None
    ) = None,
) -> SleepEdfDownloadResult:
    partial_path.unlink(
        missing_ok=True
    )

    response: Response | None = None
    checksum = hashlib.sha256()
    file_size_bytes = 0
    expected_size: int | None = None

    try:
        response = session.get(
            source_file.source_url,
            stream=True,
            timeout=(
                connect_timeout_seconds,
                read_timeout_seconds,
            ),
        )

        validate_http_response(
            response=response,
            source_url=source_file.source_url,
        )

        content_length = response.headers.get(
            "Content-Length"
        )

        content_encoding = response.headers.get(
            "Content-Encoding"
        )

        if (
            content_length
            and content_encoding
            in {None, "", "identity"}
        ):
            try:
                expected_size = int(
                    content_length
                )

            except ValueError as exc:
                raise SourceHttpError(
                    "Invalid Content-Length header: "
                    f"{content_length}"
                ) from exc

        if progress_reporter is not None:
            progress_reporter.start(
                total_bytes=expected_size
            )

        with partial_path.open(
            "wb"
        ) as destination_file:
            for chunk in response.iter_content(
                chunk_size=chunk_size_bytes,
            ):
                if not chunk:
                    continue

                destination_file.write(chunk)
                checksum.update(chunk)

                file_size_bytes += len(chunk)

                if progress_reporter is not None:
                    progress_reporter.update(
                        downloaded_bytes=(
                            file_size_bytes
                        ),
                        total_bytes=expected_size,
                    )

        if file_size_bytes == 0:
            raise SourceNetworkError(
                "Source returned an empty file: "
                f"{source_file.relative_path}"
            )

        if (
            expected_size is not None
            and expected_size != file_size_bytes
        ):
            raise SourceNetworkError(
                "Downloaded size mismatch for "
                f"{source_file.relative_path}: "
                f"expected={expected_size}, "
                f"actual={file_size_bytes}"
            )

        checksum_sha256 = checksum.hexdigest()

        if (
            checksum_sha256
            != source_file.checksum_sha256
        ):
            raise ChecksumMismatchError(
                "SHA-256 mismatch for "
                f"{source_file.relative_path}: "
                f"expected={source_file.checksum_sha256}, "
                f"actual={checksum_sha256}"
            )

        partial_path.replace(
            destination_path
        )

        if progress_reporter is not None:
            progress_reporter.complete(
                downloaded_bytes=file_size_bytes,
                total_bytes=expected_size,
            )

        return SleepEdfDownloadResult(
            source_file=source_file,
            destination_path=destination_path,
            file_size_bytes=file_size_bytes,
            checksum_sha256=checksum_sha256,
            reused_existing_file=False,
        )

    except (
        Timeout,
        RequestsConnectionError,
        ChunkedEncodingError,
        ContentDecodingError,
    ) as exc:
        error = SourceNetworkError(
            "Temporary source network failure for "
            f"{source_file.relative_path}: {exc}"
        )

        if progress_reporter is not None:
            progress_reporter.fail(
                error=error,
                downloaded_bytes=file_size_bytes,
                total_bytes=expected_size,
            )

        partial_path.unlink(
            missing_ok=True
        )

        raise error from exc

    except (
        InvalidURL,
        MissingSchema,
        InvalidSchema,
        TooManyRedirects,
    ) as exc:
        error = SourceHttpError(
            "Permanent source URL failure for "
            f"{source_file.relative_path}: {exc}"
        )

        if progress_reporter is not None:
            progress_reporter.fail(
                error=error,
                downloaded_bytes=file_size_bytes,
                total_bytes=expected_size,
            )

        partial_path.unlink(
            missing_ok=True
        )

        raise error from exc

    except RequestException as exc:
        error = SourceNetworkError(
            "Source request failure for "
            f"{source_file.relative_path}: {exc}"
        )

        if progress_reporter is not None:
            progress_reporter.fail(
                error=error,
                downloaded_bytes=file_size_bytes,
                total_bytes=expected_size,
            )

        partial_path.unlink(
            missing_ok=True
        )

        raise error from exc

    except Exception as error:
        if progress_reporter is not None:
            progress_reporter.fail(
                error=error,
                downloaded_bytes=file_size_bytes,
                total_bytes=expected_size,
            )

        partial_path.unlink(
            missing_ok=True
        )

        raise

    finally:
        if response is not None:
            response.close()



def download_sleep_edf_source_file(
    source_file: SleepEdfSourceFile,
    destination_root: Path,
    settings: Settings | None = None,
    session: Session | None = None,
    chunk_size_bytes: int = DEFAULT_CHUNK_SIZE_BYTES,
    connect_timeout_seconds: int = (
        DEFAULT_CONNECT_TIMEOUT_SECONDS
    ),
    read_timeout_seconds: int = (
        DEFAULT_READ_TIMEOUT_SECONDS
    ),
    retry_policy: RetryPolicy = (
        DEFAULT_DOWNLOAD_RETRY_POLICY
    ),
    progress_reporter: (
        DownloadProgressReporter | None
    ) = None,
) -> SleepEdfDownloadResult:
    if settings is None:
        settings = get_settings()

    validate_sleep_edf_settings(settings)

    if chunk_size_bytes <= 0:
        raise ValueError(
            "chunk_size_bytes must be positive"
        )

    destination_path = build_safe_destination_path(
        destination_root=destination_root,
        relative_path=source_file.relative_path,
    )

    existing_result = (
        get_existing_valid_file_result(
            source_file=source_file,
            destination_path=destination_path,
        )
    )

    if existing_result is not None:
        return existing_result

    destination_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    partial_path = destination_path.with_name(
        f"{destination_path.name}.part"
    )

    owns_session = session is None

    if session is None:
        session = create_download_session(
            settings
        )

    try:
        return run_with_retry(
            operation=lambda: download_once(
                source_file=source_file,
                destination_path=destination_path,
                partial_path=partial_path,
                session=session,
                chunk_size_bytes=chunk_size_bytes,
                connect_timeout_seconds=(
                    connect_timeout_seconds
                ),
                read_timeout_seconds=(
                    read_timeout_seconds
                ),
                progress_reporter=(
                    progress_reporter
                ),
            ),
            policy=retry_policy,
            retry_for=(
                SourceNetworkError,
            ),
            on_retry=lambda event: (
                print_download_retry_event(
                    relative_path=(
                        source_file.relative_path
                    ),
                    event=event,
                )
            ),
        )

    finally:
        if owns_session:
            session.close()
