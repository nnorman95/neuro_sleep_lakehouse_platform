import hashlib
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from neuro_sleep.ingestion.sleep_edf_http_downloader import (
    download_sleep_edf_source_file,
)
from neuro_sleep.reliability.errors import (
    ChecksumMismatchError,
    SourceHttpError,
)
from neuro_sleep.reliability.retry import (
    RetryPolicy,
)


@dataclass(frozen=True)
class FakeSourceFile:
    relative_path: str
    source_url: str
    checksum_sha256: str


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: bytes = b"",
        reason: str = "test",
    ) -> None:
        self.status_code = status_code
        self.payload = payload
        self.reason = reason
        self.headers = {
            "Content-Length": str(
                len(payload)
            ),
        }
        self.closed = False

    def iter_content(
        self,
        chunk_size: int,
    ):
        for start in range(
            0,
            len(self.payload),
            chunk_size,
        ):
            yield self.payload[
                start:start + chunk_size
            ]

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
        stream: bool,
        timeout: tuple[int, int],
    ) -> FakeResponse:
        self.call_count += 1

        if not self.responses:
            raise RuntimeError(
                "No fake response remains"
            )

        return self.responses.pop(0)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run_smoke_test() -> None:
    retry_policy = RetryPolicy(
        max_attempts=3,
        initial_delay_seconds=0.0,
        multiplier=2.0,
        max_delay_seconds=0.0,
        jitter_seconds=0.0,
    )

    successful_payload = (
        b"Sleep-EDF downloader retry smoke."
    )

    retry_source = FakeSourceFile(
        relative_path="RECORDS",
        source_url="https://example.test/RECORDS",
        checksum_sha256=sha256_bytes(
            successful_payload
        ),
    )

    retry_session = FakeSession(
        responses=[
            FakeResponse(
                status_code=503,
                reason="Service Unavailable",
            ),
            FakeResponse(
                status_code=200,
                payload=successful_payload,
                reason="OK",
            ),
        ]
    )

    with TemporaryDirectory() as temp_dir:
        result = (
            download_sleep_edf_source_file(
                source_file=retry_source,
                destination_root=Path(temp_dir),
                session=retry_session,
                retry_policy=retry_policy,
            )
        )

        if retry_session.call_count != 2:
            raise RuntimeError(
                "HTTP 503 did not produce "
                "exactly one retry"
            )

        if not result.destination_path.is_file():
            raise RuntimeError(
                "Successful retry did not "
                "create the destination file"
            )

    permanent_source = FakeSourceFile(
        relative_path="missing.edf",
        source_url=(
            "https://example.test/missing.edf"
        ),
        checksum_sha256="0" * 64,
    )

    permanent_session = FakeSession(
        responses=[
            FakeResponse(
                status_code=404,
                reason="Not Found",
            ),
        ]
    )

    with TemporaryDirectory() as temp_dir:
        try:
            download_sleep_edf_source_file(
                source_file=permanent_source,
                destination_root=Path(temp_dir),
                session=permanent_session,
                retry_policy=retry_policy,
            )

        except SourceHttpError:
            print(
                "http_404_not_retried=true"
            )

        else:
            raise RuntimeError(
                "HTTP 404 was not propagated"
            )

    if permanent_session.call_count != 1:
        raise RuntimeError(
            "HTTP 404 was incorrectly retried"
        )

    checksum_source = FakeSourceFile(
        relative_path="bad-checksum.edf",
        source_url=(
            "https://example.test/"
            "bad-checksum.edf"
        ),
        checksum_sha256=sha256_bytes(
            b"expected"
        ),
    )

    checksum_session = FakeSession(
        responses=[
            FakeResponse(
                status_code=200,
                payload=b"actual",
                reason="OK",
            ),
        ]
    )

    with TemporaryDirectory() as temp_dir:
        try:
            download_sleep_edf_source_file(
                source_file=checksum_source,
                destination_root=Path(temp_dir),
                session=checksum_session,
                retry_policy=retry_policy,
            )

        except ChecksumMismatchError:
            print(
                "checksum_mismatch_not_retried=true"
            )

        else:
            raise RuntimeError(
                "Checksum mismatch was not "
                "propagated"
            )

    if checksum_session.call_count != 1:
        raise RuntimeError(
            "Checksum mismatch was "
            "incorrectly retried"
        )

    print(
        "retryable_http_attempt_count="
        f"{retry_session.call_count}"
    )
    print(
        "permanent_http_attempt_count="
        f"{permanent_session.call_count}"
    )
    print(
        "checksum_attempt_count="
        f"{checksum_session.call_count}"
    )
    print(
        "http_downloader_retry_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
