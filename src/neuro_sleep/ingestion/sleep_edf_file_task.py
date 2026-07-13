import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import UUID

from botocore.client import BaseClient
from requests import Session

from neuro_sleep.config import Settings
from neuro_sleep.ingestion.bronze_file_writer import (
    write_local_file_to_bronze_and_register,
)
from neuro_sleep.ingestion.sleep_edf_http_downloader import (
    download_sleep_edf_source_file,
)
from neuro_sleep.ingestion.sleep_edf_object_state import (
    existing_upload_is_valid,
)
from neuro_sleep.ingestion.sleep_edf_object_recovery import (
    recover_existing_verified_object,
)
from neuro_sleep.ingestion.sleep_edf_remote_manifest import (
    SleepEdfRemoteManifest,
)
from neuro_sleep.observability.download_progress import (
    DownloadProgressReporter,
)
from neuro_sleep.ops.file_attempt import (
    FileAttemptResolution,
)
from neuro_sleep.sources.sleep_edf import (
    SOURCE_SYSTEM,
    SleepEdfControlArtifact,
)
from neuro_sleep.sources.sleep_edf_manifest import (
    SleepEdfSourceFile,
)


TaskStatus = Literal["uploaded", "skipped"]
RunId = UUID | str


@dataclass(frozen=True)
class SleepEdfFileTaskResult:
    status: TaskStatus
    resolution: FileAttemptResolution
    object_key: str
    file_size_bytes: int
    checksum_sha256: str | None = None

    @property
    def uploaded(self) -> bool:
        return self.status == "uploaded"

    @property
    def skipped(self) -> bool:
        return self.status == "skipped"


def calculate_bytes_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_control_artifact_to_local_file(
    artifact: SleepEdfControlArtifact,
    checksum_text: str,
    destination_root: Path,
) -> tuple[Path, str]:
    destination_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination_path = (
        destination_root
        / artifact.file_name
    )

    data = checksum_text.encode("utf-8")

    destination_path.write_bytes(data)

    checksum_sha256 = calculate_bytes_sha256(
        data
    )

    return destination_path, checksum_sha256


def run_source_file_task(
    source_file: SleepEdfSourceFile,
    destination_root: Path,
    settings: Settings,
    download_session: Session,
    storage_client: BaseClient,
    run_id: RunId,
) -> SleepEdfFileTaskResult:
    if existing_upload_is_valid(
        bucket=source_file.bucket,
        object_key=source_file.object_key,
        expected_checksum_sha256=(
            source_file.checksum_sha256
        ),
        client=storage_client,
    ):

        return SleepEdfFileTaskResult(
            status="skipped",
            resolution="existing_valid",
            object_key=source_file.object_key,
            file_size_bytes=0,
        )

    recovery_result = (
        recover_existing_verified_object(
            source_system=SOURCE_SYSTEM,
            source_url=source_file.source_url,
            bucket=source_file.bucket,
            object_key=source_file.object_key,
            file_name=source_file.file_name,
            file_type=source_file.file_type,
            expected_checksum_sha256=(
                source_file.checksum_sha256
            ),
            ingestion_run_id=run_id,
            client=storage_client,
        )
    )

    if recovery_result is not None:


        return SleepEdfFileTaskResult(
            status="skipped",
            resolution="recovered_existing",
            object_key=source_file.object_key,
            file_size_bytes=0,
        )

    download_result = (
        download_sleep_edf_source_file(
            source_file=source_file,
            destination_root=destination_root,
            settings=settings,
            session=download_session,
            progress_reporter=(
                DownloadProgressReporter(
                    relative_path=(
                        source_file.relative_path
                    ),
                )
            ),
        )
    )

    try:
        write_result = (
            write_local_file_to_bronze_and_register(
                source_system=SOURCE_SYSTEM,
                source_url=source_file.source_url,
                bucket=source_file.bucket,
                object_key=source_file.object_key,
                local_file_path=(
                    download_result.destination_path
                ),
                expected_checksum_sha256=(
                    source_file.checksum_sha256
                ),
                ingestion_run_id=run_id,
                file_name=source_file.file_name,
                file_type=source_file.file_type,
                client=storage_client,
            )
        )

    finally:
        download_result.destination_path.unlink(
            missing_ok=True
        )



    return SleepEdfFileTaskResult(
        status="uploaded",
        resolution="downloaded_and_uploaded",
        object_key=write_result.object_key,
        file_size_bytes=(
            write_result.file_size_bytes
        ),
        checksum_sha256=(
            write_result.checksum_sha256
        ),
    )


def run_control_artifact_task(
    artifact: SleepEdfControlArtifact,
    manifest: SleepEdfRemoteManifest,
    destination_root: Path,
    storage_client: BaseClient,
    run_id: RunId,
) -> SleepEdfFileTaskResult:
    local_path, checksum_sha256 = (
        write_control_artifact_to_local_file(
            artifact=artifact,
            checksum_text=manifest.checksum_text,
            destination_root=destination_root,
        )
    )

    try:
        if existing_upload_is_valid(
            bucket=artifact.bucket,
            object_key=artifact.object_key,
            expected_checksum_sha256=(
                checksum_sha256
            ),
            client=storage_client,
        ):

            return SleepEdfFileTaskResult(
                status="skipped",
                resolution="existing_valid",
                object_key=artifact.object_key,
                file_size_bytes=0,
            )

        recovery_result = (
            recover_existing_verified_object(
                source_system=SOURCE_SYSTEM,
                source_url=artifact.source_url,
                bucket=artifact.bucket,
                object_key=artifact.object_key,
                file_name=artifact.file_name,
                file_type=artifact.file_type,
                expected_checksum_sha256=(
                    checksum_sha256
                ),
                ingestion_run_id=run_id,
                client=storage_client,
            )
        )

        if recovery_result is not None:


            return SleepEdfFileTaskResult(
                status="skipped",
                resolution="recovered_existing",
                object_key=artifact.object_key,
                file_size_bytes=0,
            )

        write_result = (
            write_local_file_to_bronze_and_register(
                source_system=SOURCE_SYSTEM,
                source_url=artifact.source_url,
                bucket=artifact.bucket,
                object_key=artifact.object_key,
                local_file_path=local_path,
                expected_checksum_sha256=(
                    checksum_sha256
                ),
                ingestion_run_id=run_id,
                file_name=artifact.file_name,
                file_type=artifact.file_type,
                content_type="text/plain",
                client=storage_client,
            )
        )

    finally:
        local_path.unlink(
            missing_ok=True
        )



    return SleepEdfFileTaskResult(
        status="uploaded",
        resolution="downloaded_and_uploaded",
        object_key=write_result.object_key,
        file_size_bytes=(
            write_result.file_size_bytes
        ),
        checksum_sha256=(
            write_result.checksum_sha256
        ),
    )
