from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from botocore.client import BaseClient
from edfio import read_edf

from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)
from neuro_sleep.storage.object_storage import (
    get_object_metadata,
    get_object_storage_client,
)


@dataclass(frozen=True)
class LoadedBronzeEdf:
    bucket: str
    object_key: str
    local_path: Path
    file_size_bytes: int
    document: Any


@dataclass(frozen=True)
class LoadedBronzeEdfPair:
    psg: LoadedBronzeEdf
    hypnogram: LoadedBronzeEdf


def validate_bronze_edf_reference(
    bucket: str,
    object_key: str,
) -> None:
    if not bucket.strip():
        raise ValueError(
            "bucket cannot be empty"
        )

    if not object_key.strip():
        raise ValueError(
            "object_key cannot be empty"
        )

    if Path(object_key).suffix.lower() != ".edf":
        raise ValueError(
            "Bronze EDF object key must end "
            "with .edf"
        )


def download_bronze_edf_object(
    bucket: str,
    object_key: str,
    destination_directory: Path,
    client: BaseClient,
) -> Path:
    validate_bronze_edf_reference(
        bucket=bucket,
        object_key=object_key,
    )

    destination_directory = (
        destination_directory
        .expanduser()
        .resolve()
    )

    destination_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    metadata = get_object_metadata(
        bucket=bucket,
        object_key=object_key,
        client=client,
    )

    expected_size = metadata.get(
        "content_length"
    )

    if (
        not isinstance(expected_size, int)
        or expected_size <= 0
    ):
        raise RuntimeError(
            "Bronze EDF object has an "
            "invalid storage size: "
            f"{bucket}/{object_key}"
        )

    destination_path = (
        destination_directory
        / Path(object_key).name
    )

    run_object_storage_operation(
        operation=lambda: client.download_file(
            Bucket=bucket,
            Key=object_key,
            Filename=str(destination_path),
        ),
        operation_name=(
            f"download_file:{bucket}/"
            f"{object_key}"
        ),
    )

    if not destination_path.is_file():
        raise RuntimeError(
            "Downloaded EDF file was not "
            f"created: {destination_path}"
        )

    actual_size = (
        destination_path.stat().st_size
    )

    if actual_size != expected_size:
        destination_path.unlink(
            missing_ok=True
        )

        raise RuntimeError(
            "Downloaded EDF size mismatch: "
            f"expected={expected_size}, "
            f"actual={actual_size}, "
            f"object={bucket}/{object_key}"
        )

    return destination_path


def load_bronze_edf_object(
    bucket: str,
    object_key: str,
    destination_directory: Path,
    client: BaseClient,
) -> LoadedBronzeEdf:
    local_path = download_bronze_edf_object(
        bucket=bucket,
        object_key=object_key,
        destination_directory=(
            destination_directory
        ),
        client=client,
    )

    try:
        document = read_edf(local_path)

    except Exception:
        local_path.unlink(
            missing_ok=True
        )

        raise

    return LoadedBronzeEdf(
        bucket=bucket,
        object_key=object_key,
        local_path=local_path,
        file_size_bytes=(
            local_path.stat().st_size
        ),
        document=document,
    )


@contextmanager
def open_bronze_edf_pair(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
    client: BaseClient | None = None,
) -> Iterator[LoadedBronzeEdfPair]:
    validate_bronze_edf_reference(
        bucket=psg_bucket,
        object_key=psg_object_key,
    )

    validate_bronze_edf_reference(
        bucket=hypnogram_bucket,
        object_key=hypnogram_object_key,
    )

    if (
        psg_bucket == hypnogram_bucket
        and psg_object_key
        == hypnogram_object_key
    ):
        raise ValueError(
            "PSG and Hypnogram references "
            "must be different"
        )

    owns_client = client is None

    if client is None:
        client = get_object_storage_client()

    try:
        with TemporaryDirectory(
            prefix=(
                "neuro_sleep_bronze_edf_"
            )
        ) as temporary_directory:
            root = Path(
                temporary_directory
            )

            psg = load_bronze_edf_object(
                bucket=psg_bucket,
                object_key=psg_object_key,
                destination_directory=(
                    root / "psg"
                ),
                client=client,
            )

            hypnogram = (
                load_bronze_edf_object(
                    bucket=hypnogram_bucket,
                    object_key=(
                        hypnogram_object_key
                    ),
                    destination_directory=(
                        root / "hypnogram"
                    ),
                    client=client,
                )
            )

            yield LoadedBronzeEdfPair(
                psg=psg,
                hypnogram=hypnogram,
            )

    finally:
        if owns_client:
            client.close()
