from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from neuro_sleep.silver.parquet_schemas import CHANNELS_SCHEMA
from neuro_sleep.silver.silver_object_writer import calculate_file_sha256
from neuro_sleep.staging.recording_loader import (
    RecordingDataObject,
    _download_and_read_metadata_object,
)


VALID_CHANNEL_ROW = {
    "channel_id": "019f0000-0000-7000-8000-000000000002",
    "recording_id": "019f0000-0000-7000-8000-000000000001",
    "position": 1,
    "source_label": "EEG Fpz-Cz",
    "normalized_name": "eeg_fpz_cz",
    "sampling_frequency_hz": 100.0,
    "physical_dimension": "uV",
    "physical_min": -192.0,
    "physical_max": 192.0,
    "digital_min": -2048,
    "digital_max": 2047,
    "samples_per_data_record": 3000,
    "prefiltering": None,
}


class LocalDownloadClient:
    def __init__(self, source_path: Path) -> None:
        self.source_path = source_path

    def download_file(
        self,
        *,
        Bucket: str,
        Key: str,
        Filename: str,
    ) -> None:
        del Bucket, Key
        copyfile(self.source_path, Filename)


def _write_valid_fixture(path: Path) -> RecordingDataObject:
    table = pa.Table.from_pylist(
        [VALID_CHANNEL_ROW],
        schema=CHANNELS_SCHEMA,
    )
    pq.write_table(table, path, compression="zstd")

    return RecordingDataObject(
        dataset_name="channels",
        bucket="silver",
        object_key="phase12-fixtures/manifest_integrity.parquet",
        row_count=table.num_rows,
        file_size_bytes=path.stat().st_size,
        checksum_sha256=calculate_file_sha256(path),
    )


def _load(
    *,
    source_path: Path,
    destination: Path,
    data_object: RecordingDataObject,
) -> pa.Table:
    return _download_and_read_metadata_object(
        data_object=data_object,
        destination=destination,
        client=LocalDownloadClient(source_path),
    )


def _expect_failure(
    *,
    fixture_name: str,
    source_path: Path,
    destination: Path,
    data_object: RecordingDataObject,
    expected_message: str,
    prefix_match: bool = False,
) -> None:
    try:
        _load(
            source_path=source_path,
            destination=destination,
            data_object=data_object,
        )
    except RuntimeError as error:
        actual = str(error)
        matched = (
            actual.startswith(expected_message)
            if prefix_match
            else actual == expected_message
        )
        if not matched:
            raise RuntimeError(
                f"{fixture_name} failed for an unexpected reason: {actual}"
            ) from error

        print(f"silver_manifest_integrity_{fixture_name}_blocked=true")
        return

    raise RuntimeError(
        f"Manifest-integrity fixture was accepted: {fixture_name}"
    )


def run_smoke_test() -> None:
    with TemporaryDirectory(
        prefix="neurosleep_phase12_manifest_integrity_"
    ) as temporary_directory:
        root = Path(temporary_directory)
        source_path = root / "manifest_integrity.parquet"
        destination = root / "manifest_integrity.downloaded.parquet"

        valid_object = _write_valid_fixture(source_path)

        restored = _load(
            source_path=source_path,
            destination=destination,
            data_object=valid_object,
        )
        if restored.num_rows != 1:
            raise RuntimeError(
                "Valid manifest baseline did not preserve row count"
            )

        print("silver_manifest_integrity_valid_baseline=true")

        _expect_failure(
            fixture_name="file_size_mismatch",
            source_path=source_path,
            destination=destination,
            data_object=replace(
                valid_object,
                file_size_bytes=valid_object.file_size_bytes + 1,
            ),
            expected_message=(
                "Downloaded Silver metadata file size mismatch: "
            ),
            prefix_match=True,
        )

        bad_checksum = (
            "0" * 64
            if valid_object.checksum_sha256 != "0" * 64
            else "1" * 64
        )
        _expect_failure(
            fixture_name="checksum_mismatch",
            source_path=source_path,
            destination=destination,
            data_object=replace(
                valid_object,
                checksum_sha256=bad_checksum,
            ),
            expected_message=(
                "Downloaded Silver metadata file checksum mismatch: "
                "phase12-fixtures/manifest_integrity.parquet"
            ),
        )

        _expect_failure(
            fixture_name="row_count_mismatch",
            source_path=source_path,
            destination=destination,
            data_object=replace(
                valid_object,
                row_count=valid_object.row_count + 1,
            ),
            expected_message=(
                "Silver metadata Parquet row count does not match "
                "manifest: channels"
            ),
        )

    print("phase12_manifest_integrity_smoke_status=success")


if __name__ == "__main__":
    run_smoke_test()
