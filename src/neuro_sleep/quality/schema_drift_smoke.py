from __future__ import annotations

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


def _write_table(
    path: Path,
    table: pa.Table,
) -> RecordingDataObject:
    pq.write_table(table, path, compression="zstd")
    return RecordingDataObject(
        dataset_name="channels",
        bucket="silver",
        object_key=f"phase12-fixtures/{path.name}",
        row_count=table.num_rows,
        file_size_bytes=path.stat().st_size,
        checksum_sha256=calculate_file_sha256(path),
    )


def _load_fixture(
    *,
    source_path: Path,
    data_object: RecordingDataObject,
    destination: Path,
) -> pa.Table:
    return _download_and_read_metadata_object(
        data_object=data_object,
        destination=destination,
        client=LocalDownloadClient(source_path),
    )


def _expect_schema_failure(
    *,
    fixture_name: str,
    table: pa.Table,
    root: Path,
) -> None:
    source_path = root / f"{fixture_name}.parquet"
    destination = root / f"{fixture_name}.downloaded.parquet"
    data_object = _write_table(source_path, table)

    try:
        _load_fixture(
            source_path=source_path,
            data_object=data_object,
            destination=destination,
        )
    except RuntimeError as error:
        expected = (
            "Silver metadata Parquet schema "
            "does not match expected schema: channels"
        )
        if str(error) != expected:
            raise RuntimeError(
                f"{fixture_name} failed for an unexpected reason: {error}"
            ) from error

        print(f"silver_schema_drift_{fixture_name}_blocked=true")
        return

    raise RuntimeError(
        f"Schema-drift fixture was accepted: {fixture_name}"
    )


def run_smoke_test() -> None:
    valid_table = pa.Table.from_pylist(
        [VALID_CHANNEL_ROW],
        schema=CHANNELS_SCHEMA,
    )

    with TemporaryDirectory(
        prefix="neurosleep_phase12_schema_drift_"
    ) as temporary_directory:
        root = Path(temporary_directory)

        valid_path = root / "valid_channels.parquet"
        valid_destination = root / "valid_channels.downloaded.parquet"
        valid_object = _write_table(valid_path, valid_table)
        restored = _load_fixture(
            source_path=valid_path,
            data_object=valid_object,
            destination=valid_destination,
        )

        if not restored.schema.equals(
            CHANNELS_SCHEMA,
            check_metadata=True,
        ):
            raise RuntimeError(
                "Valid baseline schema was not preserved"
            )

        print("silver_schema_drift_valid_baseline=true")

        _expect_schema_failure(
            fixture_name="missing_column",
            table=valid_table.drop(["normalized_name"]),
            root=root,
        )

        _expect_schema_failure(
            fixture_name="extra_column",
            table=valid_table.append_column(
                "unexpected_quality_flag",
                pa.array([True], type=pa.bool_()),
            ),
            root=root,
        )

        position_index = valid_table.schema.get_field_index("position")
        _expect_schema_failure(
            fixture_name="wrong_type",
            table=valid_table.set_column(
                position_index,
                "position",
                pa.array(["1"], type=pa.string()),
            ),
            root=root,
        )

    print(
        "silver_schema_drift_manifest_size_checksum_consistent=true"
    )
    print("phase12_schema_drift_smoke_status=success")


if __name__ == "__main__":
    run_smoke_test()
