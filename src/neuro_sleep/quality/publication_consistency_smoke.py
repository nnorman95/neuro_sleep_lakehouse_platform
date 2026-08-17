from __future__ import annotations

from datetime import datetime
from uuid import UUID

import pyarrow as pa

from neuro_sleep.silver.parquet_schemas import (
    CHANNELS_SCHEMA,
    RECORDINGS_SCHEMA,
    SLEEP_STAGE_EPOCHS_SCHEMA,
    SLEEP_STAGE_INTERVALS_SCHEMA,
)
from neuro_sleep.staging.recording_loader import (
    RecordingPublication,
    _validate_loaded_publication,
)


RECORDING_ID = UUID(
    "019f0000-0000-7000-8000-000000000001"
)
OTHER_RECORDING_ID = UUID(
    "019f0000-0000-7000-8000-000000000099"
)
CHANNEL_ID = (
    "019f0000-0000-7000-8000-000000000002"
)
INTERVAL_ID = (
    "019f0000-0000-7000-8000-000000000003"
)
EPOCH_ID = (
    "019f0000-0000-7000-8000-000000000004"
)


def _publication() -> RecordingPublication:
    return RecordingPublication(
        silver_bucket="silver",
        output_prefix=(
            "sleep-edf/phase12-fixtures/"
            "publication-consistency"
        ),
        source_system="physionet_sleep_edf",
        dataset_version="1.0.0",
        collection="sleep-cassette",
        recording_key="SC4001E0-PSG",
        recording_id=RECORDING_ID,
        source_pair_id="1" * 64,
        input_fingerprint="2" * 64,
        config_id="3" * 64,
        schema_version="1.0.0",
        transform_version="1.0.0",
        psg_file_id=UUID(
            "019f0000-0000-7000-8000-000000000010"
        ),
        hypnogram_file_id=UUID(
            "019f0000-0000-7000-8000-000000000011"
        ),
        psg_bucket="bronze",
        psg_object_key="fixtures/SC4001E0-PSG.edf",
        hypnogram_bucket="bronze",
        hypnogram_object_key=(
            "fixtures/SC4001EC-Hypnogram.edf"
        ),
        psg_checksum_sha256="4" * 64,
        hypnogram_checksum_sha256="5" * 64,
        data_objects=(),
    )


def _baseline_tables() -> tuple[
    pa.Table,
    pa.Table,
    pa.Table,
    pa.Table,
]:
    recording_id = str(RECORDING_ID)

    recordings = pa.Table.from_pylist(
        [
            {
                "recording_id": recording_id,
                "source_system": (
                    "physionet_sleep_edf"
                ),
                "psg_bucket": "bronze",
                "psg_object_key": (
                    "fixtures/SC4001E0-PSG.edf"
                ),
                "hypnogram_bucket": "bronze",
                "hypnogram_object_key": (
                    "fixtures/"
                    "SC4001EC-Hypnogram.edf"
                ),
                "recording_start": datetime(
                    2026,
                    1,
                    1,
                    0,
                    0,
                    0,
                ),
                "duration_seconds": 30.0,
                "channel_count": 1,
                "annotation_count": 1,
                "in_range_epoch_count": 1,
                "out_of_range_epoch_count": 0,
                "trailing_overhang_seconds": 0.0,
            }
        ],
        schema=RECORDINGS_SCHEMA,
    )

    channels = pa.Table.from_pylist(
        [
            {
                "channel_id": CHANNEL_ID,
                "recording_id": recording_id,
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
        ],
        schema=CHANNELS_SCHEMA,
    )

    intervals = pa.Table.from_pylist(
        [
            {
                "interval_id": INTERVAL_ID,
                "recording_id": recording_id,
                "source_annotation_index": 0,
                "onset_seconds": 0.0,
                "duration_seconds": 30.0,
                "end_seconds": 30.0,
                "source_label": (
                    "Sleep stage 2"
                ),
                "normalized_stage": "N2",
                "overlap_status": "in_range",
            }
        ],
        schema=SLEEP_STAGE_INTERVALS_SCHEMA,
    )

    epochs = pa.Table.from_pylist(
        [
            {
                "epoch_id": EPOCH_ID,
                "recording_id": recording_id,
                "source_interval_id": INTERVAL_ID,
                "source_annotation_index": 0,
                "epoch_number": 0,
                "start_seconds": 0.0,
                "duration_seconds": 30.0,
                "end_seconds": 30.0,
                "source_label": (
                    "Sleep stage 2"
                ),
                "normalized_stage": "N2",
            }
        ],
        schema=SLEEP_STAGE_EPOCHS_SCHEMA,
    )

    return (
        recordings,
        channels,
        intervals,
        epochs,
    )


def _table_with_rows(
    table: pa.Table,
    rows: list[dict[str, object]],
) -> pa.Table:
    return pa.Table.from_pylist(
        rows,
        schema=table.schema,
    )


def _validate(
    *,
    recordings: pa.Table,
    channels: pa.Table,
    intervals: pa.Table,
    epochs: pa.Table,
) -> None:
    _validate_loaded_publication(
        publication=_publication(),
        recordings_table=recordings,
        channels_table=channels,
        intervals_table=intervals,
        epochs_table=epochs,
    )


def _expect_failure(
    *,
    fixture_name: str,
    expected_message: str,
    recordings: pa.Table,
    channels: pa.Table,
    intervals: pa.Table,
    epochs: pa.Table,
) -> None:
    try:
        _validate(
            recordings=recordings,
            channels=channels,
            intervals=intervals,
            epochs=epochs,
        )
    except RuntimeError as error:
        if str(error) != expected_message:
            raise RuntimeError(
                f"{fixture_name} failed for an "
                "unexpected reason: "
                f"{error}"
            ) from error

        print(
            "silver_publication_consistency_"
            f"{fixture_name}_blocked=true"
        )
        return

    raise RuntimeError(
        "Publication-consistency fixture "
        f"was accepted: {fixture_name}"
    )


def run_smoke_test() -> None:
    (
        recordings,
        channels,
        intervals,
        epochs,
    ) = _baseline_tables()

    _validate(
        recordings=recordings,
        channels=channels,
        intervals=intervals,
        epochs=epochs,
    )
    print(
        "silver_publication_consistency_"
        "valid_baseline=true"
    )

    wrong_recording_rows = channels.to_pylist()
    wrong_recording_rows[0]["recording_id"] = (
        str(OTHER_RECORDING_ID)
    )
    _expect_failure(
        fixture_name="foreign_recording_id",
        expected_message=(
            "channels contains rows for a "
            "different recording_id"
        ),
        recordings=recordings,
        channels=_table_with_rows(
            channels,
            wrong_recording_rows,
        ),
        intervals=intervals,
        epochs=epochs,
    )

    duplicate_channel_rows = channels.to_pylist()
    duplicate_channel_rows.append(
        dict(duplicate_channel_rows[0])
    )
    _expect_failure(
        fixture_name="duplicate_channel_id",
        expected_message=(
            "Duplicate channel_id values in "
            "Silver channels Parquet"
        ),
        recordings=recordings,
        channels=_table_with_rows(
            channels,
            duplicate_channel_rows,
        ),
        intervals=intervals,
        epochs=epochs,
    )

    orphan_epoch_rows = epochs.to_pylist()
    orphan_epoch_rows[0][
        "source_interval_id"
    ] = (
        "019f0000-0000-7000-8000-000000000098"
    )
    _expect_failure(
        fixture_name="orphan_interval_reference",
        expected_message=(
            "Silver epochs reference intervals "
            "absent from the same publication"
        ),
        recordings=recordings,
        channels=channels,
        intervals=intervals,
        epochs=_table_with_rows(
            epochs,
            orphan_epoch_rows,
        ),
    )

    wrong_count_rows = recordings.to_pylist()
    wrong_count_rows[0]["channel_count"] = 2
    _expect_failure(
        fixture_name="declared_channel_count",
        expected_message=(
            "Channel row count does not match "
            "recording.channel_count"
        ),
        recordings=_table_with_rows(
            recordings,
            wrong_count_rows,
        ),
        channels=channels,
        intervals=intervals,
        epochs=epochs,
    )

    print(
        "phase12_publication_consistency_"
        "smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
