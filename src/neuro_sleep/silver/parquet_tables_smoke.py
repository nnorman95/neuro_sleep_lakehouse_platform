from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from neuro_sleep.silver.bronze_edf_reader import (
    open_bronze_edf_pair,
)
from neuro_sleep.silver.parquet_schemas import (
    CHANNELS_SCHEMA,
    RECORDINGS_SCHEMA,
    SIGNALS_SCHEMA,
    SLEEP_STAGE_EPOCHS_SCHEMA,
    SLEEP_STAGE_INTERVALS_SCHEMA,
)
from neuro_sleep.silver.parquet_tables import (
    channels_to_table,
    epochs_to_table,
    intervals_to_table,
    recording_to_table,
    signal_chunk_to_table,
    write_silver_parquet,
)
from neuro_sleep.silver.recording_builder import (
    build_silver_recording,
)
from neuro_sleep.silver.signal_extractor import (
    get_signal_for_channel,
    iter_channel_signal_chunks,
)


BUCKET = "bronze"

PSG_OBJECT_KEY = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/"
    "SC4001E0-PSG.edf"
)

HYPNOGRAM_OBJECT_KEY = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/"
    "SC4001EC-Hypnogram.edf"
)


def assert_table(
    table,
    expected_schema,
    expected_rows: int,
    dataset_name: str,
) -> None:
    if not table.schema.equals(
        expected_schema,
        check_metadata=True,
    ):
        raise RuntimeError(
            "Unexpected Arrow schema: "
            f"{dataset_name}"
        )

    if table.num_rows != expected_rows:
        raise RuntimeError(
            "Unexpected Arrow row count: "
            f"{dataset_name}; "
            f"expected={expected_rows}, "
            f"actual={table.num_rows}"
        )


def run_smoke_test() -> None:
    bundle = build_silver_recording(
        psg_bucket=BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    )

    recordings_table = (
        recording_to_table(
            bundle.recording
        )
    )

    channels_table = channels_to_table(
        bundle.channels
    )

    intervals_table = (
        intervals_to_table(
            bundle.intervals
        )
    )

    epochs_table = epochs_to_table(
        bundle.epochs
    )

    with open_bronze_edf_pair(
        psg_bucket=BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    ) as pair:
        eeg_channel = (
            bundle.channels[0]
        )

        eeg_signal = (
            get_signal_for_channel(
                psg_document=(
                    pair.psg.document
                ),
                channel=eeg_channel,
            )
        )

        signal_chunk = next(
            iter_channel_signal_chunks(
                recording_id=(
                    bundle.recording_id
                ),
                channel=eeg_channel,
                signal=eeg_signal,
                recording_duration_seconds=(
                    bundle.recording
                    .duration_seconds
                ),
                chunk_duration_seconds=30.0,
                start_seconds=0.0,
                stop_seconds=30.0,
            )
        )

        signals_table = (
            signal_chunk_to_table(
                signal_chunk
            )
        )

    assert_table(
        recordings_table,
        RECORDINGS_SCHEMA,
        1,
        "recordings",
    )

    assert_table(
        channels_table,
        CHANNELS_SCHEMA,
        7,
        "channels",
    )

    assert_table(
        intervals_table,
        SLEEP_STAGE_INTERVALS_SCHEMA,
        154,
        "sleep_stage_intervals",
    )

    assert_table(
        epochs_table,
        SLEEP_STAGE_EPOCHS_SCHEMA,
        2650,
        "sleep_stage_epochs",
    )

    assert_table(
        signals_table,
        SIGNALS_SCHEMA,
        3000,
        "signals",
    )

    first_signal = (
        signals_table
        .slice(0, 1)
        .to_pylist()[0]
    )

    last_signal = (
        signals_table
        .slice(
            signals_table.num_rows - 1,
            1,
        )
        .to_pylist()[0]
    )

    if (
        first_signal["sample_index"] != 0
        or first_signal[
            "elapsed_seconds"
        ]
        != 0.0
        or first_signal[
            "epoch_number"
        ]
        != 0
    ):
        raise RuntimeError(
            "First signal row is incorrect"
        )

    if (
        last_signal["sample_index"]
        != 2999
        or last_signal[
            "epoch_number"
        ]
        != 0
    ):
        raise RuntimeError(
            "Last signal row is incorrect"
        )

    with TemporaryDirectory(
        prefix=(
            "neuro_sleep_parquet_tables_"
        )
    ) as temporary_directory:
        root = Path(
            temporary_directory
        )

        datasets = {
            "recordings": (
                recordings_table
            ),
            "channels": channels_table,
            "sleep_stage_intervals": (
                intervals_table
            ),
            "sleep_stage_epochs": (
                epochs_table
            ),
            "signals": signals_table,
        }

        for (
            dataset_name,
            table,
        ) in datasets.items():
            output_path = (
                root
                / f"{dataset_name}.parquet"
            )

            write_silver_parquet(
                table=table,
                output_path=output_path,
            )

            restored = pq.read_table(
                output_path
            )

            if restored.num_rows != (
                table.num_rows
            ):
                raise RuntimeError(
                    "Parquet row count "
                    f"mismatch: {dataset_name}"
                )

            if not restored.schema.equals(
                table.schema,
                check_metadata=True,
            ):
                raise RuntimeError(
                    "Parquet schema mismatch: "
                    f"{dataset_name}"
                )

    print("recordings_arrow_rows=1")
    print("channels_arrow_rows=7")
    print(
        "sleep_stage_intervals_arrow_rows="
        "154"
    )
    print(
        "sleep_stage_epochs_arrow_rows="
        "2650"
    )
    print("signals_arrow_rows=3000")
    print(
        "signal_sample_indexes_valid=true"
    )
    print(
        "signal_epoch_numbers_valid=true"
    )
    print(
        "parquet_atomic_write=true"
    )
    print(
        "parquet_schema_round_trip=true"
    )
    print(
        "silver_parquet_tables_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
