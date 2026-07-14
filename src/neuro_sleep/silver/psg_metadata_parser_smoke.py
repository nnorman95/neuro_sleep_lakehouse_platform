from collections.abc import Callable

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.bronze_edf_reader import (
    open_bronze_edf_pair,
)
from neuro_sleep.silver.psg_metadata_parser import (
    normalize_channel_name,
    parse_psg_metadata,
)


BUCKET = "bronze"

PSG_OBJECT_KEY = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/"
    "SC4012E0-PSG.edf"
)

HYPNOGRAM_OBJECT_KEY = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/"
    "SC4012EC-Hypnogram.edf"
)


def expect_value_error(
    operation: Callable[[], object],
    check_name: str,
) -> None:
    try:
        operation()

    except ValueError:
        print(f"{check_name}=true")
        return

    raise RuntimeError(
        f"Expected ValueError: {check_name}"
    )


def run_smoke_test() -> None:
    recording_id = new_uuid7()

    with open_bronze_edf_pair(
        psg_bucket=BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    ) as pair:
        parsed = parse_psg_metadata(
            recording_id=recording_id,
            psg_document=(
                pair.psg.document
            ),
        )

    if parsed.channel_count != 7:
        raise RuntimeError(
            "Unexpected channel count"
        )

    if (
        parsed.duration_seconds
        != 85500.0
    ):
        raise RuntimeError(
            "Unexpected PSG duration"
        )

    if (
        parsed.data_record_count
        != 2850
    ):
        raise RuntimeError(
            "Unexpected data-record count"
        )

    if (
        parsed
        .data_record_duration_seconds
        != 30.0
    ):
        raise RuntimeError(
            "Unexpected data-record "
            "duration"
        )

    expected_channel_names = (
        "eeg_fpz_cz",
        "eeg_pz_oz",
        "eog_horizontal",
        "resp_oro_nasal",
        "emg_submental",
        "temp_rectal",
        "event_marker",
    )

    actual_channel_names = tuple(
        channel.normalized_name
        for channel in parsed.channels
    )

    if (
        actual_channel_names
        != expected_channel_names
    ):
        raise RuntimeError(
            "Unexpected normalized "
            "channel names"
        )

    expected_frequencies = (
        100.0,
        100.0,
        100.0,
        1.0,
        1.0,
        1.0,
        1.0,
    )

    actual_frequencies = tuple(
        channel
        .sampling_frequency_hz
        for channel in parsed.channels
    )

    if (
        actual_frequencies
        != expected_frequencies
    ):
        raise RuntimeError(
            "Unexpected channel "
            "frequencies"
        )

    temp_rectal = next(
        channel
        for channel in parsed.channels
        if channel.normalized_name
        == "temp_rectal"
    )

    if (
        temp_rectal
        .physical_dimension
        is not None
    ):
        raise RuntimeError(
            "Missing Temp rectal unit "
            "was not normalized to None"
        )

    if (
        temp_rectal
        .samples_per_data_record
        != 30
    ):
        raise RuntimeError(
            "Unexpected Temp rectal "
            "samples per data record"
        )

    print("psg_channel_count=7")
    print(
        "psg_duration_seconds=85500.0"
    )
    print("psg_data_record_count=2850")
    print(
        "psg_data_record_duration_seconds="
        "30.0"
    )
    print(
        "channel_names_normalized=true"
    )
    print(
        "sampling_frequencies_valid=true"
    )
    print(
        "missing_channel_unit_normalized="
        "true"
    )

    if not all(
        channel.recording_id
        == recording_id
        for channel in parsed.channels
    ):
        raise RuntimeError(
            "Channel recording_id mismatch"
        )

    print(
        "channel_recording_ids_match=true"
    )

    expect_value_error(
        operation=lambda: (
            normalize_channel_name("  ")
        ),
        check_name=(
            "empty_channel_name_blocked"
        ),
    )

    print(
        "psg_metadata_parser_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
