from collections import Counter
from collections.abc import Callable
from dataclasses import replace

import numpy as np

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.bronze_edf_reader import (
    open_bronze_edf_pair,
)
from neuro_sleep.silver.psg_metadata_parser import (
    parse_psg_metadata,
)
from neuro_sleep.silver.signal_extractor import (
    get_signal_for_channel,
    iter_channel_signal_chunks,
    iter_recording_signal_chunks,
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
        psg_metadata = (
            parse_psg_metadata(
                recording_id=recording_id,
                psg_document=(
                    pair.psg.document
                ),
            )
        )

        chunks = tuple(
            iter_recording_signal_chunks(
                recording_id=recording_id,
                channels=(
                    psg_metadata.channels
                ),
                psg_document=(
                    pair.psg.document
                ),
                recording_duration_seconds=(
                    psg_metadata
                    .duration_seconds
                ),
                chunk_duration_seconds=30.0,
                start_seconds=0.0,
                stop_seconds=120.0,
            )
        )

        eeg_channel = (
            psg_metadata.channels[0]
        )

        mismatched_channel = replace(
            eeg_channel,
            source_label="Wrong label",
        )

        expect_value_error(
            operation=lambda: (
                get_signal_for_channel(
                    psg_document=(
                        pair.psg.document
                    ),
                    channel=(
                        mismatched_channel
                    ),
                )
            ),
            check_name=(
                "channel_metadata_mismatch_blocked"
            ),
        )

        eeg_signal = get_signal_for_channel(
            psg_document=(
                pair.psg.document
            ),
            channel=eeg_channel,
        )

        expect_value_error(
            operation=lambda: tuple(
                iter_channel_signal_chunks(
                    recording_id=recording_id,
                    channel=eeg_channel,
                    signal=eeg_signal,
                    recording_duration_seconds=(
                        psg_metadata
                        .duration_seconds
                    ),
                    chunk_duration_seconds=0.0,
                    start_seconds=0.0,
                    stop_seconds=30.0,
                )
            ),
            check_name=(
                "invalid_chunk_duration_blocked"
            ),
        )

    if len(chunks) != 28:
        raise RuntimeError(
            "Unexpected signal chunk count: "
            f"{len(chunks)}"
        )

    chunks_per_channel = Counter(
        chunk.normalized_name
        for chunk in chunks
    )

    if any(
        count != 4
        for count
        in chunks_per_channel.values()
    ):
        raise RuntimeError(
            "Each channel must produce four "
            "30-second chunks"
        )

    total_sample_count = sum(
        chunk.sample_count
        for chunk in chunks
    )

    if total_sample_count != 36480:
        raise RuntimeError(
            "Unexpected total sample count: "
            f"{total_sample_count}"
        )

    first_eeg_chunk = next(
        chunk
        for chunk in chunks
        if (
            chunk.normalized_name
            == "eeg_fpz_cz"
        )
    )

    if (
        first_eeg_chunk.sample_count
        != 3000
        or first_eeg_chunk
        .start_sample_index
        != 0
        or first_eeg_chunk
        .stop_sample_index
        != 3000
        or first_eeg_chunk.start_seconds
        != 0.0
        or first_eeg_chunk.stop_seconds
        != 30.0
    ):
        raise RuntimeError(
            "Unexpected first EEG chunk"
        )

    event_chunks = tuple(
        chunk
        for chunk in chunks
        if (
            chunk.normalized_name
            == "event_marker"
        )
    )

    if any(
        chunk.sample_count != 30
        for chunk in event_chunks
    ):
        raise RuntimeError(
            "Unexpected Event marker "
            "chunk size"
        )

    if not all(
        chunk.values.dtype
        == np.float64
        for chunk in chunks
    ):
        raise RuntimeError(
            "Signal values are not float64"
        )

    if any(
        chunk.values.flags.writeable
        for chunk in chunks
    ):
        raise RuntimeError(
            "Signal arrays must be "
            "read-only"
        )

    if not all(
        np.all(
            np.isfinite(
                chunk.values
            )
        )
        for chunk in chunks
    ):
        raise RuntimeError(
            "Signal chunks contain "
            "non-finite values"
        )

    print("signal_channel_count=7")
    print("signal_chunk_count=28")
    print("chunks_per_channel=4")
    print("extracted_range_seconds=120.0")
    print("total_sample_count=36480")
    print("eeg_chunk_sample_count=3000")
    print("event_chunk_sample_count=30")
    print("signal_values_dtype=float64")
    print("signal_values_read_only=true")
    print("signal_values_finite=true")

    print(
        "signal_extractor_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
