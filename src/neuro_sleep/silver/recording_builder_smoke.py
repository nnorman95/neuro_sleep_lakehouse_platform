from neuro_sleep.silver.recording_builder import (
    build_silver_recording,
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


def run_smoke_test() -> None:
    bundle = build_silver_recording(
        psg_bucket=BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    )

    recording = bundle.recording

    if recording.recording_id.version != 7:
        raise RuntimeError(
            "Recording ID is not UUIDv7"
        )

    if recording.source_system != (
        "physionet_sleep_edf"
    ):
        raise RuntimeError(
            "Unexpected source system"
        )

    if recording.duration_seconds != (
        79500.0
    ):
        raise RuntimeError(
            "Unexpected recording duration"
        )

    if recording.channel_count != 7:
        raise RuntimeError(
            "Unexpected channel count"
        )

    if recording.annotation_count != 154:
        raise RuntimeError(
            "Unexpected annotation count"
        )

    if (
        recording.in_range_epoch_count
        != 2650
    ):
        raise RuntimeError(
            "Unexpected in-range epoch "
            "count"
        )

    if (
        recording.out_of_range_epoch_count
        != 230
    ):
        raise RuntimeError(
            "Unexpected out-of-range epoch "
            "count"
        )

    if (
        recording
        .trailing_overhang_seconds
        != 6900.0
    ):
        raise RuntimeError(
            "Unexpected trailing overhang"
        )

    if bundle.channel_count != 7:
        raise RuntimeError(
            "Bundle channel count mismatch"
        )

    if bundle.interval_count != 154:
        raise RuntimeError(
            "Bundle interval count mismatch"
        )

    if bundle.epoch_count != 2650:
        raise RuntimeError(
            "Bundle epoch count mismatch"
        )

    if bundle.source_epoch_count != 2880:
        raise RuntimeError(
            "Unexpected source epoch count"
        )

    if (
        bundle.partial_overlap_epoch_count
        != 0
    ):
        raise RuntimeError(
            "Unexpected partial overlap "
            "epoch count"
        )

    if not all(
        item.recording_id
        == recording.recording_id
        for item in (
            *bundle.channels,
            *bundle.intervals,
            *bundle.epochs,
        )
    ):
        raise RuntimeError(
            "Related recording IDs do not "
            "match"
        )

    if not all(
        item.channel_id.version == 7
        for item in bundle.channels
    ):
        raise RuntimeError(
            "Channel IDs are not UUIDv7"
        )

    if not all(
        item.interval_id.version == 7
        for item in bundle.intervals
    ):
        raise RuntimeError(
            "Interval IDs are not UUIDv7"
        )

    if not all(
        item.epoch_id.version == 7
        for item in bundle.epochs
    ):
        raise RuntimeError(
            "Epoch IDs are not UUIDv7"
        )

    first_epoch = bundle.epochs[0]
    last_epoch = bundle.epochs[-1]

    if (
        first_epoch.epoch_number != 0
        or first_epoch.start_seconds
        != 0.0
    ):
        raise RuntimeError(
            "Unexpected first epoch"
        )

    if (
        last_epoch.epoch_number != 2649
        or last_epoch.end_seconds
        != 79500.0
    ):
        raise RuntimeError(
            "Unexpected last epoch"
        )

    print("recording_uuid7=true")
    print("channel_uuid7=true")
    print("interval_uuid7=true")
    print("epoch_uuid7=true")
    print(
        "silver_recording_duration_seconds="
        "79500.0"
    )
    print(
        "silver_recording_channel_count=7"
    )
    print(
        "silver_recording_interval_count=154"
    )
    print(
        "silver_recording_epoch_count=2650"
    )
    print(
        "silver_recording_excluded_epoch_count="
        "230"
    )
    print(
        "silver_recording_trailing_overhang="
        "6900.0"
    )
    print(
        "related_recording_ids_match=true"
    )
    print(
        "silver_recording_builder_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
