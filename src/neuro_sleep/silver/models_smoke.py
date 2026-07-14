from collections.abc import Callable
from datetime import datetime

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.models import (
    SilverChannel,
    SilverRecording,
    SleepStageEpoch,
    SleepStageInterval,
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
    channel_id = new_uuid7()
    interval_id = new_uuid7()
    epoch_id = new_uuid7()

    recording = SilverRecording(
        recording_id=recording_id,
        source_system="physionet_sleep_edf",
        psg_bucket="bronze",
        psg_object_key=(
            "physionet/sleep-edfx/1.0.0/"
            "sleep-cassette/"
            "SC4001E0-PSG.edf"
        ),
        hypnogram_bucket="bronze",
        hypnogram_object_key=(
            "physionet/sleep-edfx/1.0.0/"
            "sleep-cassette/"
            "SC4001EC-Hypnogram.edf"
        ),
        recording_start=datetime(
            1989,
            4,
            24,
            16,
            13,
        ),
        duration_seconds=79500.0,
        channel_count=7,
        annotation_count=154,
        in_range_epoch_count=2650,
        out_of_range_epoch_count=230,
        trailing_overhang_seconds=6900.0,
    )

    channel = SilverChannel(
        channel_id=channel_id,
        recording_id=recording_id,
        position=6,
        source_label="Temp rectal",
        normalized_name="temp_rectal",
        sampling_frequency_hz=1.0,
        physical_dimension=None,
        physical_min=34.0,
        physical_max=40.0,
        digital_min=-2849,
        digital_max=2731,
        samples_per_data_record=30,
        prefiltering=None,
    )

    interval = SleepStageInterval(
        interval_id=interval_id,
        recording_id=recording_id,
        source_annotation_index=1,
        onset_seconds=30630.0,
        duration_seconds=120.0,
        source_label="Sleep stage 1",
        normalized_stage="N1",
        overlap_status="inside_psg",
    )

    epoch = SleepStageEpoch(
        epoch_id=epoch_id,
        recording_id=recording_id,
        source_interval_id=interval_id,
        source_annotation_index=1,
        epoch_number=1021,
        start_seconds=30630.0,
        duration_seconds=30.0,
        source_label="Sleep stage 1",
        normalized_stage="N1",
    )

    if recording.channel_count != 7:
        raise RuntimeError(
            "Recording model value mismatch"
        )

    if channel.physical_dimension is not None:
        raise RuntimeError(
            "Nullable channel unit failed"
        )

    if interval.end_seconds != 30750.0:
        raise RuntimeError(
            "Interval end calculation failed"
        )

    if epoch.end_seconds != 30660.0:
        raise RuntimeError(
            "Epoch end calculation failed"
        )

    print("silver_recording_model=true")
    print("silver_channel_model=true")
    print("nullable_channel_unit=true")
    print("sleep_stage_interval_model=true")
    print("sleep_stage_epoch_model=true")

    expect_value_error(
        operation=lambda: SilverRecording(
            recording_id=new_uuid7(),
            source_system="physionet_sleep_edf",
            psg_bucket="bronze",
            psg_object_key="psg.edf",
            hypnogram_bucket="bronze",
            hypnogram_object_key=(
                "hypnogram.edf"
            ),
            recording_start=datetime(
                1989,
                4,
                24,
            ),
            duration_seconds=0.0,
            channel_count=7,
            annotation_count=1,
            in_range_epoch_count=1,
            out_of_range_epoch_count=0,
            trailing_overhang_seconds=0.0,
        ),
        check_name=(
            "invalid_recording_duration_blocked"
        ),
    )

    expect_value_error(
        operation=lambda: SilverChannel(
            channel_id=new_uuid7(),
            recording_id=recording_id,
            position=0,
            source_label="EEG Fpz-Cz",
            normalized_name="eeg_fpz_cz",
            sampling_frequency_hz=100.0,
            physical_dimension="uV",
            physical_min=-192.0,
            physical_max=192.0,
            digital_min=-2048,
            digital_max=2047,
            samples_per_data_record=3000,
            prefiltering=None,
        ),
        check_name=(
            "invalid_channel_position_blocked"
        ),
    )

    expect_value_error(
        operation=lambda: SleepStageInterval(
            interval_id=new_uuid7(),
            recording_id=recording_id,
            source_annotation_index=0,
            onset_seconds=0.0,
            duration_seconds=0.0,
            source_label="Sleep stage W",
            normalized_stage="W",
            overlap_status="inside_psg",
        ),
        check_name=(
            "invalid_interval_duration_blocked"
        ),
    )

    expect_value_error(
        operation=lambda: SleepStageEpoch(
            epoch_id=new_uuid7(),
            recording_id=recording_id,
            source_interval_id=interval_id,
            source_annotation_index=0,
            epoch_number=0,
            start_seconds=0.0,
            duration_seconds=60.0,
            source_label="Sleep stage W",
            normalized_stage="W",
        ),
        check_name=(
            "invalid_epoch_duration_blocked"
        ),
    )

    print(
        "silver_models_smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
