from collections.abc import Callable

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.bronze_edf_reader import (
    open_bronze_edf_pair,
)
from neuro_sleep.silver.epoch_expander import (
    classify_epoch_position,
    expand_sleep_stage_epochs,
)
from neuro_sleep.silver.hypnogram_parser import (
    parse_hypnogram_annotations,
)
from neuro_sleep.silver.models import (
    SleepStageInterval,
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


def run_real_data_check() -> None:
    recording_id = new_uuid7()

    with open_bronze_edf_pair(
        psg_bucket=BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    ) as pair:
        parsed = (
            parse_hypnogram_annotations(
                recording_id=recording_id,
                annotations=(
                    pair.hypnogram
                    .document
                    .annotations
                ),
                psg_duration_seconds=float(
                    pair.psg.document.duration
                ),
            )
        )

        expanded = (
            expand_sleep_stage_epochs(
                recording_id=recording_id,
                intervals=parsed.intervals,
                psg_duration_seconds=float(
                    pair.psg.document.duration
                ),
            )
        )

    if (
        expanded.source_epoch_count
        != 2880
    ):
        raise RuntimeError(
            "Unexpected source epoch count"
        )

    if (
        expanded.emitted_epoch_count
        != 2650
    ):
        raise RuntimeError(
            "Unexpected emitted epoch count"
        )

    if (
        expanded.outside_psg_epoch_count
        != 230
    ):
        raise RuntimeError(
            "Unexpected outside-PSG "
            "epoch count"
        )

    if (
        expanded
        .partial_overlap_epoch_count
        != 0
    ):
        raise RuntimeError(
            "Unexpected partial-overlap "
            "epoch count"
        )

    first_epoch = expanded.epochs[0]
    last_epoch = expanded.epochs[-1]

    if (
        first_epoch.epoch_number != 0
        or first_epoch.start_seconds
        != 0.0
        or first_epoch.end_seconds
        != 30.0
    ):
        raise RuntimeError(
            "First epoch is incorrect"
        )

    if (
        last_epoch.epoch_number
        != 2649
        or last_epoch.start_seconds
        != 79470.0
        or last_epoch.end_seconds
        != 79500.0
    ):
        raise RuntimeError(
            "Last epoch is incorrect"
        )

    emitted_stage_total = sum(
        expanded
        .emitted_stage_epoch_counts
        .values()
    )

    if (
        emitted_stage_total
        != expanded.emitted_epoch_count
    ):
        raise RuntimeError(
            "Emitted stage counts do not "
            "match epoch count"
        )

    print("source_epoch_count=2880")
    print("emitted_epoch_count=2650")
    print("outside_psg_epoch_count=230")
    print("partial_overlap_epoch_count=0")
    print("first_epoch_number=0")
    print("last_epoch_number=2649")
    print("epoch_duration_seconds=30.0")
    print("stage_epoch_counts_valid=true")


def run_unit_checks() -> None:
    recording_id = new_uuid7()

    interval = SleepStageInterval(
        interval_id=new_uuid7(),
        recording_id=recording_id,
        source_annotation_index=0,
        onset_seconds=30630.0,
        duration_seconds=120.0,
        source_label="Sleep stage 1",
        normalized_stage="N1",
        overlap_status="inside_psg",
    )

    result = expand_sleep_stage_epochs(
        recording_id=recording_id,
        intervals=(interval,),
        psg_duration_seconds=40000.0,
    )

    expected_starts = (
        30630.0,
        30660.0,
        30690.0,
        30720.0,
    )

    actual_starts = tuple(
        epoch.start_seconds
        for epoch in result.epochs
    )

    if actual_starts != expected_starts:
        raise RuntimeError(
            "120-second interval was not "
            "expanded into four epochs"
        )

    expected_epoch_numbers = (
        1021,
        1022,
        1023,
        1024,
    )

    actual_epoch_numbers = tuple(
        epoch.epoch_number
        for epoch in result.epochs
    )

    if (
        actual_epoch_numbers
        != expected_epoch_numbers
    ):
        raise RuntimeError(
            "Epoch numbering is incorrect"
        )

    if classify_epoch_position(
        start_seconds=30.0,
        end_seconds=60.0,
        psg_duration_seconds=45.0,
    ) != "partial_overlap":
        raise RuntimeError(
            "Partial overlap "
            "classification failed"
        )

    print(
        "120_second_interval_expands_to_4=true"
    )

    print(
        "epoch_numbering_from_timeline=true"
    )

    print(
        "partial_overlap_classification=true"
    )

    unaligned_interval = (
        SleepStageInterval(
            interval_id=new_uuid7(),
            recording_id=recording_id,
            source_annotation_index=1,
            onset_seconds=5.0,
            duration_seconds=30.0,
            source_label="Sleep stage W",
            normalized_stage="W",
            overlap_status="inside_psg",
        )
    )

    expect_value_error(
        operation=lambda: (
            expand_sleep_stage_epochs(
                recording_id=recording_id,
                intervals=(
                    unaligned_interval,
                ),
                psg_duration_seconds=60.0,
            )
        ),
        check_name=(
            "unaligned_epoch_blocked"
        ),
    )

    expect_value_error(
        operation=lambda: (
            expand_sleep_stage_epochs(
                recording_id=new_uuid7(),
                intervals=(interval,),
                psg_duration_seconds=(
                    40000.0
                ),
            )
        ),
        check_name=(
            "recording_id_mismatch_blocked"
        ),
    )


def run_smoke_test() -> None:
    run_real_data_check()
    run_unit_checks()

    print(
        "epoch_expander_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
