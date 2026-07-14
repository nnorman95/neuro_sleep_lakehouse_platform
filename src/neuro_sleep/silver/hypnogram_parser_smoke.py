from dataclasses import dataclass

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.bronze_edf_reader import (
    open_bronze_edf_pair,
)
from neuro_sleep.silver.hypnogram_parser import (
    determine_overlap_status,
    normalize_sleep_stage,
    parse_hypnogram_annotations,
    validate_annotation_duration,
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


@dataclass(frozen=True)
class FakeAnnotation:
    onset: float
    duration: float | None
    text: str


def expect_value_error(
    operation,
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
    with open_bronze_edf_pair(
        psg_bucket=BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    ) as pair:
        result = (
            parse_hypnogram_annotations(
                recording_id=new_uuid7(),
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

    if result.interval_count != 154:
        raise RuntimeError(
            "Unexpected interval count: "
            f"{result.interval_count}"
        )

    if (
        result.overlap_status_counts
        .get("inside_psg")
        != 153
    ):
        raise RuntimeError(
            "Unexpected inside-PSG "
            "interval count"
        )

    if (
        result.overlap_status_counts
        .get("outside_psg")
        != 1
    ):
        raise RuntimeError(
            "Unexpected outside-PSG "
            "interval count"
        )

    if (
        result.overlap_status_counts
        .get("partial_overlap", 0)
        != 0
    ):
        raise RuntimeError(
            "Unexpected partial-overlap "
            "interval count"
        )

    if (
        result.coverage_start_seconds
        != 0.0
    ):
        raise RuntimeError(
            "Unexpected coverage start"
        )

    if (
        result.coverage_end_seconds
        != 86400.0
    ):
        raise RuntimeError(
            "Unexpected coverage end"
        )

    if (
        result.trailing_overhang_seconds
        != 6900.0
    ):
        raise RuntimeError(
            "Unexpected trailing overhang"
        )

    first_interval = result.intervals[0]

    if (
        first_interval.source_label
        != "Sleep stage W"
        or first_interval.normalized_stage
        != "W"
        or first_interval.onset_seconds
        != 0.0
        or first_interval.duration_seconds
        != 30630.0
    ):
        raise RuntimeError(
            "First interval was parsed "
            "incorrectly"
        )

    outside_interval = (
        result.intervals[-1]
    )

    if (
        outside_interval.source_label
        != "Sleep stage ?"
        or outside_interval
        .normalized_stage
        != "UNKNOWN"
        or outside_interval
        .overlap_status
        != "outside_psg"
    ):
        raise RuntimeError(
            "Out-of-range interval was "
            "parsed incorrectly"
        )

    if normalize_sleep_stage(
        "Sleep stage R"
    ) != "REM":
        raise RuntimeError(
            "REM normalization failed"
        )

    if determine_overlap_status(
        onset_seconds=90.0,
        duration_seconds=30.0,
        psg_duration_seconds=100.0,
    ) != "partial_overlap":
        raise RuntimeError(
            "Partial-overlap detection "
            "failed"
        )

    print("hypnogram_interval_count=154")
    print("inside_psg_interval_count=153")
    print("outside_psg_interval_count=1")
    print("partial_overlap_interval_count=0")
    print("coverage_start_seconds=0.0")
    print("coverage_end_seconds=86400.0")
    print("trailing_overhang_seconds=6900.0")
    print("source_labels_preserved=true")
    print("sleep_stage_normalization=true")
    print("overlap_status_assignment=true")

    expect_value_error(
        operation=lambda: (
            normalize_sleep_stage(
                "Unexpected stage"
            )
        ),
        check_name=(
            "unsupported_stage_blocked"
        ),
    )

    expect_value_error(
        operation=lambda: (
            validate_annotation_duration(
                25.0
            )
        ),
        check_name=(
            "non_epoch_duration_blocked"
        ),
    )

    expect_value_error(
        operation=lambda: (
            parse_hypnogram_annotations(
                recording_id=new_uuid7(),
                annotations=(
                    FakeAnnotation(
                        onset=0.0,
                        duration=None,
                        text="Sleep stage W",
                    ),
                ),
                psg_duration_seconds=30.0,
            )
        ),
        check_name=(
            "missing_duration_blocked"
        ),
    )

    print(
        "hypnogram_parser_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
