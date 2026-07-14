from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from math import isclose
from uuid import UUID

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.models import (
    AnnotationOverlapStatus,
    NormalizedSleepStage,
    SleepStageInterval,
)


EPOCH_SECONDS = 30.0

STAGE_MAPPING: dict[
    str,
    NormalizedSleepStage,
] = {
    "Sleep stage W": "W",
    "Sleep stage 1": "N1",
    "Sleep stage 2": "N2",
    "Sleep stage 3": "N3",
    "Sleep stage 4": "N4",
    "Sleep stage R": "REM",
    "Sleep stage ?": "UNKNOWN",
    "Movement time": "MOVEMENT",
}


@dataclass(frozen=True)
class ParsedHypnogram:
    intervals: tuple[
        SleepStageInterval,
        ...,
    ]

    source_label_counts: dict[str, int]
    overlap_status_counts: dict[str, int]

    coverage_start_seconds: float
    coverage_end_seconds: float

    leading_overhang_seconds: float
    trailing_overhang_seconds: float

    @property
    def interval_count(self) -> int:
        return len(self.intervals)


def normalize_sleep_stage(
    source_label: str,
) -> NormalizedSleepStage:
    normalized_stage = STAGE_MAPPING.get(
        source_label
    )

    if normalized_stage is None:
        raise ValueError(
            "Unsupported sleep-stage label: "
            f"{source_label!r}"
        )

    return normalized_stage


def validate_annotation_duration(
    duration_seconds: float,
) -> None:
    if duration_seconds <= 0:
        raise ValueError(
            "Annotation duration must be "
            "positive"
        )

    epoch_count = (
        duration_seconds
        / EPOCH_SECONDS
    )

    if not isclose(
        epoch_count,
        round(epoch_count),
        abs_tol=1e-9,
    ):
        raise ValueError(
            "Annotation duration must be "
            "divisible by 30 seconds: "
            f"{duration_seconds}"
        )


def determine_overlap_status(
    onset_seconds: float,
    duration_seconds: float,
    psg_duration_seconds: float,
) -> AnnotationOverlapStatus:
    interval_end_seconds = (
        onset_seconds
        + duration_seconds
    )

    has_overlap = (
        interval_end_seconds > 0.0
        and onset_seconds
        < psg_duration_seconds
    )

    if not has_overlap:
        return "outside_psg"

    fully_inside = (
        onset_seconds >= 0.0
        and interval_end_seconds
        <= psg_duration_seconds
    )

    if fully_inside:
        return "inside_psg"

    return "partial_overlap"


def parse_hypnogram_annotations(
    recording_id: UUID,
    annotations: Iterable,
    psg_duration_seconds: float,
) -> ParsedHypnogram:
    if psg_duration_seconds <= 0:
        raise ValueError(
            "PSG duration must be positive"
        )

    parsed_intervals: list[
        SleepStageInterval
    ] = []

    source_label_counts: Counter[str] = (
        Counter()
    )

    overlap_status_counts: Counter[str] = (
        Counter()
    )

    for annotation_index, annotation in (
        enumerate(annotations)
    ):
        if annotation.duration is None:
            raise ValueError(
                "Hypnogram annotation has no "
                "duration at index "
                f"{annotation_index}"
            )

        source_label = str(
            annotation.text
        ).strip()

        if not source_label:
            raise ValueError(
                "Hypnogram annotation has an "
                "empty label at index "
                f"{annotation_index}"
            )

        onset_seconds = float(
            annotation.onset
        )

        duration_seconds = float(
            annotation.duration
        )

        validate_annotation_duration(
            duration_seconds
        )

        normalized_stage = (
            normalize_sleep_stage(
                source_label
            )
        )

        overlap_status = (
            determine_overlap_status(
                onset_seconds=onset_seconds,
                duration_seconds=(
                    duration_seconds
                ),
                psg_duration_seconds=(
                    psg_duration_seconds
                ),
            )
        )

        interval = SleepStageInterval(
            interval_id=new_uuid7(),
            recording_id=recording_id,
            source_annotation_index=(
                annotation_index
            ),
            onset_seconds=onset_seconds,
            duration_seconds=(
                duration_seconds
            ),
            source_label=source_label,
            normalized_stage=(
                normalized_stage
            ),
            overlap_status=overlap_status,
        )

        parsed_intervals.append(
            interval
        )

        source_label_counts[
            source_label
        ] += 1

        overlap_status_counts[
            overlap_status
        ] += 1

    if not parsed_intervals:
        raise ValueError(
            "Hypnogram contains no "
            "annotations"
        )

    coverage_start_seconds = min(
        interval.onset_seconds
        for interval in parsed_intervals
    )

    coverage_end_seconds = max(
        interval.end_seconds
        for interval in parsed_intervals
    )

    leading_overhang_seconds = max(
        0.0,
        -coverage_start_seconds,
    )

    trailing_overhang_seconds = max(
        0.0,
        coverage_end_seconds
        - psg_duration_seconds,
    )

    return ParsedHypnogram(
        intervals=tuple(
            parsed_intervals
        ),
        source_label_counts=dict(
            sorted(
                source_label_counts.items()
            )
        ),
        overlap_status_counts=dict(
            sorted(
                overlap_status_counts.items()
            )
        ),
        coverage_start_seconds=(
            coverage_start_seconds
        ),
        coverage_end_seconds=(
            coverage_end_seconds
        ),
        leading_overhang_seconds=(
            leading_overhang_seconds
        ),
        trailing_overhang_seconds=(
            trailing_overhang_seconds
        ),
    )
