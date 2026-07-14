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
    SleepStageEpoch,
    SleepStageInterval,
)


EPOCH_SECONDS = 30.0
FLOAT_TOLERANCE = 1e-9


@dataclass(frozen=True)
class ExpandedSleepStageEpochs:
    epochs: tuple[
        SleepStageEpoch,
        ...,
    ]

    source_epoch_count: int
    outside_psg_epoch_count: int
    partial_overlap_epoch_count: int

    source_stage_epoch_counts: dict[
        str,
        int,
    ]

    emitted_stage_epoch_counts: dict[
        str,
        int,
    ]

    @property
    def emitted_epoch_count(self) -> int:
        return len(self.epochs)


def is_epoch_grid_aligned(
    seconds: float,
) -> bool:
    epoch_number = (
        seconds
        / EPOCH_SECONDS
    )

    return isclose(
        epoch_number,
        round(epoch_number),
        abs_tol=FLOAT_TOLERANCE,
    )


def classify_epoch_position(
    start_seconds: float,
    end_seconds: float,
    psg_duration_seconds: float,
) -> str:
    fully_inside = (
        start_seconds >= 0.0
        and end_seconds
        <= psg_duration_seconds
    )

    if fully_inside:
        return "inside_psg"

    fully_outside = (
        end_seconds <= 0.0
        or start_seconds
        >= psg_duration_seconds
    )

    if fully_outside:
        return "outside_psg"

    return "partial_overlap"


def validate_interval_order(
    intervals: tuple[
        SleepStageInterval,
        ...,
    ],
) -> None:
    previous_annotation_index: (
        int
        | None
    ) = None

    previous_end_seconds: (
        float
        | None
    ) = None

    for interval in intervals:
        if (
            previous_annotation_index
            is not None
            and interval
            .source_annotation_index
            <= previous_annotation_index
        ):
            raise ValueError(
                "Source annotation indexes "
                "must be strictly increasing"
            )

        if (
            previous_end_seconds
            is not None
            and interval.onset_seconds
            < previous_end_seconds
            - FLOAT_TOLERANCE
        ):
            raise ValueError(
                "Sleep-stage intervals "
                "must not overlap"
            )

        previous_annotation_index = (
            interval
            .source_annotation_index
        )

        previous_end_seconds = (
            interval.end_seconds
        )


def expand_sleep_stage_epochs(
    recording_id: UUID,
    intervals: Iterable[
        SleepStageInterval
    ],
    psg_duration_seconds: float,
) -> ExpandedSleepStageEpochs:
    if psg_duration_seconds <= 0:
        raise ValueError(
            "PSG duration must be positive"
        )

    source_intervals = tuple(
        intervals
    )

    if not source_intervals:
        raise ValueError(
            "At least one sleep-stage "
            "interval is required"
        )

    validate_interval_order(
        source_intervals
    )

    epochs: list[
        SleepStageEpoch
    ] = []

    source_epoch_count = 0
    outside_psg_epoch_count = 0
    partial_overlap_epoch_count = 0

    source_stage_epoch_counts: (
        Counter[str]
    ) = Counter()

    emitted_stage_epoch_counts: (
        Counter[str]
    ) = Counter()

    emitted_epoch_numbers: set[int] = set()

    for interval in source_intervals:
        if interval.recording_id != recording_id:
            raise ValueError(
                "Interval recording_id does "
                "not match requested "
                "recording_id"
            )

        if not is_epoch_grid_aligned(
            interval.onset_seconds
        ):
            raise ValueError(
                "Interval onset is not "
                "aligned to the 30-second "
                "epoch grid: "
                f"{interval.onset_seconds}"
            )

        interval_epoch_count = round(
            interval.duration_seconds
            / EPOCH_SECONDS
        )

        for offset in range(
            interval_epoch_count
        ):
            start_seconds = (
                interval.onset_seconds
                + offset
                * EPOCH_SECONDS
            )

            end_seconds = (
                start_seconds
                + EPOCH_SECONDS
            )

            source_epoch_count += 1

            source_stage_epoch_counts[
                interval.source_label
            ] += 1

            position = (
                classify_epoch_position(
                    start_seconds=(
                        start_seconds
                    ),
                    end_seconds=end_seconds,
                    psg_duration_seconds=(
                        psg_duration_seconds
                    ),
                )
            )

            if position == "outside_psg":
                outside_psg_epoch_count += 1
                continue

            if position == "partial_overlap":
                partial_overlap_epoch_count += 1
                continue

            epoch_number = round(
                start_seconds
                / EPOCH_SECONDS
            )

            if (
                epoch_number
                in emitted_epoch_numbers
            ):
                raise ValueError(
                    "Duplicate in-range "
                    "epoch number detected: "
                    f"{epoch_number}"
                )

            emitted_epoch_numbers.add(
                epoch_number
            )

            epoch = SleepStageEpoch(
                epoch_id=new_uuid7(),
                recording_id=recording_id,
                source_interval_id=(
                    interval.interval_id
                ),
                source_annotation_index=(
                    interval
                    .source_annotation_index
                ),
                epoch_number=epoch_number,
                start_seconds=(
                    start_seconds
                ),
                duration_seconds=(
                    EPOCH_SECONDS
                ),
                source_label=(
                    interval.source_label
                ),
                normalized_stage=(
                    interval
                    .normalized_stage
                ),
            )

            epochs.append(epoch)

            emitted_stage_epoch_counts[
                interval.source_label
            ] += 1

    epochs.sort(
        key=lambda epoch: (
            epoch.epoch_number
        )
    )

    return ExpandedSleepStageEpochs(
        epochs=tuple(epochs),
        source_epoch_count=(
            source_epoch_count
        ),
        outside_psg_epoch_count=(
            outside_psg_epoch_count
        ),
        partial_overlap_epoch_count=(
            partial_overlap_epoch_count
        ),
        source_stage_epoch_counts=dict(
            sorted(
                source_stage_epoch_counts
                .items()
            )
        ),
        emitted_stage_epoch_counts=dict(
            sorted(
                emitted_stage_epoch_counts
                .items()
            )
        ),
    )
