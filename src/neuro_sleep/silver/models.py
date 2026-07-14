from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID


NormalizedSleepStage = Literal[
    "W",
    "N1",
    "N2",
    "N3",
    "N4",
    "REM",
    "UNKNOWN",
    "MOVEMENT",
]

AnnotationOverlapStatus = Literal[
    "inside_psg",
    "partial_overlap",
    "outside_psg",
]


def require_non_empty(
    value: str,
    field_name: str,
) -> None:
    if not value.strip():
        raise ValueError(
            f"{field_name} cannot be empty"
        )


def require_non_negative(
    value: int | float,
    field_name: str,
) -> None:
    if value < 0:
        raise ValueError(
            f"{field_name} cannot be negative"
        )


def require_positive(
    value: int | float,
    field_name: str,
) -> None:
    if value <= 0:
        raise ValueError(
            f"{field_name} must be positive"
        )


@dataclass(frozen=True)
class SilverRecording:
    recording_id: UUID
    source_system: str

    psg_bucket: str
    psg_object_key: str
    hypnogram_bucket: str
    hypnogram_object_key: str

    recording_start: datetime
    duration_seconds: float

    channel_count: int
    annotation_count: int
    in_range_epoch_count: int
    out_of_range_epoch_count: int

    trailing_overhang_seconds: float

    def __post_init__(self) -> None:
        for field_name, value in (
            (
                "source_system",
                self.source_system,
            ),
            (
                "psg_bucket",
                self.psg_bucket,
            ),
            (
                "psg_object_key",
                self.psg_object_key,
            ),
            (
                "hypnogram_bucket",
                self.hypnogram_bucket,
            ),
            (
                "hypnogram_object_key",
                self.hypnogram_object_key,
            ),
        ):
            require_non_empty(
                value=value,
                field_name=field_name,
            )

        require_positive(
            value=self.duration_seconds,
            field_name="duration_seconds",
        )

        for field_name, value in (
            (
                "channel_count",
                self.channel_count,
            ),
            (
                "annotation_count",
                self.annotation_count,
            ),
            (
                "in_range_epoch_count",
                self.in_range_epoch_count,
            ),
            (
                "out_of_range_epoch_count",
                self.out_of_range_epoch_count,
            ),
            (
                "trailing_overhang_seconds",
                self.trailing_overhang_seconds,
            ),
        ):
            require_non_negative(
                value=value,
                field_name=field_name,
            )


@dataclass(frozen=True)
class SilverChannel:
    channel_id: UUID
    recording_id: UUID

    position: int
    source_label: str
    normalized_name: str

    sampling_frequency_hz: float
    physical_dimension: str | None

    physical_min: float
    physical_max: float
    digital_min: int
    digital_max: int

    samples_per_data_record: int
    prefiltering: str | None

    def __post_init__(self) -> None:
        require_positive(
            value=self.position,
            field_name="position",
        )

        require_non_empty(
            value=self.source_label,
            field_name="source_label",
        )

        require_non_empty(
            value=self.normalized_name,
            field_name="normalized_name",
        )

        require_positive(
            value=self.sampling_frequency_hz,
            field_name=(
                "sampling_frequency_hz"
            ),
        )

        require_positive(
            value=self.samples_per_data_record,
            field_name=(
                "samples_per_data_record"
            ),
        )

        if self.physical_min >= self.physical_max:
            raise ValueError(
                "physical_min must be less "
                "than physical_max"
            )

        if self.digital_min >= self.digital_max:
            raise ValueError(
                "digital_min must be less "
                "than digital_max"
            )

        if (
            self.physical_dimension is not None
            and not self.physical_dimension.strip()
        ):
            raise ValueError(
                "physical_dimension must be "
                "None or a non-empty string"
            )


@dataclass(frozen=True)
class SleepStageInterval:
    interval_id: UUID
    recording_id: UUID

    source_annotation_index: int
    onset_seconds: float
    duration_seconds: float

    source_label: str
    normalized_stage: NormalizedSleepStage
    overlap_status: AnnotationOverlapStatus

    @property
    def end_seconds(self) -> float:
        return (
            self.onset_seconds
            + self.duration_seconds
        )

    def __post_init__(self) -> None:
        require_non_negative(
            value=self.source_annotation_index,
            field_name=(
                "source_annotation_index"
            ),
        )

        require_positive(
            value=self.duration_seconds,
            field_name="duration_seconds",
        )

        require_non_empty(
            value=self.source_label,
            field_name="source_label",
        )


@dataclass(frozen=True)
class SleepStageEpoch:
    epoch_id: UUID
    recording_id: UUID
    source_interval_id: UUID

    source_annotation_index: int
    epoch_number: int

    start_seconds: float
    duration_seconds: float

    source_label: str
    normalized_stage: NormalizedSleepStage

    @property
    def end_seconds(self) -> float:
        return (
            self.start_seconds
            + self.duration_seconds
        )

    def __post_init__(self) -> None:
        require_non_negative(
            value=self.source_annotation_index,
            field_name=(
                "source_annotation_index"
            ),
        )

        require_non_negative(
            value=self.epoch_number,
            field_name="epoch_number",
        )

        require_non_negative(
            value=self.start_seconds,
            field_name="start_seconds",
        )

        if self.duration_seconds != 30.0:
            raise ValueError(
                "Silver sleep-stage epochs "
                "must be exactly 30 seconds"
            )

        require_non_empty(
            value=self.source_label,
            field_name="source_label",
        )
