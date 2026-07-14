from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import isclose
import re
from uuid import UUID

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.models import (
    SilverChannel,
)


FLOAT_TOLERANCE = 1e-9


@dataclass(frozen=True)
class ParsedPsgMetadata:
    recording_start: datetime
    duration_seconds: float

    data_record_count: int
    data_record_duration_seconds: float

    channels: tuple[
        SilverChannel,
        ...,
    ]

    @property
    def channel_count(self) -> int:
        return len(self.channels)


def normalize_channel_name(
    source_label: str,
) -> str:
    normalized_name = re.sub(
        r"[^a-z0-9]+",
        "_",
        source_label.strip().lower(),
    ).strip("_")

    if not normalized_name:
        raise ValueError(
            "Channel label cannot be "
            "normalized to an empty name"
        )

    return normalized_name


def optional_text(
    value: str,
) -> str | None:
    cleaned_value = value.strip()

    if not cleaned_value:
        return None

    return cleaned_value


def build_recording_start(
    psg_document,
) -> datetime:
    return datetime.combine(
        psg_document.startdate,
        psg_document.starttime,
    )


def validate_psg_header(
    psg_document,
) -> None:
    duration_seconds = float(
        psg_document.duration
    )

    data_record_count = int(
        psg_document.num_data_records
    )

    data_record_duration_seconds = float(
        psg_document.data_record_duration
    )

    if duration_seconds <= 0:
        raise ValueError(
            "PSG duration must be positive"
        )

    if data_record_count <= 0:
        raise ValueError(
            "PSG data-record count must be "
            "positive"
        )

    if data_record_duration_seconds <= 0:
        raise ValueError(
            "PSG data-record duration must "
            "be positive"
        )

    expected_duration_seconds = (
        data_record_count
        * data_record_duration_seconds
    )

    if not isclose(
        duration_seconds,
        expected_duration_seconds,
        abs_tol=FLOAT_TOLERANCE,
    ):
        raise ValueError(
            "PSG duration does not match "
            "data-record metadata: "
            f"duration={duration_seconds}, "
            "expected="
            f"{expected_duration_seconds}"
        )

    if int(psg_document.num_signals) <= 0:
        raise ValueError(
            "PSG must contain at least one "
            "signal"
        )


def parse_psg_metadata(
    recording_id: UUID,
    psg_document,
) -> ParsedPsgMetadata:
    validate_psg_header(
        psg_document
    )

    channels: list[
        SilverChannel
    ] = []

    normalized_names: set[str] = set()

    data_record_duration_seconds = float(
        psg_document.data_record_duration
    )

    for position, signal in enumerate(
        psg_document.signals,
        start=1,
    ):
        source_label = signal.label.strip()

        if not source_label:
            raise ValueError(
                "PSG channel label cannot "
                f"be empty at position "
                f"{position}"
            )

        normalized_name = (
            normalize_channel_name(
                source_label
            )
        )

        if (
            normalized_name
            in normalized_names
        ):
            raise ValueError(
                "Duplicate normalized "
                "channel name: "
                f"{normalized_name}"
            )

        normalized_names.add(
            normalized_name
        )

        sampling_frequency_hz = float(
            signal.sampling_frequency
        )

        samples_per_data_record = int(
            signal.samples_per_data_record
        )

        expected_sampling_frequency = (
            samples_per_data_record
            / data_record_duration_seconds
        )

        if not isclose(
            sampling_frequency_hz,
            expected_sampling_frequency,
            abs_tol=FLOAT_TOLERANCE,
        ):
            raise ValueError(
                "Channel sampling frequency "
                "does not match samples per "
                "data record: "
                f"channel={source_label!r}, "
                "frequency="
                f"{sampling_frequency_hz}, "
                "expected="
                f"{expected_sampling_frequency}"
            )

        channel = SilverChannel(
            channel_id=new_uuid7(),
            recording_id=recording_id,
            position=position,
            source_label=source_label,
            normalized_name=(
                normalized_name
            ),
            sampling_frequency_hz=(
                sampling_frequency_hz
            ),
            physical_dimension=(
                optional_text(
                    signal
                    .physical_dimension
                )
            ),
            physical_min=float(
                signal.physical_range.min
            ),
            physical_max=float(
                signal.physical_range.max
            ),
            digital_min=int(
                signal.digital_range.min
            ),
            digital_max=int(
                signal.digital_range.max
            ),
            samples_per_data_record=(
                samples_per_data_record
            ),
            prefiltering=optional_text(
                signal.prefiltering
            ),
        )

        channels.append(channel)

    if (
        len(channels)
        != int(psg_document.num_signals)
    ):
        raise ValueError(
            "Parsed channel count does not "
            "match PSG header"
        )

    return ParsedPsgMetadata(
        recording_start=(
            build_recording_start(
                psg_document
            )
        ),
        duration_seconds=float(
            psg_document.duration
        ),
        data_record_count=int(
            psg_document.num_data_records
        ),
        data_record_duration_seconds=(
            data_record_duration_seconds
        ),
        channels=tuple(channels),
    )
