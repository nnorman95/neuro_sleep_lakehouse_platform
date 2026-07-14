from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from math import isclose
from uuid import UUID

import numpy as np
import numpy.typing as npt

from neuro_sleep.silver.models import (
    SilverChannel,
)


FLOAT_TOLERANCE = 1e-9
DEFAULT_CHUNK_DURATION_SECONDS = 1800.0


@dataclass(frozen=True)
class SignalSampleChunk:
    recording_id: UUID
    channel_id: UUID

    channel_position: int
    source_label: str
    normalized_name: str
    physical_dimension: str | None

    sampling_frequency_hz: float

    start_sample_index: int
    stop_sample_index: int

    start_seconds: float
    stop_seconds: float

    values: npt.NDArray[np.float64]

    @property
    def sample_count(self) -> int:
        return (
            self.stop_sample_index
            - self.start_sample_index
        )


def validate_time_range(
    start_seconds: float,
    stop_seconds: float,
    recording_duration_seconds: float,
) -> None:
    if recording_duration_seconds <= 0:
        raise ValueError(
            "Recording duration must be "
            "positive"
        )

    if start_seconds < 0:
        raise ValueError(
            "Signal extraction start must "
            "not be negative"
        )

    if stop_seconds <= start_seconds:
        raise ValueError(
            "Signal extraction stop must be "
            "greater than start"
        )

    if (
        stop_seconds
        > recording_duration_seconds
        + FLOAT_TOLERANCE
    ):
        raise ValueError(
            "Signal extraction range "
            "exceeds recording duration"
        )


def validate_channel_signal_match(
    channel: SilverChannel,
    signal,
) -> None:
    signal_label = signal.label.strip()

    if signal_label != channel.source_label:
        raise ValueError(
            "PSG signal label does not "
            "match Silver channel metadata: "
            f"signal={signal_label!r}, "
            "channel="
            f"{channel.source_label!r}"
        )

    signal_frequency = float(
        signal.sampling_frequency
    )

    if not isclose(
        signal_frequency,
        channel.sampling_frequency_hz,
        abs_tol=FLOAT_TOLERANCE,
    ):
        raise ValueError(
            "PSG signal frequency does not "
            "match Silver channel metadata: "
            f"signal={signal_frequency}, "
            "channel="
            f"{channel.sampling_frequency_hz}"
        )


def seconds_to_sample_index(
    seconds: float,
    sampling_frequency_hz: float,
) -> int:
    exact_index = (
        seconds
        * sampling_frequency_hz
    )

    rounded_index = round(
        exact_index
    )

    if not isclose(
        exact_index,
        rounded_index,
        abs_tol=FLOAT_TOLERANCE,
    ):
        raise ValueError(
            "Time boundary does not align "
            "with the channel sample grid: "
            f"seconds={seconds}, "
            "sampling_frequency_hz="
            f"{sampling_frequency_hz}"
        )

    return rounded_index


def get_signal_for_channel(
    psg_document,
    channel: SilverChannel,
):
    signal_index = (
        channel.position - 1
    )

    if (
        signal_index < 0
        or signal_index
        >= len(psg_document.signals)
    ):
        raise ValueError(
            "Silver channel position is "
            "outside the PSG signal list: "
            f"{channel.position}"
        )

    signal = psg_document.signals[
        signal_index
    ]

    validate_channel_signal_match(
        channel=channel,
        signal=signal,
    )

    return signal


def iter_channel_signal_chunks(
    recording_id: UUID,
    channel: SilverChannel,
    signal,
    recording_duration_seconds: float,
    *,
    chunk_duration_seconds: float = (
        DEFAULT_CHUNK_DURATION_SECONDS
    ),
    start_seconds: float = 0.0,
    stop_seconds: float | None = None,
) -> Iterator[SignalSampleChunk]:
    if channel.recording_id != recording_id:
        raise ValueError(
            "Channel recording_id does not "
            "match requested recording_id"
        )

    if chunk_duration_seconds <= 0:
        raise ValueError(
            "Chunk duration must be positive"
        )

    if stop_seconds is None:
        stop_seconds = (
            recording_duration_seconds
        )

    validate_time_range(
        start_seconds=start_seconds,
        stop_seconds=stop_seconds,
        recording_duration_seconds=(
            recording_duration_seconds
        ),
    )

    validate_channel_signal_match(
        channel=channel,
        signal=signal,
    )

    sampling_frequency_hz = (
        channel.sampling_frequency_hz
    )

    range_start_index = (
        seconds_to_sample_index(
            seconds=start_seconds,
            sampling_frequency_hz=(
                sampling_frequency_hz
            ),
        )
    )

    range_stop_index = (
        seconds_to_sample_index(
            seconds=stop_seconds,
            sampling_frequency_hz=(
                sampling_frequency_hz
            ),
        )
    )

    chunk_sample_count = (
        seconds_to_sample_index(
            seconds=(
                chunk_duration_seconds
            ),
            sampling_frequency_hz=(
                sampling_frequency_hz
            ),
        )
    )

    if chunk_sample_count <= 0:
        raise ValueError(
            "Chunk duration produces no "
            "samples"
        )

    for start_sample_index in range(
        range_start_index,
        range_stop_index,
        chunk_sample_count,
    ):
        stop_sample_index = min(
            start_sample_index
            + chunk_sample_count,
            range_stop_index,
        )

        chunk_start_seconds = (
            start_sample_index
            / sampling_frequency_hz
        )

        chunk_stop_seconds = (
            stop_sample_index
            / sampling_frequency_hz
        )

        values = np.asarray(
            signal.get_data_slice(
                chunk_start_seconds,
                chunk_stop_seconds,
            ),
            dtype=np.float64,
        )

        expected_sample_count = (
            stop_sample_index
            - start_sample_index
        )

        if len(values) != expected_sample_count:
            raise RuntimeError(
                "Extracted signal sample "
                "count mismatch: "
                f"expected="
                f"{expected_sample_count}, "
                f"actual={len(values)}, "
                f"channel="
                f"{channel.source_label!r}"
            )

        if not np.all(
            np.isfinite(values)
        ):
            raise ValueError(
                "Signal chunk contains "
                "non-finite physical values: "
                f"{channel.source_label!r}"
            )

        values.setflags(
            write=False
        )

        yield SignalSampleChunk(
            recording_id=recording_id,
            channel_id=channel.channel_id,
            channel_position=(
                channel.position
            ),
            source_label=(
                channel.source_label
            ),
            normalized_name=(
                channel.normalized_name
            ),
            physical_dimension=(
                channel.physical_dimension
            ),
            sampling_frequency_hz=(
                sampling_frequency_hz
            ),
            start_sample_index=(
                start_sample_index
            ),
            stop_sample_index=(
                stop_sample_index
            ),
            start_seconds=(
                chunk_start_seconds
            ),
            stop_seconds=(
                chunk_stop_seconds
            ),
            values=values,
        )


def iter_recording_signal_chunks(
    recording_id: UUID,
    channels: tuple[
        SilverChannel,
        ...,
    ],
    psg_document,
    recording_duration_seconds: float,
    *,
    chunk_duration_seconds: float = (
        DEFAULT_CHUNK_DURATION_SECONDS
    ),
    start_seconds: float = 0.0,
    stop_seconds: float | None = None,
) -> Iterator[SignalSampleChunk]:
    if not channels:
        raise ValueError(
            "At least one Silver channel is "
            "required"
        )

    for channel in channels:
        signal = get_signal_for_channel(
            psg_document=psg_document,
            channel=channel,
        )

        yield from (
            iter_channel_signal_chunks(
                recording_id=recording_id,
                channel=channel,
                signal=signal,
                recording_duration_seconds=(
                    recording_duration_seconds
                ),
                chunk_duration_seconds=(
                    chunk_duration_seconds
                ),
                start_seconds=start_seconds,
                stop_seconds=stop_seconds,
            )
        )
