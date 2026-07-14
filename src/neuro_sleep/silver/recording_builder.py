from __future__ import annotations

from dataclasses import dataclass

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.bronze_edf_reader import (
    open_bronze_edf_pair,
)
from neuro_sleep.silver.epoch_expander import (
    ExpandedSleepStageEpochs,
    expand_sleep_stage_epochs,
)
from neuro_sleep.silver.hypnogram_parser import (
    ParsedHypnogram,
    parse_hypnogram_annotations,
)
from neuro_sleep.silver.models import (
    SilverChannel,
    SilverRecording,
    SleepStageEpoch,
    SleepStageInterval,
)
from neuro_sleep.silver.psg_metadata_parser import (
    ParsedPsgMetadata,
    parse_psg_metadata,
)


SOURCE_SYSTEM = "physionet_sleep_edf"


@dataclass(frozen=True)
class SilverRecordingBundle:
    recording: SilverRecording
    channels: tuple[
        SilverChannel,
        ...,
    ]
    intervals: tuple[
        SleepStageInterval,
        ...,
    ]
    epochs: tuple[
        SleepStageEpoch,
        ...,
    ]

    source_epoch_count: int
    partial_overlap_epoch_count: int

    @property
    def recording_id(self):
        return self.recording.recording_id

    @property
    def channel_count(self) -> int:
        return len(self.channels)

    @property
    def interval_count(self) -> int:
        return len(self.intervals)

    @property
    def epoch_count(self) -> int:
        return len(self.epochs)


def validate_pair_start_metadata(
    psg_document,
    hypnogram_document,
) -> None:
    if (
        psg_document.startdate
        != hypnogram_document.startdate
        or psg_document.starttime
        != hypnogram_document.starttime
    ):
        raise ValueError(
            "PSG and Hypnogram start "
            "metadata do not match"
        )


def validate_related_recording_ids(
    bundle: SilverRecordingBundle,
) -> None:
    recording_id = (
        bundle.recording.recording_id
    )

    if any(
        channel.recording_id
        != recording_id
        for channel in bundle.channels
    ):
        raise ValueError(
            "Channel recording_id mismatch"
        )

    if any(
        interval.recording_id
        != recording_id
        for interval in bundle.intervals
    ):
        raise ValueError(
            "Interval recording_id mismatch"
        )

    if any(
        epoch.recording_id
        != recording_id
        for epoch in bundle.epochs
    ):
        raise ValueError(
            "Epoch recording_id mismatch"
        )


def build_recording_model(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
    psg_metadata: ParsedPsgMetadata,
    parsed_hypnogram: ParsedHypnogram,
    expanded_epochs: (
        ExpandedSleepStageEpochs
    ),
):
    excluded_epoch_count = (
        expanded_epochs
        .outside_psg_epoch_count
        + expanded_epochs
        .partial_overlap_epoch_count
    )

    return SilverRecording(
        recording_id=(
            psg_metadata
            .channels[0]
            .recording_id
        ),
        source_system=SOURCE_SYSTEM,
        psg_bucket=psg_bucket,
        psg_object_key=psg_object_key,
        hypnogram_bucket=(
            hypnogram_bucket
        ),
        hypnogram_object_key=(
            hypnogram_object_key
        ),
        recording_start=(
            psg_metadata.recording_start
        ),
        duration_seconds=(
            psg_metadata.duration_seconds
        ),
        channel_count=(
            psg_metadata.channel_count
        ),
        annotation_count=(
            parsed_hypnogram.interval_count
        ),
        in_range_epoch_count=(
            expanded_epochs
            .emitted_epoch_count
        ),
        out_of_range_epoch_count=(
            excluded_epoch_count
        ),
        trailing_overhang_seconds=(
            parsed_hypnogram
            .trailing_overhang_seconds
        ),
    )


def build_silver_recording(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
) -> SilverRecordingBundle:
    recording_id = new_uuid7()

    with open_bronze_edf_pair(
        psg_bucket=psg_bucket,
        psg_object_key=psg_object_key,
        hypnogram_bucket=hypnogram_bucket,
        hypnogram_object_key=(
            hypnogram_object_key
        ),
    ) as pair:
        validate_pair_start_metadata(
            psg_document=(
                pair.psg.document
            ),
            hypnogram_document=(
                pair.hypnogram.document
            ),
        )

        psg_metadata = (
            parse_psg_metadata(
                recording_id=recording_id,
                psg_document=(
                    pair.psg.document
                ),
            )
        )

        parsed_hypnogram = (
            parse_hypnogram_annotations(
                recording_id=recording_id,
                annotations=(
                    pair.hypnogram
                    .document
                    .annotations
                ),
                psg_duration_seconds=(
                    psg_metadata
                    .duration_seconds
                ),
            )
        )

        expanded_epochs = (
            expand_sleep_stage_epochs(
                recording_id=recording_id,
                intervals=(
                    parsed_hypnogram
                    .intervals
                ),
                psg_duration_seconds=(
                    psg_metadata
                    .duration_seconds
                ),
            )
        )

    recording = build_recording_model(
        psg_bucket=psg_bucket,
        psg_object_key=psg_object_key,
        hypnogram_bucket=hypnogram_bucket,
        hypnogram_object_key=(
            hypnogram_object_key
        ),
        psg_metadata=psg_metadata,
        parsed_hypnogram=(
            parsed_hypnogram
        ),
        expanded_epochs=(
            expanded_epochs
        ),
    )

    bundle = SilverRecordingBundle(
        recording=recording,
        channels=psg_metadata.channels,
        intervals=(
            parsed_hypnogram.intervals
        ),
        epochs=expanded_epochs.epochs,
        source_epoch_count=(
            expanded_epochs
            .source_epoch_count
        ),
        partial_overlap_epoch_count=(
            expanded_epochs
            .partial_overlap_epoch_count
        ),
    )

    validate_related_recording_ids(
        bundle
    )

    return bundle
