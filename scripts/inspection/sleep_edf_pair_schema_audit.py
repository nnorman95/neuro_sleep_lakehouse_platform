from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from math import isclose
from pathlib import Path
from tempfile import TemporaryDirectory

from botocore.client import BaseClient
from edfio import read_edf

from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
)


BUCKET = "bronze"

PREFIX = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/"
)

EPOCH_SECONDS = 30.0

PAIRS = (
    (
        "SC4001E0-PSG.edf",
        "SC4001EC-Hypnogram.edf",
    ),
    (
        "SC4002E0-PSG.edf",
        "SC4002EC-Hypnogram.edf",
    ),
    (
        "SC4011E0-PSG.edf",
        "SC4011EH-Hypnogram.edf",
    ),
    (
        "SC4012E0-PSG.edf",
        "SC4012EC-Hypnogram.edf",
    ),
)


@dataclass(frozen=True)
class ChannelSchema:
    position: int
    label: str
    sampling_frequency_hz: float
    physical_dimension: str


def download_object(
    client: BaseClient,
    file_name: str,
    destination: Path,
) -> None:
    object_key = PREFIX + file_name

    run_object_storage_operation(
        operation=lambda: client.download_file(
            Bucket=BUCKET,
            Key=object_key,
            Filename=str(destination),
        ),
        operation_name=(
            f"download_file:{BUCKET}/"
            f"{object_key}"
        ),
    )


def is_multiple_of_epoch(
    duration_seconds: float,
) -> bool:
    epoch_count = (
        duration_seconds
        / EPOCH_SECONDS
    )

    return isclose(
        epoch_count,
        round(epoch_count),
        abs_tol=1e-9,
    )


def build_channel_schema(
    psg,
) -> tuple[ChannelSchema, ...]:
    return tuple(
        ChannelSchema(
            position=index,
            label=signal.label,
            sampling_frequency_hz=float(
                signal.sampling_frequency
            ),
            physical_dimension=(
                signal.physical_dimension
            ),
        )
        for index, signal in enumerate(
            psg.signals,
            start=1,
        )
    )


def print_channel_schema(
    schema: tuple[ChannelSchema, ...],
) -> None:
    for channel in schema:
        print(
            "channel | "
            f"position={channel.position} | "
            f"label={channel.label!r} | "
            "sampling_frequency_hz="
            f"{channel.sampling_frequency_hz} | "
            f"unit={channel.physical_dimension!r}"
        )


def print_schema_difference(
    reference_schema: tuple[
        ChannelSchema,
        ...,
    ],
    current_schema: tuple[
        ChannelSchema,
        ...,
    ],
) -> None:
    reference_by_position = {
        channel.position: channel
        for channel in reference_schema
    }

    current_by_position = {
        channel.position: channel
        for channel in current_schema
    }

    positions = sorted(
        set(reference_by_position)
        | set(current_by_position)
    )

    for position in positions:
        reference_channel = (
            reference_by_position.get(position)
        )

        current_channel = (
            current_by_position.get(position)
        )

        if reference_channel == current_channel:
            continue

        print(
            "schema_difference | "
            f"position={position} | "
            f"reference={reference_channel} | "
            f"current={current_channel}"
        )


def calculate_epoch_counts(
    annotations,
    psg_duration_seconds: float,
) -> tuple[int, int, int]:
    total_epoch_count = 0
    in_psg_epoch_count = 0
    outside_psg_epoch_count = 0

    for annotation in annotations:
        if annotation.duration is None:
            continue

        onset_seconds = float(
            annotation.onset
        )

        duration_seconds = float(
            annotation.duration
        )

        epoch_count = round(
            duration_seconds
            / EPOCH_SECONDS
        )

        total_epoch_count += epoch_count

        for epoch_offset in range(
            epoch_count
        ):
            epoch_start = (
                onset_seconds
                + epoch_offset
                * EPOCH_SECONDS
            )

            epoch_end = (
                epoch_start
                + EPOCH_SECONDS
            )

            if (
                epoch_end > 0.0
                and epoch_start
                < psg_duration_seconds
            ):
                in_psg_epoch_count += 1
            else:
                outside_psg_epoch_count += 1

    return (
        total_epoch_count,
        in_psg_epoch_count,
        outside_psg_epoch_count,
    )


def get_coverage_status(
    coverage_start: float,
    coverage_end: float,
    psg_duration_seconds: float,
) -> str:
    extends_before = (
        coverage_start < 0.0
    )

    extends_after = (
        coverage_end
        > psg_duration_seconds
    )

    if extends_before and extends_after:
        return "extends_before_and_after"

    if extends_before:
        return "extends_before"

    if extends_after:
        return "extends_after"

    exact_start = isclose(
        coverage_start,
        0.0,
        abs_tol=1e-9,
    )

    exact_end = isclose(
        coverage_end,
        psg_duration_seconds,
        abs_tol=1e-9,
    )

    if exact_start and exact_end:
        return "exact"

    return "inside_psg"


def audit_pair(
    client: BaseClient,
    root: Path,
    psg_name: str,
    hypnogram_name: str,
    reference_channel_schema: (
        tuple[ChannelSchema, ...]
        | None
    ),
) -> tuple[
    tuple[ChannelSchema, ...],
    Counter[str],
    bool,
]:
    psg_path = root / psg_name
    hypnogram_path = root / hypnogram_name

    download_object(
        client=client,
        file_name=psg_name,
        destination=psg_path,
    )

    download_object(
        client=client,
        file_name=hypnogram_name,
        destination=hypnogram_path,
    )

    psg = read_edf(psg_path)
    hypnogram = read_edf(
        hypnogram_path
    )

    annotations = tuple(
        hypnogram.annotations
    )

    if not annotations:
        raise RuntimeError(
            "No annotations found in "
            f"{hypnogram_name}"
        )

    pair_start_matches = (
        psg.startdate
        == hypnogram.startdate
        and psg.starttime
        == hypnogram.starttime
    )

    if not pair_start_matches:
        raise RuntimeError(
            "PSG/Hypnogram start mismatch: "
            f"{psg_name} and "
            f"{hypnogram_name}"
        )

    channel_schema = (
        build_channel_schema(psg)
    )

    if reference_channel_schema is None:
        reference_channel_schema = (
            channel_schema
        )

    channel_schema_matches = (
        channel_schema
        == reference_channel_schema
    )

    stage_counts: Counter[str] = Counter(
        annotation.text
        for annotation in annotations
    )

    coverage_start = min(
        float(annotation.onset)
        for annotation in annotations
    )

    coverage_end = max(
        float(annotation.onset)
        + (
            float(annotation.duration)
            if annotation.duration
            is not None
            else 0.0
        )
        for annotation in annotations
    )

    invalid_durations = [
        float(annotation.duration)
        for annotation in annotations
        if annotation.duration
        is not None
        and not is_multiple_of_epoch(
            float(annotation.duration)
        )
    ]

    if invalid_durations:
        raise RuntimeError(
            "Annotation durations are not "
            "multiples of 30 seconds in "
            f"{hypnogram_name}: "
            f"{invalid_durations[:5]}"
        )

    psg_duration_seconds = float(
        psg.duration
    )

    overlap_start = max(
        0.0,
        coverage_start,
    )

    overlap_end = min(
        psg_duration_seconds,
        coverage_end,
    )

    if overlap_end <= overlap_start:
        raise RuntimeError(
            "Hypnogram has no time overlap "
            f"with PSG: {hypnogram_name}"
        )

    trailing_overhang_seconds = max(
        0.0,
        coverage_end
        - psg_duration_seconds,
    )

    (
        total_epoch_count,
        in_psg_epoch_count,
        outside_psg_epoch_count,
    ) = calculate_epoch_counts(
        annotations=annotations,
        psg_duration_seconds=(
            psg_duration_seconds
        ),
    )

    coverage_status = get_coverage_status(
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        psg_duration_seconds=(
            psg_duration_seconds
        ),
    )

    print()
    print(f"pair={psg_name}")
    print(
        "hypnogram="
        f"{hypnogram_name}"
    )
    print(
        "psg_duration_seconds="
        f"{psg_duration_seconds}"
    )
    print(
        "annotation_count="
        f"{len(annotations)}"
    )
    print(
        "expanded_epoch_count_total="
        f"{total_epoch_count}"
    )
    print(
        "expanded_epoch_count_in_psg="
        f"{in_psg_epoch_count}"
    )
    print(
        "expanded_epoch_count_outside_psg="
        f"{outside_psg_epoch_count}"
    )
    print(
        "annotation_coverage_start="
        f"{coverage_start}"
    )
    print(
        "annotation_coverage_end="
        f"{coverage_end}"
    )
    print(
        "annotation_coverage_status="
        f"{coverage_status}"
    )
    print(
        "trailing_overhang_seconds="
        f"{trailing_overhang_seconds}"
    )
    print(
        "pair_start_matches="
        f"{str(pair_start_matches).lower()}"
    )
    print(
        "channel_schema_matches_reference="
        f"{str(channel_schema_matches).lower()}"
    )
    print(
        "annotation_durations_"
        "multiple_of_30=true"
    )

    print()
    print("CHANNEL SCHEMA")
    print_channel_schema(
        channel_schema
    )

    if not channel_schema_matches:
        print()
        print(
            "CHANNEL SCHEMA DIFFERENCES"
        )

        print_schema_difference(
            reference_schema=(
                reference_channel_schema
            ),
            current_schema=channel_schema,
        )

    for label, count in sorted(
        stage_counts.items()
    ):
        print(
            f"stage | {label} | "
            f"interval_count={count}"
        )

    return (
        reference_channel_schema,
        stage_counts,
        channel_schema_matches,
    )


def run_audit() -> None:
    all_stage_labels: Counter[str] = (
        Counter()
    )

    reference_channel_schema: (
        tuple[ChannelSchema, ...]
        | None
    ) = None

    matching_schema_count = 0
    differing_schema_count = 0

    client = get_object_storage_client()

    try:
        with TemporaryDirectory(
            prefix=(
                "neuro_sleep_pair_audit_"
            )
        ) as temporary_directory:
            root = Path(
                temporary_directory
            )

            for (
                psg_name,
                hypnogram_name,
            ) in PAIRS:
                (
                    reference_channel_schema,
                    stage_counts,
                    channel_schema_matches,
                ) = audit_pair(
                    client=client,
                    root=root,
                    psg_name=psg_name,
                    hypnogram_name=(
                        hypnogram_name
                    ),
                    reference_channel_schema=(
                        reference_channel_schema
                    ),
                )

                all_stage_labels.update(
                    stage_counts
                )

                if channel_schema_matches:
                    matching_schema_count += 1
                else:
                    differing_schema_count += 1

    finally:
        client.close()

    print()
    print("=== GLOBAL STAGE LABELS ===")

    for label, count in sorted(
        all_stage_labels.items()
    ):
        print(
            f"{label} | "
            f"interval_count={count}"
        )

    print()
    print(f"pair_count={len(PAIRS)}")
    print(
        "matching_channel_schema_count="
        f"{matching_schema_count}"
    )
    print(
        "differing_channel_schema_count="
        f"{differing_schema_count}"
    )
    print(
        "temporary_file_cleanup="
        "automatic"
    )
    print(
        "edf_pair_schema_audit_status="
        "success"
    )


if __name__ == "__main__":
    run_audit()
