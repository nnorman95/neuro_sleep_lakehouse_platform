from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Iterable

from neuro_sleep.config import (
    Settings,
    get_settings,
)
from neuro_sleep.raw.file_registry import (
    list_raw_files_by_bucket_prefix,
)
from neuro_sleep.raw.models import RawFileRecord
from neuro_sleep.sources.sleep_edf import (
    BRONZE_BUCKET,
    SOURCE_SYSTEM,
)
from neuro_sleep.sources.sleep_edf_manifest import (
    SleepEdfSourceFile,
    classify_sleep_edf_source_file,
)


@dataclass(frozen=True)
class SleepEdfRecordingPair:
    dataset_version: str
    study_folder: str
    recording_key: str

    psg_bucket: str
    psg_object_key: str

    hypnogram_bucket: str
    hypnogram_object_key: str

    silver_root_prefix: str


def build_dataset_prefix(
    dataset_version: str,
) -> str:
    return (
        "physionet/sleep-edfx/"
        f"{dataset_version}/"
    )


def to_source_file(
    raw_file: RawFileRecord,
    dataset_version: str,
) -> SleepEdfSourceFile | None:
    dataset_prefix = build_dataset_prefix(
        dataset_version
    )

    if not raw_file.object_key.startswith(
        dataset_prefix
    ):
        return None

    if raw_file.status != "uploaded":
        return None

    if raw_file.source_system != SOURCE_SYSTEM:
        return None

    checksum = raw_file.checksum_sha256

    if checksum is None:
        return None

    relative_path = raw_file.object_key[
        len(dataset_prefix):
    ]

    source_file = classify_sleep_edf_source_file(
        relative_path=relative_path,
        checksum_sha256=checksum,
        dataset_version=dataset_version,
    )

    if source_file.file_role not in {
        "psg",
        "hypnogram",
    }:
        return None

    return source_file


def build_silver_root_prefix(
    psg_object_key: str,
) -> str:
    suffix = "-PSG.edf"

    if not psg_object_key.endswith(suffix):
        raise ValueError(
            "PSG object key must end with "
            f"{suffix}: {psg_object_key}"
        )

    return psg_object_key[
        :-len(suffix)
    ]


def build_recording_pairs(
    raw_files: Iterable[RawFileRecord],
    settings: Settings,
) -> tuple[SleepEdfRecordingPair, ...]:
    enabled_study_folders: set[str] = set()

    if (
        settings.data_profile == "full"
        or settings.sleep_edf_include_cassette
    ):
        enabled_study_folders.add(
            "sleep-cassette"
        )

    if (
        settings.data_profile == "full"
        or settings.sleep_edf_include_telemetry
    ):
        enabled_study_folders.add(
            "sleep-telemetry"
        )

    grouped_files: dict[
        tuple[str, str],
        dict[str, RawFileRecord],
    ] = defaultdict(dict)

    for raw_file in raw_files:
        source_file = to_source_file(
            raw_file=raw_file,
            dataset_version=(
                settings.sleep_edf_version
            ),
        )

        if source_file is None:
            continue

        study_folder = (
            source_file.study_folder
        )
        recording_key = (
            source_file.recording_key
        )

        if (
            study_folder is None
            or recording_key is None
            or study_folder
            not in enabled_study_folders
        ):
            continue

        group_key = (
            study_folder,
            recording_key,
        )

        existing_role = grouped_files[
            group_key
        ].get(source_file.file_role)

        if existing_role is not None:
            raise RuntimeError(
                "Duplicate Sleep-EDF "
                f"{source_file.file_role} file "
                f"for {group_key}: "
                f"{existing_role.object_key}, "
                f"{raw_file.object_key}"
            )

        grouped_files[group_key][
            source_file.file_role
        ] = raw_file

    incomplete_pairs = {
        group_key: sorted(role_map)
        for group_key, role_map
        in grouped_files.items()
        if set(role_map) != {
            "psg",
            "hypnogram",
        }
    }

    if incomplete_pairs:
        preview = list(
            incomplete_pairs.items()
        )[:5]

        raise RuntimeError(
            "Incomplete uploaded Sleep-EDF "
            f"recording pairs: {preview}"
        )

    pairs: list[SleepEdfRecordingPair] = []

    for (
        study_folder,
        recording_key,
    ), role_map in grouped_files.items():
        psg_file = role_map["psg"]
        hypnogram_file = role_map[
            "hypnogram"
        ]

        pairs.append(
            SleepEdfRecordingPair(
                dataset_version=(
                    settings.sleep_edf_version
                ),
                study_folder=study_folder,
                recording_key=recording_key,
                psg_bucket=psg_file.bucket,
                psg_object_key=(
                    psg_file.object_key
                ),
                hypnogram_bucket=(
                    hypnogram_file.bucket
                ),
                hypnogram_object_key=(
                    hypnogram_file.object_key
                ),
                silver_root_prefix=(
                    build_silver_root_prefix(
                        psg_file.object_key
                    )
                ),
            )
        )

    pairs.sort(
        key=lambda pair: (
            pair.study_folder,
            pair.recording_key,
            pair.psg_object_key,
        )
    )

    if (
        settings.data_profile == "sample"
        and settings.sleep_edf_max_recordings
        > 0
    ):
        pairs = pairs[
            :settings.sleep_edf_max_recordings
        ]

    return tuple(pairs)


def discover_sleep_edf_recording_pairs(
    settings: Settings | None = None,
) -> tuple[SleepEdfRecordingPair, ...]:
    if settings is None:
        settings = get_settings()

    dataset_prefix = build_dataset_prefix(
        settings.sleep_edf_version
    )

    raw_files = (
        list_raw_files_by_bucket_prefix(
            bucket=BRONZE_BUCKET,
            prefix=dataset_prefix,
        )
    )

    pairs = build_recording_pairs(
        raw_files=raw_files,
        settings=settings,
    )

    if not pairs:
        raise RuntimeError(
            "No complete uploaded Sleep-EDF "
            "recording pairs were discovered"
        )

    return pairs
