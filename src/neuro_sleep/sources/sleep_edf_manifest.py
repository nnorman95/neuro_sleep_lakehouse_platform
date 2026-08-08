from dataclasses import dataclass
from pathlib import PurePosixPath
from string import hexdigits
from typing import Literal

from neuro_sleep.config import Settings, get_settings
from neuro_sleep.sources.sleep_edf import (
    BASE_URL_TEMPLATE,
    BRONZE_BUCKET,
    SOURCE_SYSTEM,
)


SleepEdfFileRole = Literal[
    "psg",
    "hypnogram",
    "metadata",
]

STUDY_FOLDERS = {
    "sleep-cassette",
    "sleep-telemetry",
}


@dataclass(frozen=True)
class SleepEdfSourceFile:
    dataset_version: str
    relative_path: str
    checksum_sha256: str
    file_role: SleepEdfFileRole
    recording_key: str | None
    study_folder: str | None

    @property
    def source_system(self) -> str:
        return SOURCE_SYSTEM

    @property
    def source_url(self) -> str:
        base_url = BASE_URL_TEMPLATE.format(
            version=self.dataset_version
        )

        return f"{base_url}/{self.relative_path}"

    @property
    def bucket(self) -> str:
        return BRONZE_BUCKET

    @property
    def object_key(self) -> str:
        return (
            "physionet/sleep-edfx/"
            f"{self.dataset_version}/"
            f"{self.relative_path}"
        )

    @property
    def file_name(self) -> str:
        return PurePosixPath(
            self.relative_path
        ).name

    @property
    def file_type(self) -> str:
        suffix = PurePosixPath(
            self.relative_path
        ).suffix

        if not suffix:
            return "unknown"

        return suffix.lstrip(".").lower()


def _validate_relative_path(
    relative_path: str,
) -> None:
    if not relative_path:
        raise ValueError(
            "relative_path must not be empty"
        )

    if "\\" in relative_path:
        raise ValueError(
            "Backslashes are not allowed in "
            f"source paths: {relative_path}"
        )

    path = PurePosixPath(relative_path)

    if path.is_absolute():
        raise ValueError(
            "Absolute source path is not allowed: "
            f"{relative_path}"
        )

    if ".." in path.parts:
        raise ValueError(
            "Parent path traversal is not allowed: "
            f"{relative_path}"
        )


def _validate_checksum(
    checksum_sha256: str,
) -> str:
    normalized_checksum = (
        checksum_sha256.strip().lower()
    )

    if len(normalized_checksum) != 64:
        raise ValueError(
            "SHA-256 checksum must contain "
            "exactly 64 hexadecimal characters"
        )

    if any(
        character not in hexdigits
        for character in normalized_checksum
    ):
        raise ValueError(
            "SHA-256 checksum contains "
            "non-hexadecimal characters"
        )

    return normalized_checksum


def _derive_recording_key(
    file_name: str,
) -> str | None:
    if "-" not in file_name:
        return None

    recording_prefix = file_name.split(
        "-",
        1,
    )[0]

    if len(recording_prefix) < 2:
        return None

    # PSG and Hypnogram filenames differ in the
    # final character before "-PSG" / "-Hypnogram".
    # Removing that character creates their shared key.
    return recording_prefix[:-1]


def classify_sleep_edf_source_file(
    relative_path: str,
    checksum_sha256: str,
    dataset_version: str,
) -> SleepEdfSourceFile:
    relative_path = relative_path.strip()

    _validate_relative_path(relative_path)

    checksum_sha256 = _validate_checksum(
        checksum_sha256
    )

    path = PurePosixPath(relative_path)
    file_name = path.name

    first_part = (
        path.parts[0]
        if path.parts
        else None
    )

    study_folder = (
        first_part
        if first_part in STUDY_FOLDERS
        else None
    )

    if (
        study_folder is not None
        and file_name.endswith("-PSG.edf")
    ):
        file_role: SleepEdfFileRole = "psg"
        recording_key = _derive_recording_key(
            file_name
        )

    elif (
        study_folder is not None
        and file_name.endswith(
            "-Hypnogram.edf"
        )
    ):
        file_role = "hypnogram"
        recording_key = _derive_recording_key(
            file_name
        )

    else:
        file_role = "metadata"
        recording_key = None

    return SleepEdfSourceFile(
        dataset_version=dataset_version,
        relative_path=relative_path,
        checksum_sha256=checksum_sha256,
        file_role=file_role,
        recording_key=recording_key,
        study_folder=study_folder,
    )


def parse_sleep_edf_checksum_manifest(
    checksum_text: str,
    dataset_version: str,
) -> list[SleepEdfSourceFile]:
    files: list[SleepEdfSourceFile] = []
    seen_paths: set[str] = set()

    for line_number, raw_line in enumerate(
        checksum_text.splitlines(),
        start=1,
    ):
        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#"):
            continue

        parts = line.split(maxsplit=1)

        if len(parts) != 2:
            raise ValueError(
                "Invalid SHA256SUMS line "
                f"{line_number}: {raw_line!r}"
            )

        checksum_sha256 = parts[0]
        relative_path = parts[1].lstrip(
            "*"
        ).strip()

        source_file = (
            classify_sleep_edf_source_file(
                relative_path=relative_path,
                checksum_sha256=checksum_sha256,
                dataset_version=dataset_version,
            )
        )

        if source_file.relative_path in seen_paths:
            continue

        seen_paths.add(
            source_file.relative_path
        )
        files.append(source_file)

    return sorted(
        files,
        key=lambda file: file.relative_path,
    )


def validate_complete_recording_pairs(
    files: list[SleepEdfSourceFile],
) -> None:
    roles_by_recording: dict[str, set[str]] = {}

    for file in files:
        if file.recording_key is None:
            continue

        roles_by_recording.setdefault(
            file.recording_key,
            set(),
        ).add(file.file_role)

    incomplete_recordings = {
        recording_key: roles
        for recording_key, roles
        in roles_by_recording.items()
        if roles != {"psg", "hypnogram"}
    }

    if incomplete_recordings:
        preview = list(
            incomplete_recordings.items()
        )[:5]

        raise RuntimeError(
            "Incomplete PSG/Hypnogram pairs: "
            f"{preview}"
        )


def select_sleep_edf_source_files(
    files: list[SleepEdfSourceFile],
    max_recordings: int,
    include_cassette: bool,
    include_telemetry: bool,
    include_metadata: bool,
    recording_keys: tuple[str, ...] = (),
) -> list[SleepEdfSourceFile]:
    if max_recordings < 0:
        raise ValueError(
            "max_recordings must be 0 or "
            "a positive integer"
        )

    enabled_study_folders: set[str] = set()

    if include_cassette:
        enabled_study_folders.add(
            "sleep-cassette"
        )

    if include_telemetry:
        enabled_study_folders.add(
            "sleep-telemetry"
        )

    eligible_data_files = [
        file
        for file in files
        if (
            file.file_role
            in {"psg", "hypnogram"}
            and file.study_folder
            in enabled_study_folders
        )
    ]

    available_recording_keys = sorted(
        {
            file.recording_key
            for file in eligible_data_files
            if file.recording_key is not None
        }
    )

    available_recording_key_set = set(
        available_recording_keys
    )

    if recording_keys:
        selected_recording_keys = set(
            recording_keys
        )

        missing_recording_keys = sorted(
            selected_recording_keys
            - available_recording_key_set
        )

        if missing_recording_keys:
            raise ValueError(
                "Requested Sleep-EDF recording "
                "keys are not available in the "
                "enabled collections: "
                f"{missing_recording_keys}"
            )

    elif max_recordings == 0:
        selected_recording_keys = (
            available_recording_key_set
        )

    else:
        selected_recording_keys = set(
            available_recording_keys[
                :max_recordings
            ]
        )

    selected_files = [
        file
        for file in eligible_data_files
        if file.recording_key
        in selected_recording_keys
    ]

    if include_metadata:
        selected_files.extend(
            file
            for file in files
            if file.file_role == "metadata"
        )

    selected_files = sorted(
        {
            file.relative_path: file
            for file in selected_files
        }.values(),
        key=lambda file: file.relative_path,
    )

    validate_complete_recording_pairs(
        selected_files
    )

    return selected_files


def build_sleep_edf_source_manifest(
    checksum_text: str,
    settings: Settings | None = None,
) -> list[SleepEdfSourceFile]:
    if settings is None:
        settings = get_settings()

    parsed_files = (
        parse_sleep_edf_checksum_manifest(
            checksum_text=checksum_text,
            dataset_version=(
                settings.sleep_edf_version
            ),
        )
    )

    if settings.data_profile == "full":
        validate_complete_recording_pairs(
            parsed_files
        )

        return parsed_files

    return select_sleep_edf_source_files(
        files=parsed_files,
        max_recordings=(
            settings.sleep_edf_max_recordings
        ),
        include_cassette=(
            settings.sleep_edf_include_cassette
        ),
        include_telemetry=(
            settings.sleep_edf_include_telemetry
        ),
        include_metadata=(
            settings.sleep_edf_include_metadata
        ),
        recording_keys=(
            settings.sleep_edf_recording_keys
        ),
    )


def print_sleep_edf_source_manifest(
    files: list[SleepEdfSourceFile],
) -> None:
    recording_keys = {
        file.recording_key
        for file in files
        if file.recording_key is not None
    }

    print(
        f"manifest_file_count={len(files)}"
    )
    print(
        "manifest_recording_count="
        f"{len(recording_keys)}"
    )

    for file in files:
        print(
            f"{file.file_role:10} | "
            f"{file.bucket:7} | "
            f"{file.object_key}"
        )
