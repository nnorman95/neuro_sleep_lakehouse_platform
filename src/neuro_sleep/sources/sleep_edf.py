from dataclasses import dataclass
from typing import Literal

from neuro_sleep.config import Settings, get_settings


SOURCE_SYSTEM = "physionet_sleep_edf"
DATASET_NAME = "Sleep-EDF Database Expanded"
DEFAULT_DATASET_VERSION = "1.0.0"

BASE_URL_TEMPLATE = (
    "https://physionet.org/files/"
    "sleep-edfx/{version}"
)

BRONZE_BUCKET = "bronze"

RECORDS_FILE_NAME = "RECORDS"
CHECKSUM_FILE_NAME = "SHA256SUMS.txt"

SleepEdfFolder = Literal[
    "sleep-cassette",
    "sleep-telemetry",
]


@dataclass(frozen=True)
class SleepEdfControlArtifact:
    dataset_version: str
    file_name: str
    artifact_role: str = "checksum_manifest"

    @property
    def source_system(self) -> str:
        return SOURCE_SYSTEM

    @property
    def source_url(self) -> str:
        return (
            f"{get_sleep_edf_base_url(self.dataset_version)}/"
            f"{self.file_name}"
        )

    @property
    def bucket(self) -> str:
        return BRONZE_BUCKET

    @property
    def object_key(self) -> str:
        return (
            "physionet/sleep-edfx/"
            f"{self.dataset_version}/"
            f"{self.file_name}"
        )

    @property
    def file_type(self) -> str:
        return "txt"


@dataclass(frozen=True)
class SleepEdfDatasetArea:
    source_system: str
    dataset_name: str
    dataset_version: str
    folder: SleepEdfFolder
    description: str
    expected_formats: tuple[str, ...]

    @property
    def base_url(self) -> str:
        return BASE_URL_TEMPLATE.format(
            version=self.dataset_version
        )

    @property
    def source_url(self) -> str:
        return f"{self.base_url}/{self.folder}/"

    @property
    def bucket(self) -> str:
        return BRONZE_BUCKET

    @property
    def object_key_prefix(self) -> str:
        return (
            "physionet/sleep-edfx/"
            f"{self.dataset_version}/"
            f"{self.folder}/"
        )


def get_sleep_edf_base_url(
    dataset_version: str,
) -> str:
    return BASE_URL_TEMPLATE.format(
        version=dataset_version
    )


def get_sleep_edf_records_url(
    dataset_version: str,
) -> str:
    return (
        f"{get_sleep_edf_base_url(dataset_version)}/"
        f"{RECORDS_FILE_NAME}"
    )


def get_sleep_edf_checksum_url(
    dataset_version: str,
) -> str:
    return (
        f"{get_sleep_edf_base_url(dataset_version)}/"
        f"{CHECKSUM_FILE_NAME}"
    )


def build_sleep_edf_control_artifacts(
    settings: Settings | None = None,
) -> tuple[SleepEdfControlArtifact, ...]:
    if settings is None:
        settings = get_settings()

    validate_sleep_edf_settings(settings)

    return (
        SleepEdfControlArtifact(
            dataset_version=settings.sleep_edf_version,
            file_name=CHECKSUM_FILE_NAME,
        ),
    )


def validate_sleep_edf_settings(
    settings: Settings | None = None,
) -> None:
    if settings is None:
        settings = get_settings()

    if settings.active_source != "sleep_edf":
        raise ValueError(
            "ACTIVE_SOURCE must be 'sleep_edf', "
            f"got '{settings.active_source}'"
        )

    if (
        settings.data_profile == "sample"
        and not settings.sleep_edf_include_cassette
        and not settings.sleep_edf_include_telemetry
    ):
        raise ValueError(
            "At least one Sleep-EDF study collection "
            "must be enabled"
        )


def build_sleep_edf_dataset_areas(
    settings: Settings | None = None,
) -> list[SleepEdfDatasetArea]:
    if settings is None:
        settings = get_settings()

    validate_sleep_edf_settings(settings)

    if settings.data_profile == "full":
        include_cassette = True
        include_telemetry = True
    else:
        include_cassette = (
            settings.sleep_edf_include_cassette
        )
        include_telemetry = (
            settings.sleep_edf_include_telemetry
        )

    areas: list[SleepEdfDatasetArea] = []

    if include_cassette:
        areas.append(
            SleepEdfDatasetArea(
                source_system=SOURCE_SYSTEM,
                dataset_name=DATASET_NAME,
                dataset_version=(
                    settings.sleep_edf_version
                ),
                folder="sleep-cassette",
                description=(
                    "Sleep Cassette PSG recordings "
                    "and matching hypnograms."
                ),
                expected_formats=("edf",),
            )
        )

    if include_telemetry:
        areas.append(
            SleepEdfDatasetArea(
                source_system=SOURCE_SYSTEM,
                dataset_name=DATASET_NAME,
                dataset_version=(
                    settings.sleep_edf_version
                ),
                folder="sleep-telemetry",
                description=(
                    "Sleep Telemetry PSG recordings "
                    "and matching hypnograms."
                ),
                expected_formats=("edf",),
            )
        )

    return areas


def print_dataset_areas(
    areas: list[SleepEdfDatasetArea],
) -> None:
    print(f"dataset_name={DATASET_NAME}")
    print(f"source_system={SOURCE_SYSTEM}")
    print(f"area_count={len(areas)}")

    for area in areas:
        formats = ",".join(area.expected_formats)

        print(
            f"{area.folder:16} | "
            f"{area.bucket:7} | "
            f"{area.object_key_prefix} | "
            f"formats={formats} | "
            f"{area.source_url}"
        )


def main() -> None:
    settings = get_settings()

    validate_sleep_edf_settings(settings)

    areas = build_sleep_edf_dataset_areas(
        settings
    )

    print_dataset_areas(areas)

    print(
        "records_url="
        f"{get_sleep_edf_records_url(
            settings.sleep_edf_version
        )}"
    )

    print(
        "checksum_url="
        f"{get_sleep_edf_checksum_url(
            settings.sleep_edf_version
        )}"
    )

    print("access_model=open")
    print("credentials_required=false")
    print("source_check_status=success")


if __name__ == "__main__":
    main()
