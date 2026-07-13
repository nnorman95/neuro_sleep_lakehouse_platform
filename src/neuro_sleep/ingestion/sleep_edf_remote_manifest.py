from dataclasses import dataclass
from pathlib import PurePosixPath

import requests

from neuro_sleep.config import Settings, get_settings
from neuro_sleep.reliability.errors import (
    SourceContentError,
)
from neuro_sleep.reliability.source_http import (
    fetch_text_with_retry,
)
from neuro_sleep.sources.sleep_edf import (
    SleepEdfControlArtifact,
    build_sleep_edf_control_artifacts,
    get_sleep_edf_checksum_url,
    get_sleep_edf_records_url,
    validate_sleep_edf_settings,
)
from neuro_sleep.sources.sleep_edf_manifest import (
    SleepEdfSourceFile,
    build_sleep_edf_source_manifest,
    parse_sleep_edf_checksum_manifest,
    validate_complete_recording_pairs,
)


KNOWN_VERSION_COUNTS = {
    "1.0.0": {
        "records": 197,
        "checksum_entries": 398,
    },
}


@dataclass(frozen=True)
class SleepEdfRemoteManifest:
    dataset_version: str
    records_url: str
    checksum_url: str

    records_text: str
    checksum_text: str

    record_paths: tuple[str, ...]
    all_files: tuple[SleepEdfSourceFile, ...]
    selected_files: tuple[SleepEdfSourceFile, ...]
    control_artifacts: tuple[SleepEdfControlArtifact, ...]

    @property
    def psg_file_count(self) -> int:
        return sum(
            file.file_role == "psg"
            for file in self.all_files
        )

    @property
    def hypnogram_file_count(self) -> int:
        return sum(
            file.file_role == "hypnogram"
            for file in self.all_files
        )

    @property
    def metadata_file_count(self) -> int:
        return sum(
            file.file_role == "metadata"
            for file in self.all_files
        )

    @property
    def recording_count(self) -> int:
        return len(
            {
                file.recording_key
                for file in self.all_files
                if file.recording_key is not None
            }
        )

    @property
    def selected_recording_count(self) -> int:
        return len(
            {
                file.recording_key
                for file in self.selected_files
                if file.recording_key is not None
            }
        )

    @property
    def full_extract_object_count(self) -> int:
        return (
            len(self.all_files)
            + len(self.control_artifacts)
        )

    @property
    def selected_extract_object_count(self) -> int:
        return (
            len(self.selected_files)
            + len(self.control_artifacts)
        )


def create_http_session(
    settings: Settings,
) -> requests.Session:
    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": (
                f"{settings.project_name}/{settings.env}"
            ),
            "Accept": "text/plain,*/*",
        }
    )

    return session




def parse_sleep_edf_records(
    records_text: str,
) -> tuple[str, ...]:
    record_paths: list[str] = []
    seen_paths: set[str] = set()

    for line_number, raw_line in enumerate(
        records_text.splitlines(),
        start=1,
    ):
        relative_path = raw_line.strip()

        if not relative_path:
            continue

        if relative_path.startswith("#"):
            continue

        if "\\" in relative_path:
            raise SourceContentError(
                "Backslashes are not allowed in RECORDS "
                f"line {line_number}: {relative_path}"
            )

        path = PurePosixPath(relative_path)

        if path.is_absolute():
            raise SourceContentError(
                "Absolute path is not allowed in RECORDS "
                f"line {line_number}: {relative_path}"
            )

        if ".." in path.parts:
            raise SourceContentError(
                "Parent path traversal is not allowed in "
                f"RECORDS line {line_number}: "
                f"{relative_path}"
            )

        if not relative_path.endswith("-PSG.edf"):
            raise SourceContentError(
                "Unexpected non-PSG file in RECORDS "
                f"line {line_number}: {relative_path}"
            )

        if relative_path in seen_paths:
            continue

        seen_paths.add(relative_path)
        record_paths.append(relative_path)

    if not record_paths:
        raise SourceContentError(
            "No PSG files were found in RECORDS"
        )

    return tuple(record_paths)


def validate_remote_manifest(
    dataset_version: str,
    record_paths: tuple[str, ...],
    all_files: list[SleepEdfSourceFile],
    selected_files: list[SleepEdfSourceFile],
    control_artifacts: tuple[
        SleepEdfControlArtifact,
        ...,
    ],
) -> None:
    record_path_set = set(record_paths)

    checksum_psg_paths = {
        file.relative_path
        for file in all_files
        if file.file_role == "psg"
    }

    missing_from_checksums = sorted(
        record_path_set - checksum_psg_paths
    )

    extra_psg_files = sorted(
        checksum_psg_paths - record_path_set
    )

    if missing_from_checksums:
        raise SourceContentError(
            "PSG files from RECORDS are missing from "
            f"SHA256SUMS.txt: {missing_from_checksums[:5]}"
        )

    if extra_psg_files:
        raise SourceContentError(
            "SHA256SUMS.txt contains PSG files not listed "
            f"in RECORDS: {extra_psg_files[:5]}"
        )

    validate_complete_recording_pairs(all_files)
    validate_complete_recording_pairs(selected_files)

    all_paths = {
        file.relative_path
        for file in all_files
    }

    selected_paths = {
        file.relative_path
        for file in selected_files
    }

    unknown_selected_paths = sorted(
        selected_paths - all_paths
    )

    if unknown_selected_paths:
        raise SourceContentError(
            "Selected files are missing from the complete "
            f"manifest: {unknown_selected_paths[:5]}"
        )

    if len(control_artifacts) != 1:
        raise SourceContentError(
            "Exactly one checksum control artifact "
            "was expected"
        )

    checksum_artifact = control_artifacts[0]

    if checksum_artifact.file_name != "SHA256SUMS.txt":
        raise SourceContentError(
            "Unexpected checksum control artifact: "
            f"{checksum_artifact.file_name}"
        )

    if "SHA256SUMS.txt" in all_paths:
        raise SourceContentError(
            "SHA256SUMS.txt must be handled separately "
            "as a control artifact"
        )

    known_counts = KNOWN_VERSION_COUNTS.get(
        dataset_version
    )

    if known_counts is not None:
        expected_records = known_counts["records"]
        expected_checksum_entries = (
            known_counts["checksum_entries"]
        )

        if len(record_paths) != expected_records:
            raise SourceContentError(
                "Unexpected RECORDS entry count for "
                f"version {dataset_version}: "
                f"expected={expected_records}, "
                f"actual={len(record_paths)}"
            )

        if len(all_files) != expected_checksum_entries:
            raise SourceContentError(
                "Unexpected SHA256SUMS entry count for "
                f"version {dataset_version}: "
                f"expected={expected_checksum_entries}, "
                f"actual={len(all_files)}"
            )


def fetch_sleep_edf_remote_manifest(
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> SleepEdfRemoteManifest:
    if settings is None:
        settings = get_settings()

    validate_sleep_edf_settings(settings)

    records_url = get_sleep_edf_records_url(
        settings.sleep_edf_version
    )

    checksum_url = get_sleep_edf_checksum_url(
        settings.sleep_edf_version
    )

    owns_session = session is None

    if session is None:
        session = create_http_session(settings)

    try:
        records_text = fetch_text_with_retry(
            session=session,
            url=records_url,
            resource_name="RECORDS",
        )

        checksum_text = fetch_text_with_retry(
            session=session,
            url=checksum_url,
            resource_name="SHA256SUMS.txt",
        )

        try:
            record_paths = parse_sleep_edf_records(
                records_text
            )

            all_files = (
                parse_sleep_edf_checksum_manifest(
                    checksum_text=checksum_text,
                    dataset_version=(
                        settings.sleep_edf_version
                    ),
                )
            )

            selected_files = (
                build_sleep_edf_source_manifest(
                    checksum_text=checksum_text,
                    settings=settings,
                )
            )

            control_artifacts = (
                build_sleep_edf_control_artifacts(
                    settings=settings
                )
            )

            validate_remote_manifest(
                dataset_version=(
                    settings.sleep_edf_version
                ),
                record_paths=record_paths,
                all_files=all_files,
                selected_files=selected_files,
                control_artifacts=control_artifacts,
            )

        except SourceContentError:
            raise

        except (ValueError, RuntimeError) as exc:
            raise SourceContentError(
                "Invalid Sleep-EDF remote manifest: "
                f"{exc}"
            ) from exc

        return SleepEdfRemoteManifest(
            dataset_version=settings.sleep_edf_version,
            records_url=records_url,
            checksum_url=checksum_url,
            records_text=records_text,
            checksum_text=checksum_text,
            record_paths=record_paths,
            all_files=tuple(all_files),
            selected_files=tuple(selected_files),
            control_artifacts=control_artifacts,
        )

    finally:
        if owns_session:
            session.close()


def print_remote_manifest_summary(
    manifest: SleepEdfRemoteManifest,
) -> None:
    print(
        f"dataset_version={manifest.dataset_version}"
    )
    print(f"records_url={manifest.records_url}")
    print(f"checksum_url={manifest.checksum_url}")

    print(
        "records_entry_count="
        f"{len(manifest.record_paths)}"
    )

    print(
        "checksum_entry_count="
        f"{len(manifest.all_files)}"
    )

    print(
        f"psg_file_count={manifest.psg_file_count}"
    )

    print(
        "hypnogram_file_count="
        f"{manifest.hypnogram_file_count}"
    )

    print(
        "metadata_file_count="
        f"{manifest.metadata_file_count}"
    )

    print(
        f"recording_count={manifest.recording_count}"
    )

    print(
        "control_artifact_count="
        f"{len(manifest.control_artifacts)}"
    )

    print(
        "full_extract_object_count="
        f"{manifest.full_extract_object_count}"
    )

    print(
        "selected_source_file_count="
        f"{len(manifest.selected_files)}"
    )

    print(
        "selected_recording_count="
        f"{manifest.selected_recording_count}"
    )

    print(
        "selected_extract_object_count="
        f"{manifest.selected_extract_object_count}"
    )

    preview_limit = 12

    for source_file in manifest.selected_files[
        :preview_limit
    ]:
        print(
            f"selected={source_file.file_role:10} | "
            f"{source_file.relative_path}"
        )

    omitted_count = (
        len(manifest.selected_files)
        - preview_limit
    )

    if omitted_count > 0:
        print(
            f"selected_preview_omitted={omitted_count}"
        )

    for artifact in manifest.control_artifacts:
        print(
            f"control={artifact.artifact_role:17} | "
            f"{artifact.object_key}"
        )


def main() -> None:
    settings = get_settings()

    manifest = fetch_sleep_edf_remote_manifest(
        settings=settings
    )

    print(f"data_profile={settings.data_profile}")

    print_remote_manifest_summary(manifest)

    print("real_http_requests=2")
    print("downloaded_edf_files=0")
    print("remote_manifest_status=success")


if __name__ == "__main__":
    main()
