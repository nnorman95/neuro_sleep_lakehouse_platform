from dataclasses import replace
from datetime import datetime, timezone

from neuro_sleep.config import (
    get_settings,
)
from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.raw.models import RawFileRecord
from neuro_sleep.silver.batch_discovery import (
    build_recording_pairs,
    discover_sleep_edf_recording_pairs,
)


def build_fake_raw_file(
    *,
    object_key: str,
    file_name: str,
) -> RawFileRecord:
    return RawFileRecord(
        file_id=new_uuid7(),
        source_system="physionet_sleep_edf",
        source_url=None,
        bucket="bronze",
        object_key=object_key,
        file_name=file_name,
        file_type="edf",
        file_size_bytes=1,
        checksum_sha256="0" * 64,
        ingestion_run_id=new_uuid7(),
        status="uploaded",
        ingested_at=datetime.now(
            timezone.utc
        ),
    )


def run_smoke_test() -> None:
    settings = get_settings()

    pairs = (
        discover_sleep_edf_recording_pairs(
            settings=settings
        )
    )

    if not pairs:
        raise RuntimeError(
            "Batch discovery returned no pairs"
        )

    pair_keys = {
        (
            pair.study_folder,
            pair.recording_key,
        )
        for pair in pairs
    }

    if len(pair_keys) != len(pairs):
        raise RuntimeError(
            "Batch discovery returned "
            "duplicate recording pairs"
        )

    for pair in pairs:
        if not pair.psg_object_key.endswith(
            "-PSG.edf"
        ):
            raise RuntimeError(
                "Discovered PSG key is invalid"
            )

        if not (
            pair.hypnogram_object_key.endswith(
                "-Hypnogram.edf"
            )
        ):
            raise RuntimeError(
                "Discovered Hypnogram key "
                "is invalid"
            )

        if not pair.silver_root_prefix.endswith(
            pair.psg_object_key
            .split("/")[-1]
            .removesuffix("-PSG.edf")
        ):
            raise RuntimeError(
                "Silver root prefix does not "
                "match the PSG recording"
            )

    if (
        settings.data_profile == "sample"
        and settings.sleep_edf_max_recordings
        > 0
        and len(pairs)
        > settings.sleep_edf_max_recordings
    ):
        raise RuntimeError(
            "Batch discovery ignored the "
            "sample recording limit"
        )

    fake_prefix = (
        "physionet/sleep-edfx/"
        f"{settings.sleep_edf_version}/"
        "sleep-cassette/"
    )

    incomplete_files = (
        build_fake_raw_file(
            object_key=(
                fake_prefix
                + "ZZ0001A0-PSG.edf"
            ),
            file_name=(
                "ZZ0001A0-PSG.edf"
            ),
        ),
    )

    try:
        build_recording_pairs(
            raw_files=incomplete_files,
            settings=settings,
        )

    except RuntimeError:
        pass

    else:
        raise RuntimeError(
            "Incomplete recording pair "
            "was not blocked"
        )

    complete_files = (
        build_fake_raw_file(
            object_key=(
                fake_prefix
                + "ZZ0001A0-PSG.edf"
            ),
            file_name=(
                "ZZ0001A0-PSG.edf"
            ),
        ),
        build_fake_raw_file(
            object_key=(
                fake_prefix
                + "ZZ0001AC-Hypnogram.edf"
            ),
            file_name=(
                "ZZ0001AC-Hypnogram.edf"
            ),
        ),
    )

    complete_settings = replace(
        settings,
        data_profile="sample",
        sleep_edf_max_recordings=1,
        sleep_edf_include_cassette=True,
        sleep_edf_include_telemetry=False,
    )

    complete_pairs = build_recording_pairs(
        raw_files=complete_files,
        settings=complete_settings,
    )

    if len(complete_pairs) != 1:
        raise RuntimeError(
            "Complete fake recording pair "
            "was not discovered"
        )

    cassette_count = sum(
        pair.study_folder
        == "sleep-cassette"
        for pair in pairs
    )

    telemetry_count = sum(
        pair.study_folder
        == "sleep-telemetry"
        for pair in pairs
    )

    print(
        f"batch_recording_count={len(pairs)}"
    )
    print(
        f"batch_cassette_count={cassette_count}"
    )
    print(
        f"batch_telemetry_count={telemetry_count}"
    )
    print(
        "batch_recording_keys_unique=true"
    )
    print(
        "batch_pairs_complete=true"
    )
    print(
        "batch_root_prefixes_valid=true"
    )
    print(
        "incomplete_batch_pair_blocked=true"
    )

    for index, pair in enumerate(
        pairs,
        start=1,
    ):
        print(
            "batch_pair="
            f"{index}/{len(pairs)}|"
            f"{pair.study_folder}|"
            f"{pair.recording_key}|"
            f"{pair.psg_object_key}|"
            f"{pair.hypnogram_object_key}"
        )

    print(
        "silver_batch_discovery_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
