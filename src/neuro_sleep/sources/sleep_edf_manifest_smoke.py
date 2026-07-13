from neuro_sleep.sources.sleep_edf import (
    build_sleep_edf_control_artifacts,
)
from neuro_sleep.sources.sleep_edf_manifest import (
    parse_sleep_edf_checksum_manifest,
    print_sleep_edf_source_manifest,
    select_sleep_edf_source_files,
)


SAMPLE_CHECKSUMS = """
1111111111111111111111111111111111111111111111111111111111111111 sleep-cassette/SC4001E0-PSG.edf
2222222222222222222222222222222222222222222222222222222222222222 sleep-cassette/SC4001EC-Hypnogram.edf
3333333333333333333333333333333333333333333333333333333333333333 sleep-cassette/SC4002E0-PSG.edf
4444444444444444444444444444444444444444444444444444444444444444 sleep-cassette/SC4002EC-Hypnogram.edf
5555555555555555555555555555555555555555555555555555555555555555 sleep-telemetry/ST7011J0-PSG.edf
6666666666666666666666666666666666666666666666666666666666666666 sleep-telemetry/ST7011JP-Hypnogram.edf
7777777777777777777777777777777777777777777777777777777777777777 RECORDS
8888888888888888888888888888888888888888888888888888888888888888 SC-subjects.xls
9999999999999999999999999999999999999999999999999999999999999999 ST-subjects.xls
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa SC-cassette.xml
"""


def run_smoke_test() -> None:
    parsed_files = parse_sleep_edf_checksum_manifest(
        checksum_text=SAMPLE_CHECKSUMS,
        dataset_version="1.0.0",
    )

    limited_files = select_sleep_edf_source_files(
        files=parsed_files,
        max_recordings=2,
        include_cassette=True,
        include_telemetry=True,
        include_metadata=True,
    )

    full_files = select_sleep_edf_source_files(
        files=parsed_files,
        max_recordings=0,
        include_cassette=True,
        include_telemetry=True,
        include_metadata=True,
    )

    control_artifacts = (
        build_sleep_edf_control_artifacts()
    )

    if len(control_artifacts) != 1:
        raise RuntimeError(
            "Expected exactly one control artifact"
        )

    control_artifact = control_artifacts[0]

    if control_artifact.file_name != "SHA256SUMS.txt":
        raise RuntimeError(
            "Unexpected control artifact name: "
            f"{control_artifact.file_name}"
        )

    if not control_artifact.object_key.endswith(
        "/SHA256SUMS.txt"
    ):
        raise RuntimeError(
            "Unexpected control artifact object key: "
            f"{control_artifact.object_key}"
        )

    expected_parsed_count = 10
    expected_limited_count = 8
    expected_full_count = 10

    if len(parsed_files) != expected_parsed_count:
        raise RuntimeError(
            "Unexpected parsed file count: "
            f"{len(parsed_files)}"
        )

    if len(limited_files) != expected_limited_count:
        raise RuntimeError(
            "Unexpected limited file count: "
            f"{len(limited_files)}"
        )

    if len(full_files) != expected_full_count:
        raise RuntimeError(
            "Full mode did not select all source files"
        )

    limited_recordings = {
        file.recording_key
        for file in limited_files
        if file.recording_key is not None
    }

    expected_recordings = {
        "SC4001E",
        "SC4002E",
    }

    if limited_recordings != expected_recordings:
        raise RuntimeError(
            "Unexpected selected recordings: "
            f"{limited_recordings}"
        )

    incomplete_files = [
        file
        for file in parsed_files
        if file.relative_path
        != (
            "sleep-cassette/"
            "SC4001EC-Hypnogram.edf"
        )
    ]

    try:
        select_sleep_edf_source_files(
            files=incomplete_files,
            max_recordings=1,
            include_cassette=True,
            include_telemetry=False,
            include_metadata=False,
        )
    except RuntimeError as exc:
        print(f"incomplete_pair_blocked={exc}")
    else:
        raise RuntimeError(
            "Incomplete recording pair was not blocked"
        )

    try:
        parse_sleep_edf_checksum_manifest(
            checksum_text=(
                "b" * 64
                + " ../../.env"
            ),
            dataset_version="1.0.0",
        )
    except ValueError as exc:
        print(f"unsafe_path_blocked={exc}")
    else:
        raise RuntimeError(
            "Unsafe path was not blocked"
        )

    print(
        f"parsed_file_count={len(parsed_files)}"
    )
    print(
        "limited_recording_count="
        f"{len(limited_recordings)}"
    )
    print(
        "limited_file_count="
        f"{len(limited_files)}"
    )
    print(
        f"full_file_count={len(full_files)}"
    )
    print(
        "control_artifact_count="
        f"{len(control_artifacts)}"
    )
    print(
        "control_artifact_object_key="
        f"{control_artifact.object_key}"
    )
    print("downloaded_bytes=0")

    print_sleep_edf_source_manifest(
        limited_files
    )

    print("smoke_test_status=success")


if __name__ == "__main__":
    run_smoke_test()
