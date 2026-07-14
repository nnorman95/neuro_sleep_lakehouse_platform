from pathlib import Path

from neuro_sleep.silver.bronze_edf_reader import (
    open_bronze_edf_pair,
)


BUCKET = "bronze"

PSG_OBJECT_KEY = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/"
    "SC4001E0-PSG.edf"
)

HYPNOGRAM_OBJECT_KEY = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/"
    "SC4001EC-Hypnogram.edf"
)


def run_smoke_test() -> None:
    psg_path: Path | None = None
    hypnogram_path: Path | None = None

    with open_bronze_edf_pair(
        psg_bucket=BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    ) as pair:
        psg_path = pair.psg.local_path
        hypnogram_path = (
            pair.hypnogram.local_path
        )

        if not psg_path.is_file():
            raise RuntimeError(
                "Temporary PSG file missing"
            )

        if not hypnogram_path.is_file():
            raise RuntimeError(
                "Temporary Hypnogram "
                "file missing"
            )

        if pair.psg.file_size_bytes <= 0:
            raise RuntimeError(
                "PSG file is empty"
            )

        if (
            pair.hypnogram
            .file_size_bytes
            <= 0
        ):
            raise RuntimeError(
                "Hypnogram file is empty"
            )

        if pair.psg.document.num_signals != 7:
            raise RuntimeError(
                "Unexpected PSG signal count"
            )

        annotations = tuple(
            pair.hypnogram
            .document
            .annotations
        )

        if not annotations:
            raise RuntimeError(
                "Hypnogram annotations "
                "were not loaded"
            )

        if (
            pair.psg.document.startdate
            != pair.hypnogram
            .document
            .startdate
            or pair.psg.document.starttime
            != pair.hypnogram
            .document
            .starttime
        ):
            raise RuntimeError(
                "PSG/Hypnogram start "
                "metadata mismatch"
            )

        print(
            "bronze_psg_downloaded=true"
        )
        print(
            "bronze_hypnogram_downloaded=true"
        )
        print(
            "download_size_verified=true"
        )
        print(
            "psg_signal_count=7"
        )
        print(
            "hypnogram_annotation_count="
            f"{len(annotations)}"
        )
        print(
            "pair_start_metadata_matches=true"
        )

    if psg_path is None:
        raise RuntimeError(
            "PSG path was not captured"
        )

    if hypnogram_path is None:
        raise RuntimeError(
            "Hypnogram path was not captured"
        )

    if psg_path.exists():
        raise RuntimeError(
            "Temporary PSG file was not "
            "cleaned up"
        )

    if hypnogram_path.exists():
        raise RuntimeError(
            "Temporary Hypnogram file was "
            "not cleaned up"
        )

    print(
        "temporary_edf_cleanup=true"
    )
    print(
        "bronze_edf_reader_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
