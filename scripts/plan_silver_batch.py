from neuro_sleep.silver.batch_discovery import (
    discover_sleep_edf_recording_pairs,
)


def main() -> None:
    pairs = (
        discover_sleep_edf_recording_pairs()
    )

    print(
        f"batch_recording_count={len(pairs)}"
    )

    for index, pair in enumerate(
        pairs,
        start=1,
    ):
        print(
            f"{index}/{len(pairs)} "
            f"collection={pair.study_folder} "
            f"recording_key="
            f"{pair.recording_key}"
        )
        print(
            f"  psg={pair.psg_bucket}/"
            f"{pair.psg_object_key}"
        )
        print(
            "  hypnogram="
            f"{pair.hypnogram_bucket}/"
            f"{pair.hypnogram_object_key}"
        )
        print(
            "  silver_root_prefix="
            f"{pair.silver_root_prefix}"
        )

    print(
        "silver_batch_plan_status=success"
    )


if __name__ == "__main__":
    main()
