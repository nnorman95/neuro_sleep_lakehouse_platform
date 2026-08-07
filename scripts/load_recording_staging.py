from neuro_sleep.staging.recording_load_job import (
    run_tracked_recording_staging_job,
)


def main() -> None:
    tracked_result = (
        run_tracked_recording_staging_job()
    )
    result = tracked_result.load_result

    print(f"run_id={tracked_result.run_id}")
    print(f"status={result.status}")
    print(
        "publication_count="
        f"{result.publication_count}"
    )
    print(
        "publications_written="
        f"{result.publications_written}"
    )
    print(
        "publications_skipped="
        f"{result.publications_skipped}"
    )
    print(
        "recordings_count="
        f"{result.recordings_count}"
    )
    print(
        "channels_count="
        f"{result.channels_count}"
    )
    print(
        "interval_count="
        f"{result.interval_count}"
    )
    print(
        f"epoch_count={result.epoch_count}"
    )
    print(
        f"rows_written={result.rows_written}"
    )
    print(
        "files_processed="
        f"{result.files_processed}"
    )
    print(
        "recording_staging_load=success"
    )


if __name__ == "__main__":
    main()
