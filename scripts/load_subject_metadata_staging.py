from neuro_sleep.staging.subject_metadata_load_job import (
    run_tracked_subject_metadata_staging_job,
)


def main() -> None:
    tracked_result = (
        run_tracked_subject_metadata_staging_job()
    )
    result = tracked_result.load_result

    print(f"run_id={tracked_result.run_id}")
    print(f"status={result.status}")
    print(
        "input_fingerprint="
        f"{result.input_fingerprint}"
    )
    print(
        "output_prefix="
        f"{result.output_prefix}"
    )
    print(
        f"subject_count={result.subject_count}"
    )
    print(
        "recording_context_count="
        f"{result.recording_context_count}"
    )
    print(
        f"rows_written={result.rows_written}"
    )
    print(
        "files_processed="
        f"{result.files_processed}"
    )
    print(
        "subject_metadata_staging_load="
        "success"
    )


if __name__ == "__main__":
    main()
