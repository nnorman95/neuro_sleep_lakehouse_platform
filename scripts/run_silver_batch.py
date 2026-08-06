from neuro_sleep.silver.batch_job import (
    run_silver_batch,
)


def main() -> int:
    result = run_silver_batch(
        continue_on_error=True,
        signal_chunk_duration_seconds=1800.0,
        signal_start_seconds=0.0,
        signal_stop_seconds=None,
        verify_payload_checksums=True,
    )

    print(
        "silver_batch_recording_count="
        f"{result.recording_count}"
    )
    print(
        "silver_batch_written_count="
        f"{result.written_count}"
    )
    print(
        "silver_batch_skipped_count="
        f"{result.skipped_count}"
    )
    print(
        "silver_batch_failed_count="
        f"{result.failed_count}"
    )
    print(
        "silver_batch_total_row_count="
        f"{result.total_row_count}"
    )

    for index, item in enumerate(
        result.items,
        start=1,
    ):
        print(
            "silver_batch_item="
            f"{index}/{result.recording_count}|"
            f"{item.pair.study_folder}|"
            f"{item.pair.recording_key}|"
            f"{item.status}|"
            f"run_id={item.run_id}|"
            f"recording_id="
            f"{item.recording_id}|"
            f"rows={item.row_count}|"
            f"data_objects="
            f"{item.data_object_count}|"
            f"error_type="
            f"{item.error_type}|"
            f"error_message="
            f"{item.error_message}"
        )

    print(
        "silver_batch_run_status="
        + (
            "success"
            if result.passed
            else "failed"
        )
    )

    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
