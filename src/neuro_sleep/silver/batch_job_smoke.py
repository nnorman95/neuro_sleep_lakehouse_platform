from contextlib import redirect_stdout
from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import neuro_sleep.silver.batch_job as batch_job
from neuro_sleep.config import get_settings
from neuro_sleep.identifiers import new_uuid7
from neuro_sleep.silver.batch_discovery import (
    SleepEdfRecordingPair,
)


def build_pair(
    recording_key: str,
) -> SleepEdfRecordingPair:
    base = (
        "physionet/sleep-edfx/1.0.0/"
        "sleep-cassette/"
        f"{recording_key}"
    )

    return SleepEdfRecordingPair(
        dataset_version="1.0.0",
        study_folder="sleep-cassette",
        recording_key=recording_key,
        psg_bucket="bronze",
        psg_object_key=(
            f"{base}0-PSG.edf"
        ),
        hypnogram_bucket="bronze",
        hypnogram_object_key=(
            f"{base}C-Hypnogram.edf"
        ),
        silver_root_prefix=(
            f"{base}0"
        ),
    )


def build_tracked_result(
    *,
    status: str,
    row_count: int,
):
    pipeline_result = SimpleNamespace(
        data_object_count=18,
        total_object_count=19,
    )

    return SimpleNamespace(
        run_id=new_uuid7(),
        status=status,
        recording_id=new_uuid7(),
        output_prefix=(
            "smoke-tests/silver-batch/"
            f"{status}"
        ),
        row_count=row_count,
        pipeline_result=pipeline_result,
    )


def run_continue_case() -> None:
    pairs = (
        build_pair("AA0001A"),
        build_pair("AA0002A"),
        build_pair("AA0003A"),
    )

    written_result = build_tracked_result(
        status="written",
        row_count=100,
    )

    skipped_result = build_tracked_result(
        status="skipped",
        row_count=200,
    )

    simulated_error = RuntimeError(
        "Simulated batch item failure"
    )

    tracked_job = Mock(
        side_effect=(
            written_result,
            skipped_result,
            simulated_error,
        )
    )

    progress_output = StringIO()

    with (
        patch.object(
            batch_job,
            "discover_sleep_edf_recording_pairs",
            return_value=pairs,
        ),
        patch.object(
            batch_job,
            "run_tracked_silver_job",
            tracked_job,
        ),
        patch.object(
            batch_job,
            "emit_event",
        ),
        patch.object(
            batch_job,
            "emit_exception",
        ),
        redirect_stdout(progress_output),
    ):
        result = batch_job.run_silver_batch(
            settings=get_settings(),
            continue_on_error=True,
            signal_stop_seconds=60.0,
        )

    rendered_progress = (
        progress_output.getvalue()
    )

    required_progress_fragments = (
        "[0/3] | 0%",
        "[1/3] | 33%",
        "[2/3] | 67%",
        "[3/3] | 100%",
        "AA0001A | written",
        "AA0002A | skipped",
        "AA0003A | failed",
        "rows=100",
        "rows=200",
        "elapsed=",
    )

    for fragment in (
        required_progress_fragments
    ):
        if fragment not in rendered_progress:
            raise RuntimeError(
                "Missing batch progress "
                f"fragment: {fragment}"
            )

    if result.recording_count != 3:
        raise RuntimeError(
            "Batch recording count is wrong"
        )

    if result.written_count != 1:
        raise RuntimeError(
            "Batch written count is wrong"
        )

    if result.skipped_count != 1:
        raise RuntimeError(
            "Batch skipped count is wrong"
        )

    if result.failed_count != 1:
        raise RuntimeError(
            "Batch failed count is wrong"
        )

    if result.successful_count != 2:
        raise RuntimeError(
            "Batch successful count is wrong"
        )

    if result.total_row_count != 300:
        raise RuntimeError(
            "Batch row count is wrong"
        )

    if result.passed:
        raise RuntimeError(
            "Failed batch was marked passed"
        )

    failed_item = result.items[2]

    if (
        failed_item.error_type
        != "RuntimeError"
    ):
        raise RuntimeError(
            "Batch error type was not kept"
        )

    if (
        failed_item.error_message
        != str(simulated_error)
    ):
        raise RuntimeError(
            "Batch error message was not kept"
        )

    if tracked_job.call_count != 3:
        raise RuntimeError(
            "Batch did not attempt every pair"
        )

    print(
        "batch_written_count=1"
    )
    print(
        "batch_skipped_count=1"
    )
    print(
        "batch_failed_count=1"
    )
    print(
        "batch_continue_on_error=true"
    )
    print(
        "batch_failure_details_preserved=true"
    )
    print(
        "batch_progress_percentages=true"
    )
    print(
        "batch_progress_recording_status=true"
    )
    print(
        "batch_progress_rows_and_elapsed=true"
    )


def run_fail_fast_case() -> None:
    pairs = (
        build_pair("BB0001A"),
        build_pair("BB0002A"),
    )

    simulated_error = RuntimeError(
        "Simulated fail-fast error"
    )

    tracked_job = Mock(
        side_effect=simulated_error
    )

    with (
        patch.object(
            batch_job,
            "discover_sleep_edf_recording_pairs",
            return_value=pairs,
        ),
        patch.object(
            batch_job,
            "run_tracked_silver_job",
            tracked_job,
        ),
        patch.object(
            batch_job,
            "emit_event",
        ),
        patch.object(
            batch_job,
            "emit_exception",
        ),
    ):
        try:
            batch_job.run_silver_batch(
                settings=get_settings(),
                continue_on_error=False,
                signal_stop_seconds=60.0,
            )

        except RuntimeError as error:
            if error is not simulated_error:
                raise RuntimeError(
                    "Fail-fast changed the "
                    "original exception"
                ) from error

        else:
            raise RuntimeError(
                "Fail-fast batch did not raise"
            )

    if tracked_job.call_count != 1:
        raise RuntimeError(
            "Fail-fast batch attempted more "
            "than one pair"
        )

    print(
        "batch_fail_fast=true"
    )
    print(
        "batch_original_error_preserved=true"
    )


def run_smoke_test() -> None:
    run_continue_case()
    run_fail_fast_case()

    print(
        "silver_batch_job_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
