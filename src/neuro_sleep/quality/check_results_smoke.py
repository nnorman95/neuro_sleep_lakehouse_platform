from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_failed,
    finish_pipeline_run_success,
    start_pipeline_run,
)
from neuro_sleep.quality.check_results import (
    create_quality_check_result,
    delete_quality_check_results_for_run,
    get_quality_check_results_for_run,
)


SOURCE_SYSTEM = "physionet_sleep_edf"
PIPELINE_NAME = "quality_check_results_smoke_test"
TASK_NAME = "persist_and_read_quality_results"


def run_smoke_test() -> None:
    run_id = start_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        task_name=TASK_NAME,
        source_system=SOURCE_SYSTEM,
    )

    recording_id = new_uuid7()

    try:
        passed_result_id = (
            create_quality_check_result(
                pipeline_run_id=run_id,
                source_system=SOURCE_SYSTEM,
                data_layer="silver",
                dataset_name=(
                    "silver_recording"
                ),
                recording_id=recording_id,
                record_key=(
                    "smoke-tests/"
                    "quality-check-results"
                ),
                check_name=(
                    "silver_quality_summary"
                ),
                severity="info",
                status="passed",
                rows_checked=10,
                rows_failed=0,
                details={
                    "error_count": 0,
                    "warning_count": 0,
                },
            )
        )

        failed_result_id = (
            create_quality_check_result(
                pipeline_run_id=run_id,
                source_system=SOURCE_SYSTEM,
                data_layer="silver",
                dataset_name=(
                    "silver_recording"
                ),
                recording_id=recording_id,
                record_key=(
                    "smoke-tests/"
                    "quality-check-results"
                ),
                check_name=(
                    "recording_duration_check"
                ),
                severity="error",
                status="failed",
                rows_checked=1,
                rows_failed=1,
                error_code=(
                    "SMOKE_INVALID_DURATION"
                ),
                message=(
                    "Simulated quality failure."
                ),
                details={
                    "duration_seconds": -1,
                },
            )
        )

        results = (
            get_quality_check_results_for_run(
                pipeline_run_id=run_id
            )
        )

        if len(results) != 2:
            raise RuntimeError(
                "Unexpected quality-result count"
            )

        result_ids = {
            result.quality_result_id
            for result in results
        }

        if result_ids != {
            passed_result_id,
            failed_result_id,
        }:
            raise RuntimeError(
                "Persisted quality-result IDs "
                "do not match"
            )

        if any(
            result.quality_result_id.version
            != 7
            for result in results
        ):
            raise RuntimeError(
                "Quality-result ID is not UUIDv7"
            )

        failed_results = [
            result
            for result in results
            if result.status == "failed"
        ]

        if len(failed_results) != 1:
            raise RuntimeError(
                "Failed quality result was "
                "not persisted"
            )

        if (
            failed_results[0]
            .details["duration_seconds"]
            != -1
        ):
            raise RuntimeError(
                "Quality-result JSON details "
                "did not round-trip"
            )

        try:
            create_quality_check_result(
                pipeline_run_id=run_id,
                data_layer="invalid-layer",
                dataset_name="test",
                check_name="test",
                severity="info",
                status="passed",
            )

        except ValueError:
            pass

        else:
            raise RuntimeError(
                "Invalid quality data layer "
                "was not blocked"
            )

        deleted_count = (
            delete_quality_check_results_for_run(
                pipeline_run_id=run_id
            )
        )

        if deleted_count != 2:
            raise RuntimeError(
                "Quality-result cleanup count "
                "is incorrect"
            )

        remaining_results = (
            get_quality_check_results_for_run(
                pipeline_run_id=run_id
            )
        )

        if remaining_results:
            raise RuntimeError(
                "Quality smoke rows survived "
                "cleanup"
            )

        finish_pipeline_run_success(
            run_id=run_id,
            rows_read=2,
            rows_written=2,
            files_processed=0,
            records_quarantined=0,
        )

        print(
            "quality_results_inserted=2"
        )
        print(
            "quality_result_uuid7=true"
        )
        print(
            "quality_result_json_round_trip=true"
        )
        print(
            "invalid_quality_layer_blocked=true"
        )
        print(
            "quality_results_cleanup=true"
        )
        print(
            "quality_check_results_smoke_status="
            "success"
        )

    except Exception as error:
        delete_quality_check_results_for_run(
            pipeline_run_id=run_id
        )

        finish_pipeline_run_failed(
            run_id=run_id,
            error_message=str(error),
            rows_read=0,
            rows_written=0,
            files_processed=0,
            records_quarantined=0,
        )

        raise


if __name__ == "__main__":
    run_smoke_test()
