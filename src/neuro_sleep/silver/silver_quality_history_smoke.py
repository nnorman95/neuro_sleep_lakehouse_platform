from neuro_sleep.quality.check_results import (
    delete_quality_check_results_for_run,
    get_quality_check_results_for_run,
)
from neuro_sleep.silver.silver_job import (
    run_tracked_silver_job,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
)
from neuro_sleep.identifiers import new_uuid7


BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

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


def delete_smoke_prefix(
    root_prefix: str,
) -> None:
    client = get_object_storage_client()

    try:
        objects = list_object_summaries(
            bucket=SILVER_BUCKET,
            prefix=root_prefix + "/",
            client=client,
        )

        for item in objects:
            client.delete_object(
                Bucket=SILVER_BUCKET,
                Key=item.object_key,
            )

    finally:
        client.close()


def run_smoke_test() -> None:
    smoke_id = new_uuid7()

    root_prefix = (
        "smoke-tests/"
        "silver-quality-history/"
        f"run_id={smoke_id}"
    )

    written_run_id = None
    skipped_run_id = None

    try:
        written_result = (
            run_tracked_silver_job(
                psg_bucket=BRONZE_BUCKET,
                psg_object_key=(
                    PSG_OBJECT_KEY
                ),
                hypnogram_bucket=(
                    BRONZE_BUCKET
                ),
                hypnogram_object_key=(
                    HYPNOGRAM_OBJECT_KEY
                ),
                silver_bucket=(
                    SILVER_BUCKET
                ),
                root_prefix=root_prefix,
                signal_chunk_duration_seconds=(
                    30.0
                ),
                signal_start_seconds=0.0,
                signal_stop_seconds=60.0,
                verify_payload_checksums=True,
            )
        )

        written_run_id = written_result.run_id

        if written_result.status != "written":
            raise RuntimeError(
                "Quality-history Silver run "
                "was not written"
            )

        written_rows = (
            get_quality_check_results_for_run(
                written_run_id
            )
        )

        if len(written_rows) != 4:
            raise RuntimeError(
                "Expected one quality summary "
                "and three warning rows"
            )

        summaries = [
            row
            for row in written_rows
            if row.check_name
            == "silver_quality_gate"
        ]

        if len(summaries) != 1:
            raise RuntimeError(
                "Silver quality summary row "
                "is missing"
            )

        summary = summaries[0]

        if (
            summary.status != "warning"
            or summary.severity != "warning"
        ):
            raise RuntimeError(
                "Silver quality summary does "
                "not reflect warnings"
            )

        expected_codes = {
            "TRAILING_HYPNOGRAM_OVERHANG",
            "MISSING_CHANNEL_UNITS",
            "SPECIAL_SLEEP_STAGE_LABELS",
        }

        actual_codes = {
            row.error_code
            for row in written_rows
            if row.error_code is not None
        }

        if actual_codes != expected_codes:
            raise RuntimeError(
                "Persisted Silver warning "
                "codes are incorrect"
            )

        skipped_result = (
            run_tracked_silver_job(
                psg_bucket=BRONZE_BUCKET,
                psg_object_key=(
                    PSG_OBJECT_KEY
                ),
                hypnogram_bucket=(
                    BRONZE_BUCKET
                ),
                hypnogram_object_key=(
                    HYPNOGRAM_OBJECT_KEY
                ),
                silver_bucket=(
                    SILVER_BUCKET
                ),
                root_prefix=root_prefix,
                signal_chunk_duration_seconds=(
                    30.0
                ),
                signal_start_seconds=0.0,
                signal_stop_seconds=60.0,
                verify_payload_checksums=True,
            )
        )

        skipped_run_id = skipped_result.run_id

        if skipped_result.status != "skipped":
            raise RuntimeError(
                "Completed quality-history "
                "output was not skipped"
            )

        skipped_rows = (
            get_quality_check_results_for_run(
                skipped_run_id
            )
        )

        if len(skipped_rows) != 1:
            raise RuntimeError(
                "Skipped Silver run should "
                "have one quality-history row"
            )

        if skipped_rows[0].status != "skipped":
            raise RuntimeError(
                "Skipped quality result has "
                "the wrong status"
            )

        print(
            "silver_quality_summary_persisted=true"
        )
        print(
            "silver_warning_rows_persisted=3"
        )
        print(
            "silver_warning_codes_match=true"
        )
        print(
            "skipped_quality_history_persisted=true"
        )
        print(
            "silver_quality_history_smoke_status="
            "success"
        )

    finally:
        if written_run_id is not None:
            delete_quality_check_results_for_run(
                written_run_id
            )

        if skipped_run_id is not None:
            delete_quality_check_results_for_run(
                skipped_run_id
            )

        delete_smoke_prefix(
            root_prefix=root_prefix
        )

    print(
        "silver_quality_history_cleanup=true"
    )


if __name__ == "__main__":
    run_smoke_test()
