from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.silver_pipeline import (
    run_silver_pipeline,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
)


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


def run_smoke_test() -> None:
    smoke_run_id = new_uuid7()

    root_prefix = (
        "smoke-tests/"
        "full-silver-pipeline/"
        f"run_id={smoke_run_id}"
    )

    client = get_object_storage_client()

    try:
        first_result = run_silver_pipeline(
            psg_bucket=BRONZE_BUCKET,
            psg_object_key=PSG_OBJECT_KEY,
            hypnogram_bucket=BRONZE_BUCKET,
            hypnogram_object_key=(
                HYPNOGRAM_OBJECT_KEY
            ),
            silver_bucket=SILVER_BUCKET,
            root_prefix=root_prefix,
            signal_chunk_duration_seconds=30.0,
            signal_start_seconds=0.0,
            signal_stop_seconds=60.0,
            verify_payload_checksums=True,
            client=client,
        )

        if first_result.status != "written":
            raise RuntimeError(
                "First full Silver pipeline "
                "run was not written"
            )

        if (
            first_result.recording_id.version
            != 7
        ):
            raise RuntimeError(
                "Pipeline recording_id is "
                "not UUIDv7"
            )

        if (
            first_result.data_object_count
            != 18
        ):
            raise RuntimeError(
                "Unexpected data object count"
            )

        if (
            first_result.total_object_count
            != 19
        ):
            raise RuntimeError(
                "Unexpected total object count"
            )

        if first_result.row_count != 21052:
            raise RuntimeError(
                "Unexpected Silver row count"
            )

        if not (
            first_result
            .reconciliation_report
            .passed
        ):
            raise RuntimeError(
                "First pipeline "
                "reconciliation failed"
            )

        if (
            first_result
            .reconciliation_report
            .verified_payload_checksum_count
            != 18
        ):
            raise RuntimeError(
                "First pipeline did not "
                "verify all payload checksums"
            )

        print(
            "first_full_silver_run_status="
            "written"
        )
        print(
            "first_full_silver_reconciliation="
            "passed"
        )
        print(
            "first_full_silver_checksums=18"
        )

        second_result = run_silver_pipeline(
            psg_bucket=BRONZE_BUCKET,
            psg_object_key=PSG_OBJECT_KEY,
            hypnogram_bucket=BRONZE_BUCKET,
            hypnogram_object_key=(
                HYPNOGRAM_OBJECT_KEY
            ),
            silver_bucket=SILVER_BUCKET,
            root_prefix=root_prefix,
            signal_chunk_duration_seconds=30.0,
            signal_start_seconds=0.0,
            signal_stop_seconds=60.0,
            verify_payload_checksums=True,
            client=client,
        )

        if second_result.status != "skipped":
            raise RuntimeError(
                "Second full Silver pipeline "
                "run was not skipped"
            )

        if (
            second_result.recording_id
            != first_result.recording_id
        ):
            raise RuntimeError(
                "Second run did not reuse "
                "recording UUIDv7"
            )

        if (
            second_result.output_prefix
            != first_result.output_prefix
        ):
            raise RuntimeError(
                "Second run changed output "
                "prefix"
            )

        if not (
            second_result
            .reconciliation_report
            .passed
        ):
            raise RuntimeError(
                "Skipped pipeline output "
                "failed reconciliation"
            )

        if (
            second_result
            .reconciliation_report
            .verified_payload_checksum_count
            != 18
        ):
            raise RuntimeError(
                "Skipped pipeline did not "
                "verify all payload checksums"
            )

        stored_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    first_result
                    .output_prefix
                    + "/"
                ),
                client=client,
            )
        )

        if len(stored_objects) != 19:
            raise RuntimeError(
                "Unexpected final Silver "
                "object count"
            )

        print(
            "second_full_silver_run_status="
            "skipped"
        )
        print(
            "second_full_silver_reconciliation="
            "passed"
        )
        print(
            "recording_uuid7_reused=true"
        )
        print(
            "silver_data_object_count=18"
        )
        print(
            "silver_success_manifest_count=1"
        )
        print(
            "silver_total_object_count=19"
        )
        print(
            "silver_total_row_count=21052"
        )
        print(
            "full_silver_pipeline_end_to_end="
            "true"
        )

    finally:
        stored_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    root_prefix + "/"
                ),
                client=client,
            )
        )

        for item in stored_objects:
            client.delete_object(
                Bucket=SILVER_BUCKET,
                Key=item.object_key,
            )

        remaining_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    root_prefix + "/"
                ),
                client=client,
            )
        )

        client.close()

    if remaining_objects:
        raise RuntimeError(
            "Full Silver pipeline smoke "
            "objects were not cleaned up"
        )

    print(
        "full_silver_pipeline_cleanup=true"
    )
    print(
        "full_silver_pipeline_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
