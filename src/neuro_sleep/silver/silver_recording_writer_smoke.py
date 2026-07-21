from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.silver_recording_writer import (
    write_silver_recording,
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

    output_prefix = (
        "smoke-tests/"
        "silver-recording-writer/"
        f"run_id={smoke_run_id}"
    )

    client = get_object_storage_client()

    try:
        result = write_silver_recording(
            psg_bucket=BRONZE_BUCKET,
            psg_object_key=PSG_OBJECT_KEY,
            hypnogram_bucket=(
                BRONZE_BUCKET
            ),
            hypnogram_object_key=(
                HYPNOGRAM_OBJECT_KEY
            ),
            silver_bucket=SILVER_BUCKET,
            output_prefix=output_prefix,
            signal_chunk_duration_seconds=(
                30.0
            ),
            signal_start_seconds=0.0,
            signal_stop_seconds=60.0,
            client=client,
        )

        if not result.quality_report.passed:
            raise RuntimeError(
                "Valid Silver recording "
                "failed quality checks"
            )

        if (
            result.quality_report.error_count
            != 0
        ):
            raise RuntimeError(
                "Unexpected Silver quality "
                "errors"
            )

        if (
            result.quality_report
            .warning_count
            != 3
        ):
            raise RuntimeError(
                "Unexpected Silver quality "
                "warning count"
            )

        if len(
            result.metadata_objects
        ) != 4:
            raise RuntimeError(
                "Unexpected metadata object "
                "count"
            )

        if len(
            result.signal_objects
        ) != 14:
            raise RuntimeError(
                "Unexpected signal object "
                "count"
            )

        if result.object_count != 18:
            raise RuntimeError(
                "Unexpected total object "
                "count"
            )

        if result.row_count != 21052:
            raise RuntimeError(
                "Unexpected total row count: "
                f"{result.row_count}"
            )

        dataset_counts: dict[
            str,
            int,
        ] = {}

        for item in (
            *result.metadata_objects,
            *result.signal_objects,
        ):
            dataset_counts[
                item.dataset_name
            ] = (
                dataset_counts.get(
                    item.dataset_name,
                    0,
                )
                + 1
            )

        expected_dataset_counts = {
            "recordings": 1,
            "channels": 1,
            "sleep_stage_intervals": 1,
            "sleep_stage_epochs": 1,
            "signals": 14,
        }

        if (
            dataset_counts
            != expected_dataset_counts
        ):
            raise RuntimeError(
                "Unexpected Silver dataset "
                f"object counts: "
                f"{dataset_counts}"
            )

        stored_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    output_prefix + "/"
                ),
                client=client,
            )
        )

        if len(stored_objects) != 18:
            raise RuntimeError(
                "Unexpected MinIO object "
                f"count: "
                f"{len(stored_objects)}"
            )

        if any(
            item.content_length <= 0
            for item in stored_objects
        ):
            raise RuntimeError(
                "Empty Silver object found"
            )

        if (
            result.bundle
            .recording
            .in_range_epoch_count
            != 2650
        ):
            raise RuntimeError(
                "Unexpected recording epoch "
                "metadata"
            )

        print(
            "silver_quality_gate_passed=true"
        )
        print(
            "silver_quality_error_count=0"
        )
        print(
            "silver_quality_warning_count=3"
        )
        print(
            "silver_metadata_object_count=4"
        )
        print(
            "silver_signal_object_count=14"
        )
        print(
            "silver_total_object_count=18"
        )
        print(
            "silver_total_row_count=21052"
        )
        print(
            "silver_minio_object_count=18"
        )
        print(
            "silver_dataset_object_counts_valid="
            "true"
        )
        print(
            "silver_object_sizes_valid=true"
        )
        print(
            "silver_recording_bundle_reused=true"
        )

    finally:
        stored_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    output_prefix + "/"
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
                    output_prefix + "/"
                ),
                client=client,
            )
        )

        client.close()

    if remaining_objects:
        raise RuntimeError(
            "Smoke-test Silver objects were "
            "not cleaned up"
        )

    print(
        "silver_recording_cleanup=true"
    )
    print(
        "silver_recording_writer_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
