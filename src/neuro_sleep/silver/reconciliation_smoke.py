from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.idempotency import (
    write_silver_recording_idempotent,
)
from neuro_sleep.silver.reconciliation import (
    reconcile_silver_output,
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
        "silver-reconciliation/"
        f"run_id={smoke_run_id}"
    )

    client = get_object_storage_client()

    try:
        write_result = (
            write_silver_recording_idempotent(
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
                client=client,
            )
        )

        output_prefix = (
            write_result.output_prefix
        )

        report = reconcile_silver_output(
            bucket=SILVER_BUCKET,
            output_prefix=output_prefix,
            verify_payload_checksums=True,
            client=client,
        )

        if not report.passed:
            raise RuntimeError(
                "Valid Silver output failed "
                "reconciliation"
            )

        if report.error_count != 0:
            raise RuntimeError(
                "Unexpected reconciliation "
                "errors"
            )

        if (
            report.expected_data_object_count
            != 18
            or report.actual_data_object_count
            != 18
        ):
            raise RuntimeError(
                "Unexpected reconciled object "
                "count"
            )

        if (
            report.expected_row_count
            != 21052
            or report.actual_row_count
            != 21052
        ):
            raise RuntimeError(
                "Unexpected reconciled row "
                "count"
            )

        if (
            report
            .verified_payload_checksum_count
            != 18
        ):
            raise RuntimeError(
                "Not all payload checksums "
                "were verified"
            )

        print(
            "silver_reconciliation_passed=true"
        )
        print(
            "reconciled_data_object_count=18"
        )
        print(
            "reconciled_row_count=21052"
        )
        print(
            "payload_checksums_verified=18"
        )

        unexpected_key = (
            f"{output_prefix}/"
            "unexpected-object.txt"
        )

        client.put_object(
            Bucket=SILVER_BUCKET,
            Key=unexpected_key,
            Body=b"unexpected",
            ContentLength=10,
            ContentType="text/plain",
        )

        extra_report = (
            reconcile_silver_output(
                bucket=SILVER_BUCKET,
                output_prefix=(
                    output_prefix
                ),
                client=client,
            )
        )

        if not any(
            issue.code
            == "UNEXPECTED_OBJECT"
            for issue in extra_report.issues
        ):
            raise RuntimeError(
                "Unexpected object was not "
                "detected"
            )

        print(
            "unexpected_object_detected=true"
        )

        client.delete_object(
            Bucket=SILVER_BUCKET,
            Key=unexpected_key,
        )

        if write_result.write_result is None:
            raise RuntimeError(
                "First idempotent run did not "
                "return write_result"
            )

        object_to_remove = (
            write_result
            .write_result
            .metadata_objects[0]
            .object_key
        )

        client.delete_object(
            Bucket=SILVER_BUCKET,
            Key=object_to_remove,
        )

        missing_report = (
            reconcile_silver_output(
                bucket=SILVER_BUCKET,
                output_prefix=(
                    output_prefix
                ),
                client=client,
            )
        )

        if not any(
            issue.code == "MISSING_OBJECT"
            for issue in missing_report.issues
        ):
            raise RuntimeError(
                "Missing object was not "
                "detected"
            )

        print(
            "missing_object_detected=true"
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
            "Reconciliation smoke objects "
            "were not cleaned up"
        )

    print(
        "silver_reconciliation_cleanup=true"
    )
    print(
        "silver_reconciliation_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
