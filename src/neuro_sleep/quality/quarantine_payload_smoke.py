import hashlib
import json
from typing import Any

from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_failed,
    finish_pipeline_run_success,
    start_pipeline_run,
)
from neuro_sleep.quality.quarantine import (
    create_quarantine_record,
    delete_quarantine_record_for_smoke_test,
    get_quarantine_record,
)
from neuro_sleep.storage.object_storage import (
    delete_object,
    get_object_metadata,
    get_object_storage_client,
    put_bytes_object,
    validate_required_buckets,
)


SOURCE_SYSTEM = "physionet_sleep_edf"
PIPELINE_NAME = "quarantine_payload_pointer_smoke_test"
TASK_NAME = "store_payload_in_minio_and_register_pointer"

BUCKET = "quarantine"
OBJECT_KEY = "smoke-tests/quarantine-payload/test-payload.json"
RECORD_KEY = "smoke-tests/quarantine-payload/test-record"
ERROR_CODE = "SMOKE_TEST_LARGE_PAYLOAD"

PAYLOAD = {
    "example": "large rejected payload",
    "reason": "smoke_test",
    "storage_strategy": "minio_pointer",
}


def encode_json_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")


def calculate_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run_smoke_test() -> None:
    run_id = start_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        task_name=TASK_NAME,
        source_system=SOURCE_SYSTEM,
    )

    client = get_object_storage_client()

    try:
        validate_required_buckets(
            required_buckets=["quarantine"],
            client=client,
        )

        delete_object(
            bucket=BUCKET,
            object_key=OBJECT_KEY,
            client=client,
        )

        delete_quarantine_record_for_smoke_test(
            source_system=SOURCE_SYSTEM,
            record_key=RECORD_KEY,
            error_code=ERROR_CODE,
        )

        payload_bytes = encode_json_payload(PAYLOAD)
        checksum_sha256 = calculate_sha256(payload_bytes)

        put_bytes_object(
            bucket=BUCKET,
            object_key=OBJECT_KEY,
            data=payload_bytes,
            content_type="application/json",
            client=client,
        )

        metadata = get_object_metadata(
            bucket=BUCKET,
            object_key=OBJECT_KEY,
            client=client,
        )

        quarantine_id = create_quarantine_record(
            source_system=SOURCE_SYSTEM,
            record_key=RECORD_KEY,
            raw_payload=None,
            error_code=ERROR_CODE,
            error_message="Smoke test payload stored in MinIO quarantine bucket.",
            severity="error",
            pipeline_run_id=run_id,
            status="open",
            payload_bucket=BUCKET,
            payload_object_key=OBJECT_KEY,
            payload_size_bytes=metadata["content_length"],
            payload_checksum_sha256=checksum_sha256,
        )

        row = get_quarantine_record(quarantine_id)

        print(f"run_id={run_id}")
        print(f"quarantine_id={row[0]}")
        print(f"source_system={row[1]}")
        print(f"record_key={row[3]}")
        print(f"raw_payload={row[4]}")
        print(f"error_code={row[5]}")
        print(f"error_message={row[6]}")
        print(f"severity={row[7]}")
        print(f"status={row[9]}")
        print(f"payload_bucket={row[10]}")
        print(f"payload_object_key={row[11]}")
        print(f"payload_size_bytes={row[12]}")
        print(f"payload_checksum_sha256={row[13]}")

        if row[10] != BUCKET:
            raise RuntimeError("Unexpected payload_bucket in quarantine record")

        if row[11] != OBJECT_KEY:
            raise RuntimeError("Unexpected payload_object_key in quarantine record")

        if row[13] != checksum_sha256:
            raise RuntimeError("Unexpected payload_checksum_sha256 in quarantine record")

        delete_object(
            bucket=BUCKET,
            object_key=OBJECT_KEY,
            client=client,
        )

        delete_quarantine_record_for_smoke_test(
            source_system=SOURCE_SYSTEM,
            record_key=RECORD_KEY,
            error_code=ERROR_CODE,
        )

        finish_pipeline_run_success(
            run_id=run_id,
            rows_read=1,
            rows_written=0,
            files_processed=0,
            records_quarantined=1,
        )

        print("smoke_test_cleanup=done")
        print("smoke_test_status=success")

    except Exception as exc:
        finish_pipeline_run_failed(
            run_id=run_id,
            error_message=str(exc),
            rows_read=1,
            rows_written=0,
            files_processed=0,
            records_quarantined=1,
        )

        raise


if __name__ == "__main__":
    run_smoke_test()
