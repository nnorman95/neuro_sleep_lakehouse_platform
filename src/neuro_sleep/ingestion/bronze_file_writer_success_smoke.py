import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory

from neuro_sleep.ingestion.bronze_file_writer import (
    write_local_file_to_bronze_and_register,
)
from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_failed,
    finish_pipeline_run_success,
    start_pipeline_run,
)
from neuro_sleep.raw.file_registry import (
    delete_raw_file_for_smoke_test,
    get_raw_file_by_object_key,
)
from neuro_sleep.storage.object_storage import (
    delete_object,
    get_object_storage_client,
)


SOURCE_SYSTEM = "physionet_sleep_edf"
PIPELINE_NAME = "bronze_file_writer_success_smoke"
TASK_NAME = "write_local_file_to_bronze_and_register"
BUCKET = "bronze"
OBJECT_KEY = "smoke-tests/bronze-file-writer/test-object.txt"
SOURCE_URL = "https://example.local/smoke-tests/bronze-file-writer/test-object.txt"


def run_smoke_test() -> None:
    run_id = start_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        task_name=TASK_NAME,
        source_system=SOURCE_SYSTEM,
    )

    client = get_object_storage_client()

    try:
        delete_object(bucket=BUCKET, object_key=OBJECT_KEY, client=client)
        delete_raw_file_for_smoke_test(bucket=BUCKET, object_key=OBJECT_KEY)

        payload = b"NeuroSleep production Bronze file writer smoke test."
        expected_checksum = hashlib.sha256(payload).hexdigest()

        with TemporaryDirectory(prefix="neuro_sleep_bronze_writer_") as temp_dir:
            local_path = Path(temp_dir) / "test-object.txt"
            local_path.write_bytes(payload)

            result = write_local_file_to_bronze_and_register(
                source_system=SOURCE_SYSTEM,
                source_url=SOURCE_URL,
                bucket=BUCKET,
                object_key=OBJECT_KEY,
                local_file_path=local_path,
                expected_checksum_sha256=expected_checksum,
                ingestion_run_id=run_id,
                content_type="text/plain",
                client=client,
            )

        row = get_raw_file_by_object_key(bucket=BUCKET, object_key=OBJECT_KEY)

        if row is None:
            raise RuntimeError("Bronze writer registry row was not found")
        if row.status != "uploaded":
            raise RuntimeError(f"Unexpected registry status: {row.status}")
        if row.checksum_sha256 != expected_checksum:
            raise RuntimeError("Registry checksum mismatch")
        if result.checksum_sha256 != expected_checksum:
            raise RuntimeError("Writer result checksum mismatch")

        finish_pipeline_run_success(
            run_id=run_id,
            rows_read=1,
            rows_written=1,
            files_processed=1,
            records_quarantined=0,
        )

        print("production_bronze_writer_path=true")
        print("registry_status=uploaded")
        print("checksum_match=true")
        print("bronze_file_writer_success_smoke_status=success")

    except Exception as error:
        finish_pipeline_run_failed(
            run_id=run_id,
            error_message=str(error),
            rows_read=1,
            rows_written=0,
            files_processed=0,
            records_quarantined=0,
        )
        raise

    finally:
        try:
            delete_object(bucket=BUCKET, object_key=OBJECT_KEY, client=client)
        finally:
            delete_raw_file_for_smoke_test(bucket=BUCKET, object_key=OBJECT_KEY)
            client.close()


if __name__ == "__main__":
    run_smoke_test()
