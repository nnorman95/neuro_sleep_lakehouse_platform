from neuro_sleep.ingestion.bronze_writer import write_bytes_to_bronze_and_register
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
PIPELINE_NAME = "bronze_writer_smoke_test"
TASK_NAME = "write_bytes_to_bronze_and_register"

BUCKET = "bronze"
OBJECT_KEY = "smoke-tests/bronze-writer/test-object.txt"
SOURCE_URL = "https://example.local/smoke-test/bronze-writer/test-object.txt"


def run_smoke_test() -> None:
    run_id = start_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        task_name=TASK_NAME,
        source_system=SOURCE_SYSTEM,
    )

    client = get_object_storage_client()

    try:
        delete_object(
            bucket=BUCKET,
            object_key=OBJECT_KEY,
            client=client,
        )

        delete_raw_file_for_smoke_test(
            bucket=BUCKET,
            object_key=OBJECT_KEY,
        )

        data = b"NeuroSleep reusable bronze writer smoke test."

        result = write_bytes_to_bronze_and_register(
            source_system=SOURCE_SYSTEM,
            source_url=SOURCE_URL,
            bucket=BUCKET,
            object_key=OBJECT_KEY,
            data=data,
            ingestion_run_id=run_id,
            content_type="text/plain",
        )

        row = get_raw_file_by_object_key(
            bucket=BUCKET,
            object_key=OBJECT_KEY,
        )

        if row is None:
            raise RuntimeError("Bronze writer registry record was not found")

        print(f"run_id={run_id}")
        print(f"file_id={result.file_id}")
        print(f"bucket={result.bucket}")
        print(f"object_key={result.object_key}")
        print(f"file_name={result.file_name}")
        print(f"file_type={result.file_type}")
        print(f"file_size_bytes={result.file_size_bytes}")
        print(f"checksum_sha256={result.checksum_sha256}")
        print(f"registry_status={row.status}")

        delete_object(
            bucket=BUCKET,
            object_key=OBJECT_KEY,
            client=client,
        )

        delete_raw_file_for_smoke_test(
            bucket=BUCKET,
            object_key=OBJECT_KEY,
        )

        finish_pipeline_run_success(
            run_id=run_id,
            rows_read=1,
            rows_written=1,
            files_processed=1,
            records_quarantined=0,
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
            records_quarantined=0,
        )

        raise


if __name__ == "__main__":
    run_smoke_test()
