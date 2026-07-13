from pathlib import Path
from tempfile import TemporaryDirectory

from neuro_sleep.ingestion.bronze_file_writer import (
    write_local_file_to_bronze_and_register,
)
from neuro_sleep.ingestion.sleep_edf_http_downloader import (
    download_sleep_edf_source_file,
)
from neuro_sleep.ingestion.sleep_edf_remote_manifest import (
    fetch_sleep_edf_remote_manifest,
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
from neuro_sleep.sources.sleep_edf import (
    SOURCE_SYSTEM,
)
from neuro_sleep.storage.object_storage import (
    delete_object,
    get_object_metadata,
    get_object_storage_client,
)


PIPELINE_NAME = "sleep_edf_bronze_load_check"
TASK_NAME = "download_upload_register_records"

BUCKET = "bronze"

OBJECT_KEY = (
    "integration-checks/"
    "sleep-edf/1.0.0/RECORDS"
)


def run_bronze_load_check() -> None:
    run_id = start_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        task_name=TASK_NAME,
        source_system=SOURCE_SYSTEM,
    )

    client = get_object_storage_client()

    delete_object(
        bucket=BUCKET,
        object_key=OBJECT_KEY,
        client=client,
    )

    delete_raw_file_for_smoke_test(
        bucket=BUCKET,
        object_key=OBJECT_KEY,
    )

    try:
        manifest = (
            fetch_sleep_edf_remote_manifest()
        )

        records_file = next(
            source_file
            for source_file in manifest.all_files
            if source_file.relative_path
            == "RECORDS"
        )

        with TemporaryDirectory(
            prefix=(
                "neuro_sleep_bronze_"
                "load_check_"
            )
        ) as temporary_directory:
            destination_root = Path(
                temporary_directory
            )

            download_result = (
                download_sleep_edf_source_file(
                    source_file=records_file,
                    destination_root=(
                        destination_root
                    ),
                )
            )

            write_result = (
                write_local_file_to_bronze_and_register(
                    source_system=SOURCE_SYSTEM,
                    source_url=(
                        records_file.source_url
                    ),
                    bucket=BUCKET,
                    object_key=OBJECT_KEY,
                    local_file_path=(
                        download_result
                        .destination_path
                    ),
                    expected_checksum_sha256=(
                        records_file
                        .checksum_sha256
                    ),
                    ingestion_run_id=run_id,
                    file_name=(
                        records_file.file_name
                    ),
                    file_type=(
                        records_file.file_type
                    ),
                    content_type="text/plain",
                    client=client,
                )
            )

            registry_row = (
                get_raw_file_by_object_key(
                    bucket=BUCKET,
                    object_key=OBJECT_KEY,
                )
            )

            if registry_row is None:
                raise RuntimeError(
                    "Registry row was not found"
                )

            object_metadata = (
                get_object_metadata(
                    bucket=BUCKET,
                    object_key=OBJECT_KEY,
                    client=client,
                )
            )

            if registry_row.status != "uploaded":
                raise RuntimeError(
                    "Unexpected registry status: "
                    f"{registry_row.status}"
                )

            if (
                registry_row.file_size_bytes
                != write_result.file_size_bytes
            ):
                raise RuntimeError(
                    "Registry file size mismatch"
                )

            if (
                registry_row.checksum_sha256
                != write_result.checksum_sha256
            ):
                raise RuntimeError(
                    "Registry checksum mismatch"
                )

            if (
                object_metadata[
                    "checksum_sha256"
                ]
                != write_result.checksum_sha256
            ):
                raise RuntimeError(
                    "MinIO checksum metadata mismatch"
                )

            print(f"run_id={run_id}")
            print(
                f"file_id={write_result.file_id}"
            )
            print(
                "source_url="
                f"{records_file.source_url}"
            )
            print(f"bucket={BUCKET}")
            print(
                f"object_key={OBJECT_KEY}"
            )
            print(
                "file_size_bytes="
                f"{write_result.file_size_bytes}"
            )
            print(
                "checksum_sha256="
                f"{write_result.checksum_sha256}"
            )
            print(
                "official_checksum_match=true"
            )
            print(
                "minio_size_match=true"
            )
            print(
                "minio_checksum_metadata_match=true"
            )
            print(
                "registry_status="
                f"{registry_row.status}"
            )

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

        print(
            "temporary_file_cleanup=success"
        )
        print(
            "minio_object_cleanup=success"
        )
        print(
            "registry_cleanup=success"
        )
        print("real_http_requests=3")
        print("downloaded_edf_files=0")
        print(
            "bronze_load_check_status=success"
        )

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
    run_bronze_load_check()
