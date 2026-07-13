import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory

from neuro_sleep.raw.file_registry import (
    delete_raw_file_for_smoke_test,
    mark_raw_file_uploaded,
    register_raw_file,
)
from neuro_sleep.reconciliation.bronze_reconciliation import (
    reconcile_bronze_prefix,
)
from neuro_sleep.storage.object_storage import (
    delete_object,
    get_object_storage_client,
    put_file_object,
)


BUCKET = "bronze"

PREFIX = (
    "smoke-tests/reconciliation/"
)

HEALTHY_KEY = PREFIX + "healthy.txt"

MISSING_STORAGE_KEY = (
    PREFIX + "missing-storage.txt"
)

MISSING_REGISTRY_KEY = (
    PREFIX + "missing-registry.txt"
)

MISMATCH_KEY = (
    PREFIX + "metadata-mismatch.txt"
)

OBJECT_KEYS = (
    HEALTHY_KEY,
    MISSING_STORAGE_KEY,
    MISSING_REGISTRY_KEY,
    MISMATCH_KEY,
)


def checksum_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def register_uploaded_file(
    object_key: str,
    file_size_bytes: int,
    checksum_sha256: str,
) -> None:
    file_name = object_key.rsplit(
        "/",
        1,
    )[-1]

    file_id = register_raw_file(
        source_system=(
            "reconciliation_smoke_test"
        ),
        source_url=(
            "https://example.local/"
            f"{file_name}"
        ),
        bucket=BUCKET,
        object_key=object_key,
        file_name=file_name,
        file_type="txt",
    )

    mark_raw_file_uploaded(
        file_id=file_id,
        file_size_bytes=file_size_bytes,
        checksum_sha256=checksum_sha256,
    )


def cleanup(
    client,
) -> None:
    for object_key in OBJECT_KEYS:
        delete_object(
            bucket=BUCKET,
            object_key=object_key,
            client=client,
        )

        delete_raw_file_for_smoke_test(
            bucket=BUCKET,
            object_key=object_key,
        )


def run_smoke_test() -> None:
    client = get_object_storage_client()

    try:
        cleanup(client)

        healthy_data = (
            b"healthy reconciliation object"
        )

        missing_storage_data = (
            b"missing storage object"
        )

        missing_registry_data = (
            b"missing registry object"
        )

        mismatch_data = (
            b"metadata mismatch object"
        )

        healthy_checksum = checksum_bytes(
            healthy_data
        )

        missing_storage_checksum = (
            checksum_bytes(
                missing_storage_data
            )
        )

        missing_registry_checksum = (
            checksum_bytes(
                missing_registry_data
            )
        )

        mismatch_storage_checksum = (
            checksum_bytes(
                mismatch_data
            )
        )

        with TemporaryDirectory(
            prefix=(
                "neuro_sleep_"
                "reconciliation_smoke_"
            )
        ) as temporary_directory:
            root = Path(
                temporary_directory
            )

            healthy_path = (
                root / "healthy.txt"
            )

            missing_registry_path = (
                root / "missing-registry.txt"
            )

            mismatch_path = (
                root / "metadata-mismatch.txt"
            )

            healthy_path.write_bytes(
                healthy_data
            )

            missing_registry_path.write_bytes(
                missing_registry_data
            )

            mismatch_path.write_bytes(
                mismatch_data
            )

            put_file_object(
                bucket=BUCKET,
                object_key=HEALTHY_KEY,
                file_path=healthy_path,
                checksum_sha256=(
                    healthy_checksum
                ),
                client=client,
            )

            put_file_object(
                bucket=BUCKET,
                object_key=(
                    MISSING_REGISTRY_KEY
                ),
                file_path=(
                    missing_registry_path
                ),
                checksum_sha256=(
                    missing_registry_checksum
                ),
                client=client,
            )

            put_file_object(
                bucket=BUCKET,
                object_key=MISMATCH_KEY,
                file_path=mismatch_path,
                checksum_sha256=(
                    mismatch_storage_checksum
                ),
                client=client,
            )

        register_uploaded_file(
            object_key=HEALTHY_KEY,
            file_size_bytes=len(
                healthy_data
            ),
            checksum_sha256=(
                healthy_checksum
            ),
        )

        register_uploaded_file(
            object_key=MISSING_STORAGE_KEY,
            file_size_bytes=len(
                missing_storage_data
            ),
            checksum_sha256=(
                missing_storage_checksum
            ),
        )

        register_uploaded_file(
            object_key=MISMATCH_KEY,
            file_size_bytes=len(
                mismatch_data
            ),
            checksum_sha256="0" * 64,
        )

        results = reconcile_bronze_prefix(
            bucket=BUCKET,
            prefix=PREFIX,
            client=client,
        )

        status_by_key = {
            result.object_key: result.status
            for result in results
        }

        expected_status_by_key = {
            HEALTHY_KEY: "healthy",
            MISSING_STORAGE_KEY: (
                "missing_in_storage"
            ),
            MISSING_REGISTRY_KEY: (
                "missing_in_registry"
            ),
            MISMATCH_KEY: (
                "metadata_mismatch"
            ),
        }

        if status_by_key != (
            expected_status_by_key
        ):
            raise RuntimeError(
                "Unexpected reconciliation "
                f"results: {status_by_key}"
            )

        mismatch_result = next(
            result
            for result in results
            if result.object_key
            == MISMATCH_KEY
        )

        if (
            "SHA256 checksum differs"
            not in mismatch_result.reason
        ):
            raise RuntimeError(
                "Checksum mismatch reason "
                "was not recorded"
            )

        print(
            "healthy_object_detected=true"
        )
        print(
            "missing_storage_detected=true"
        )
        print(
            "missing_registry_detected=true"
        )
        print(
            "metadata_mismatch_detected=true"
        )
        print(
            "reconciliation_result_count=4"
        )
        print(
            "bronze_reconciliation_smoke_status="
            "success"
        )

    finally:
        try:
            cleanup(client)

        finally:
            client.close()


if __name__ == "__main__":
    run_smoke_test()
