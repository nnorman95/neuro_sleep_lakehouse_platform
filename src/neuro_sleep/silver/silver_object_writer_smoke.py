from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.parquet_tables import (
    recording_to_table,
)
from neuro_sleep.silver.recording_builder import (
    build_silver_recording,
)
from neuro_sleep.silver.silver_object_writer import (
    PARQUET_CONTENT_TYPE,
    upload_silver_table,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
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
    bundle = build_silver_recording(
        psg_bucket=BRONZE_BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=(
            BRONZE_BUCKET
        ),
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    )

    table = recording_to_table(
        bundle.recording
    )

    smoke_run_id = new_uuid7()

    object_key = (
        "smoke-tests/"
        "silver-object-writer/"
        f"run_id={smoke_run_id}/"
        "recordings.parquet"
    )

    client = get_object_storage_client()

    try:
        result = upload_silver_table(
            table=table,
            bucket=SILVER_BUCKET,
            object_key=object_key,
            client=client,
        )

        head = client.head_object(
            Bucket=SILVER_BUCKET,
            Key=object_key,
        )

        if (
            int(head["ContentLength"])
            != result.file_size_bytes
        ):
            raise RuntimeError(
                "Stored object size mismatch"
            )

        if head.get("ContentType") != (
            PARQUET_CONTENT_TYPE
        ):
            raise RuntimeError(
                "Stored content type mismatch"
            )

        metadata = head.get(
            "Metadata",
            {},
        )

        if (
            metadata.get(
                "dataset_name"
            )
            != "recordings"
        ):
            raise RuntimeError(
                "Stored dataset metadata "
                "mismatch"
            )

        if (
            metadata.get("row_count")
            != "1"
        ):
            raise RuntimeError(
                "Stored row-count metadata "
                "mismatch"
            )

        if (
            metadata.get(
                "checksum_sha256"
            )
            != result.checksum_sha256
        ):
            raise RuntimeError(
                "Stored checksum metadata "
                "mismatch"
            )

        with TemporaryDirectory(
            prefix=(
                "neuro_sleep_silver_"
                "download_"
            )
        ) as temporary_directory:
            download_path = (
                Path(temporary_directory)
                / "recordings.parquet"
            )

            client.download_file(
                Bucket=SILVER_BUCKET,
                Key=object_key,
                Filename=str(
                    download_path
                ),
            )

            restored_checksum = sha256(
                download_path.read_bytes()
            ).hexdigest()

            if (
                restored_checksum
                != result.checksum_sha256
            ):
                raise RuntimeError(
                    "Downloaded Parquet "
                    "checksum mismatch"
                )

            restored = pq.read_table(
                download_path
            )

            if restored.num_rows != 1:
                raise RuntimeError(
                    "Downloaded Parquet row "
                    "count mismatch"
                )

            restored_row = (
                restored.to_pylist()[0]
            )

            if (
                restored_row[
                    "recording_id"
                ]
                != str(
                    bundle.recording_id
                )
            ):
                raise RuntimeError(
                    "Downloaded recording ID "
                    "mismatch"
                )

        print(
            "silver_object_uploaded=true"
        )
        print(
            "silver_object_size_verified=true"
        )
        print(
            "silver_object_metadata_verified="
            "true"
        )
        print(
            "silver_object_checksum_verified="
            "true"
        )
        print(
            "silver_parquet_downloaded=true"
        )
        print(
            "downloaded_checksum_verified="
            "true"
        )
        print(
            "silver_parquet_row_count=1"
        )
        print(
            "silver_parquet_round_trip=true"
        )

    finally:
        client.delete_object(
            Bucket=SILVER_BUCKET,
            Key=object_key,
        )

        client.close()

    print(
        "silver_object_cleanup=true"
    )
    print(
        "silver_object_writer_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
