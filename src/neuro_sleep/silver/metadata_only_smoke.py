from __future__ import annotations

from neuro_sleep.identifiers import new_uuid7
from neuro_sleep.silver.idempotency import read_success_manifest, write_silver_recording_idempotent
from neuro_sleep.storage.object_storage import get_object_storage_client

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
PSG_OBJECT_KEY = "physionet/sleep-edfx/1.0.0/sleep-cassette/SC4001E0-PSG.edf"
HYPNOGRAM_OBJECT_KEY = "physionet/sleep-edfx/1.0.0/sleep-cassette/SC4001EC-Hypnogram.edf"


def list_keys(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item.get("Key")
            if isinstance(key, str):
                keys.append(key)
    return keys


def run_smoke_test() -> None:
    root_prefix = f"smoke-tests/silver-metadata-only/run_id={new_uuid7()}"
    client = get_object_storage_client()
    try:
        first = write_silver_recording_idempotent(
            psg_bucket=BRONZE_BUCKET,
            psg_object_key=PSG_OBJECT_KEY,
            hypnogram_bucket=BRONZE_BUCKET,
            hypnogram_object_key=HYPNOGRAM_OBJECT_KEY,
            silver_bucket=SILVER_BUCKET,
            root_prefix=root_prefix,
            include_signals=False,
            client=client,
        )
        second = write_silver_recording_idempotent(
            psg_bucket=BRONZE_BUCKET,
            psg_object_key=PSG_OBJECT_KEY,
            hypnogram_bucket=BRONZE_BUCKET,
            hypnogram_object_key=HYPNOGRAM_OBJECT_KEY,
            silver_bucket=SILVER_BUCKET,
            root_prefix=root_prefix,
            include_signals=False,
            client=client,
        )
        if first.status != "written" or second.status != "skipped":
            raise RuntimeError("metadata-only idempotency failed")
        if first.data_object_count != 4:
            raise RuntimeError("metadata-only mode must write 4 data objects")
        keys = list_keys(client, SILVER_BUCKET, first.output_prefix + "/")
        if any("/signals/" in key for key in keys):
            raise RuntimeError("metadata-only mode wrote signal objects")
        manifest = read_success_manifest(
            bucket=SILVER_BUCKET,
            output_prefix=first.output_prefix,
            client=client,
        )
        transform_config = manifest.get("transform_config")
        if not isinstance(transform_config, dict) or transform_config.get("include_signals") is not False:
            raise RuntimeError("manifest mode marker missing")
        print("metadata_only_data_object_count=4")
        print("metadata_only_signal_object_count=0")
        print("metadata_only_rerun_status=skipped")
        print("silver_metadata_only_smoke_status=success")
    finally:
        for key in list_keys(client, SILVER_BUCKET, root_prefix + "/"):
            client.delete_object(Bucket=SILVER_BUCKET, Key=key)
        client.close()


if __name__ == "__main__":
    run_smoke_test()
