from pathlib import Path
from tempfile import TemporaryDirectory

from neuro_sleep.ingestion.sleep_edf_http_downloader import (
    download_sleep_edf_source_file,
)
from neuro_sleep.ingestion.sleep_edf_remote_manifest import (
    fetch_sleep_edf_remote_manifest,
)


def run_download_check() -> None:
    manifest = fetch_sleep_edf_remote_manifest()

    records_file = next(
        source_file
        for source_file in manifest.all_files
        if source_file.relative_path == "RECORDS"
    )

    with TemporaryDirectory(
        prefix="neuro_sleep_download_check_"
    ) as temporary_directory:
        destination_root = Path(
            temporary_directory
        )

        first_result = (
            download_sleep_edf_source_file(
                source_file=records_file,
                destination_root=destination_root,
            )
        )

        if first_result.reused_existing_file:
            raise RuntimeError(
                "First download was unexpectedly reused"
            )

        second_result = (
            download_sleep_edf_source_file(
                source_file=records_file,
                destination_root=destination_root,
            )
        )

        if not second_result.reused_existing_file:
            raise RuntimeError(
                "Second download did not reuse "
                "the verified local file"
            )

        if (
            first_result.checksum_sha256
            != records_file.checksum_sha256
        ):
            raise RuntimeError(
                "Downloaded checksum does not match "
                "the official manifest"
            )

        if (
            first_result.file_size_bytes
            != second_result.file_size_bytes
        ):
            raise RuntimeError(
                "Reused file size does not match "
                "the downloaded file size"
            )

        if (
            first_result.checksum_sha256
            != second_result.checksum_sha256
        ):
            raise RuntimeError(
                "Reused file checksum does not match "
                "the downloaded file checksum"
            )

        print(
            f"source_url={records_file.source_url}"
        )
        print(
            "destination_path="
            f"{first_result.destination_path}"
        )
        print(
            "file_size_bytes="
            f"{first_result.file_size_bytes}"
        )
        print(
            "checksum_sha256="
            f"{first_result.checksum_sha256}"
        )
        print("official_checksum_match=true")
        print(
            "first_download_reused="
            f"{str(first_result.reused_existing_file).lower()}"
        )
        print(
            "second_download_reused="
            f"{str(second_result.reused_existing_file).lower()}"
        )

    print("temporary_file_cleanup=success")
    print("downloaded_edf_files=0")
    print("http_download_check_status=success")


if __name__ == "__main__":
    run_download_check()
