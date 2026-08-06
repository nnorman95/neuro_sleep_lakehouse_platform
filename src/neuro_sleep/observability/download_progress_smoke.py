import json
from io import StringIO
from unittest.mock import patch

import neuro_sleep.observability.download_progress as download_progress

from neuro_sleep.observability.download_progress import (
    DownloadProgressReporter,
)


def run_smoke_test() -> None:
    pretty_output = StringIO()

    reporter = DownloadProgressReporter(
        relative_path=(
            "sleep-cassette/"
            "SC4001E0-PSG.edf"
        ),
        stream=pretty_output,
        output_format="pretty",
        interactive=True,
        minimum_update_interval_seconds=0.0,
    )

    with patch.object(
        download_progress,
        "format_console_timestamp",
        return_value="18:30:00",
    ):
        reporter.start(
            total_bytes=1000
        )

        reporter.update(
            downloaded_bytes=500
        )

        reporter.complete(
            downloaded_bytes=1000
        )

    pretty_text = (
        pretty_output.getvalue()
    )

    if "50.0%" not in pretty_text:
        raise RuntimeError(
            "Live percentage was not displayed"
        )

    if "1000 B" not in pretty_text:
        raise RuntimeError(
            "Downloaded byte count missing"
        )

    if "Downloaded" not in pretty_text:
        raise RuntimeError(
            "Completion message missing"
        )

    if "\r" not in pretty_text:
        raise RuntimeError(
            "Live line update is missing"
        )

    if "18:30:00" not in pretty_text:
        raise RuntimeError(
            "Download progress did not use "
            "the shared UTC timestamp"
        )

    print("live_percentage_rendered=true")
    print("single_line_refresh_enabled=true")
    print("download_completion_rendered=true")
    print(
        "download_timestamp_utc=true"
    )

    json_output = StringIO()

    json_reporter = DownloadProgressReporter(
        relative_path="RECORDS-v1",
        stream=json_output,
        output_format="json",
        interactive=False,
        minimum_update_interval_seconds=0.0,
    )

    original_stream = json_reporter.stream

    import contextlib

    with contextlib.redirect_stdout(
        original_stream
    ):
        json_reporter.start(
            total_bytes=100
        )

        json_reporter.update(
            downloaded_bytes=50
        )

        json_reporter.complete(
            downloaded_bytes=100
        )

    events = [
        json.loads(line)
        for line in json_output.getvalue().splitlines()
        if line.strip()
    ]

    event_names = [
        event["event"]
        for event in events
    ]

    if "file_download_started" not in event_names:
        raise RuntimeError(
            "JSON download start missing"
        )

    if "file_download_progress" not in event_names:
        raise RuntimeError(
            "JSON download progress missing"
        )

    if "file_download_completed" not in event_names:
        raise RuntimeError(
            "JSON download completion missing"
        )

    print("json_download_events=true")
    print(
        "download_progress_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
