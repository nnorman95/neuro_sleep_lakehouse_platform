import json
from datetime import datetime
from io import StringIO
from pathlib import Path
from neuro_sleep.identifiers import new_uuid7

from neuro_sleep.observability.structured_logging import (
    emit_event,
    emit_exception,
    format_console_timestamp,
)


def run_smoke_test() -> None:
    run_id = new_uuid7()

    json_output = StringIO()

    payload = emit_event(
        event="file_progress",
        stream=json_output,
        output_format="json",
        minimum_level="DEBUG",
        run_id=run_id,
        pipeline_name="sleep_edf_extract",
        object_key=Path(
            "physionet/test.edf"
        ),
        object_status="skipped",
        processed_object_count=3,
        total_object_count=7,
        progress_percent=42.86,
        tags={"sleep", "edf"},
    )

    parsed_payload = json.loads(
        json_output.getvalue().strip()
    )

    if parsed_payload != payload:
        raise RuntimeError(
            "Serialized JSON payload mismatch"
        )

    if parsed_payload["run_id"] != str(run_id):
        raise RuntimeError(
            "UUID was not normalized"
        )

    if parsed_payload["tags"] != [
        "edf",
        "sleep",
    ]:
        raise RuntimeError(
            "Set normalization is unstable"
        )

    timestamp = datetime.fromisoformat(
        parsed_payload["timestamp_utc"]
    )

    if timestamp.tzinfo is None:
        raise RuntimeError(
            "Timestamp has no timezone"
        )

    print("structured_json_is_valid=true")
    print("uuid_normalization=true")
    print("utc_timestamp=true")

    utc_console_timestamp = (
        format_console_timestamp(
            "2026-08-06T18:30:00+00:00"
        )
    )

    offset_console_timestamp = (
        format_console_timestamp(
            "2026-08-06T22:30:00+04:00"
        )
    )

    if (
        utc_console_timestamp
        != "18:30:00"
        or offset_console_timestamp
        != "18:30:00"
    ):
        raise RuntimeError(
            "Console timestamp is not "
            "normalized to UTC"
        )

    print(
        "console_timestamp_utc_normalized="
        "true"
    )

    pretty_output = StringIO()

    emit_event(
        event="file_progress",
        stream=pretty_output,
        output_format="pretty",
        run_id=run_id,
        object_key=(
            "physionet/sleep-edfx/1.0.0/"
            "sleep-cassette/test.edf"
        ),
        object_status="skipped",
        processed_object_count=3,
        total_object_count=7,
        progress_percent=42.86,
        file_size_bytes=0,
    )

    pretty_line = (
        pretty_output.getvalue().strip()
    )

    if pretty_line.startswith("{"):
        raise RuntimeError(
            "Pretty output contains raw JSON"
        )

    if "[3/7]" not in pretty_line:
        raise RuntimeError(
            "Pretty progress counter missing"
        )

    if "SKIP" not in pretty_line:
        raise RuntimeError(
            "Pretty status missing"
        )

    if "sleep-cassette/test.edf" not in pretty_line:
        raise RuntimeError(
            "Compact object key missing"
        )

    print("pretty_console_output=true")

    debug_output = StringIO()

    emit_event(
        event="heartbeat_updated",
        level="DEBUG",
        stream=debug_output,
        output_format="pretty",
        minimum_level="INFO",
        heartbeat_update_count=2,
    )

    if debug_output.getvalue():
        raise RuntimeError(
            "DEBUG event was not hidden"
        )

    print("debug_heartbeat_hidden=true")

    exception_output = StringIO()

    emit_exception(
        event="pipeline_failed",
        error=RuntimeError(
            "Smoke test failure"
        ),
        stream=exception_output,
        output_format="pretty",
        run_id=run_id,
    )

    exception_line = (
        exception_output.getvalue().strip()
    )

    if "Smoke test failure" not in exception_line:
        raise RuntimeError(
            "Pretty exception message missing"
        )

    print("exception_logging=true")

    try:
        emit_event(
            event="invalid_event",
            output_format="json",
            stream=StringIO(),
            timestamp_utc="forbidden",
        )

    except ValueError:
        print(
            "reserved_field_override_blocked=true"
        )

    else:
        raise RuntimeError(
            "Reserved field override "
            "was not blocked"
        )

    print(
        "structured_logging_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
