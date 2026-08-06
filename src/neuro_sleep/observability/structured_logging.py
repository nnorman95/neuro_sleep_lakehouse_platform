import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO
from uuid import UUID


VALID_LOG_LEVELS = {
    "DEBUG",
    "INFO",
    "WARNING",
    "ERROR",
    "CRITICAL",
}

LOG_LEVEL_VALUES = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}

VALID_LOG_FORMATS = {
    "pretty",
    "json",
}

RESERVED_EVENT_FIELDS = {
    "timestamp_utc",
    "level",
    "event",
}

DEFAULT_LOG_FORMAT = "pretty"
DEFAULT_LOG_LEVEL = "INFO"


def format_console_timestamp(
    value: datetime | str | None = None,
) -> str:
    if value is None:
        timestamp = datetime.now(
            timezone.utc
        )

    elif isinstance(value, str):
        timestamp = datetime.fromisoformat(
            value
        )

    else:
        timestamp = value

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(
            tzinfo=timezone.utc
        )

    return timestamp.astimezone(
        timezone.utc
    ).strftime("%H:%M:%S")


def normalize_log_value(
    value: Any,
) -> Any:
    if value is None:
        return None

    if isinstance(
        value,
        (str, int, float, bool),
    ):
        return value

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(
                tzinfo=timezone.utc
            )

        return value.astimezone(
            timezone.utc
        ).isoformat()

    if isinstance(value, Mapping):
        return {
            str(key): normalize_log_value(item)
            for key, item in value.items()
        }

    if isinstance(value, (set, frozenset)):
        return [
            normalize_log_value(item)
            for item in sorted(
                value,
                key=str,
            )
        ]

    if isinstance(value, Sequence):
        return [
            normalize_log_value(item)
            for item in value
        ]

    return str(value)


def normalize_log_level(
    level: str,
) -> str:
    normalized_level = level.strip().upper()

    if normalized_level not in VALID_LOG_LEVELS:
        raise ValueError(
            f"Invalid structured log level: {level}"
        )

    return normalized_level


def resolve_log_format(
    output_format: str | None = None,
) -> str:
    if output_format is None:
        output_format = os.getenv(
            "NEURO_SLEEP_LOG_FORMAT",
            DEFAULT_LOG_FORMAT,
        )

    normalized_format = (
        output_format.strip().lower()
    )

    if normalized_format not in VALID_LOG_FORMATS:
        raise ValueError(
            "Invalid log format: "
            f"{output_format}"
        )

    return normalized_format


def resolve_minimum_log_level(
    minimum_level: str | None = None,
) -> str:
    if minimum_level is None:
        minimum_level = os.getenv(
            "NEURO_SLEEP_LOG_LEVEL",
            DEFAULT_LOG_LEVEL,
        )

    return normalize_log_level(
        minimum_level
    )


def should_emit_level(
    level: str,
    minimum_level: str,
) -> bool:
    return (
        LOG_LEVEL_VALUES[level]
        >= LOG_LEVEL_VALUES[minimum_level]
    )


def build_event_payload(
    event: str,
    level: str = "INFO",
    **fields: Any,
) -> dict[str, Any]:
    normalized_event = event.strip()

    if not normalized_event:
        raise ValueError(
            "Structured log event cannot be empty"
        )

    normalized_level = normalize_log_level(
        level
    )

    reserved_fields = (
        RESERVED_EVENT_FIELDS.intersection(
            fields
        )
    )

    if reserved_fields:
        reserved_names = ", ".join(
            sorted(reserved_fields)
        )

        raise ValueError(
            "Reserved structured log fields "
            f"cannot be overridden: {reserved_names}"
        )

    payload: dict[str, Any] = {
        "timestamp_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "level": normalized_level,
        "event": normalized_event,
    }

    payload.update(
        {
            key: normalize_log_value(value)
            for key, value in fields.items()
        }
    )

    return payload


def compact_object_key(
    object_key: str,
) -> str:
    parts = [
        part
        for part in object_key.split("/")
        if part
    ]

    if (
        len(parts) >= 4
        and parts[0] == "physionet"
        and parts[1] == "sleep-edfx"
    ):
        return "/".join(parts[3:])

    if len(parts) > 2:
        return "/".join(parts[-2:])

    return object_key


def format_byte_count(
    byte_count: Any,
) -> str:
    if not isinstance(byte_count, (int, float)):
        return str(byte_count)

    value = float(byte_count)

    units = [
        "B",
        "KB",
        "MB",
        "GB",
        "TB",
    ]

    unit = units[0]

    for candidate in units:
        unit = candidate

        if abs(value) < 1024 or candidate == units[-1]:
            break

        value /= 1024

    if unit == "B":
        return f"{int(value)} B"

    return f"{value:.1f} {unit}"


def format_pretty_event(
    payload: dict[str, Any],
) -> str | None:
    timestamp = format_console_timestamp(
        str(payload["timestamp_utc"])
    )

    event = str(payload["event"])

    if event == "pipeline_lock_acquired":
        return (
            f"{timestamp} 🔒 Pipeline lock acquired"
        )

    if event == "pipeline_lock_released":
        return (
            f"{timestamp} 🔓 Pipeline lock released"
        )

    if event == "pipeline_concurrent_blocked":
        return (
            f"{timestamp} ⛔ Extract blocked  "
            "another run is already active"
        )

    if event == "pipeline_lock_acquire_failed":
        return (
            f"{timestamp} ✗ Lock acquisition failed  "
            f"{payload.get('error_message')}"
        )

    if event == "pipeline_lock_release_failed":
        return (
            f"{timestamp} ! Lock release failed  "
            f"{payload.get('error_message')}"
        )

    if event == "pipeline_started":
        run_id = str(
            payload.get("run_id", "")
        )[:8]

        profile = payload.get(
            "data_profile",
            "unknown",
        )

        return (
            f"{timestamp} ▶ Extract started  "
            f"run={run_id}  profile={profile}"
        )

    if event == "heartbeat_started":
        interval = payload.get(
            "interval_seconds",
            "?",
        )

        if (
            isinstance(interval, float)
            and interval.is_integer()
        ):
            interval = int(interval)

        return (
            f"{timestamp} ♥ Heartbeat active  "
            f"every={interval}s"
        )

    if event == "manifest_loaded":
        recordings = payload.get(
            "selected_recording_count",
            0,
        )

        objects = payload.get(
            "selected_extract_object_count",
            0,
        )

        return (
            f"{timestamp} ◇ Manifest loaded  "
            f"recordings={recordings}  "
            f"objects={objects}"
        )

    if event == "file_progress":
        processed = payload.get(
            "processed_object_count",
            0,
        )

        total = payload.get(
            "total_object_count",
            0,
        )

        status = str(
            payload.get(
                "object_status",
                "unknown",
            )
        ).upper()

        status_labels = {
            "SKIPPED": "SKIP",
            "UPLOADED": "UPLOAD",
            "RECOVERED": "RECOVER",
        }

        status_label = status_labels.get(
            status,
            status,
        )

        object_key = compact_object_key(
            str(
                payload.get(
                    "object_key",
                    "unknown",
                )
            )
        )

        size_suffix = ""

        if status == "UPLOADED":
            size_suffix = (
                "  "
                + format_byte_count(
                    payload.get(
                        "file_size_bytes",
                        0,
                    )
                )
            )

        return (
            f"{timestamp} "
            f"[{processed}/{total}] "
            f"{status_label:<7} "
            f"{object_key}"
            f"{size_suffix}"
        )

    if event == "heartbeat_stopped":
        updates = payload.get(
            "heartbeat_update_count",
            0,
        )

        failures = payload.get(
            "heartbeat_failure_count",
            0,
        )

        return (
            f"{timestamp} ♥ Heartbeat stopped  "
            f"updates={updates}  "
            f"failures={failures}"
        )

    if event == "pipeline_completed":
        uploaded = payload.get(
            "uploaded_object_count",
            0,
        )

        skipped = payload.get(
            "skipped_object_count",
            0,
        )

        uploaded_bytes = format_byte_count(
            payload.get(
                "uploaded_bytes_total",
                0,
            )
        )

        return (
            f"{timestamp} ✓ Completed  "
            f"uploaded={uploaded}  "
            f"skipped={skipped}  "
            f"bytes={uploaded_bytes}"
        )

    if event == "retry_scheduled":
        component = str(
            payload.get(
                "component",
                "operation",
            )
        )

        component_labels = {
            "source_download": "download",
            "source_manifest": "manifest",
            "object_storage": "MinIO",
            "database": "PostgreSQL",
        }

        component_label = (
            component_labels.get(
                component,
                component,
            )
        )

        resource = payload.get(
            "resource"
        )

        operation = payload.get(
            "operation"
        )

        target = resource or operation or "unknown"

        if resource is not None:
            target = compact_object_key(
                str(target)
            )

        next_attempt = payload.get(
            "next_attempt",
            "?",
        )

        delay_seconds = float(
            payload.get(
                "delay_seconds",
                0,
            )
        )

        error_type = payload.get(
            "error_type",
            "UnknownError",
        )

        return (
            f"{timestamp} ↻ Retry {component_label}  "
            f"{target}  "
            f"attempt={next_attempt}  "
            f"in={delay_seconds:.1f}s  "
            f"reason={error_type}"
        )

    if event == "pipeline_failed":
        error_type = payload.get(
            "error_type",
            "Error",
        )

        error_message = payload.get(
            "error_message",
            "Unknown error",
        )

        return (
            f"{timestamp} ✗ Failed  "
            f"{error_type}: {error_message}"
        )

    if event == "heartbeat_update_failed":
        return (
            f"{timestamp} ! Heartbeat error  "
            f"{payload.get('error_message')}"
        )

    if event == "heartbeat_stop_failed":
        return (
            f"{timestamp} ! Heartbeat stop failed  "
            f"{payload.get('error_message')}"
        )

    if event == "pipeline_status_update_failed":
        return (
            f"{timestamp} ! Pipeline status "
            f"update failed  "
            f"{payload.get('error_message')}"
        )

    if event == "resource_cleanup_failed":
        return (
            f"{timestamp} ! Cleanup failed  "
            f"{payload.get('resource_type')}  "
            f"{payload.get('error_message')}"
        )

    if event == "resource_cleanup_completed":
        cleanup_errors = int(
            payload.get(
                "cleanup_error_count",
                0,
            )
        )

        if cleanup_errors == 0:
            return None

        return (
            f"{timestamp} ! Cleanup completed  "
            f"errors={cleanup_errors}"
        )

    level = str(payload["level"])

    return (
        f"{timestamp} "
        f"{level:<8} "
        f"{event}"
    )


def emit_event(
    event: str,
    level: str = "INFO",
    stream: TextIO | None = None,
    output_format: str | None = None,
    minimum_level: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    if stream is None:
        stream = sys.stdout

    payload = build_event_payload(
        event=event,
        level=level,
        **fields,
    )

    resolved_minimum_level = (
        resolve_minimum_log_level(
            minimum_level
        )
    )

    if not should_emit_level(
        level=str(payload["level"]),
        minimum_level=resolved_minimum_level,
    ):
        return payload

    resolved_format = resolve_log_format(
        output_format
    )

    if resolved_format == "json":
        output_line = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    else:
        output_line = format_pretty_event(
            payload
        )

    if output_line is not None:
        print(
            output_line,
            file=stream,
            flush=True,
        )

    return payload


def emit_exception(
    event: str,
    error: BaseException,
    stream: TextIO | None = None,
    output_format: str | None = None,
    minimum_level: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    return emit_event(
        event=event,
        level="ERROR",
        stream=stream,
        output_format=output_format,
        minimum_level=minimum_level,
        error_type=type(error).__name__,
        error_message=str(error),
        **fields,
    )
