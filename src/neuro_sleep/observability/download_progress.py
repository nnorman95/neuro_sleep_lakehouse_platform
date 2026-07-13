import sys
from datetime import datetime
from pathlib import PurePosixPath
from time import perf_counter
from typing import TextIO

from neuro_sleep.observability.structured_logging import (
    emit_event,
    emit_exception,
    format_byte_count,
    resolve_log_format,
)


def format_duration(
    seconds: float,
) -> str:
    if seconds < 0:
        seconds = 0

    rounded_seconds = int(round(seconds))

    if rounded_seconds < 60:
        return f"{rounded_seconds}s"

    minutes, remaining_seconds = divmod(
        rounded_seconds,
        60,
    )

    if minutes < 60:
        return (
            f"{minutes}m "
            f"{remaining_seconds}s"
        )

    hours, remaining_minutes = divmod(
        minutes,
        60,
    )

    return (
        f"{hours}h "
        f"{remaining_minutes}m"
    )


class DownloadProgressReporter:
    def __init__(
        self,
        relative_path: str,
        stream: TextIO | None = None,
        output_format: str | None = None,
        interactive: bool | None = None,
        bar_width: int = 20,
        minimum_update_interval_seconds: float = 0.1,
    ) -> None:
        if bar_width <= 0:
            raise ValueError(
                "bar_width must be positive"
            )

        if minimum_update_interval_seconds < 0:
            raise ValueError(
                "minimum update interval "
                "cannot be negative"
            )

        if stream is None:
            stream = sys.stdout

        if interactive is None:
            interactive = bool(
                hasattr(stream, "isatty")
                and stream.isatty()
            )

        self.relative_path = relative_path
        self.file_name = PurePosixPath(
            relative_path
        ).name

        self.stream = stream
        self.output_format = resolve_log_format(
            output_format
        )
        self.interactive = interactive
        self.bar_width = bar_width

        self.minimum_update_interval_seconds = (
            minimum_update_interval_seconds
        )

        self.started_at = 0.0
        self.last_render_at = 0.0

        self.downloaded_bytes = 0
        self.total_bytes: int | None = None

        self.last_json_percent_bucket = -1
        self.line_is_active = False

    def _calculate_speed(self) -> float:
        elapsed_seconds = (
            perf_counter() - self.started_at
        )

        if elapsed_seconds <= 0:
            return 0.0

        return (
            self.downloaded_bytes
            / elapsed_seconds
        )

    def _calculate_percent(
        self,
    ) -> float | None:
        if (
            self.total_bytes is None
            or self.total_bytes <= 0
        ):
            return None

        percent = (
            self.downloaded_bytes
            / self.total_bytes
            * 100
        )

        return max(
            0.0,
            min(percent, 100.0),
        )

    def _calculate_eta(
        self,
        speed_bytes_per_second: float,
    ) -> float | None:
        if (
            self.total_bytes is None
            or speed_bytes_per_second <= 0
        ):
            return None

        remaining_bytes = max(
            self.total_bytes
            - self.downloaded_bytes,
            0,
        )

        return (
            remaining_bytes
            / speed_bytes_per_second
        )

    def _build_pretty_line(
        self,
        completed: bool = False,
    ) -> str:
        timestamp = datetime.now().strftime(
            "%H:%M:%S"
        )

        speed = self._calculate_speed()
        percent = self._calculate_percent()
        eta_seconds = self._calculate_eta(
            speed
        )

        if completed:
            return (
                f"{timestamp} ✓ Downloaded  "
                f"{self.file_name}  "
                f"{format_byte_count(self.downloaded_bytes)}"
            )

        if percent is None:
            return (
                f"{timestamp} ↓ {self.file_name}  "
                f"{format_byte_count(self.downloaded_bytes)}  "
                f"{format_byte_count(speed)}/s"
            )

        filled_width = round(
            self.bar_width
            * percent
            / 100
        )

        empty_width = (
            self.bar_width - filled_width
        )

        progress_bar = (
            "█" * filled_width
            + "░" * empty_width
        )

        downloaded_text = format_byte_count(
            self.downloaded_bytes
        )

        total_text = format_byte_count(
            self.total_bytes
        )

        eta_text = ""

        if eta_seconds is not None:
            eta_text = (
                "  ETA "
                + format_duration(eta_seconds)
            )

        return (
            f"{timestamp} ↓ {self.file_name}  "
            f"[{progress_bar}] "
            f"{percent:5.1f}%  "
            f"{downloaded_text}/{total_text}  "
            f"{format_byte_count(speed)}/s"
            f"{eta_text}"
        )

    def _render_pretty(
        self,
        completed: bool = False,
        force: bool = False,
    ) -> None:
        current_time = perf_counter()

        if (
            not force
            and current_time - self.last_render_at
            < self.minimum_update_interval_seconds
        ):
            return

        self.last_render_at = current_time

        line = self._build_pretty_line(
            completed=completed
        )

        if self.interactive:
            ending = "\n" if completed else ""

            print(
                "\r\033[2K" + line,
                end=ending,
                file=self.stream,
                flush=True,
            )

            self.line_is_active = not completed

        elif completed:
            print(
                line,
                file=self.stream,
                flush=True,
            )

    def _emit_json_progress(
        self,
        force: bool = False,
    ) -> None:
        percent = self._calculate_percent()

        if percent is None:
            if not force:
                return

            percent_bucket = -1

        else:
            percent_bucket = int(
                percent // 5
            )

            if (
                not force
                and percent_bucket
                == self.last_json_percent_bucket
            ):
                return

        self.last_json_percent_bucket = (
            percent_bucket
        )

        speed = self._calculate_speed()

        emit_event(
            event="file_download_progress",
            output_format="json",
            relative_path=self.relative_path,
            downloaded_bytes=self.downloaded_bytes,
            total_bytes=self.total_bytes,
            download_percent=percent,
            speed_bytes_per_second=round(
                speed,
                2,
            ),
        )

    def start(
        self,
        total_bytes: int | None,
    ) -> None:
        self.started_at = perf_counter()
        self.last_render_at = 0.0

        self.downloaded_bytes = 0
        self.total_bytes = total_bytes

        self.last_json_percent_bucket = -1
        self.line_is_active = False

        if self.output_format == "json":
            emit_event(
                event="file_download_started",
                output_format="json",
                relative_path=self.relative_path,
                total_bytes=total_bytes,
            )

        else:
            self._render_pretty(
                force=True
            )

    def update(
        self,
        downloaded_bytes: int,
        total_bytes: int | None = None,
    ) -> None:
        self.downloaded_bytes = (
            downloaded_bytes
        )

        if total_bytes is not None:
            self.total_bytes = total_bytes

        if self.output_format == "json":
            self._emit_json_progress()

        else:
            self._render_pretty()

    def complete(
        self,
        downloaded_bytes: int,
        total_bytes: int | None = None,
    ) -> None:
        self.downloaded_bytes = (
            downloaded_bytes
        )

        if total_bytes is not None:
            self.total_bytes = total_bytes

        if self.output_format == "json":
            emit_event(
                event="file_download_completed",
                output_format="json",
                relative_path=self.relative_path,
                downloaded_bytes=(
                    self.downloaded_bytes
                ),
                total_bytes=self.total_bytes,
                download_percent=(
                    self._calculate_percent()
                ),
                speed_bytes_per_second=round(
                    self._calculate_speed(),
                    2,
                ),
            )

        else:
            self._render_pretty(
                completed=True,
                force=True,
            )

    def fail(
        self,
        error: BaseException,
        downloaded_bytes: int,
        total_bytes: int | None = None,
    ) -> None:
        self.downloaded_bytes = (
            downloaded_bytes
        )

        if total_bytes is not None:
            self.total_bytes = total_bytes

        if self.output_format == "json":
            emit_exception(
                event="file_download_failed",
                error=error,
                output_format="json",
                relative_path=self.relative_path,
                downloaded_bytes=(
                    self.downloaded_bytes
                ),
                total_bytes=self.total_bytes,
            )

            return

        if self.line_is_active:
            print(
                file=self.stream,
                flush=True,
            )

            self.line_is_active = False

        timestamp = datetime.now().strftime(
            "%H:%M:%S"
        )

        print(
            f"{timestamp} ✗ Download failed  "
            f"{self.file_name}  "
            f"{type(error).__name__}: {error}",
            file=self.stream,
            flush=True,
        )
