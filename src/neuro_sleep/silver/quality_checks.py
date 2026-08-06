from __future__ import annotations

from dataclasses import dataclass
from math import isclose
from typing import Literal

from neuro_sleep.silver.recording_builder import (
    SilverRecordingBundle,
)


EPOCH_SECONDS = 30.0
FLOAT_TOLERANCE = 1e-9

QualitySeverity = Literal[
    "warning",
    "error",
]


class SilverQualityError(ValueError):
    def __init__(
        self,
        report: "SilverQualityReport",
    ) -> None:
        self.report = report

        details = "; ".join(
            f"{issue.code}: {issue.message}"
            for issue in report.issues
            if issue.severity == "error"
        )

        super().__init__(
            "Silver quality checks failed: "
            f"{details}"
        )


@dataclass(frozen=True)
class QualityIssue:
    code: str
    severity: QualitySeverity
    message: str


@dataclass(frozen=True)
class SilverQualityReport:
    issues: tuple[QualityIssue, ...]

    @property
    def error_count(self) -> int:
        return sum(
            issue.severity == "error"
            for issue in self.issues
        )

    @property
    def warning_count(self) -> int:
        return sum(
            issue.severity == "warning"
            for issue in self.issues
        )

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    def raise_for_errors(self) -> None:
        errors = [
            issue
            for issue in self.issues
            if issue.severity == "error"
        ]

        if not errors:
            return

        raise SilverQualityError(
            report=self
        )


def add_issue(
    issues: list[QualityIssue],
    code: str,
    severity: QualitySeverity,
    message: str,
) -> None:
    issues.append(
        QualityIssue(
            code=code,
            severity=severity,
            message=message,
        )
    )


def check_recording_metadata(
    bundle: SilverRecordingBundle,
    issues: list[QualityIssue],
) -> None:
    recording = bundle.recording

    if recording.recording_id.version != 7:
        add_issue(
            issues,
            code="INVALID_RECORDING_UUID",
            severity="error",
            message=(
                "recording_id is not UUIDv7"
            ),
        )

    if recording.duration_seconds <= 0:
        add_issue(
            issues,
            code="INVALID_RECORDING_DURATION",
            severity="error",
            message=(
                "duration_seconds must be "
                "positive"
            ),
        )

    expected_epoch_count_exact = (
        recording.duration_seconds
        / EPOCH_SECONDS
    )

    expected_epoch_count = round(
        expected_epoch_count_exact
    )

    if not isclose(
        expected_epoch_count_exact,
        expected_epoch_count,
        abs_tol=FLOAT_TOLERANCE,
    ):
        add_issue(
            issues,
            code="RECORDING_NOT_EPOCH_ALIGNED",
            severity="error",
            message=(
                "recording duration is not "
                "divisible by 30 seconds"
            ),
        )

    elif (
        recording.in_range_epoch_count
        != expected_epoch_count
    ):
        add_issue(
            issues,
            code="IN_RANGE_EPOCH_COUNT_MISMATCH",
            severity="error",
            message=(
                "in_range_epoch_count does "
                "not match recording duration"
            ),
        )

    if (
        recording.channel_count
        != bundle.channel_count
    ):
        add_issue(
            issues,
            code="CHANNEL_COUNT_MISMATCH",
            severity="error",
            message=(
                "recording channel_count does "
                "not match bundle channels"
            ),
        )

    if (
        recording.annotation_count
        != bundle.interval_count
    ):
        add_issue(
            issues,
            code="INTERVAL_COUNT_MISMATCH",
            severity="error",
            message=(
                "recording annotation_count "
                "does not match intervals"
            ),
        )

    if (
        recording.in_range_epoch_count
        != bundle.epoch_count
    ):
        add_issue(
            issues,
            code="EPOCH_COUNT_MISMATCH",
            severity="error",
            message=(
                "recording in-range count "
                "does not match emitted epochs"
            ),
        )

    expected_source_epoch_count = (
        recording.in_range_epoch_count
        + recording.out_of_range_epoch_count
    )

    if (
        bundle.source_epoch_count
        != expected_source_epoch_count
    ):
        add_issue(
            issues,
            code="SOURCE_EPOCH_COUNT_MISMATCH",
            severity="error",
            message=(
                "source epoch count does not "
                "match in-range plus excluded "
                "epochs"
            ),
        )

    if (
        recording.trailing_overhang_seconds
        > 0
    ):
        add_issue(
            issues,
            code="TRAILING_HYPNOGRAM_OVERHANG",
            severity="warning",
            message=(
                "Hypnogram extends beyond PSG "
                "by "
                f"{recording.trailing_overhang_seconds} "
                "seconds"
            ),
        )


def check_related_ids(
    bundle: SilverRecordingBundle,
    issues: list[QualityIssue],
) -> None:
    recording_id = bundle.recording_id

    related_items = (
        *bundle.channels,
        *bundle.intervals,
        *bundle.epochs,
    )

    if any(
        item.recording_id != recording_id
        for item in related_items
    ):
        add_issue(
            issues,
            code="RELATED_RECORDING_ID_MISMATCH",
            severity="error",
            message=(
                "at least one related entity "
                "uses another recording_id"
            ),
        )

    identifiers = (
        [channel.channel_id for channel in bundle.channels]
        + [interval.interval_id for interval in bundle.intervals]
        + [epoch.epoch_id for epoch in bundle.epochs]
    )

    if any(
        identifier.version != 7
        for identifier in identifiers
    ):
        add_issue(
            issues,
            code="INVALID_RELATED_UUID",
            severity="error",
            message=(
                "at least one related entity "
                "identifier is not UUIDv7"
            ),
        )

    if len(identifiers) != len(
        set(identifiers)
    ):
        add_issue(
            issues,
            code="DUPLICATE_ENTITY_ID",
            severity="error",
            message=(
                "duplicate entity identifiers "
                "were detected"
            ),
        )


def check_channels(
    bundle: SilverRecordingBundle,
    issues: list[QualityIssue],
) -> None:
    positions = [
        channel.position
        for channel in bundle.channels
    ]

    normalized_names = [
        channel.normalized_name
        for channel in bundle.channels
    ]

    if len(positions) != len(
        set(positions)
    ):
        add_issue(
            issues,
            code="DUPLICATE_CHANNEL_POSITION",
            severity="error",
            message=(
                "channel positions must be "
                "unique per recording"
            ),
        )

    if len(normalized_names) != len(
        set(normalized_names)
    ):
        add_issue(
            issues,
            code="DUPLICATE_CHANNEL_NAME",
            severity="error",
            message=(
                "normalized channel names "
                "must be unique per recording"
            ),
        )

    expected_positions = list(
        range(
            1,
            len(bundle.channels) + 1,
        )
    )

    if sorted(positions) != (
        expected_positions
    ):
        add_issue(
            issues,
            code="NON_CONTIGUOUS_CHANNEL_POSITIONS",
            severity="error",
            message=(
                "channel positions must form "
                "a contiguous sequence from 1"
            ),
        )

    missing_unit_channels = [
        channel.normalized_name
        for channel in bundle.channels
        if channel.physical_dimension
        is None
    ]

    if missing_unit_channels:
        add_issue(
            issues,
            code="MISSING_CHANNEL_UNITS",
            severity="warning",
            message=(
                "physical unit is missing for "
                + ", ".join(
                    missing_unit_channels
                )
            ),
        )


def check_intervals(
    bundle: SilverRecordingBundle,
    issues: list[QualityIssue],
) -> None:
    previous_end_seconds: (
        float
        | None
    ) = None

    previous_annotation_index: (
        int
        | None
    ) = None

    for interval in bundle.intervals:
        if (
            previous_annotation_index
            is not None
            and interval.source_annotation_index
            <= previous_annotation_index
        ):
            add_issue(
                issues,
                code="INTERVAL_ORDER_INVALID",
                severity="error",
                message=(
                    "source annotation indexes "
                    "are not strictly increasing"
                ),
            )
            break

        if (
            previous_end_seconds
            is not None
            and interval.onset_seconds
            < previous_end_seconds
            - FLOAT_TOLERANCE
        ):
            add_issue(
                issues,
                code="INTERVAL_OVERLAP",
                severity="error",
                message=(
                    "source annotation "
                    "intervals overlap"
                ),
            )
            break

        previous_annotation_index = (
            interval.source_annotation_index
        )

        previous_end_seconds = (
            interval.end_seconds
        )

    special_labels = sorted(
        {
            interval.normalized_stage
            for interval in bundle.intervals
            if interval.normalized_stage
            in {
                "UNKNOWN",
                "MOVEMENT",
            }
        }
    )

    if special_labels:
        add_issue(
            issues,
            code="SPECIAL_SLEEP_STAGE_LABELS",
            severity="warning",
            message=(
                "non-standard analytical "
                "stages are present: "
                + ", ".join(special_labels)
            ),
        )


def check_epochs(
    bundle: SilverRecordingBundle,
    issues: list[QualityIssue],
) -> None:
    if not bundle.epochs:
        add_issue(
            issues,
            code="NO_EMITTED_EPOCHS",
            severity="error",
            message=(
                "recording has no in-range "
                "sleep-stage epochs"
            ),
        )
        return

    epoch_numbers = [
        epoch.epoch_number
        for epoch in bundle.epochs
    ]

    if len(epoch_numbers) != len(
        set(epoch_numbers)
    ):
        add_issue(
            issues,
            code="DUPLICATE_EPOCH_NUMBER",
            severity="error",
            message=(
                "duplicate epoch numbers were "
                "detected"
            ),
        )

    expected_epoch_numbers = list(
        range(len(bundle.epochs))
    )

    if epoch_numbers != (
        expected_epoch_numbers
    ):
        add_issue(
            issues,
            code="NON_CONTIGUOUS_EPOCHS",
            severity="error",
            message=(
                "epochs must be ordered and "
                "contiguous from zero"
            ),
        )

    first_epoch = bundle.epochs[0]
    last_epoch = bundle.epochs[-1]

    if not isclose(
        first_epoch.start_seconds,
        0.0,
        abs_tol=FLOAT_TOLERANCE,
    ):
        add_issue(
            issues,
            code="EPOCH_COVERAGE_START_MISMATCH",
            severity="error",
            message=(
                "first emitted epoch does not "
                "start at zero"
            ),
        )

    if not isclose(
        last_epoch.end_seconds,
        bundle.recording.duration_seconds,
        abs_tol=FLOAT_TOLERANCE,
    ):
        add_issue(
            issues,
            code="EPOCH_COVERAGE_END_MISMATCH",
            severity="error",
            message=(
                "last emitted epoch does not "
                "end at recording duration"
            ),
        )

    for previous, current in zip(
        bundle.epochs,
        bundle.epochs[1:],
    ):
        if not isclose(
            previous.end_seconds,
            current.start_seconds,
            abs_tol=FLOAT_TOLERANCE,
        ):
            add_issue(
                issues,
                code="EPOCH_TIMELINE_GAP",
                severity="error",
                message=(
                    "a gap or overlap exists "
                    "between emitted epochs"
                ),
            )
            break


def run_silver_quality_checks(
    bundle: SilverRecordingBundle,
) -> SilverQualityReport:
    issues: list[QualityIssue] = []

    check_recording_metadata(
        bundle=bundle,
        issues=issues,
    )

    check_related_ids(
        bundle=bundle,
        issues=issues,
    )

    check_channels(
        bundle=bundle,
        issues=issues,
    )

    check_intervals(
        bundle=bundle,
        issues=issues,
    )

    check_epochs(
        bundle=bundle,
        issues=issues,
    )

    return SilverQualityReport(
        issues=tuple(issues)
    )
