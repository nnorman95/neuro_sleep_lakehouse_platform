from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal


SubjectCollection = Literal[
    "sleep-cassette",
    "sleep-telemetry",
]

NormalizedSex = Literal[
    "F",
    "M",
]

Treatment = Literal[
    "placebo",
    "temazepam",
]


@dataclass(frozen=True)
class NormalizedSubjectMetadata:
    collection: SubjectCollection
    source_subject_id: str
    source_subject_number: int
    age_years: int
    sex: NormalizedSex


@dataclass(frozen=True)
class RecordingSubjectContext:
    collection: SubjectCollection
    recording_key: str
    source_subject_id: str
    night_number: int
    lights_off_seconds: int
    treatment: Treatment | None


@dataclass(frozen=True)
class SubjectMetadataBundle:
    subjects: tuple[
        NormalizedSubjectMetadata,
        ...,
    ]
    recording_contexts: tuple[
        RecordingSubjectContext,
        ...,
    ]

    def subject_by_source_id(
        self,
    ) -> dict[
        str,
        NormalizedSubjectMetadata,
    ]:
        return {
            subject.source_subject_id: (
                subject
            )
            for subject in self.subjects
        }

    def context_by_recording_key(
        self,
    ) -> dict[
        str,
        RecordingSubjectContext,
    ]:
        return {
            context.recording_key: context
            for context
            in self.recording_contexts
        }


def integer_value(
    value: object,
    field_name: str,
) -> int:
    if isinstance(value, bool):
        raise ValueError(
            f"{field_name} cannot be boolean"
        )

    if not isinstance(
        value,
        (int, float),
    ):
        raise ValueError(
            f"{field_name} must be numeric"
        )

    integer = int(value)

    if float(value) != float(integer):
        raise ValueError(
            f"{field_name} must be an integer"
        )

    return integer


def excel_time_to_seconds(
    value: object,
) -> int:
    if not isinstance(
        value,
        (int, float),
    ):
        raise ValueError(
            "lights_off must be numeric"
        )

    fraction = float(value)

    if not 0.0 <= fraction < 1.0:
        raise ValueError(
            "lights_off must be an Excel "
            "time fraction in [0, 1)"
        )

    return round(
        fraction * 24 * 60 * 60
    )


def sc_source_subject_id(
    subject_number: int,
) -> str:
    return f"SC{subject_number:02d}"


def st_source_subject_id(
    subject_number: int,
) -> str:
    return f"ST{subject_number:02d}"


def sc_recording_key(
    subject_number: int,
    night_number: int,
) -> str:
    return (
        f"SC4{subject_number:02d}"
        f"{night_number}E"
    )


def st_recording_key(
    subject_number: int,
    night_number: int,
) -> str:
    return (
        f"ST7{subject_number:02d}"
        f"{night_number}J"
    )


def normalize_sc_sex(
    value: object,
) -> NormalizedSex:
    code = integer_value(
        value,
        "SC sex",
    )

    if code == 1:
        return "F"

    if code == 2:
        return "M"

    raise ValueError(
        "SC sex must use 1=F or 2=M"
    )


def normalize_st_sex(
    value: object,
) -> NormalizedSex:
    code = integer_value(
        value,
        "ST sex",
    )

    if code == 1:
        return "M"

    if code == 2:
        return "F"

    raise ValueError(
        "ST sex must use 1=M or 2=F"
    )


def validate_subject(
    subject: NormalizedSubjectMetadata,
) -> None:
    if subject.source_subject_number < 0:
        raise ValueError(
            "source_subject_number cannot "
            "be negative"
        )

    if not 0 < subject.age_years <= 120:
        raise ValueError(
            "age_years must be between "
            "1 and 120"
        )

    if not subject.source_subject_id:
        raise ValueError(
            "source_subject_id cannot "
            "be empty"
        )


def validate_context(
    context: RecordingSubjectContext,
) -> None:
    if context.night_number <= 0:
        raise ValueError(
            "night_number must be positive"
        )

    if not (
        0
        <= context.lights_off_seconds
        < 24 * 60 * 60
    ):
        raise ValueError(
            "lights_off_seconds must be "
            "inside one day"
        )

    if not context.recording_key:
        raise ValueError(
            "recording_key cannot be empty"
        )


def build_subject_metadata_bundle(
    subjects: Iterable[
        NormalizedSubjectMetadata
    ],
    recording_contexts: Iterable[
        RecordingSubjectContext
    ],
) -> SubjectMetadataBundle:
    subject_map: dict[
        str,
        NormalizedSubjectMetadata,
    ] = {}

    for subject in subjects:
        validate_subject(subject)

        existing = subject_map.get(
            subject.source_subject_id
        )

        if (
            existing is not None
            and existing != subject
        ):
            raise ValueError(
                "Conflicting demographic "
                "metadata for "
                f"{subject.source_subject_id}"
            )

        subject_map[
            subject.source_subject_id
        ] = subject

    if not subject_map:
        raise ValueError(
            "At least one subject is required"
        )

    context_map: dict[
        str,
        RecordingSubjectContext,
    ] = {}

    for context in recording_contexts:
        validate_context(context)

        if (
            context.source_subject_id
            not in subject_map
        ):
            raise ValueError(
                "Recording context references "
                "an unknown subject: "
                f"{context.source_subject_id}"
            )

        if (
            context.recording_key
            in context_map
        ):
            raise ValueError(
                "Duplicate recording context: "
                f"{context.recording_key}"
            )

        context_map[
            context.recording_key
        ] = context

    if not context_map:
        raise ValueError(
            "At least one recording context "
            "is required"
        )

    return SubjectMetadataBundle(
        subjects=tuple(
            sorted(
                subject_map.values(),
                key=lambda item: (
                    item.collection,
                    item.source_subject_number,
                ),
            )
        ),
        recording_contexts=tuple(
            sorted(
                context_map.values(),
                key=lambda item: (
                    item.collection,
                    item.recording_key,
                ),
            )
        ),
    )


def parse_sc_rows(
    rows: Iterable[
        tuple[object, ...]
    ],
) -> SubjectMetadataBundle:
    subjects: list[
        NormalizedSubjectMetadata
    ] = []

    contexts: list[
        RecordingSubjectContext
    ] = []

    for row_number, row in enumerate(
        rows,
        start=2,
    ):
        if len(row) != 5:
            raise ValueError(
                "SC metadata row "
                f"{row_number} must contain "
                "5 columns"
            )

        (
            subject_value,
            night_value,
            age_value,
            sex_value,
            lights_off_value,
        ) = row

        subject_number = integer_value(
            subject_value,
            "SC subject",
        )
        night_number = integer_value(
            night_value,
            "SC night",
        )
        age_years = integer_value(
            age_value,
            "SC age",
        )
        source_subject_id = (
            sc_source_subject_id(
                subject_number
            )
        )

        subjects.append(
            NormalizedSubjectMetadata(
                collection="sleep-cassette",
                source_subject_id=(
                    source_subject_id
                ),
                source_subject_number=(
                    subject_number
                ),
                age_years=age_years,
                sex=normalize_sc_sex(
                    sex_value
                ),
            )
        )

        contexts.append(
            RecordingSubjectContext(
                collection="sleep-cassette",
                recording_key=(
                    sc_recording_key(
                        subject_number,
                        night_number,
                    )
                ),
                source_subject_id=(
                    source_subject_id
                ),
                night_number=night_number,
                lights_off_seconds=(
                    excel_time_to_seconds(
                        lights_off_value
                    )
                ),
                treatment=None,
            )
        )

    return build_subject_metadata_bundle(
        subjects,
        contexts,
    )


def parse_st_rows(
    rows: Iterable[
        tuple[object, ...]
    ],
) -> SubjectMetadataBundle:
    subjects: list[
        NormalizedSubjectMetadata
    ] = []

    contexts: list[
        RecordingSubjectContext
    ] = []

    for row_number, row in enumerate(
        rows,
        start=3,
    ):
        if len(row) != 7:
            raise ValueError(
                "ST metadata row "
                f"{row_number} must contain "
                "7 columns"
            )

        (
            subject_value,
            age_value,
            sex_value,
            placebo_night_value,
            placebo_lights_off_value,
            temazepam_night_value,
            temazepam_lights_off_value,
        ) = row

        subject_number = integer_value(
            subject_value,
            "ST subject",
        )
        age_years = integer_value(
            age_value,
            "ST age",
        )
        source_subject_id = (
            st_source_subject_id(
                subject_number
            )
        )

        subjects.append(
            NormalizedSubjectMetadata(
                collection="sleep-telemetry",
                source_subject_id=(
                    source_subject_id
                ),
                source_subject_number=(
                    subject_number
                ),
                age_years=age_years,
                sex=normalize_st_sex(
                    sex_value
                ),
            )
        )

        for (
            treatment,
            night_value,
            lights_off_value,
        ) in (
            (
                "placebo",
                placebo_night_value,
                placebo_lights_off_value,
            ),
            (
                "temazepam",
                temazepam_night_value,
                temazepam_lights_off_value,
            ),
        ):
            night_number = integer_value(
                night_value,
                f"ST {treatment} night",
            )

            contexts.append(
                RecordingSubjectContext(
                    collection=(
                        "sleep-telemetry"
                    ),
                    recording_key=(
                        st_recording_key(
                            subject_number,
                            night_number,
                        )
                    ),
                    source_subject_id=(
                        source_subject_id
                    ),
                    night_number=(
                        night_number
                    ),
                    lights_off_seconds=(
                        excel_time_to_seconds(
                            lights_off_value
                        )
                    ),
                    treatment=treatment,
                )
            )

    return build_subject_metadata_bundle(
        subjects,
        contexts,
    )


def non_empty_sheet(
    workbook: object,
) -> object:
    sheets = workbook.sheets()

    for sheet in sheets:
        if sheet.nrows > 0:
            return sheet

    raise ValueError(
        "Workbook contains no non-empty sheet"
    )


def sheet_rows(
    sheet: object,
    start_row_index: int,
) -> tuple[
    tuple[object, ...],
    ...,
]:
    return tuple(
        tuple(
            sheet.cell_value(
                row_index,
                column_index,
            )
            for column_index
            in range(sheet.ncols)
        )
        for row_index
        in range(
            start_row_index,
            sheet.nrows,
        )
    )


def parse_sc_workbook(
    path: str | Path,
) -> SubjectMetadataBundle:
    import xlrd

    workbook = xlrd.open_workbook(
        str(path)
    )
    sheet = non_empty_sheet(workbook)

    expected_headers = (
        "subject",
        "night",
        "age",
        "sex (F=1)",
        "LightsOff",
    )

    actual_headers = tuple(
        sheet.cell_value(
            0,
            column_index,
        )
        for column_index
        in range(sheet.ncols)
    )

    if actual_headers != expected_headers:
        raise ValueError(
            "Unexpected SC metadata headers: "
            f"{actual_headers}"
        )

    return parse_sc_rows(
        sheet_rows(
            sheet,
            start_row_index=1,
        )
    )


def parse_st_workbook(
    path: str | Path,
) -> SubjectMetadataBundle:
    import xlrd

    workbook = xlrd.open_workbook(
        str(path)
    )
    sheet = non_empty_sheet(workbook)

    expected_second_headers = (
        "Nr",
        "Age",
        "M1/F2",
        "night nr",
        "lights off",
        "night nr",
        "lights off",
    )

    actual_second_headers = tuple(
        sheet.cell_value(
            1,
            column_index,
        )
        for column_index
        in range(sheet.ncols)
    )

    if (
        actual_second_headers
        != expected_second_headers
    ):
        raise ValueError(
            "Unexpected ST metadata headers: "
            f"{actual_second_headers}"
        )

    return parse_st_rows(
        sheet_rows(
            sheet,
            start_row_index=2,
        )
    )


def merge_subject_metadata_bundles(
    *bundles: SubjectMetadataBundle,
) -> SubjectMetadataBundle:
    return build_subject_metadata_bundle(
        (
            subject
            for bundle in bundles
            for subject in bundle.subjects
        ),
        (
            context
            for bundle in bundles
            for context
            in bundle.recording_contexts
        ),
    )
