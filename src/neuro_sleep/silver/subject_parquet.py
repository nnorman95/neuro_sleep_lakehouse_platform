from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha256

import pyarrow as pa

from neuro_sleep.silver.parquet_schemas import (
    build_silver_schema,
)
from neuro_sleep.silver.subject_metadata import (
    NormalizedSubjectMetadata,
    RecordingSubjectContext,
)


SUBJECTS_SCHEMA = build_silver_schema(
    dataset_name="subjects",
    fields=[
        pa.field(
            "subject_key",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "source_system",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "dataset_version",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "collection",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "source_subject_id",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "source_subject_number",
            pa.int16(),
            nullable=False,
        ),
        pa.field(
            "age_years",
            pa.int16(),
            nullable=False,
        ),
        pa.field(
            "sex",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "source_bucket",
            pa.string(),
            nullable=False,
        ),
        pa.field(
            "source_object_key",
            pa.string(),
            nullable=False,
        ),
    ],
)


RECORDING_CONTEXTS_SCHEMA = (
    build_silver_schema(
        dataset_name="recording_contexts",
        fields=[
            pa.field(
                "recording_key",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "subject_key",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "source_system",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "dataset_version",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "collection",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "night_number",
                pa.int16(),
                nullable=False,
            ),
            pa.field(
                "lights_off_seconds",
                pa.int32(),
                nullable=False,
            ),
            pa.field(
                "treatment",
                pa.string(),
                nullable=True,
            ),
            pa.field(
                "source_bucket",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "source_object_key",
                pa.string(),
                nullable=False,
            ),
        ],
    )
)


def validate_lineage_value(
    value: str,
    field_name: str,
) -> str:
    normalized = value.strip()

    if not normalized:
        raise ValueError(
            f"{field_name} cannot be empty"
        )

    return normalized


def build_subject_key(
    *,
    source_system: str,
    dataset_version: str,
    collection: str,
    source_subject_id: str,
) -> str:
    identity_parts = (
        validate_lineage_value(
            source_system,
            "source_system",
        ),
        validate_lineage_value(
            dataset_version,
            "dataset_version",
        ),
        validate_lineage_value(
            collection,
            "collection",
        ),
        validate_lineage_value(
            source_subject_id,
            "source_subject_id",
        ),
    )

    canonical_identity = "\x1f".join(
        identity_parts
    )

    return sha256(
        canonical_identity.encode("utf-8")
    ).hexdigest()


def subjects_to_table(
    subjects: Iterable[
        NormalizedSubjectMetadata
    ],
    *,
    source_system: str,
    dataset_version: str,
    source_bucket: str,
    source_object_keys: dict[str, str],
) -> pa.Table:
    normalized_source_system = (
        validate_lineage_value(
            source_system,
            "source_system",
        )
    )
    normalized_dataset_version = (
        validate_lineage_value(
            dataset_version,
            "dataset_version",
        )
    )
    normalized_source_bucket = (
        validate_lineage_value(
            source_bucket,
            "source_bucket",
        )
    )

    rows: list[dict[str, object]] = []
    seen_subject_keys: set[str] = set()

    for subject in subjects:
        try:
            source_object_key = (
                source_object_keys[
                    subject.collection
                ]
            )

        except KeyError as error:
            raise ValueError(
                "Missing source object key "
                "for collection: "
                f"{subject.collection}"
            ) from error

        normalized_object_key = (
            validate_lineage_value(
                source_object_key,
                "source_object_key",
            )
        )

        subject_key = build_subject_key(
            source_system=(
                normalized_source_system
            ),
            dataset_version=(
                normalized_dataset_version
            ),
            collection=subject.collection,
            source_subject_id=(
                subject.source_subject_id
            ),
        )

        if subject_key in seen_subject_keys:
            raise ValueError(
                "Duplicate subject identity: "
                f"{subject.source_subject_id}"
            )

        seen_subject_keys.add(subject_key)

        rows.append(
            {
                "subject_key": subject_key,
                "source_system": (
                    normalized_source_system
                ),
                "dataset_version": (
                    normalized_dataset_version
                ),
                "collection": (
                    subject.collection
                ),
                "source_subject_id": (
                    subject.source_subject_id
                ),
                "source_subject_number": (
                    subject
                    .source_subject_number
                ),
                "age_years": (
                    subject.age_years
                ),
                "sex": subject.sex,
                "source_bucket": (
                    normalized_source_bucket
                ),
                "source_object_key": (
                    normalized_object_key
                ),
            }
        )

    if not rows:
        raise ValueError(
            "At least one subject is required"
        )

    return pa.Table.from_pylist(
        rows,
        schema=SUBJECTS_SCHEMA,
    )


def recording_contexts_to_table(
    contexts: Iterable[
        RecordingSubjectContext
    ],
    *,
    source_system: str,
    dataset_version: str,
    source_bucket: str,
    source_object_keys: dict[str, str],
) -> pa.Table:
    normalized_source_system = (
        validate_lineage_value(
            source_system,
            "source_system",
        )
    )
    normalized_dataset_version = (
        validate_lineage_value(
            dataset_version,
            "dataset_version",
        )
    )
    normalized_source_bucket = (
        validate_lineage_value(
            source_bucket,
            "source_bucket",
        )
    )

    rows: list[dict[str, object]] = []
    seen_recording_keys: set[str] = set()

    for context in contexts:
        if (
            context.recording_key
            in seen_recording_keys
        ):
            raise ValueError(
                "Duplicate recording context: "
                f"{context.recording_key}"
            )

        seen_recording_keys.add(
            context.recording_key
        )

        try:
            source_object_key = (
                source_object_keys[
                    context.collection
                ]
            )

        except KeyError as error:
            raise ValueError(
                "Missing source object key "
                "for collection: "
                f"{context.collection}"
            ) from error

        subject_key = build_subject_key(
            source_system=(
                normalized_source_system
            ),
            dataset_version=(
                normalized_dataset_version
            ),
            collection=context.collection,
            source_subject_id=(
                context.source_subject_id
            ),
        )

        rows.append(
            {
                "recording_key": (
                    context.recording_key
                ),
                "subject_key": subject_key,
                "source_system": (
                    normalized_source_system
                ),
                "dataset_version": (
                    normalized_dataset_version
                ),
                "collection": (
                    context.collection
                ),
                "night_number": (
                    context.night_number
                ),
                "lights_off_seconds": (
                    context.lights_off_seconds
                ),
                "treatment": (
                    context.treatment
                ),
                "source_bucket": (
                    normalized_source_bucket
                ),
                "source_object_key": (
                    validate_lineage_value(
                        source_object_key,
                        "source_object_key",
                    )
                ),
            }
        )

    if not rows:
        raise ValueError(
            "At least one recording context "
            "is required"
        )

    return pa.Table.from_pylist(
        rows,
        schema=RECORDING_CONTEXTS_SCHEMA,
    )
