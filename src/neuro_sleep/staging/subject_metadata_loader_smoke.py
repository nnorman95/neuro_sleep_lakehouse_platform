from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from neuro_sleep.config import (
    get_settings,
)
from neuro_sleep.db.postgres import (
    get_postgres_connection,
)
from neuro_sleep.ops.pipeline_run import (
    get_pipeline_run_status,
)
from neuro_sleep.staging.subject_metadata_load_job import (
    run_tracked_subject_metadata_staging_job,
)
from neuro_sleep.staging.subject_metadata_loader import (
    SubjectMetadataPublication,
    resolve_current_subject_metadata_publication,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
)


@dataclass(frozen=True)
class PublicationState:
    subject_count: int
    recording_context_count: int
    subject_run_ids: frozenset[UUID]
    context_run_ids: frozenset[UUID]
    orphan_context_count: int


def _read_publication_state(
    publication: SubjectMetadataPublication,
) -> PublicationState:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    count(*),
                    array_agg(
                        distinct staging_load_run_id
                    )
                from staging.silver_subjects
                where metadata_input_fingerprint = %s
                  and source_system = %s
                  and dataset_version = %s
                  and schema_version = %s
                  and transform_version = %s
                  and silver_bucket = %s
                  and silver_output_prefix = %s;
                """,
                (
                    publication.input_fingerprint,
                    publication.source_system,
                    publication.dataset_version,
                    publication.schema_version,
                    publication.transform_version,
                    publication.silver_bucket,
                    publication.output_prefix,
                ),
            )
            subject_row = cursor.fetchone()
            if subject_row is None:
                raise RuntimeError(
                    "Failed to read staged subject state"
                )

            cursor.execute(
                """
                select
                    count(*),
                    array_agg(
                        distinct staging_load_run_id
                    )
                from staging.silver_recording_contexts
                where metadata_input_fingerprint = %s
                  and source_system = %s
                  and dataset_version = %s
                  and schema_version = %s
                  and transform_version = %s
                  and silver_bucket = %s
                  and silver_output_prefix = %s;
                """,
                (
                    publication.input_fingerprint,
                    publication.source_system,
                    publication.dataset_version,
                    publication.schema_version,
                    publication.transform_version,
                    publication.silver_bucket,
                    publication.output_prefix,
                ),
            )
            context_row = cursor.fetchone()
            if context_row is None:
                raise RuntimeError(
                    "Failed to read staged context state"
                )

            cursor.execute(
                """
                select count(*)
                from staging.silver_recording_contexts c
                left join staging.silver_subjects s
                  on s.subject_key = c.subject_key
                 and s.metadata_input_fingerprint =
                     c.metadata_input_fingerprint
                 and s.source_system = c.source_system
                 and s.dataset_version =
                     c.dataset_version
                 and s.collection = c.collection
                where c.metadata_input_fingerprint = %s
                  and c.source_system = %s
                  and c.dataset_version = %s
                  and c.schema_version = %s
                  and c.transform_version = %s
                  and c.silver_bucket = %s
                  and c.silver_output_prefix = %s
                  and s.subject_key is null;
                """,
                (
                    publication.input_fingerprint,
                    publication.source_system,
                    publication.dataset_version,
                    publication.schema_version,
                    publication.transform_version,
                    publication.silver_bucket,
                    publication.output_prefix,
                ),
            )
            orphan_row = cursor.fetchone()
            if orphan_row is None:
                raise RuntimeError(
                    "Failed to read orphan context count"
                )

    subject_run_ids = frozenset(
        subject_row[1] or []
    )
    context_run_ids = frozenset(
        context_row[1] or []
    )

    return PublicationState(
        subject_count=subject_row[0],
        recording_context_count=context_row[0],
        subject_run_ids=subject_run_ids,
        context_run_ids=context_run_ids,
        orphan_context_count=orphan_row[0],
    )


def _assert_complete_state(
    *,
    publication: SubjectMetadataPublication,
    state: PublicationState,
) -> UUID:
    if (
        state.subject_count
        != publication.subject_count
    ):
        raise RuntimeError(
            "Unexpected staged subject count: "
            f"{state.subject_count}"
        )

    if (
        state.recording_context_count
        != publication.recording_context_count
    ):
        raise RuntimeError(
            "Unexpected staged recording-context "
            f"count: {state.recording_context_count}"
        )

    if state.orphan_context_count != 0:
        raise RuntimeError(
            "Staged recording contexts contain "
            f"{state.orphan_context_count} orphans"
        )

    if len(state.subject_run_ids) != 1:
        raise RuntimeError(
            "Subjects must reference exactly one "
            "staging load run"
        )

    if (
        state.subject_run_ids
        != state.context_run_ids
    ):
        raise RuntimeError(
            "Subjects and recording contexts must "
            "reference the same staging load run"
        )

    return next(iter(state.subject_run_ids))


def _delete_test_state(
    *,
    publication: SubjectMetadataPublication,
    inserted_by_test: bool,
    created_run_ids: list[UUID],
) -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            if inserted_by_test:
                cursor.execute(
                    """
                    delete from
                        staging.silver_recording_contexts
                    where metadata_input_fingerprint = %s
                      and source_system = %s
                      and dataset_version = %s
                      and silver_output_prefix = %s;
                    """,
                    (
                        publication.input_fingerprint,
                        publication.source_system,
                        publication.dataset_version,
                        publication.output_prefix,
                    ),
                )
                cursor.execute(
                    """
                    delete from staging.silver_subjects
                    where metadata_input_fingerprint = %s
                      and source_system = %s
                      and dataset_version = %s
                      and silver_output_prefix = %s;
                    """,
                    (
                        publication.input_fingerprint,
                        publication.source_system,
                        publication.dataset_version,
                        publication.output_prefix,
                    ),
                )

            for run_id in created_run_ids:
                cursor.execute(
                    """
                    delete from ops.pipeline_run
                    where run_id = %s;
                    """,
                    (run_id,),
                )


def run_smoke_test() -> None:
    settings = get_settings()
    client = get_object_storage_client(
        settings
    )

    publication: (
        SubjectMetadataPublication | None
    ) = None
    created_run_ids: list[UUID] = []
    inserted_by_test = False

    try:
        publication = (
            resolve_current_subject_metadata_publication(
                settings=settings,
                client=client,
            )
        )
        initial_state = _read_publication_state(
            publication
        )

        expected_empty = (
            initial_state.subject_count == 0
            and initial_state
            .recording_context_count == 0
        )
        expected_complete = (
            initial_state.subject_count
            == publication.subject_count
            and initial_state
            .recording_context_count
            == publication.recording_context_count
        )

        if not (
            expected_empty
            or expected_complete
        ):
            raise RuntimeError(
                "Subject metadata staging contains "
                "a partial publication"
            )

        first = (
            run_tracked_subject_metadata_staging_job(
                settings=settings,
                client=client,
            )
        )
        created_run_ids.append(first.run_id)

        if expected_empty:
            inserted_by_test = True

            if first.load_result.status != "written":
                raise RuntimeError(
                    "First load into empty staging "
                    "must be written"
                )
            if (
                first.load_result.rows_written
                != (
                    publication.subject_count
                    + publication
                    .recording_context_count
                )
            ):
                raise RuntimeError(
                    "Written staging row count "
                    "is incorrect"
                )
            if (
                first.load_result.files_processed
                != 2
            ):
                raise RuntimeError(
                    "Written staging load must "
                    "process two Parquet files"
                )

            first_run = get_pipeline_run_status(
                first.run_id
            )
            if first_run.status != "success":
                raise RuntimeError(
                    "Written staging load was not "
                    "tracked as success"
                )
            if (
                first_run.rows_written
                != first.load_result.rows_written
            ):
                raise RuntimeError(
                    "Tracked written row count "
                    "does not match the loader"
                )

            second = (
                run_tracked_subject_metadata_staging_job(
                    settings=settings,
                    client=client,
                )
            )
            created_run_ids.append(
                second.run_id
            )
        else:
            second = first

        if second.load_result.status != "skipped":
            raise RuntimeError(
                "Completed staging publication "
                "must be skipped on rerun"
            )
        if (
            second.load_result.rows_written != 0
            or second.load_result.files_processed
            != 0
        ):
            raise RuntimeError(
                "Skipped staging load must not "
                "write rows or process files"
            )

        skipped_run = get_pipeline_run_status(
            second.run_id
        )
        if skipped_run.status != "skipped":
            raise RuntimeError(
                "Idempotent staging load was not "
                "tracked as skipped"
            )

        final_state = _read_publication_state(
            publication
        )
        source_load_run_id = (
            _assert_complete_state(
                publication=publication,
                state=final_state,
            )
        )
        source_load_run = get_pipeline_run_status(
            source_load_run_id
        )

        if source_load_run.status != "success":
            raise RuntimeError(
                "Staged publication does not "
                "reference a successful load run"
            )
        if (
            source_load_run.rows_written
            != (
                publication.subject_count
                + publication
                .recording_context_count
            )
        ):
            raise RuntimeError(
                "Source staging run has an "
                "unexpected rows_written value"
            )
        if source_load_run.files_processed != 2:
            raise RuntimeError(
                "Source staging run must record "
                "two processed files"
            )

        print(
            "subject_metadata_staging_"
            f"preexisting={expected_complete}"
        )
        print(
            "subject_metadata_staging_"
            f"written_path_tested={expected_empty}"
        )
        print(
            "subject_metadata_staging_"
            "idempotent_skip=true"
        )
        print(
            "subject_metadata_staging_"
            f"subject_count={final_state.subject_count}"
        )
        print(
            "subject_metadata_staging_"
            "recording_context_count="
            f"{final_state.recording_context_count}"
        )
        print(
            "subject_metadata_staging_"
            "orphan_context_count=0"
        )
        print(
            "subject_metadata_staging_"
            "lineage_run_consistent=true"
        )
        print(
            "subject_metadata_staging_"
            "loader_smoke_status=success"
        )
    finally:
        if publication is not None:
            _delete_test_state(
                publication=publication,
                inserted_by_test=(
                    inserted_by_test
                ),
                created_run_ids=(
                    created_run_ids
                ),
            )

        client.close()


if __name__ == "__main__":
    run_smoke_test()
