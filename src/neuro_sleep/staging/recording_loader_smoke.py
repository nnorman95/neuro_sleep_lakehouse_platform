from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from neuro_sleep.config import get_settings
from neuro_sleep.db.postgres import (
    get_postgres_connection,
)
from neuro_sleep.ops.pipeline_run import (
    get_pipeline_run_status,
)
from neuro_sleep.staging.recording_load_job import (
    run_tracked_recording_staging_job,
)
from neuro_sleep.staging.recording_loader import (
    RecordingPublication,
    discover_current_recording_publications,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
)


@dataclass(frozen=True)
class RecordingStagingState:
    recording_count: int
    channel_count: int
    interval_count: int
    epoch_count: int
    orphan_channel_count: int
    orphan_interval_count: int
    orphan_epoch_recording_count: int
    orphan_epoch_interval_count: int
    unresolved_context_count: int
    legacy_recording_count: int


def _expected_counts(
    publications: tuple[
        RecordingPublication,
        ...,
    ],
) -> tuple[int, int, int, int]:
    return (
        sum(
            item.object_for(
                "recordings"
            ).row_count
            for item in publications
        ),
        sum(
            item.object_for(
                "channels"
            ).row_count
            for item in publications
        ),
        sum(
            item.object_for(
                "sleep_stage_intervals"
            ).row_count
            for item in publications
        ),
        sum(
            item.object_for(
                "sleep_stage_epochs"
            ).row_count
            for item in publications
        ),
    )


def _read_state() -> RecordingStagingState:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    (select count(*)
                     from staging.silver_recordings),
                    (select count(*)
                     from staging.silver_channels),
                    (select count(*)
                     from staging.silver_sleep_stage_intervals),
                    (select count(*)
                     from staging.silver_sleep_stage_epochs),
                    (
                        select count(*)
                        from staging.silver_channels c
                        left join staging.silver_recordings r
                          on r.recording_id = c.recording_id
                        where r.recording_id is null
                    ),
                    (
                        select count(*)
                        from staging.silver_sleep_stage_intervals i
                        left join staging.silver_recordings r
                          on r.recording_id = i.recording_id
                        where r.recording_id is null
                    ),
                    (
                        select count(*)
                        from staging.silver_sleep_stage_epochs e
                        left join staging.silver_recordings r
                          on r.recording_id = e.recording_id
                        where r.recording_id is null
                    ),
                    (
                        select count(*)
                        from staging.silver_sleep_stage_epochs e
                        left join staging.silver_sleep_stage_intervals i
                          on i.interval_id = e.source_interval_id
                        where i.interval_id is null
                    ),
                    (
                        select count(*)
                        from staging.silver_recordings r
                        left join staging.silver_recording_contexts c
                          on c.source_system = r.source_system
                         and c.dataset_version = r.dataset_version
                         and c.collection = r.collection
                         and c.recording_key = r.recording_key
                        where c.recording_key is null
                    ),
                    (
                        select count(*)
                        from staging.silver_recordings
                        where transform_version <> '1.1.0'
                    );
                """
            )
            row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            "Failed to read recording staging state"
        )

    return RecordingStagingState(*row)


def _validate_publication_rows(
    publications: tuple[
        RecordingPublication,
        ...,
    ],
) -> set[UUID]:
    expected_by_recording = {
        publication.recording_id: (
            publication.output_prefix,
            publication.source_system,
            publication.dataset_version,
            publication.collection,
            publication.recording_key,
            publication.object_for(
                "channels"
            ).row_count,
            publication.object_for(
                "sleep_stage_intervals"
            ).row_count,
            publication.object_for(
                "sleep_stage_epochs"
            ).row_count,
        )
        for publication in publications
    }

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    r.recording_id,
                    r.silver_output_prefix,
                    r.source_system,
                    r.dataset_version,
                    r.collection,
                    r.recording_key,
                    r.staging_load_run_id,
                    (
                        select count(*)
                        from staging.silver_channels c
                        where c.recording_id =
                            r.recording_id
                    ),
                    (
                        select count(*)
                        from
                            staging.silver_sleep_stage_intervals i
                        where i.recording_id =
                            r.recording_id
                    ),
                    (
                        select count(*)
                        from
                            staging.silver_sleep_stage_epochs e
                        where e.recording_id =
                            r.recording_id
                    )
                from staging.silver_recordings r;
                """
            )
            rows = cursor.fetchall()

    actual_ids = {
        row[0]
        for row in rows
    }
    expected_ids = set(
        expected_by_recording
    )

    if actual_ids != expected_ids:
        raise RuntimeError(
            "Staged recording IDs do not match "
            "current Silver publications"
        )

    run_ids: set[UUID] = set()

    for row in rows:
        (
            recording_id,
            output_prefix,
            source_system,
            dataset_version,
            collection,
            recording_key,
            staging_load_run_id,
            channel_count,
            interval_count,
            epoch_count,
        ) = row

        expected = expected_by_recording[
            recording_id
        ]
        actual = (
            output_prefix,
            source_system,
            dataset_version,
            collection,
            recording_key,
            channel_count,
            interval_count,
            epoch_count,
        )

        if actual != expected:
            raise RuntimeError(
                "Staged recording publication "
                "does not match current Silver "
                f"publication: {recording_id}"
            )

        run_ids.add(staging_load_run_id)

    return run_ids


def _cleanup_test_state(
    *,
    inserted_by_test: bool,
    publications: tuple[
        RecordingPublication,
        ...,
    ],
    created_run_ids: list[UUID],
) -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            if inserted_by_test:
                recording_ids = [
                    item.recording_id
                    for item in publications
                ]
                cursor.execute(
                    """
                    delete from staging.silver_recordings
                    where recording_id = any(%s);
                    """,
                    (recording_ids,),
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

    publications: tuple[
        RecordingPublication,
        ...,
    ] = ()
    created_run_ids: list[UUID] = []
    inserted_by_test = False

    try:
        publications = (
            discover_current_recording_publications(
                settings=settings,
                client=client,
            )
        )

        if not publications:
            raise RuntimeError(
                "No current compatible recording "
                "publications were discovered"
            )

        expected_counts = _expected_counts(
            publications
        )
        initial_state = _read_state()
        initial_counts = (
            initial_state.recording_count,
            initial_state.channel_count,
            initial_state.interval_count,
            initial_state.epoch_count,
        )

        expected_empty = (
            initial_counts == (0, 0, 0, 0)
        )
        expected_complete = (
            initial_counts == expected_counts
        )

        if not (
            expected_empty
            or expected_complete
        ):
            raise RuntimeError(
                "Recording staging contains a "
                "partial or unexpected publication "
                f"state: {initial_counts}"
            )

        first = (
            run_tracked_recording_staging_job(
                settings=settings,
                client=client,
            )
        )
        created_run_ids.append(first.run_id)

        if expected_empty:
            inserted_by_test = True

            if first.load_result.status != "written":
                raise RuntimeError(
                    "First recording load into "
                    "empty staging must be written"
                )

            if (
                first.load_result.rows_written
                != sum(expected_counts)
            ):
                raise RuntimeError(
                    "Written recording staging row "
                    "count is incorrect"
                )

            expected_files_processed = sum(
                publication.expected_files
                for publication in publications
            )
            if (
                first.load_result.files_processed
                != expected_files_processed
            ):
                raise RuntimeError(
                    "Written recording staging file "
                    "count is incorrect: expected "
                    f"{expected_files_processed}, got "
                    f"{first.load_result.files_processed}"
                )

            written_run = get_pipeline_run_status(
                first.run_id
            )
            if written_run.status != "success":
                raise RuntimeError(
                    "Written recording staging run "
                    "was not tracked as success"
                )

            second = (
                run_tracked_recording_staging_job(
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
                "Completed recording staging must "
                "be skipped on rerun"
            )

        if (
            second.load_result.rows_written != 0
            or second.load_result.files_processed
            != 0
        ):
            raise RuntimeError(
                "Skipped recording staging load "
                "must not write rows or process "
                "files"
            )

        skipped_run = get_pipeline_run_status(
            second.run_id
        )
        if skipped_run.status != "skipped":
            raise RuntimeError(
                "Idempotent recording staging run "
                "was not tracked as skipped"
            )

        final_state = _read_state()
        final_counts = (
            final_state.recording_count,
            final_state.channel_count,
            final_state.interval_count,
            final_state.epoch_count,
        )

        if final_counts != expected_counts:
            raise RuntimeError(
                "Final recording staging counts "
                "do not match current Silver "
                f"publications: {final_counts}"
            )

        orphan_total = (
            final_state.orphan_channel_count
            + final_state.orphan_interval_count
            + final_state
            .orphan_epoch_recording_count
            + final_state
            .orphan_epoch_interval_count
        )
        if orphan_total != 0:
            raise RuntimeError(
                "Recording staging contains orphan "
                f"rows: {orphan_total}"
            )

        if (
            final_state.unresolved_context_count
            != 0
        ):
            raise RuntimeError(
                "A staged recording does not "
                "resolve to recording context"
            )

        if final_state.legacy_recording_count != 0:
            raise RuntimeError(
                "Legacy Silver recording version "
                "was loaded into staging"
            )

        source_run_ids = (
            _validate_publication_rows(
                publications
            )
        )

        for source_run_id in source_run_ids:
            source_run = get_pipeline_run_status(
                source_run_id
            )
            if source_run.status != "success":
                raise RuntimeError(
                    "Staged recording references "
                    "a non-successful load run"
                )

        print(
            "recording_staging_"
            f"preexisting={expected_complete}"
        )
        print(
            "recording_staging_"
            f"written_path_tested={expected_empty}"
        )
        print(
            "recording_staging_"
            "idempotent_skip=true"
        )
        print(
            "recording_staging_"
            f"publication_count={len(publications)}"
        )
        print(
            "recording_staging_"
            f"recording_count={final_state.recording_count}"
        )
        print(
            "recording_staging_"
            f"channel_count={final_state.channel_count}"
        )
        print(
            "recording_staging_"
            f"interval_count={final_state.interval_count}"
        )
        print(
            "recording_staging_"
            f"epoch_count={final_state.epoch_count}"
        )
        print(
            "recording_staging_"
            "orphan_rows=0"
        )
        print(
            "recording_staging_"
            "recording_context_resolution=true"
        )
        print(
            "recording_staging_"
            "legacy_publication_excluded=true"
        )
        print(
            "recording_staging_"
            "loader_smoke_status=success"
        )
    finally:
        if publications:
            _cleanup_test_state(
                inserted_by_test=inserted_by_test,
                publications=publications,
                created_run_ids=created_run_ids,
            )

        client.close()


if __name__ == "__main__":
    run_smoke_test()
