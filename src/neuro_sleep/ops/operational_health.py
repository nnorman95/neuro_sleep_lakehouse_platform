from argparse import ArgumentParser
from dataclasses import dataclass

from neuro_sleep.db.postgres import (
    get_postgres_connection,
)


DEFAULT_STALE_AFTER_SECONDS = 120
DEFAULT_FAILURE_LOOKBACK_HOURS = 24

PIPELINE_STATUSES = (
    "started",
    "success",
    "failed",
    "skipped",
    "warning",
)
FILE_ATTEMPT_STATUSES = (
    "started",
    "uploaded",
    "skipped",
    "failed",
)


@dataclass(frozen=True)
class OperationalHealthSummary:
    pipeline_status_counts: dict[str, int]
    file_attempt_status_counts: dict[str, int]
    active_pipeline_runs: int
    stale_pipeline_runs: int
    started_file_attempts: int
    orphaned_started_file_attempts: int
    active_quarantine_records: int
    recent_failed_pipeline_runs: int
    recent_failed_file_attempts: int
    stale_after_seconds: int
    failure_lookback_hours: int

    @property
    def current_attention_required(self) -> bool:
        return any(
            (
                self.stale_pipeline_runs > 0,
                self.orphaned_started_file_attempts > 0,
                self.active_quarantine_records > 0,
            )
        )

    @property
    def health_status(self) -> str:
        if self.current_attention_required:
            return "attention_required"
        return "healthy"


def _validate_positive(
    name: str,
    value: int,
) -> None:
    if value <= 0:
        raise ValueError(
            f"{name} must be a positive integer"
        )


def _status_counts(
    cursor,
    table_name: str,
) -> dict[str, int]:
    if table_name not in {
        "ops.pipeline_run",
        "ops.file_attempt",
    }:
        raise ValueError(
            f"Unsupported operational table: {table_name}"
        )

    cursor.execute(
        f"""
        select
            status,
            count(*)
        from {table_name}
        group by status
        order by status;
        """
    )

    return {
        row[0]: int(row[1])
        for row in cursor.fetchall()
    }


def load_operational_health_summary(
    stale_after_seconds: int = (
        DEFAULT_STALE_AFTER_SECONDS
    ),
    failure_lookback_hours: int = (
        DEFAULT_FAILURE_LOOKBACK_HOURS
    ),
) -> OperationalHealthSummary:
    _validate_positive(
        "stale_after_seconds",
        stale_after_seconds,
    )
    _validate_positive(
        "failure_lookback_hours",
        failure_lookback_hours,
    )

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            pipeline_status_counts = _status_counts(
                cursor,
                "ops.pipeline_run",
            )
            file_attempt_status_counts = _status_counts(
                cursor,
                "ops.file_attempt",
            )

            cursor.execute(
                """
                select
                    count(*) filter (
                        where status = 'started'
                    ),
                    count(*) filter (
                        where status = 'started'
                          and heartbeat_at
                              < now()
                                - (
                                    %s
                                    * interval '1 second'
                                )
                    ),
                    count(*) filter (
                        where status = 'failed'
                          and coalesce(
                              finished_at,
                              started_at
                          )
                              >= now()
                                - (
                                    %s
                                    * interval '1 hour'
                                )
                    )
                from ops.pipeline_run;
                """,
                (
                    stale_after_seconds,
                    failure_lookback_hours,
                ),
            )
            pipeline_row = cursor.fetchone()
            if pipeline_row is None:
                raise RuntimeError(
                    "Pipeline health query returned no row"
                )

            cursor.execute(
                """
                select
                    count(*) filter (
                        where fa.status = 'started'
                    ),
                    count(*) filter (
                        where fa.status = 'started'
                          and pr.status <> 'started'
                    ),
                    count(*) filter (
                        where fa.status = 'failed'
                          and coalesce(
                              fa.finished_at,
                              fa.started_at
                          )
                              >= now()
                                - (
                                    %s
                                    * interval '1 hour'
                                )
                    )
                from ops.file_attempt as fa
                join ops.pipeline_run as pr
                  on pr.run_id = fa.pipeline_run_id;
                """,
                (failure_lookback_hours,),
            )
            file_attempt_row = cursor.fetchone()
            if file_attempt_row is None:
                raise RuntimeError(
                    "File-attempt health query "
                    "returned no row"
                )

            cursor.execute(
                """
                select count(*)
                from quality.quarantine_records
                where status in (
                    'open',
                    'reviewed'
                );
                """
            )
            quarantine_row = cursor.fetchone()
            if quarantine_row is None:
                raise RuntimeError(
                    "Quarantine health query "
                    "returned no row"
                )

    return OperationalHealthSummary(
        pipeline_status_counts=(
            pipeline_status_counts
        ),
        file_attempt_status_counts=(
            file_attempt_status_counts
        ),
        active_pipeline_runs=int(
            pipeline_row[0]
        ),
        stale_pipeline_runs=int(
            pipeline_row[1]
        ),
        recent_failed_pipeline_runs=int(
            pipeline_row[2]
        ),
        started_file_attempts=int(
            file_attempt_row[0]
        ),
        orphaned_started_file_attempts=int(
            file_attempt_row[1]
        ),
        recent_failed_file_attempts=int(
            file_attempt_row[2]
        ),
        active_quarantine_records=int(
            quarantine_row[0]
        ),
        stale_after_seconds=(
            stale_after_seconds
        ),
        failure_lookback_hours=(
            failure_lookback_hours
        ),
    )


def render_operational_health_summary(
    summary: OperationalHealthSummary,
) -> list[str]:
    lines = [
        "=== OPERATIONAL HEALTH ===",
        (
            "stale_after_seconds="
            f"{summary.stale_after_seconds}"
        ),
        (
            "failure_lookback_hours="
            f"{summary.failure_lookback_hours}"
        ),
    ]

    for status in PIPELINE_STATUSES:
        lines.append(
            "pipeline_runs_"
            f"{status}="
            f"{summary.pipeline_status_counts.get(status, 0)}"
        )

    for status in FILE_ATTEMPT_STATUSES:
        lines.append(
            "file_attempts_"
            f"{status}="
            f"{summary.file_attempt_status_counts.get(status, 0)}"
        )

    lines.extend(
        [
            (
                "current_active_pipeline_runs="
                f"{summary.active_pipeline_runs}"
            ),
            (
                "current_stale_pipeline_runs="
                f"{summary.stale_pipeline_runs}"
            ),
            (
                "current_started_file_attempts="
                f"{summary.started_file_attempts}"
            ),
            (
                "current_orphaned_started_file_attempts="
                f"{summary.orphaned_started_file_attempts}"
            ),
            (
                "current_active_quarantine_records="
                f"{summary.active_quarantine_records}"
            ),
            (
                "recent_failed_pipeline_runs="
                f"{summary.recent_failed_pipeline_runs}"
            ),
            (
                "recent_failed_file_attempts="
                f"{summary.recent_failed_file_attempts}"
            ),
            (
                "historical_failed_pipeline_runs="
                f"{summary.pipeline_status_counts.get('failed', 0)}"
            ),
            (
                "historical_failed_file_attempts="
                f"{summary.file_attempt_status_counts.get('failed', 0)}"
            ),
            (
                "operational_attention_required="
                + str(
                    summary.current_attention_required
                ).lower()
            ),
            (
                "operational_health_status="
                f"{summary.health_status}"
            ),
        ]
    )

    return lines


def _build_argument_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description=(
            "Show a read-only NeuroSleep "
            "operational health summary."
        )
    )
    parser.add_argument(
        "--stale-after-seconds",
        type=int,
        default=DEFAULT_STALE_AFTER_SECONDS,
        help=(
            "Treat a started pipeline run as "
            "stale after this heartbeat age."
        ),
    )
    parser.add_argument(
        "--failure-lookback-hours",
        type=int,
        default=DEFAULT_FAILURE_LOOKBACK_HOURS,
        help=(
            "Count terminal failures inside "
            "this recent-history window."
        ),
    )
    return parser


def main() -> None:
    args = _build_argument_parser().parse_args()
    summary = load_operational_health_summary(
        stale_after_seconds=(
            args.stale_after_seconds
        ),
        failure_lookback_hours=(
            args.failure_lookback_hours
        ),
    )

    for line in render_operational_health_summary(
        summary
    ):
        print(line)


if __name__ == "__main__":
    main()
