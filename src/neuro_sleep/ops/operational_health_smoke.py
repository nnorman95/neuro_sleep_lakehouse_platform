from neuro_sleep.ops.operational_health import (
    OperationalHealthSummary,
    load_operational_health_summary,
    render_operational_health_summary,
)


def _summary(
    *,
    stale_pipeline_runs: int = 0,
    orphaned_started_file_attempts: int = 0,
    active_quarantine_records: int = 0,
) -> OperationalHealthSummary:
    return OperationalHealthSummary(
        pipeline_status_counts={
            "success": 12,
            "failed": 5,
            "skipped": 3,
        },
        file_attempt_status_counts={
            "uploaded": 8,
            "skipped": 4,
            "failed": 1,
        },
        active_pipeline_runs=0,
        stale_pipeline_runs=(
            stale_pipeline_runs
        ),
        started_file_attempts=0,
        orphaned_started_file_attempts=(
            orphaned_started_file_attempts
        ),
        active_quarantine_records=(
            active_quarantine_records
        ),
        recent_failed_pipeline_runs=2,
        recent_failed_file_attempts=1,
        stale_after_seconds=120,
        failure_lookback_hours=24,
    )


def run_smoke_test() -> None:
    healthy = _summary()

    if healthy.current_attention_required:
        raise RuntimeError(
            "Historical terminal failures "
            "incorrectly triggered current attention"
        )
    if healthy.health_status != "healthy":
        raise RuntimeError(
            "Healthy fixture was not classified healthy"
        )

    print(
        "operational_history_not_current_incident=true"
    )
    print(
        "operational_healthy_classification=true"
    )

    stale = _summary(
        stale_pipeline_runs=1
    )
    if (
        not stale.current_attention_required
        or stale.health_status
        != "attention_required"
    ):
        raise RuntimeError(
            "Stale pipeline was not actionable"
        )
    print(
        "operational_stale_pipeline_detection=true"
    )

    orphaned = _summary(
        orphaned_started_file_attempts=1
    )
    if not orphaned.current_attention_required:
        raise RuntimeError(
            "Orphaned file attempt was not actionable"
        )
    print(
        "operational_orphaned_file_attempt_detection=true"
    )

    quarantine = _summary(
        active_quarantine_records=1
    )
    if not quarantine.current_attention_required:
        raise RuntimeError(
            "Active quarantine was not actionable"
        )
    print(
        "operational_active_quarantine_detection=true"
    )

    rendered = "\n".join(
        render_operational_health_summary(
            healthy
        )
    )
    required_markers = (
        "historical_failed_pipeline_runs=5",
        "historical_failed_file_attempts=1",
        "recent_failed_pipeline_runs=2",
        "operational_attention_required=false",
        "operational_health_status=healthy",
    )

    for marker in required_markers:
        if marker not in rendered:
            raise RuntimeError(
                "Operational rendering is "
                f"missing marker: {marker}"
            )

    print(
        "operational_summary_rendering=true"
    )

    for name, kwargs in (
        (
            "stale_after_seconds",
            {
                "stale_after_seconds": 0,
            },
        ),
        (
            "failure_lookback_hours",
            {
                "failure_lookback_hours": 0,
            },
        ),
    ):
        try:
            load_operational_health_summary(
                **kwargs
            )
        except ValueError:
            print(
                f"operational_{name}_validation=true"
            )
        else:
            raise RuntimeError(
                f"{name} accepted zero"
            )

    print(
        "operational_health_smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
