from __future__ import annotations

import subprocess
from datetime import timedelta

from airflow.sdk import dag, task


PROJECT_ROOT = "/opt/neurosleep"


@task
def run_project_command(command: list[str]) -> None:
    print("neurosleep_command=" + " ".join(command))
    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
    )


@dag(
    dag_id="neurosleep_lakehouse_pipeline",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["neurosleep", "phase10", "pipeline"],
)
def neurosleep_lakehouse_pipeline():
    extract_bronze = run_project_command.override(
        task_id="extract_bronze",
    )(
        [
            "python",
            "-m",
            "neuro_sleep.ingestion.sleep_edf_extract",
        ]
    )

    build_subject_metadata_silver = (
        run_project_command.override(
            task_id="build_subject_metadata_silver",
        )(
            [
                "python",
                "scripts/run_silver_subject_metadata.py",
            ]
        )
    )

    build_recording_silver = (
        run_project_command.override(
            task_id="build_recording_silver",
        )(
            [
                "python",
                "scripts/run_silver_batch.py",
            ]
        )
    )

    load_subject_metadata_staging = (
        run_project_command.override(
            task_id="load_subject_metadata_staging",
        )(
            [
                "python",
                "scripts/load_subject_metadata_staging.py",
            ]
        )
    )

    load_recording_staging = (
        run_project_command.override(
            task_id="load_recording_staging",
        )(
            [
                "python",
                "scripts/load_recording_staging.py",
            ]
        )
    )

    build_warehouse_and_marts = (
        run_project_command.override(
            task_id="build_warehouse_and_marts",
        )(
            [
                "bash",
                "scripts/run_dbt.sh",
                "build",
            ]
        )
    )

    build_gold_signal_features = (
        run_project_command.override(
            task_id="build_gold_signal_features",
        )(
            [
                "bash",
                "scripts/run_gold_signal_features.sh",
            ]
        )
    )

    build_integrated_signal_features = (
        run_project_command.override(
            task_id="build_integrated_signal_features",
        )(
            [
                "bash",
                "scripts/run_integrated_signal_features.sh",
            ]
        )
    )

    extract_bronze >> [
        build_subject_metadata_silver,
        build_recording_silver,
    ]

    (
        build_subject_metadata_silver
        >> load_subject_metadata_staging
    )

    build_recording_silver >> [
        load_recording_staging,
        build_gold_signal_features,
    ]

    [
        load_subject_metadata_staging,
        load_recording_staging,
    ] >> build_warehouse_and_marts

    [
        build_warehouse_and_marts,
        build_gold_signal_features,
    ] >> build_integrated_signal_features


neurosleep_lakehouse_pipeline()
