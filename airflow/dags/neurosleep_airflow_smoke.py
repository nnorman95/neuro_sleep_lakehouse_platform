from __future__ import annotations

from airflow.sdk import dag, task


@dag(
    dag_id="neurosleep_airflow_smoke",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["neurosleep", "phase10", "smoke"],
)
def neurosleep_airflow_smoke():
    @task
    def runtime_identity() -> str:
        print("neurosleep_airflow_runtime=success")
        return "airflow-foundation"

    @task
    def dependency_check(upstream_value: str) -> None:
        if upstream_value != "airflow-foundation":
            raise RuntimeError(
                "Unexpected upstream value in Airflow foundation smoke DAG"
            )
        print("neurosleep_airflow_dependency=success")

    dependency_check(runtime_identity())


neurosleep_airflow_smoke()
