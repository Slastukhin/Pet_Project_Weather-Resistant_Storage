from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="publish_marts_to_clickhouse",
    description="Build and test stg, core and marts, then publish configured tables to ClickHouse",
    schedule="20 * * * *",
    start_date=pendulum.datetime(2026, 9, 28, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=50),
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["dbt", "marts", "clickhouse"],
)
def publish_marts_to_clickhouse():
    @task
    def rebuild_marts() -> list[dict]:
        from common.marts import load_marts, run_dbt

        marts = load_marts()
        run_dbt(marts)
        return marts

    @task
    def publish(mart: dict) -> dict:
        from common.marts import publish_mart

        return publish_mart(mart)

    publish.expand(mart=rebuild_marts())


publish_marts_to_clickhouse()
