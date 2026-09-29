from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from psycopg.types.json import Json

from common.weather import DDL, http_session

log = logging.getLogger(__name__)

DWH_CONN_ID = "dwh_postgres"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = (
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "precipitation_probability",
    "surface_pressure"
)
FORECAST_DAYS = 3
REQUEST_TIMEOUT_SEC = 60


@dag(
    dag_id="weather_forecast_hourly",
    description="Снимок прогноза на 3 дня по 20 городам -> RAW (JSONB)",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 8, 30, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["weather", "raw", "forecast"],
)
def weather_forecast_hourly():
    @task
    def create_raw_objects() -> None:
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        hook.run(DDL)
        log.info("RAW-объекты готовы")

    @task
    def resolve_issued_at(**context) -> str:
        issued_at = context.get("data_interval_start")
        if issued_at is None:
            issued_at = context["dag_run"].run_after
        if issued_at is None:
            raise ValueError("Не удалось определить момент выпуска прогноза")

        log.info("Момент выпуска прогноза: %s", issued_at)
        return issued_at.isoformat()

    @task
    def list_cities() -> list[dict[str, Any]]:
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        rows = hook.get_records(
            "SELECT city_slug, city_name, latitude, longitude "
            "FROM raw.cities ORDER BY city_slug"
        )
        if not rows:
            raise ValueError(
                "Справочник raw.cities пуст. Сначала запустите DAG weather_history_backfill."
            )

        cities = []
        for row in rows:
            cities.append(
                {
                    "slug": row[0],
                    "name": row[1],
                    "lat": float(row[2]),
                    "lon": float(row[3]),
                }
            )
        log.info("Городов в справочнике: %s", len(cities))
        return cities

    @task(max_active_tis_per_dag=4)
    def fetch_forecast(city: dict[str, Any], issued_at: str) -> dict[str, Any]:
        # Параметры запроса. От города к городу меняется только точка на карте
        query = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "hourly": ",".join(HOURLY_VARIABLES),
            "forecast_days": FORECAST_DAYS,
            "timezone": "UTC",
        }

        session = http_session()
        response = session.get(FORECAST_URL, params=query, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        payload = response.json()

        hours = len(payload.get("hourly", {}).get("time", []))
        if hours == 0:
            raise ValueError(f"{city['name']}: API вернул пустой hourly.time")

        insert_sql = """
            INSERT INTO raw.weather_forecast (city_slug, issued_at, forecast_days, payload)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT weather_forecast_pk
            DO UPDATE SET payload       = EXCLUDED.payload,
                          forecast_days = EXCLUDED.forecast_days,
                          loaded_at     = now()
        """
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        hook.run(
            insert_sql,
            parameters=(city["slug"], issued_at, FORECAST_DAYS, Json(payload)),
        )
        log.info("%s: сохранён прогноз на %s часов", city["name"], hours)

        return {"city": city["name"], "hours": hours}

    @task
    def summarize(results: list[dict[str, Any]]) -> None:
        expected_hours = FORECAST_DAYS * 24
        total_hours = 0
        incomplete = []

        for item in results:
            total_hours += item["hours"]
            if item["hours"] < expected_hours:
                incomplete.append(item["city"])

        log.info("Снимок готов: городов %s, часов суммарно %s", len(results), total_hours)
        if incomplete:
            log.warning("Неполный прогноз (<%s часов): %s", expected_hours, incomplete)

    # Сборка графа 
    tables = create_raw_objects()
    issued_at = resolve_issued_at()
    cities = list_cities()

    tables >> cities

    loaded = fetch_forecast.partial(issued_at=issued_at).expand(city=cities)

    summarize(loaded)


weather_forecast_hourly()
