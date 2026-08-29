from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from psycopg.types.json import Json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.sdk import Param

log = logging.getLogger(__name__)

DWH_CONN_ID = "dwh_postgres"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARIABLES = ("temperature_2m", "apparent_temperature", "precipitation")
ARCHIVE_LAG_DAYS = 6
HISTORY_YEARS = 2
REQUEST_TIMEOUT_SEC = 120

CITIES: list[dict[str, Any]] = [
    {"name": "Moscow", "lat": 55.76, "lon": 37.62},
    {"name": "Saint Petersburg", "lat": 59.94, "lon": 30.31},
    {"name": "Novosibirsk", "lat": 55.03, "lon": 82.92},
    {"name": "Yekaterinburg", "lat": 56.84, "lon": 60.60},
    {"name": "Kazan", "lat": 55.79, "lon": 49.11},
    {"name": "Nizhny Novgorod", "lat": 56.33, "lon": 44.01},
    {"name": "Chelyabinsk", "lat": 55.15, "lon": 61.40},
    {"name": "Samara", "lat": 53.18, "lon": 50.12},
    {"name": "Omsk", "lat": 54.97, "lon": 73.38},
    {"name": "Rostov-on-Don", "lat": 47.22, "lon": 39.71},
    {"name": "Ufa", "lat": 54.73, "lon": 55.96},
    {"name": "Krasnoyarsk", "lat": 56.01, "lon": 92.87},
    {"name": "Perm", "lat": 58.00, "lon": 56.24},
    {"name": "Voronezh", "lat": 51.66, "lon": 39.20},
    {"name": "Volgograd", "lat": 48.71, "lon": 44.51},
    {"name": "Krasnodar", "lat": 45.02, "lon": 38.97},
    {"name": "Saratov", "lat": 51.53, "lon": 46.04},
    {"name": "Tyumen", "lat": 57.15, "lon": 65.53},
    {"name": "Tolyatti", "lat": 53.51, "lon": 49.42},
    {"name": "Izhevsk", "lat": 56.85, "lon": 53.22},
]

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.cities (
    city_slug  text PRIMARY KEY,
    city_name  text NOT NULL,
    latitude   numeric(9, 5) NOT NULL,
    longitude  numeric(9, 5) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.weather_archive (
    city_slug    text NOT NULL REFERENCES raw.cities (city_slug),
    period_start date NOT NULL,
    period_end   date NOT NULL,
    payload      jsonb NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT weather_archive_pk PRIMARY KEY (city_slug, period_start, period_end)
);
"""


def city_slug(name: str) -> str:
    return name.lower().replace(" ", "_").replace("-", "_")


def http_session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


@dag(
    dag_id="weather_history_backfill",
    description="Разовый ETL: 2 года истории погоды по 20 городам в RAW (JSONB)",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_tasks=4,
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "start_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Начало периода, YYYY-MM-DD. Пусто = 2 года назад от даты запуска",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Конец периода, YYYY-MM-DD. Пусто = дата запуска минус 6 дней (лаг ERA5)",
        ),
    },
    tags=["weather", "raw", "history"],
)
def weather_history_backfill():
    @task
    def create_raw_objects() -> None:
        PostgresHook(postgres_conn_id=DWH_CONN_ID).run(DDL)
        log.info("RAW-объекты готовы")

    @task
    def seed_cities() -> list[dict[str, Any]]:
        cities = [
            {"slug": city_slug(c["name"]), "name": c["name"], "lat": c["lat"], "lon": c["lon"]}
            for c in CITIES
        ]
        sql = """
            INSERT INTO raw.cities (city_slug, city_name, latitude, longitude)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (city_slug) DO UPDATE
               SET city_name  = EXCLUDED.city_name,
                   latitude   = EXCLUDED.latitude,
                   longitude  = EXCLUDED.longitude,
                   updated_at = now()
        """
        rows = [(c["slug"], c["name"], c["lat"], c["lon"]) for c in cities]
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        finally:
            conn.close()
        log.info("Справочник городов: %s строк", len(rows))
        return cities

    @task
    def resolve_window(**context) -> dict[str, str]:
        params = context["params"]
        dag_run = context["dag_run"]
        anchor = context.get("data_interval_end") or dag_run.run_after or dag_run.logical_date
        if anchor is None:
            raise ValueError("Не удалось определить опорную дату запуска")

        anchor_dt = pendulum.instance(anchor).subtract(days=ARCHIVE_LAG_DAYS)
        end = pendulum.parse(params["end_date"]).date() if params.get("end_date") else anchor_dt.date()
        start = (
            pendulum.parse(params["start_date"]).date()
            if params.get("start_date")
            else anchor_dt.subtract(years=HISTORY_YEARS).date()
        )
        if start > end:
            raise ValueError(f"Некорректный период: start={start} > end={end}")

        window = {"start_date": start.isoformat(), "end_date": end.isoformat()}
        log.info("Окно загрузки: %s", window)
        return window

    @task(max_active_tis_per_dag=4)
    def load_city_archive(city: dict[str, Any], window: dict[str, str]) -> dict[str, Any]:
        """Один город за весь период -> одна строка JSONB в RAW."""
        query = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "start_date": window["start_date"],
            "end_date": window["end_date"],
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": "UTC",
        }
        response = http_session().get(ARCHIVE_URL, params=query, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        payload = response.json()

        hours = len(payload.get("hourly", {}).get("time", []))
        if hours == 0:
            raise ValueError(f"{city['name']}: пустой hourly.time за {window}")

        insert_sql = """
            INSERT INTO raw.weather_archive (city_slug, period_start, period_end, payload)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT weather_archive_pk
            DO UPDATE SET payload = EXCLUDED.payload, loaded_at = now()
        """
        PostgresHook(postgres_conn_id=DWH_CONN_ID).run(
            insert_sql,
            parameters=(city["slug"], window["start_date"], window["end_date"], Json(payload)),
        )
        log.info("%s: загружено %s часов", city["name"], hours)
        return {"city": city["name"], "hours": hours}

    @task
    def summarize(results: list[dict[str, Any]]) -> None:
        total = sum(r["hours"] for r in results)
        log.info("Готово: %s городов, %s часов суммарно", len(results), total)

    tables = create_raw_objects()
    cities = seed_cities()
    window = resolve_window()

    tables >> cities
    loaded = load_city_archive.partial(window=window).expand(city=cities)
    summarize(loaded)


weather_history_backfill()