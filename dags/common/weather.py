from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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


def city_slug(name: str) -> str:
    """Стабильный технический ключ города: 'Rostov-on-Don' -> 'rostov_on_don'."""
    return name.lower().replace(" ", "_").replace("-", "_")


def http_session() -> requests.Session:
    """Session с ретраями на сетевые сбои и коды 429/5xx."""
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session



DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.cities (
    city_slug  text PRIMARY KEY,
    city_name  text NOT NULL,
    latitude   numeric(9, 5)    NOT NULL,
    longitude  numeric(9, 5)    NOT NULL,
    updated_at timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.weather_archive (
    city_slug    text   NOT NULL REFERENCES raw.cities (city_slug),
    period_start date   NOT NULL,
    period_end   date   NOT NULL,
    payload      jsonb  NOT NULL,
    loaded_at    timestamptz    NOT NULL DEFAULT now(),
    CONSTRAINT weather_archive_pk PRIMARY KEY (city_slug, period_start, period_end)
);

CREATE TABLE IF NOT EXISTS raw.weather_forecast (
    city_slug     text  NOT NULL REFERENCES raw.cities (city_slug),
    issued_at     timestamptz   NOT NULL,
    forecast_days int   NOT NULL,
    payload       jsonb NOT NULL,
    loaded_at     timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT weather_forecast_pk PRIMARY KEY (city_slug, issued_at)
);

CREATE TABLE IF NOT EXISTS raw.kaggle_coffee (
    hour_of_day     int NOT NULL,
    cash_type       text NOT NULL,
    money           numeric(3,1) NOT NULL,
    coffee_name     text NOT NULL,
    time_of_day     text NOT NULL,
    weekday         text NOT NULL,
    month_name      text NOT NULL,
    weekdaysort     int NOT NULL,
    monthsort       int NOT NULL,
    date            date NOT NULL,
    time            time NOT NULL
);
"""
