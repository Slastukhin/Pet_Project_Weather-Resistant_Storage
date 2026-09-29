from __future__ import annotations

import logging
import os
import subprocess
import tempfile
import zipfile
from datetime import timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from common.weather import DDL

log = logging.getLogger(__name__)

DWH_CONN_ID = "dwh_postgres"
KAGGLE_DATASET = "navjotkaushal/coffee-sales-dataset"
DOWNLOAD_TIMEOUT_SEC = 300


@dag(
    dag_id="load_kaggle_coffee",
    description="Однократная загрузка датасета продаж кофе с Kaggle в RAW",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=15),
    },
    tags=["kaggle", "coffee", "raw"],
)
def load_kaggle_coffee():
    @task
    def create_raw_objects() -> None:
        """Схема и таблицы RAW. Идемпотентно: CREATE ... IF NOT EXISTS."""
        PostgresHook(postgres_conn_id=DWH_CONN_ID).run(DDL)
        log.info("RAW-объекты готовы")

    @task
    def download_and_load() -> int:
        """Скачать архив с Kaggle, распаковать, залить CSV в raw.kaggle_coffee."""
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        rows = hook.get_first("SELECT count(*) FROM raw.kaggle_coffee")[0]
        if rows:
            log.info("Датасет уже загружен: %s строк", rows)
            return rows
        # Временная папка живёт только внутри with и удаляется сама.
        with tempfile.TemporaryDirectory(prefix="kaggle_coffee_") as tmp_dir:
            log.info("Скачивание датасета %s", KAGGLE_DATASET)
            subprocess.run(
                ["kaggle", "datasets", "download", KAGGLE_DATASET,
                 "--path", tmp_dir, "--force"],
                check=True,
                capture_output=True,
                text=True,
                timeout=DOWNLOAD_TIMEOUT_SEC,
            )

            zip_files = [f for f in os.listdir(tmp_dir) if f.endswith(".zip")]
            if not zip_files:
                raise FileNotFoundError("Не найден zip-файл с датасетом")
            zip_path = os.path.join(tmp_dir, zip_files[0])

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(tmp_dir)

            csv_files = [f for f in os.listdir(tmp_dir) if f.endswith(".csv")]
            if not csv_files:
                raise FileNotFoundError("В распакованном датасете нет CSV-файлов")
            csv_path = os.path.join(tmp_dir, csv_files[0])
            log.info("Найден CSV: %s", csv_path)

            with hook.get_conn() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("LOCK TABLE raw.kaggle_coffee IN EXCLUSIVE MODE")
                    cursor.execute("SELECT count(*) FROM raw.kaggle_coffee")
                    if cursor.fetchone()[0] == 0:
                        with cursor.copy("COPY raw.kaggle_coffee FROM STDIN WITH (FORMAT CSV, HEADER true)") as copy:
                            with open(csv_path, "rb") as source:
                                while chunk := source.read(65536):
                                    copy.write(chunk)

            rows = hook.get_first("SELECT count(*) FROM raw.kaggle_coffee")[0]
            if rows == 0:
                raise ValueError("После COPY таблица пуста — что-то пошло не так")
            log.info("Загружено строк: %s", rows)
            return rows

    create_raw_objects() >> download_and_load()


load_kaggle_coffee()
