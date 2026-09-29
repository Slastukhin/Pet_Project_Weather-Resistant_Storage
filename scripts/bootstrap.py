"""Prepare the demo without overwriting existing RAW or dashboard data."""

import logging
import sys

from restore_raw import connect, restore

sys.path.insert(0, "/opt/airflow/dags")
from common.marts import load_marts, publish_mart, run_dbt


def main():
    logging.basicConfig(level=logging.INFO)
    restore()
    connection = connect()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS public")
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('weather_demo_bootstrap'))")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS public.project_bootstrap (
                        name text PRIMARY KEY,
                        completed_at timestamptz NOT NULL DEFAULT now()
                    )
                """)
                cursor.execute("SELECT 1 FROM public.project_bootstrap WHERE name='demo_v1'")
                if cursor.fetchone():
                    logging.info("Demo already initialized; no data replaced")
                    return
                marts = load_marts()
                run_dbt(marts)
                for mart in marts:
                    publish_mart(mart)
                cursor.execute("INSERT INTO public.project_bootstrap(name) VALUES ('demo_v1')")
        logging.info("Demo is ready")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
