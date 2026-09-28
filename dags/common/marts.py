from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)
DAGS_DIR = Path(__file__).resolve().parents[1]


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid SQL identifier: {value}")
    return value


def load_marts() -> list[dict]:
    with (DAGS_DIR / "config" / "marts.json").open() as file:
        marts = json.load(file)
    if not marts:
        raise ValueError("No marts configured")
    targets = set()
    for mart in marts:
        for name in ("model", "source_schema", "source_table", "target_database", "target_table"):
            identifier(mart[name])
        for name in mart["columns"] + mart["unique_key"] + mart["sum_columns"]:
            identifier(name)
        target = (mart["target_database"], mart["target_table"])
        if target in targets:
            raise ValueError(f"Duplicate target: {target}")
        targets.add(target)
    return marts


def run_dbt(marts: list[dict]) -> None:
    connection = PostgresHook(postgres_conn_id="dwh_postgres").get_connection("dwh_postgres")
    environment = os.environ.copy()
    environment.update({
        "DWH_POSTGRES_USER": connection.login,
        "DWH_POSTGRES_PASSWORD": connection.password,
        "DWH_POSTGRES_DB": connection.schema,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    })
    executable = "/opt/airflow/dbt-venv/bin/dbt"
    source = Path("/opt/airflow/dbt/my_dwh")

    # Each run has its own dbt artifacts and does not overwrite local results.
    with tempfile.TemporaryDirectory(prefix="publish-marts-") as directory:
        project = Path(directory) / "my_dwh"
        shutil.copytree(source, project, ignore=shutil.ignore_patterns("target", "logs", ".user.yml"))
        common = ["--project-dir", str(project), "--profiles-dir", str(project)]
        commands = [
            [executable, "deps", *common],
            [executable, "run", *common, "--threads", "1", "--select",
             *["+" + mart["model"] for mart in marts]],
        ]
        for command in commands:
            log.info("Running: %s", " ".join(command))
            subprocess.run(command, env=environment, check=True, timeout=2400)


def clickhouse_query(session: requests.Session, query: str, data=None) -> str:
    host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    port = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
    response = session.post(
        f"http://{host}:{port}/",
        params={"query": query, "date_time_input_format": "best_effort", "wait_end_of_query": "1"},
        data=data,
        timeout=(10, 1200),
    )
    response.raise_for_status()
    return response.text.strip()


def json_value(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Unsupported value: {type(value).__name__}")


def stream_rows(cursor, columns: list[str]):
    while rows := cursor.fetchmany(5000):
        lines = [
            json.dumps(dict(zip(columns, row)), default=json_value, allow_nan=False)
            for row in rows
        ]
        yield ("\n".join(lines) + "\n").encode("utf-8")


def publish_mart(mart: dict) -> dict:
    source = f"{mart['source_schema']}.{mart['source_table']}"
    database = mart["target_database"]
    target = f"{database}.{mart['target_table']}"
    staging = target + "__loading"
    columns = ", ".join(mart["columns"])
    sums = ", ".join(f"sum({name}) AS {name}" for name in mart["sum_columns"])
    statistics = "count(*) AS row_count" + (", " + sums if sums else "")
    hook = PostgresHook(postgres_conn_id="dwh_postgres")

    with requests.Session() as session:
        session.auth = (os.environ["CLICKHOUSE_USER"], os.environ["CLICKHOUSE_PASSWORD"])
        with hook.get_conn() as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (target,))
                if not cursor.fetchone()[0]:
                    raise RuntimeError(f"Another publication is running for {target}")
                cursor.execute(f"SELECT {statistics} FROM {source}")
                expected = cursor.fetchone()
                if expected[0] == 0:
                    raise ValueError(f"Refusing to publish an empty mart: {source}")

            engine = clickhouse_query(
                session, f"SELECT engine FROM system.databases WHERE name = '{database}'"
            )
            if not engine:
                clickhouse_query(session, f"CREATE DATABASE {database} ENGINE = Atomic")
            elif engine != "Atomic":
                raise ValueError(f"Database {database} must use the Atomic engine")

            clickhouse_query(session, f"DROP TABLE IF EXISTS {staging}")
            ddl = (DAGS_DIR / "sql" / "clickhouse" / mart["ddl_file"]).read_text()
            clickhouse_query(session, ddl.format(table=staging))

            # The count, sums and exported rows use the same PostgreSQL snapshot.
            with connection.cursor(name="mart_export") as cursor:
                cursor.execute(f"SELECT {columns} FROM {source}")
                clickhouse_query(
                    session,
                    f"INSERT INTO {staging} ({columns}) FORMAT JSONEachRow",
                    data=stream_rows(cursor, mart["columns"]),
                )

            actual = json.loads(clickhouse_query(
                session, f"SELECT {statistics} FROM {staging} FORMAT JSONEachRow"
            ))
            fields = ["row_count", *mart["sum_columns"]]
            for field, value in zip(fields, expected):
                if Decimal(str(actual[field])) != Decimal(str(value)):
                    raise ValueError(f"Publication mismatch in {field}: {value} != {actual[field]}")

            key = ", ".join(mart["unique_key"])
            distinct_count = clickhouse_query(session, f"SELECT uniqExact(tuple({key})) FROM {staging}")
            if int(distinct_count) != expected[0]:
                raise ValueError(f"Duplicate keys in {staging}")

            if clickhouse_query(session, f"EXISTS TABLE {target}") == "1":
                clickhouse_query(session, f"EXCHANGE TABLES {target} AND {staging}")
                clickhouse_query(session, f"DROP TABLE {staging}")
            else:
                clickhouse_query(session, f"RENAME TABLE {staging} TO {target}")
            with connection.cursor() as cursor:
                cursor.execute("COMMIT")

    log.info("Published %s: %s", target, actual)
    return {"table": target, **actual}
