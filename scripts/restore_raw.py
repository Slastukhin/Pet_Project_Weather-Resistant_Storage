"""Load the teaching snapshot only into a database without a RAW layer."""

import gzip
import hashlib
import json
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql

DEMO = Path(__file__).resolve().parents[1] / "demo" / "raw"


def connect():
    return psycopg2.connect(
        host=os.environ.get("DWH_POSTGRES_HOST", "localhost"),
        port=os.environ.get("DWH_POSTGRES_PORT", "5433"),
        user=os.environ.get("DWH_POSTGRES_USER", "dwh"),
        password=os.environ.get("DWH_POSTGRES_PASSWORD", "dwh"),
        dbname=os.environ.get("DWH_POSTGRES_DB", "dwh"),
        connect_timeout=15,
    )


def restore():
    manifest = json.loads((DEMO / "manifest.json").read_text())
    connection = connect()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('weather_demo_restore'))")
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='raw'")
                existing = {row[0] for row in cursor.fetchall()}
                if existing:
                    missing = set(manifest["tables"]) - existing
                    if missing:
                        raise RuntimeError(f"RAW already exists but is incomplete: {sorted(missing)}. No data overwritten.")
                    print("RAW already exists; keeping its data", flush=True)
                    return
                for name, expected in manifest["files"].items():
                    actual = hashlib.sha256((DEMO / name).read_bytes()).hexdigest()
                    if actual != expected:
                        raise ValueError(f"Snapshot checksum mismatch: {name}")
                cursor.execute((DEMO / "schema.sql").read_text())
                for table in manifest["tables"]:
                    command = sql.SQL("COPY raw.{} FROM STDIN WITH (FORMAT CSV, HEADER true)").format(sql.Identifier(table))
                    with gzip.open(DEMO / f"{table}.csv.gz", "rt") as source:
                        cursor.copy_expert(command.as_string(connection), source)
                    print(f"Restored raw.{table}", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    restore()
