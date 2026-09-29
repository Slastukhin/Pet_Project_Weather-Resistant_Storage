"""Export public teaching data and dashboard definitions, never user accounts."""

import gzip
import hashlib
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "demo"
TABLES = ["cities", "kaggle_coffee", "weather_archive", "weather_forecast", "synthetic_coffee_sales"]


def sql(service, user, database, query):
    command = ["docker", "compose", "exec", "-T", service,
               "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", user,
               "-d", database, "-At", "-c", query]
    return subprocess.check_output(command, cwd=ROOT, text=True)


def records(query):
    output = sql("metabase-db", "metabase", "metabase", query)
    return [json.loads(line) for line in output.splitlines() if line]


def export_raw():
    raw = DEMO / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    command = ["docker", "compose", "exec", "-T", "dwh-postgres", "sh", "-c",
               'exec pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --schema=raw --schema-only --no-owner --no-acl']
    ddl = subprocess.check_output(command, cwd=ROOT, text=True)
    ddl = "\n".join(line for line in ddl.splitlines() if not line.startswith("\\"))
    (raw / "schema.sql").write_text(ddl + "\n")
    manifest = {"tables": [], "files": {}}
    for table in TABLES:
        command = ["docker", "compose", "exec", "-T", "dwh-postgres", "sh", "-c",
                   'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"',
                   "sh", f"COPY raw.{table} TO STDOUT WITH (FORMAT CSV, HEADER true)"]
        with subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE) as process:
            with gzip.open(raw / f"{table}.csv.gz", "wb") as output:
                while block := process.stdout.read(1024 * 1024):
                    output.write(block)
            if process.wait():
                raise RuntimeError(f"Could not export {table}")
        manifest["tables"].append(table)
        print(f"Exported raw.{table}", flush=True)
    for path in [raw / "schema.sql", *sorted(raw.glob("*.csv.gz"))]:
        manifest["files"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (raw / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def export_dashboards():
    cards = records("""
        select row_to_json(c) from (
          select id, name, description, display, dataset_query::json,
                 visualization_settings::json, parameters::json, parameter_mappings::json
          from report_card
          where not archived and database_id in (
            select id from metabase_database where engine = 'clickhouse'
          ) order by id
        ) c
    """)
    dashboards = records("""
        select row_to_json(d) from (
          select id, name, description, parameters::json, width
          from report_dashboard
          where not archived and id in (
            select dc.dashboard_id from report_dashboardcard dc
            join report_card c on c.id = dc.card_id
            join metabase_database db on db.id = c.database_id
            where db.engine = 'clickhouse' and not c.archived
          ) order by id
        ) d
    """)
    dashboard_ids = ",".join(str(d["id"]) for d in dashboards)
    if not dashboard_ids or not cards:
        raise RuntimeError("No ClickHouse dashboards to export")
    dashcards = records(f"""
        select row_to_json(d) from (
          select dc.id, dc.dashboard_id, dc.card_id, dc.size_x, dc.size_y,
                 dc.row, dc.col, dc.parameter_mappings::json,
                 dc.visualization_settings::json, dc.dashboard_tab_id,
                 coalesce((select json_agg(json_build_object('id', s.card_id) order by s.position)
                           from dashboardcard_series s where s.dashboardcard_id=dc.id), '[]') as series
          from report_dashboardcard dc
          left join report_card c on c.id = dc.card_id
          where dc.dashboard_id in ({dashboard_ids})
            and (dc.card_id is null or not c.archived)
          order by dc.id
        ) d
    """)
    tables = records("""
        select row_to_json(t) from (
          select id, name, schema from metabase_table
          where active and db_id in (select id from metabase_database where engine='clickhouse')
        ) t
    """)
    fields = records("""
        select row_to_json(f) from (
          select f.id, f.table_id, f.name, f.display_name, f.semantic_type, f.settings::json
          from metabase_field f join metabase_table t on t.id=f.table_id
          where f.active and t.active
            and t.db_id in (select id from metabase_database where engine='clickhouse')
        ) f
    """)
    tabs = records(f"select row_to_json(t) from (select id, dashboard_id, name, position from dashboard_tab where dashboard_id in ({dashboard_ids}) order by position) t")
    export = dict(version=1, metabase_version="v0.55.11", cards=cards, dashboards=dashboards,
                  dashcards=dashcards, tables=tables, fields=fields, tabs=tabs)
    (DEMO / "dashboards.json").write_text(json.dumps(export, ensure_ascii=False, indent=2) + "\n")
    print(f"Exported {len(cards)} questions and {len(dashboards)} dashboards")


if __name__ == "__main__":
    export_raw()
    export_dashboards()
