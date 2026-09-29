"""Import dashboard definitions into a new Metabase installation."""

import json
import logging
import os
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
STATE = Path(os.environ.get("METABASE_IMPORT_STATE", "/opt/project/state/metabase.json"))


def initialization_action(properties, state):
    if not properties.get("has-user-setup"):
        return "setup"
    if not state.get("owned"):
        return "skip_existing"
    if state.get("complete"):
        return "skip_complete"
    return "resume"


def remap(value, tables, fields, cards, database):
    if isinstance(value, list):
        if len(value) > 1 and value[0] == "field" and isinstance(value[1], int):
            return ["field", fields[value[1]], *[remap(v, tables, fields, cards, database) for v in value[2:]]]
        return [remap(v, tables, fields, cards, database) for v in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key == "database":
                item = database
            elif key == "source-table" and isinstance(item, int):
                item = tables[item]
            elif key == "source-table" and isinstance(item, str) and item.startswith("card__"):
                item = "card__" + str(cards[int(item[6:])])
            elif key == "card_id" and isinstance(item, int):
                item = cards[item]
            elif key == "field_id" and isinstance(item, int):
                item = fields[item]
            else:
                item = remap(item, tables, fields, cards, database)
            if key.startswith("["):
                key = json.dumps(remap(json.loads(key), tables, fields, cards, database), separators=(",", ":"))
            result[key] = item
        return result
    return value


def main():
    logging.basicConfig(level=logging.INFO)
    session = requests.Session()
    base = os.environ.get("METABASE_URL", "http://metabase:3000").rstrip("/")

    def api(method, path, payload=None):
        response = session.request(method, base + "/api" + path, json=payload, timeout=180)
        response.raise_for_status()
        return response.json() if response.content else None

    def save():
        STATE.parent.mkdir(parents=True, exist_ok=True)
        temporary = STATE.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, indent=2))
        temporary.replace(STATE)

    properties = api("GET", "/session/properties")
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    action = initialization_action(properties, state)
    if action == "skip_existing":
        logging.info("Existing Metabase installation: accounts and dashboards are unchanged")
        return
    if action == "skip_complete":
        logging.info("Dashboards already imported")
        return
    email = os.environ.get("METABASE_ADMIN_EMAIL", "admin@example.com")
    password = os.environ.get("METABASE_ADMIN_PASSWORD", "WeatherDemo2026!")
    if action == "setup":
        state = {"owned": True, "cards": {}, "dashboards": {}}
        save()
        api("POST", "/setup", {
            "token": properties["setup-token"],
            "user": {"email": email, "password": password, "first_name": "Demo", "last_name": "Admin"},
            "prefs": {"site_name": "Coffee and weather", "site_locale": "ru"},
        })
    login = api("POST", "/session", {"username": email, "password": password})
    session.headers["X-Metabase-Session"] = login["id"]
    try:
        bundle = json.loads((ROOT / "demo" / "dashboards.json").read_text())
        if "database" not in state:
            database = api("POST", "/database", {
                "name": "marts", "engine": "clickhouse", "is_full_sync": True,
                "details": {"host": os.environ.get("CLICKHOUSE_HOST", "clickhouse"), "port": 8123,
                            "dbname": "marts", "user": os.environ["CLICKHOUSE_USER"],
                            "password": os.environ["CLICKHOUSE_PASSWORD"], "ssl": False,
                            "scan-all-databases": False},
            })
            state["database"] = database["id"]
            save()
        database = state["database"]
        api("POST", f"/database/{database}/sync_schema")
        tables, fields = {}, {}
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            metadata = api("GET", f"/database/{database}/metadata")
            current_tables = {(t.get("schema"), t["name"]): t for t in metadata["tables"]}
            try:
                tables = {t["id"]: current_tables[(t["schema"], t["name"])]["id"] for t in bundle["tables"]}
                current_fields = {(t["id"], f["name"]): f["id"] for t in metadata["tables"] for f in t["fields"]}
                fields = {f["id"]: current_fields[(tables[f["table_id"]], f["name"])] for f in bundle["fields"]}
                break
            except KeyError:
                time.sleep(3)
        else:
            raise TimeoutError("Metabase has not discovered all mart columns")
        for field in bundle["fields"]:
            settings = {key: field[key] for key in ("display_name", "semantic_type", "settings") if field.get(key) is not None}
            api("PUT", f"/field/{fields[field['id']]}", settings)
        if "collection" not in state:
            state["collection"] = api("POST", "/collection", {"name": "Кофе и погода"})["id"]
            save()
        card_ids = {int(old): new for old, new in state["cards"].items()}
        for card in bundle["cards"]:
            if card["id"] in card_ids:
                continue
            payload = {key: value for key, value in card.items() if key != "id" and value is not None}
            payload = remap(payload, tables, fields, card_ids, database)
            payload["collection_id"] = state["collection"]
            created = api("POST", "/card", payload)
            card_ids[card["id"]] = created["id"]
            state["cards"][str(card["id"])] = created["id"]
            save()
        for dashboard in bundle["dashboards"]:
            old_id = str(dashboard["id"])
            if old_id not in state["dashboards"]:
                created = api("POST", "/dashboard", {
                    "name": dashboard["name"], "description": dashboard.get("description") or "",
                    "collection_id": state["collection"],
                    "parameters": remap(dashboard["parameters"], tables, fields, card_ids, database),
                })
                state["dashboards"][old_id] = created["id"]
                save()
            new_id = state["dashboards"][old_id]
            dashboard_tabs = [t for t in bundle["tabs"] if t["dashboard_id"] == dashboard["id"]]
            tab_ids = {t["id"]: -(i + 1) for i, t in enumerate(dashboard_tabs)}
            rows = []
            for entry in bundle["dashcards"]:
                if entry["dashboard_id"] != dashboard["id"]:
                    continue
                row = {key: entry[key] for key in ("size_x", "size_y", "row", "col")}
                row.update({
                    "id": -entry["id"],
                    "card_id": card_ids[entry["card_id"]] if entry["card_id"] else None,
                    "dashboard_tab_id": tab_ids.get(entry["dashboard_tab_id"]),
                    "series": [{"id": card_ids[s["id"]]} for s in entry["series"]],
                    "parameter_mappings": remap(entry["parameter_mappings"], tables, fields, card_ids, database),
                    "visualization_settings": remap(entry["visualization_settings"], tables, fields, card_ids, database),
                })
                rows.append(row)
            api("PUT", f"/dashboard/{new_id}", {
                "width": dashboard.get("width") or "fixed", "dashcards": rows,
                "tabs": [{"id": tab_ids[t["id"]], "name": t["name"]} for t in dashboard_tabs],
            })
            logging.info("Imported dashboard: %s", dashboard["name"])
        state["complete"] = True
        save()
    finally:
        api("DELETE", "/session")


if __name__ == "__main__":
    main()
