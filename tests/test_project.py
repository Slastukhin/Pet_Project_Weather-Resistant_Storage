import json
import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))
sys.path.insert(0, str(ROOT / "scripts"))

from common.marts import identifier, json_value, load_marts, stream_rows
from import_dashboards import remap


class ProjectTests(unittest.TestCase):
    def test_sql_identifiers(self):
        self.assertEqual(identifier("mart_sales_daily"), "mart_sales_daily")
        for value in ["a; drop table x", "a.b", "", "a b"]:
            with self.assertRaises(ValueError):
                identifier(value)

    def test_money_is_not_converted_to_float(self):
        self.assertEqual(json_value(Decimal("29354069.84")), "29354069.84")

    def test_dates_use_utc(self):
        self.assertEqual(json_value(date(2024, 3, 1)), "2024-03-01")
        value = datetime(2024, 3, 1, tzinfo=timezone.utc)
        self.assertEqual(json_value(value), "2024-03-01T00:00:00.000000+00:00")

    def test_marts_have_sql_models_and_matching_ddl(self):
        import re
        for mart in load_marts():
            self.assertTrue((ROOT / "dbt/my_dwh/models/marts" / (mart["model"] + ".sql")).is_file())
            ddl = (ROOT / "dags/sql/clickhouse" / mart["ddl_file"]).read_text()
            names = re.findall(r"^    ([a-z_]+)\s", ddl, re.MULTILINE)
            self.assertEqual(names, mart["columns"])

    def test_export_rows_keep_decimal_precision(self):
        class Cursor:
            def __init__(self):
                self.batches = iter([[(Decimal("10.10"), None, True)], []])

            def fetchmany(self, size):
                return next(self.batches)

        rows = b"".join(stream_rows(Cursor(), ["amount", "temperature", "synthetic"]))
        self.assertEqual(json.loads(rows), {"amount": "10.10", "temperature": None, "synthetic": True})

    def test_dashboard_ids_are_remapped(self):
        query = {"database": 2, "query": {"source-table": 9, "aggregation": [["sum", ["field", 79, None]]]}}
        result = remap(query, {9: 19}, {79: 179}, {}, 12)
        self.assertEqual(result["database"], 12)
        self.assertEqual(result["query"]["source-table"], 19)
        self.assertEqual(result["query"]["aggregation"][0][1][1], 179)
        self.assertEqual(query["database"], 2)

    def test_dashboard_export_contains_no_accounts(self):
        bundle = json.loads((ROOT / "demo/dashboards.json").read_text())
        self.assertEqual(len(bundle["dashboards"]), 3)
        self.assertNotIn("users", bundle)
        self.assertNotIn("sessions", bundle)
        cards = {card["id"] for card in bundle["cards"]}
        for row in bundle["dashcards"]:
            if row["card_id"] is not None:
                self.assertIn(row["card_id"], cards)


if __name__ == "__main__":
    unittest.main()
