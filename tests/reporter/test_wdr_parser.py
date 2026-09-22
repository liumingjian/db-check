from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from reporter.wdr.errors import WDRParseError
from reporter.wdr.html_parser import parse_wdr_html

ROOT = Path(__file__).resolve().parents[2]
NODE_WDR = Path("/Users/lmj/Documents/temp/wdr_node_dn_6001_6002_6003.html")
PARSER_FIXTURES = ROOT / "tests" / "reporter" / "fixtures"


def _load_payload_fixture(name: str) -> dict[str, object]:
    return json.loads((PARSER_FIXTURES / name).read_text(encoding="utf-8"))


def _insert_before_first_table(html: str, table: str) -> str:
    table_start = html.lower().index("<table")
    return html[:table_start] + table + html[table_start:]


class WDRParserTests(unittest.TestCase):
    maxDiff = None

    def test_parse_sample_wdr_html_extracts_core_payload(self) -> None:
        wdr = parse_wdr_html(ROOT / "resources" / "wdr_cluster.html")
        self.assertIn("dn_6001_6002_6003", wdr.metadata.node_names)
        self.assertIn("postgres", wdr.metadata.db_names)
        self.assertEqual(wdr.metadata.report_scope, "Cluster")

        self.assertGreaterEqual(wdr.database_stat["count"], 1)
        self.assertGreaterEqual(wdr.load_profile["workload"]["count"], 1)
        self.assertIsInstance(wdr.instance_efficiency["buffer_hit_pct"], float)
        self.assertAlmostEqual(float(wdr.instance_efficiency["buffer_hit_pct"]), 99.74, places=2)
        self.assertGreaterEqual(wdr.io_profile["count"], 1)
        self.assertGreaterEqual(wdr.sql["by_elapsed_time"]["count"], 1)
        self.assertGreaterEqual(wdr.sql["by_cpu_time"]["count"], 1)

        resp = wdr.load_profile.get("sql_response_time", {})
        self.assertIsInstance(resp, dict)
        self.assertEqual(resp.get("p95_us"), 4898.0)
        self.assertEqual(resp.get("p80_us"), 613.0)
        items = resp.get("items", [])
        self.assertIsInstance(items, list)
        p95 = next((item for item in items if isinstance(item, dict) and item.get("metric") == "SQL response time P95(us)"), None)
        self.assertIsNotNone(p95)
        self.assertEqual(p95["value"], 4898.0)
        self.assertEqual(wdr.wait_events, {"items": [], "count": 0})
        self.assertEqual(wdr.to_result_payload(), _load_payload_fixture("wdr_cluster_payload.json"))

    def test_wait_event_matches_use_document_order(self) -> None:
        tables = """
<table summary="This table displays Top 10 Events by Total Wait Time">
<tr><th>Event</th><th>Waits</th><th>Total Wait Time(us)</th><th>Avg Wait Time(us)</th><th>Type</th></tr>
<tr><td>first event</td><td>3</td><td>9</td><td>3</td><td>first</td></tr>
</table>
<table summary="This table displays Top 10 Events by Total Wait Time">
<tr><th>Event</th><th>Waits</th><th>Total Wait Time(us)</th><th>Avg Wait Time(us)</th><th>Type</th></tr>
<tr><td>second event</td><td>4</td><td>16</td><td>4</td><td>second</td></tr>
</table>
"""
        html = _insert_before_first_table(
            (ROOT / "resources" / "wdr_cluster.html").read_text(encoding="utf-8"),
            tables,
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "wdr.html"
            path.write_text(html, encoding="utf-8")
            wdr = parse_wdr_html(path)

        self.assertEqual(
            wdr.wait_events,
            {
                "items": [
                    {
                        "event": "first event",
                        "waits": 3,
                        "total_wait_time_us": 9.0,
                        "avg_wait_time_us": 3.0,
                        "type": "first",
                    }
                ],
                "count": 1,
            },
        )

    @unittest.skipUnless(NODE_WDR.exists(), "local node-level WDR fixture is not available")
    def test_parse_node_wdr_fills_missing_node_name_from_report_node(self) -> None:
        wdr = parse_wdr_html(NODE_WDR)

        self.assertEqual(wdr.metadata.report_scope, "Node")
        self.assertEqual(wdr.metadata.report_node, "dn_6001_6002_6003")
        self.assertIn("dn_6001_6002_6003", wdr.metadata.node_names)
        elapsed_items = wdr.sql["by_elapsed_time"]["items"]
        self.assertTrue(elapsed_items)
        self.assertTrue(all(item["node_name"] == "dn_6001_6002_6003" for item in elapsed_items))

    def test_missing_required_table_fails_fast(self) -> None:
        html = """<!doctype html><html><body>
<table summary="This table displays Database Stat">
<tr><th>Node Name</th><th>DB Name</th><th>Backends</th></tr>
<tr><td>dn_1</td><td>postgres</td><td>1</td></tr>
</table>
</body></html>
"""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "wdr.html"
            path.write_text(html, encoding="utf-8")
            with self.assertRaises(WDRParseError) as ctx:
                parse_wdr_html(path)
            self.assertEqual(
                str(ctx.exception),
                "missing required WDR table: summary contains 'report type'",
            )
