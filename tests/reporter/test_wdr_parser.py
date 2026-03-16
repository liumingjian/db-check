from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from reporter.wdr.errors import WDRParseError
from reporter.wdr.html_parser import parse_wdr_html

ROOT = Path(__file__).resolve().parents[2]


class WDRParserTests(unittest.TestCase):
    def test_parse_sample_wdr_html_extracts_core_payload(self) -> None:
        wdr = parse_wdr_html(ROOT / "resources" / "wdr_cluster.html")
        self.assertIn("dn_6001_6002_6003", wdr.metadata.node_names)
        self.assertIn("postgres", wdr.metadata.db_names)

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
            self.assertIn("missing required WDR table", str(ctx.exception))
