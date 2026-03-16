from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from reporter.awr.errors import AWRParseError
from reporter.awr.html_parser import parse_awr_html

ROOT = Path(__file__).resolve().parents[2]


class AWRParserTests(unittest.TestCase):
    def test_parse_sample_awr_html_extracts_core_payload(self) -> None:
        awr = parse_awr_html(ROOT / "resources" / "awrrpt_1_19321_19322.html")
        self.assertEqual(awr.metadata.db_name, "ORACC")
        self.assertEqual(awr.metadata.db_id, 2668322570)

        efficiency = awr.instance_efficiency
        self.assertIsInstance(efficiency["execute_to_parse_pct"], float)
        self.assertIsInstance(efficiency["soft_parse_pct"], float)
        self.assertIsInstance(efficiency["library_hit_pct"], float)
        self.assertIsInstance(efficiency["buffer_hit_pct"], float)

        self.assertNotEqual(awr.wait_classes["top_non_cpu_wait_class"].upper(), "DB CPU")
        self.assertIsInstance(awr.wait_classes["top_non_cpu_pct_db_time"], float)
        self.assertNotEqual(awr.top_foreground_events["top_non_cpu_event"].upper(), "DB CPU")
        self.assertIsInstance(awr.top_foreground_events["top_non_cpu_pct_db_time"], float)

        self.assertGreaterEqual(awr.load_profile["count"], 1)
        self.assertGreaterEqual(awr.sql["by_elapsed_time"]["count"], 1)
        self.assertGreaterEqual(awr.sql["by_cpu_time"]["count"], 1)

    def test_missing_required_table_fails_fast(self) -> None:
        html = """<!doctype html><html><body>
<table summary="This table displays database instance information">
<tr><th>DB Name</th><th>DB Id</th></tr>
<tr><td>ORACC</td><td>2668322570</td></tr>
</table>
</body></html>
"""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "awrrpt.html"
            path.write_text(html, encoding="utf-8")
            with self.assertRaises(AWRParseError) as ctx:
                parse_awr_html(path)
            self.assertIn("missing required AWR table", str(ctx.exception))

