from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from reporter.awr.errors import AWRParseError
from reporter.awr.html_parser import parse_awr_html

ROOT = Path(__file__).resolve().parents[2]
PARSER_FIXTURES = ROOT / "tests" / "reporter" / "fixtures"


def _load_payload_fixture(name: str) -> dict[str, object]:
    return json.loads((PARSER_FIXTURES / name).read_text(encoding="utf-8"))


def _insert_before_first_table(html: str, table: str) -> str:
    table_start = html.lower().index("<table")
    return html[:table_start] + table + html[table_start:]


class AWRParserTests(unittest.TestCase):
    maxDiff = None

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
        self.assertEqual(awr.to_result_payload(), _load_payload_fixture("awr_sample_payload.json"))

    def test_optional_cache_table_uses_first_match_and_retains_units(self) -> None:
        table = """
<table summary="This table displays cache sizes and other statistics">
<tr><th>Metric</th><th>Begin</th><th>End</th></tr>
<tr><td>Injected cache:</td><td>1.25K</td><td>2ms</td></tr>
</table>
"""
        html = _insert_before_first_table(
            (ROOT / "resources" / "awrrpt_1_19321_19322.html").read_text(encoding="utf-8"),
            table,
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "awrrpt.html"
            path.write_text(html, encoding="utf-8")
            awr = parse_awr_html(path)

        self.assertEqual(
            awr.appendix["cache_sizes"],
            {
                "items": [
                    {
                        "metric": "Injected cache",
                        "begin_value": 1.25,
                        "begin_unit": "K",
                        "end_value": 2.0,
                        "end_unit": "ms",
                    }
                ],
                "count": 1,
            },
        )

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
            self.assertEqual(
                str(ctx.exception),
                "missing required AWR table: summary contains 'load profile'",
            )
