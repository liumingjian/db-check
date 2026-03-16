from __future__ import annotations

import unittest
from pathlib import Path

from reporter.content.gaussdb_database_sections import build_gaussdb_database_sections
from reporter.wdr.html_parser import parse_wdr_html

ROOT = Path(__file__).resolve().parents[2]


class GaussDBWDRSectionTests(unittest.TestCase):
    def test_gaussdb_database_sections_append_wdr_section_when_present(self) -> None:
        wdr = parse_wdr_html(ROOT / "resources" / "wdr_cluster.html")
        result = {"db": {"wdr": wdr.to_result_payload()}}

        sections = build_gaussdb_database_sections(result)
        titles = [section.title for section in sections]
        self.assertIn("2.2.6 WDR 分析", titles)
        self.assertEqual(titles[-1], "2.2.6 WDR 分析")

        wdr_section = sections[-1]
        table_titles = [table.title for table in wdr_section.tables]
        for expected in (
            "Database Stat",
            "Load Profile",
            "Instance Efficiency Percentages",
            "IO Profile",
            "SQL ordered by Elapsed Time",
            "SQL ordered by CPU Time",
        ):
            self.assertIn(expected, table_titles)

    def test_gaussdb_database_sections_do_not_include_wdr_section_when_missing(self) -> None:
        sections = build_gaussdb_database_sections({})
        titles = [section.title for section in sections]
        self.assertNotIn("2.2.6 WDR 分析", titles)

