from __future__ import annotations

import unittest
from pathlib import Path

from reporter.content.gaussdb_database_sections import build_gaussdb_database_sections
from reporter.wdr.html_parser import parse_wdr_html

ROOT = Path(__file__).resolve().parents[2]


class GaussDBWDRSectionTests(unittest.TestCase):
    def test_gaussdb_database_sections_add_heading3_wdr_section_when_present(self) -> None:
        wdr = parse_wdr_html(ROOT / "resources" / "wdr_cluster.html")
        result = {"db": {"wdr": wdr.to_result_payload()}}

        sections = build_gaussdb_database_sections(result)
        titles = [section.title for section in sections]
        self.assertNotIn("2.2.6 WDR 分析", titles)
        self.assertEqual(titles[-1], "2.2.8 WDR 性能洞察")

        wdr_section = sections[-1]
        table_titles = [table.title for table in wdr_section.tables]
        for expected in (
            "WDR 报告来源",
            "WDR 负载概要",
            "WDR 实例效率",
            "WDR IO 概要",
            "WDR Top SQL - Elapsed",
            "WDR Top SQL - CPU",
        ):
            self.assertIn(expected, table_titles)

    def test_gaussdb_database_sections_do_not_include_wdr_section_when_missing(self) -> None:
        sections = build_gaussdb_database_sections({})
        titles = [section.title for section in sections]
        self.assertNotIn("2.2.6 WDR 分析", titles)
        self.assertNotIn("2.2.8 WDR 性能洞察", titles)
