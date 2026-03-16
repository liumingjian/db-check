from __future__ import annotations

import unittest
from pathlib import Path

from reporter.awr.html_parser import parse_awr_html
from reporter.content.oracle_database_sections import build_oracle_database_sections

ROOT = Path(__file__).resolve().parents[2]


class OracleAWRSectionTests(unittest.TestCase):
    def test_oracle_database_sections_append_awr_section_when_present(self) -> None:
        awr = parse_awr_html(ROOT / "resources" / "awrrpt_1_19321_19322.html")
        result = {"db": {"awr": awr.to_result_payload()}}

        sections = build_oracle_database_sections(result)
        titles = [section.title for section in sections]
        self.assertIn("2.2.7 AWR 分析", titles)
        self.assertEqual(titles[-1], "2.2.7 AWR 分析")

        awr_section = sections[-1]
        table_titles = [table.title for table in awr_section.tables]
        for expected in (
            "Load Profile",
            "Instance Efficiency Percentages",
            "Top 10 Foreground Events by Total Wait Time",
            "Wait Classes by Total Wait Time",
            "SQL ordered by Elapsed Time",
            "SQL ordered by CPU Time",
        ):
            self.assertIn(expected, table_titles)
        for expected in ("Memory Statistics", "Shared Pool Statistics", "Cache Sizes"):
            self.assertIn(expected, table_titles)

    def test_oracle_database_sections_do_not_include_awr_section_when_missing(self) -> None:
        sections = build_oracle_database_sections({})
        titles = [section.title for section in sections]
        self.assertNotIn("2.2.7 AWR 分析", titles)
