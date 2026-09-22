from __future__ import annotations

import math
import unittest

from reporter.html_tables import HTMLTable
from reporter.html_tables import HTMLTableCollector
from reporter.html_tables import normalize_text
from reporter.html_tables import normalized_header
from reporter.html_tables import parse_number
from reporter.html_tables import row_dict


class HTMLTablesTests(unittest.TestCase):
    def test_collector_normalizes_whitespace_and_nested_formatting_tags(self) -> None:
        collector = HTMLTableCollector()
        collector.feed(
            """
            <h3> Report <em> heading </em> </h3>
            <table summary="Example table">
              <tr><th> Metric&nbsp;Name </th><th> Value </th></tr>
              <tr><td> total <strong>wait</strong> time </td><td> 1\n  2 </td></tr>
            </table>
            """
        )

        self.assertEqual(
            collector.tables,
            [
                HTMLTable(
                    summary="Example table",
                    heading="Report heading",
                    rows=(("Metric Name", "Value"), ("total wait time", "1 2")),
                )
            ],
        )
        self.assertEqual(normalize_text("  one\xa0\n two  "), "one two")

    def test_row_mapping_preserves_duplicate_headers_and_ignores_missing_cells(self) -> None:
        table = HTMLTable(
            summary="Example table",
            heading="",
            rows=(("First Header", "First Header", "Third Header"),),
        )

        self.assertEqual(
            row_dict(normalized_header(table), ("first", "second")),
            {"first header": "second"},
        )

    def test_parse_number_keeps_existing_numeric_semantics(self) -> None:
        self.assertEqual(parse_number(" 1,234.5 "), 1234.5)
        self.assertEqual(parse_number(".25"), 0.25)
        self.assertEqual(parse_number("2K"), 2000.0)
        self.assertIsNone(parse_number("not a number"))
        self.assertTrue(math.isnan(parse_number("NaN")))
        self.assertTrue(math.isinf(parse_number("Infinity")))
