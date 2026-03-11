from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from docx import Document

from reporter.cli.render_template_docx import run
from reporter.content.mysql_report_builder import build_mysql_report_view

ROOT = Path(__file__).resolve().parents[2]


class TemplateDocxRendererTests(unittest.TestCase):
    def test_template_docx_cli_generates_styled_report(self) -> None:
        result = json.loads((ROOT / "contracts" / "result.sample.json").read_text(encoding="utf-8"))
        summary = json.loads((ROOT / "contracts" / "summary.sample.json").read_text(encoding="utf-8"))
        meta = json.loads((ROOT / "reporter" / "templates" / "report-meta.sample.json").read_text(encoding="utf-8"))
        report_view = build_mysql_report_view(result, summary, meta)
        temp_dir = Path(tempfile.mkdtemp())
        try:
            report_md = temp_dir / "report.md"
            report_md.write_text("# placeholder\n", encoding="utf-8")
            report_view_path = temp_dir / "report-view.json"
            report_view_path.write_text(json.dumps(report_view.to_dict(), ensure_ascii=False), encoding="utf-8")
            out_docx = temp_dir / "styled-report.docx"
            code = run(
                [
                    "--report-md",
                    str(report_md),
                    "--report-view",
                    str(report_view_path),
                    "--template",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                    "--out",
                    str(out_docx),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue(out_docx.exists())
            document = Document(str(out_docx))
            texts = [paragraph.text for paragraph in document.paragraphs]
            self.assertIn("文档控制", texts)
            self.assertIn("巡检总结", texts)
            self.assertNotIn("第一章 巡检总结", texts)
            self.assertLess(texts.index("文档控制"), len(texts) - 1)
            self.assertEqual(texts[0], "")
            self.assertGreater(len(document.tables), 5)
            self.assertEqual(document.tables[0].style.name, "Table Grid")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
