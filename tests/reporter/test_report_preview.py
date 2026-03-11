from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from reporter.cli.generate_report_meta import MetaOptions, build_report_meta
from reporter.cli.db_report_preview import run
from reporter.content.mysql_report_builder import build_mysql_report_view
from reporter.renderer.markdown_preview import render_markdown_preview

ROOT = Path(__file__).resolve().parents[2]


class ReportPreviewTests(unittest.TestCase):
    def test_build_report_view_contains_template_chapters(self) -> None:
        result = json.loads((ROOT / "contracts" / "result.sample.json").read_text(encoding="utf-8"))
        summary = json.loads((ROOT / "contracts" / "summary.sample.json").read_text(encoding="utf-8"))
        meta = json.loads((ROOT / "reporter" / "templates" / "report-meta.sample.json").read_text(encoding="utf-8"))

        report = build_mysql_report_view(result, summary, meta)
        markdown = render_markdown_preview(report)

        self.assertEqual(report.sections[0].title, "文档控制")
        self.assertIn("## 第一章 巡检总结", markdown)
        self.assertIn("## 第二章 巡检明细", markdown)
        self.assertIn("### 2.2 MySQL基础信息", markdown)
        self.assertIn("| 风险等级 | 风险标识 | 定义 | 建议响应时效 |", markdown)
        self.assertIn("| 检查维度 | 风险标识 | 关键发现 |", markdown)
        self.assertIn("| 风险标识 | 检查维度 | 风险描述 | 影响分析 | 整改建议 |", markdown)
        self.assertIn("| 指标 | 当前值 | 说明 |", markdown)
        self.assertIn("本次巡检共检查", markdown)
        self.assertIn("占用空间top 10的索引", markdown)
        self.assertIn("物理IO top 10的表", markdown)
        self.assertIn("使用临时表的SQL top10", markdown)
        self.assertIn("无索引SQL top10", markdown)
        self.assertIn("最近备份记录", markdown)

    def test_replication_section_is_na_when_replication_not_configured(self) -> None:
        result = {
            "meta": {"collect_time": "2026-03-10T10:00:00+08:00", "db_host": "127.0.0.1", "db_port": 3306},
            "db": {
                "basic_info": {"uptime_seconds": 3600},
                "replication": {"enabled": False},
                "config_check": {},
                "storage": {"database_sizes": [], "table_index_counts": {}, "triggers_procedures_events": {}},
                "security": {"anonymous_users": [], "empty_password_users": [], "super_privilege_users": []},
                "performance": {},
                "backup": {},
                "sql_analysis": {},
            },
            "os": {"cpu": {"samples": []}, "memory": {"samples": []}, "filesystem": [], "system_info": {}},
        }
        summary = {
            "generated_at": "2026-03-10T10:00:00+08:00",
            "overall_risk": "low",
            "counts": {"total_checks": 0, "normal": 0, "warning": 0, "critical": 0, "unevaluated": 0, "not_applicable": 1},
            "abnormal_items": [],
            "na_items": [{"check_id": "2.0", "reason": "replication is not configured on this instance"}],
        }
        meta = {"doc_info": {"document_name": "MySQL巡检报告"}, "scope": {}}

        report = build_mysql_report_view(result, summary, meta)
        markdown = render_markdown_preview(report)

        self.assertIn("当前实例未配置复制，本节按不适用处理。", markdown)
        self.assertIn("状态: 不适用", markdown)

    def test_build_report_meta_uses_result_and_summary_defaults(self) -> None:
        result = json.loads((ROOT / "contracts" / "result.sample.json").read_text(encoding="utf-8"))
        summary = json.loads((ROOT / "contracts" / "summary.sample.json").read_text(encoding="utf-8"))
        options = MetaOptions(mysql_version="8.0", document_name="report.docx")

        meta = build_report_meta(result, summary, options)

        self.assertEqual("report.docx", meta["doc_info"]["document_name"])
        self.assertEqual("db-check", meta["doc_info"]["author"])
        self.assertEqual("db-check", meta["change_log"][0]["author"])
        self.assertEqual("mysql巡检报告", meta["change_log"][0]["change"])
        self.assertEqual("周海波", meta["review_log"][0]["name"])
        self.assertEqual("Standalone", meta["scope"]["architecture_role"])
        self.assertEqual("/data/mysql/", meta["scope"]["data_dir"])
        self.assertEqual("192.168.1.101:3306", meta["scope"]["inspection_target"])

    def test_build_report_meta_supports_custom_inspector_and_change_log(self) -> None:
        result = json.loads((ROOT / "contracts" / "result.sample.json").read_text(encoding="utf-8"))
        summary = json.loads((ROOT / "contracts" / "summary.sample.json").read_text(encoding="utf-8"))
        options = replace(
            MetaOptions(mysql_version="8.0", document_name="mysql巡检报告.docx"),
            inspector="刘明建",
            change_description="巡检报告首次出具",
        )

        meta = build_report_meta(result, summary, options)

        self.assertEqual("mysql巡检报告.docx", meta["doc_info"]["document_name"])
        self.assertEqual("刘明建", meta["doc_info"]["author"])
        self.assertEqual("刘明建", meta["change_log"][0]["author"])
        self.assertEqual("巡检报告首次出具", meta["change_log"][0]["change"])

    def test_build_report_meta_uses_override_data_dir_when_provided(self) -> None:
        result = json.loads((ROOT / "contracts" / "result.sample.json").read_text(encoding="utf-8"))
        summary = json.loads((ROOT / "contracts" / "summary.sample.json").read_text(encoding="utf-8"))

        meta = build_report_meta(result, summary, MetaOptions(mysql_version="8.0", data_dir="/custom/mysql"))

        self.assertEqual("/custom/mysql", meta["scope"]["data_dir"])

    def test_preview_cli_generates_report_markdown_and_json(self) -> None:
        temp_path = Path(tempfile.mkdtemp())
        try:
            out_md = temp_path / "report.md"
            out_json = temp_path / "report-view.json"
            code = run(
                [
                    "--result",
                    str(ROOT / "contracts" / "result.sample.json"),
                    "--summary",
                    str(ROOT / "contracts" / "summary.sample.json"),
                    "--meta",
                    str(ROOT / "reporter" / "templates" / "report-meta.sample.json"),
                    "--out-md",
                    str(out_md),
                    "--out-json",
                    str(out_json),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue(out_md.exists())
            self.assertTrue(out_json.exists())
            markdown = out_md.read_text(encoding="utf-8")
            self.assertIn("巡检总结", markdown)
            self.assertIn("本次巡检共检查", markdown)
            self.assertIn("字段说明：", markdown)
            self.assertIn("`obj_sch`: 对象所属库名", markdown)
            self.assertIn("占用空间top 10的索引", markdown)
            self.assertIn("物理IO top 10的表", markdown)
            self.assertIn("全表扫描的表top10", markdown)
            self.assertIn("使用临时表的SQL top10", markdown)
            self.assertIn("行操作次数top10", markdown)
            self.assertIn("无索引SQL top10", markdown)
            self.assertIn("全库权限用户", markdown)
            self.assertIn("当前 contracts 在 MySQL 5.6/5.7/8.0 上尚未形成一致的组件级内存分布明细", markdown)
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertIn("sections", payload)
            alarm_table = self._find_table(payload["sections"], "巡检告警定义")
            self.assertIsNotNone(alarm_table)
            self.assertEqual([12, 8, 58, 22], alarm_table["column_width_weights"])
            health_table = self._find_table(payload["sections"], "综合健康评估")
            self.assertIsNotNone(health_table)
            self.assertEqual([18, 12, 70], health_table["column_width_weights"])
            metadata_lock_table = self._find_table(payload["sections"], "元数据锁信息")
            self.assertIsNotNone(metadata_lock_table)
            self.assertTrue(metadata_lock_table["field_notes"])
            self.assertEqual([10, 12, 12, 16, 18, 16, 16], metadata_lock_table["column_width_weights"])
        finally:
            shutil.rmtree(temp_path, ignore_errors=True)

    def test_preview_cli_can_generate_report_view_without_markdown(self) -> None:
        temp_path = Path(tempfile.mkdtemp())
        try:
            out_json = temp_path / "report-view.json"
            code = run(
                [
                    "--result",
                    str(ROOT / "contracts" / "result.sample.json"),
                    "--summary",
                    str(ROOT / "contracts" / "summary.sample.json"),
                    "--meta",
                    str(ROOT / "reporter" / "templates" / "report-meta.sample.json"),
                    "--out-json",
                    str(out_json),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue(out_json.exists())
            self.assertFalse((temp_path / "report.md").exists())
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertIn("sections", payload)
        finally:
            shutil.rmtree(temp_path, ignore_errors=True)

    def _find_table(self, sections: list[dict], table_title: str) -> dict | None:
        for section in sections:
            table = self._find_table_in_section(section, table_title)
            if table:
                return table
        return None

    def _find_table_in_section(self, section: dict, table_title: str) -> dict | None:
        for table in section.get("tables", []):
            if isinstance(table, dict) and table.get("title") == table_title:
                return table
        for child in section.get("children", []):
            if not isinstance(child, dict):
                continue
            table = self._find_table_in_section(child, table_title)
            if table:
                return table
        return None


if __name__ == "__main__":
    unittest.main()
