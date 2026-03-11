from __future__ import annotations

import json
import os
import re
import subprocess
import unittest
from pathlib import Path
from typing import Any

from docx import Document

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tests" / "e2e" / "run_docker_e2e.sh"
EXPECTED_VERSIONS = ("5.6", "5.7", "8.0")
ARTIFACT_PATTERN = re.compile(r"^\[INFO\] artifacts\[(?P<version>[^\]]+)\]: (?P<path>.+)$", re.MULTILINE)
SYSTEM_METRIC_LABELS = (
    "CPU 使用率",
    "CPU iowait",
    "内存使用率",
    "磁盘使用率",
    "文件描述符使用率",
    "MySQL fd 使用率",
)


class DockerE2ETests(unittest.TestCase):
    def test_e2e_script_configures_remote_os_target(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        for expected in (
            'OS_TARGET_HOST="127.0.0.1"',
            'OS_TARGET_PORT="12222"',
            'OS_TARGET_USER="root"',
            'OS_TARGET_PASSWORD="rootpwd"',
            '--os-host "$OS_TARGET_HOST"',
            '--os-port "$OS_TARGET_PORT"',
            '--os-username "$OS_TARGET_USER"',
            '--os-password "$OS_TARGET_PASSWORD"',
        ):
            self.assertIn(expected, script)

    def test_run_docker_e2e_script(self) -> None:
        if os.getenv("DBCHECK_RUN_DOCKER_E2E") != "1":
            self.skipTest("set DBCHECK_RUN_DOCKER_E2E=1 to enable docker e2e")
        completed = subprocess.run(
            [str(SCRIPT)],
            cwd=ROOT,
            env=dict(os.environ),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, msg=f"stdout:\n{completed.stdout}\n\nstderr:\n{completed.stderr}")
        run_dirs = self._extract_run_dirs(completed.stdout)
        self.assertEqual(set(EXPECTED_VERSIONS), set(run_dirs))
        for version, run_dir in run_dirs.items():
            with self.subTest(mysql_version=version):
                result = self._load_json(run_dir / "result.json")
                summary = self._load_json(run_dir / "summary.json")
                report_meta = self._load_json(run_dir / "report-meta.json")
                report_view = self._load_json(run_dir / "report-view.json")
                report_markdown = self._load_text(run_dir / "report.md")
                report_docx = self._load_docx(run_dir / "report.docx")
                self._assert_db_path_coverage(summary)
                self._assert_summary_semantics(summary)
                self._assert_scenario_hits(result)
                self._assert_remote_os_collection(result)
                self._assert_markdown_report(version, report_markdown, report_meta, report_view, summary)
                self._assert_docx_system_metrics(report_docx)

    def _extract_run_dirs(self, stdout_text: str) -> dict[str, Path]:
        matches = ARTIFACT_PATTERN.findall(stdout_text)
        self.assertTrue(matches, msg=f"artifacts path not found in stdout:\n{stdout_text}")
        return {version: Path(path) for version, path in matches}

    def _load_json(self, path: Path) -> dict[str, Any]:
        self.assertTrue(path.exists(), msg=f"missing json artifact: {path}")
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _load_text(self, path: Path) -> str:
        self.assertTrue(path.exists(), msg=f"missing text artifact: {path}")
        return path.read_text(encoding="utf-8")

    def _load_docx(self, path: Path) -> Document:
        self.assertTrue(path.exists(), msg=f"missing docx artifact: {path}")
        return Document(str(path))

    def _items(self, payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
        value = payload.get(key, {})
        if not isinstance(value, dict):
            return []
        items = value.get("items", [])
        if not isinstance(items, list):
            return []
        return [item for item in items if isinstance(item, dict)]

    def _assert_db_path_coverage(self, summary: dict[str, Any]) -> None:
        unevaluated = summary.get("unevaluated_items", [])
        self.assertIsInstance(unevaluated, list)
        db_no_data = [
            item
            for item in unevaluated
            if isinstance(item, dict)
            and str(item.get("reason", "")).startswith("no data extracted from path: db.")
        ]
        self.assertEqual([], db_no_data, msg=f"db path extraction gaps found: {db_no_data[:5]}")

    def _assert_summary_semantics(self, summary: dict[str, Any]) -> None:
        abnormal_items = summary.get("abnormal_items", [])
        abnormal_ids = {item.get("check_id") for item in abnormal_items if isinstance(item, dict)}
        na_items = summary.get("na_items", [])
        na_ids = {item.get("check_id") for item in na_items if isinstance(item, dict)}

        self.assertTrue({"2.0", "2.1", "2.2", "2.3", "2.4", "2.5", "2.6", "2.7", "2.8", "2.11", "2.12"}.issubset(na_ids))
        self.assertNotIn("4.4", abnormal_ids)
        self.assertNotIn("4.5", abnormal_ids)
        self.assertNotIn("4.13", abnormal_ids)
        self.assertNotIn("4.17", abnormal_ids)
        self.assertNotIn("4.18", abnormal_ids)
        self.assertNotIn("10.5", abnormal_ids)

    def _assert_remote_os_collection(self, result: dict[str, Any]) -> None:
        os_section = result.get("os", {}) if isinstance(result.get("os"), dict) else {}
        system_info = os_section.get("system_info", {}) if isinstance(os_section.get("system_info"), dict) else {}
        self.assertEqual("os-target", system_info.get("hostname"))
        self.assertEqual("linux", str(system_info.get("os", "")).lower())
        self.assertTrue(os_section.get("filesystem", {}).get("samples"))
        self.assertTrue(os_section.get("network", {}).get("samples"))

    def _assert_scenario_hits(self, result: dict[str, Any]) -> None:
        db = result.get("db", {}) if isinstance(result.get("db"), dict) else {}
        config = db.get("config_check", {}) if isinstance(db.get("config_check"), dict) else {}
        perf = db.get("performance", {}) if isinstance(db.get("performance"), dict) else {}
        sql_analysis = db.get("sql_analysis", {}) if isinstance(db.get("sql_analysis"), dict) else {}
        storage = db.get("storage", {}) if isinstance(db.get("storage"), dict) else {}

        self.assertEqual("ON", str(config.get("slow_query_log", "")))
        self.assertGreater(float(perf.get("slow_queries_count", 0)), 0)
        self.assertGreaterEqual(float(perf.get("row_lock_waits_delta", 0)), 1)

        full_scan = sql_analysis.get("full_scan_sqls", {}) if isinstance(sql_analysis.get("full_scan_sqls"), dict) else {}
        full_scan_items = full_scan.get("items", []) if isinstance(full_scan.get("items"), list) else []
        self.assertGreater(len(full_scan_items), 0)

        no_pk = storage.get("tables_without_pk", {}) if isinstance(storage.get("tables_without_pk"), dict) else {}
        no_pk_items = no_pk.get("items", []) if isinstance(no_pk.get("items"), list) else []
        self.assertGreater(len(no_pk_items), 0)
        self.assertGreater(len(self._items(storage, "top_indexes_by_size")), 0)
        self.assertGreater(len(self._items(storage, "tables_with_many_indexes")), 0)
        self.assertGreater(len(self._items(storage, "wide_composite_indexes")), 0)
        self.assertGreater(len(self._items(perf, "top_tables_by_io")), 0)
        self.assertGreater(len(self._items(perf, "full_scan_tables")), 0)
        self.assertGreater(len(self._items(perf, "row_ops_top_tables")), 0)
        self.assertGreater(len(self._items(sql_analysis, "tmp_table_sqls")), 0)

    def _assert_markdown_report(
        self,
        version: str,
        report_markdown: str,
        report_meta: dict[str, Any],
        report_view: dict[str, Any],
        summary: dict[str, Any],
    ) -> None:
        title = report_meta.get("doc_info", {}).get("document_name", "")
        self.assertIn(title, report_markdown)
        self.assertEqual("report.docx", title)
        self.assertEqual("db-check", report_meta.get("doc_info", {}).get("author", ""))
        self.assertEqual("db-check", report_meta.get("change_log", [{}])[0].get("author", ""))
        self.assertEqual("mysql巡检报告", report_meta.get("change_log", [{}])[0].get("change", ""))
        self.assertEqual("周海波", report_meta.get("review_log", [{}])[0].get("name", ""))
        self.assertEqual(version, report_meta.get("scope", {}).get("database_version", ""))
        self.assertIn("## 文档控制", report_markdown)
        self.assertIn("## 第一章 巡检总结", report_markdown)
        self.assertIn("## 第二章 巡检明细", report_markdown)
        self.assertIn("本次巡检共检查", report_markdown)
        self.assertIn("当前实例未配置复制，本节按不适用处理。", report_markdown)
        self.assertIn("字段说明：", report_markdown)
        self.assertIn("慢SQL top10", report_markdown)
        self.assertIn("全表扫描的SQL top10", report_markdown)
        self.assertIn("占用空间top 10的索引", report_markdown)
        self.assertIn("单张表超过6个索引的对象", report_markdown)
        self.assertIn("联合索引的字段个数大于4的对象", report_markdown)
        self.assertIn("物理IO top 10的表", report_markdown)
        self.assertIn("全表扫描的表top10", report_markdown)
        self.assertIn("使用临时表的SQL top10", report_markdown)
        self.assertIn("行操作次数top10", report_markdown)
        self.assertIn("无索引SQL top10", report_markdown)
        self.assertIn("没有主键或唯一键的表", report_markdown)
        self.assertIn("元数据锁信息", report_markdown)
        self.assertIn("Top等待事件", report_markdown)
        self.assertIn("最近备份记录", report_markdown)
        self.assertIn("全库权限用户", report_markdown)
        self.assertIn("认证插件分布", report_markdown)
        self.assertIn("未采集", report_markdown)
        self.assertIn("当前 contracts 在 MySQL 5.6/5.7/8.0 上尚未形成一致的组件级内存分布明细", report_markdown)
        self.assertNotIn("当前 contracts 仅有聚合值，未输出对象级清单。", report_markdown)
        self.assertNotRegex(report_markdown, r"\| 磁盘使用率 \| [^|]+ \| \s*\|")
        self.assertNotIn("| redo log |  |", report_markdown)
        self.assertIn("| innodb_flush_method | O_DIRECT |", report_markdown)
        self.assertIn("myisam_case", report_markdown)
        self.assertIn("wide_table_case", report_markdown)
        self.assertIn("auto_inc_case", report_markdown)
        self.assertIn("redundant_index_case", report_markdown)
        self.assertIn("many_index_case", report_markdown)
        self.assertIn("wide_composite_index_case", report_markdown)
        if version in {"5.7", "8.0"}:
            self.assertIn("ddl_lock_case", report_markdown)
        self.assertIn(str(summary.get("counts", {}).get("total_checks", "")), report_markdown)
        sections = report_view.get("sections", [])
        self.assertTrue(sections)
        alarm_table = self._find_table(sections, "巡检告警定义")
        self.assertIsNotNone(alarm_table)
        self.assertEqual([12, 8, 58, 22], alarm_table.get("column_width_weights"))
        health_table = self._find_table(sections, "综合健康评估")
        self.assertIsNotNone(health_table)
        self.assertEqual([18, 12, 70], health_table.get("column_width_weights"))
        metadata_lock_table = self._find_table(sections, "元数据锁信息")
        self.assertIsNotNone(metadata_lock_table)
        self.assertTrue(metadata_lock_table.get("field_notes"))

    def _assert_docx_system_metrics(self, document: Document) -> None:
        table = self._find_docx_table(document, "指标", "当前值", "说明")
        self.assertIsNotNone(table, msg="系统指标表未生成到 report.docx")
        value_by_label = {
            row.cells[0].text.strip(): row.cells[1].text.strip()
            for row in table.rows[1:]
            if len(row.cells) >= 2
        }
        for label in SYSTEM_METRIC_LABELS:
            self.assertIn(label, value_by_label, msg=f"系统指标缺少行: {label}")
            self.assertNotEqual("", value_by_label[label], msg=f"系统指标值为空: {label}")

    def _find_table(self, sections: list[dict[str, Any]], table_title: str) -> dict[str, Any] | None:
        for section in sections:
            table = self._find_table_in_section(section, table_title)
            if table:
                return table
        return None

    def _find_table_in_section(self, section: dict[str, Any], table_title: str) -> dict[str, Any] | None:
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

    def _find_docx_table(self, document: Document, *headers: str):
        expected = list(headers)
        for table in document.tables:
            if [cell.text for cell in table.rows[0].cells] == expected:
                return table
        return None


if __name__ == "__main__":
    unittest.main()
