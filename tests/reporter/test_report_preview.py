from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from reporter.cli.generate_report_meta import MetaOptions, build_report_meta
from reporter.cli.db_report_preview import run
from reporter.content.report_builder import build_report_view
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
        self.assertIn("### 2.1 系统指标", markdown)
        self.assertIn("#### 2.1.3 内存明细", markdown)
        self.assertIn("#### 2.1.5 磁盘IO明细", markdown)
        self.assertIn("### 2.2 MySQL基础信息", markdown)
        self.assertIn("| 风险等级 | 定义 | 建议响应时效 |", markdown)
        self.assertIn("| 检查维度 | 风险等级 | 关键发现 |", markdown)
        self.assertIn("| 风险等级 | 检查维度 | 风险描述 | 影响分析 | 整改建议 |", markdown)
        self.assertNotIn("风险标识", markdown)
        self.assertIn("巡检结论摘要", markdown)
        self.assertIn("**中风险**", markdown)
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
            "os": {"cpu": {"samples": []}, "memory": {"samples": []}, "filesystem": {"samples": []}, "system_info": {}},
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
        options = MetaOptions(database_version="8.0", db_type="mysql", document_name="report.docx", change_description="mysql巡检报告")

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
            MetaOptions(database_version="8.0", db_type="mysql", document_name="mysql巡检报告.docx"),
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

        meta = build_report_meta(result, summary, MetaOptions(database_version="8.0", db_type="mysql", data_dir="/custom/mysql"))

        self.assertEqual("/custom/mysql", meta["scope"]["data_dir"])

    def test_build_report_view_supports_gaussdb(self) -> None:
        result = _gauss_sql_first_result()
        summary = {
            "generated_at": "2026-03-12T00:30:05+08:00",
            "overall_risk": "high",
            "counts": {"total_checks": 21, "normal": 10, "warning": 6, "critical": 3, "unevaluated": 0, "not_applicable": 2},
            "abnormal_items": [{"check_id": "2.1", "name": "数据库连接状态", "dimension_name": "基础连通性", "level": "critical", "current_value": "abnormal", "reason": "critical threshold hit", "advice": "立即排查数据库连接可用性与本地环境"}],
            "na_items": [{"check_id": "8.2", "reason": "当前环境未返回主备追赶检查结果"}],
        }
        meta = build_report_meta(result, summary, MetaOptions(database_version="505.2.1.SPC1000", db_type="gaussdb", document_name="gaussdb-report.docx"))

        report = build_report_view(result, summary, meta)
        markdown = render_markdown_preview(report)

        self.assertEqual(report.sections[1].title, "第一章 巡检总结")
        self.assertEqual([item.title for item in report.sections[1].children], ["1.1 巡检告警定义", "1.2 巡检范围", "1.3 综合健康评估", "1.4 风险发现与整改建议", "1.5 巡检结论"])
        self.assertEqual([item.title for item in report.sections[2].children], ["2.1 系统指标", "2.2 数据库指标"])
        db_titles = [item.title for item in report.sections[2].children[1].children]
        self.assertEqual(db_titles, ["2.2.1 数据库概览", "2.2.2 参数配置", "2.2.3 存储容量", "2.2.4 连接会话", "2.2.5 事务与锁", "2.2.6 性能统计", "2.2.7 SQL 分析"])
        config_tables = [table.title for table in report.sections[2].children[1].children[1].tables]
        self.assertEqual(config_tables, ["参数值检查", "核心参数快照", "参数与配置结论"])
        self.assertIn("gaussdb巡检报告", meta["change_log"][0]["change"])
        self.assertEqual("Cluster", meta["scope"]["architecture_role"])
        self.assertNotIn("gs_check", markdown)
        self.assertNotIn("omMonitor", markdown)
        self.assertNotIn("运行日志", markdown)
        self.assertNotIn("数据一致性", markdown)

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
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertIn("sections", payload)
            alarm_table = self._find_table(payload["sections"], "巡检告警定义")
            self.assertIsNotNone(alarm_table)
            self.assertEqual([12, 66, 22], alarm_table["column_width_weights"])
            health_table = self._find_table(payload["sections"], "综合健康评估")
            self.assertIsNotNone(health_table)
            self.assertEqual([18, 12, 70], health_table["column_width_weights"])
            conclusion_table = self._find_table(payload["sections"], "巡检结论摘要")
            self.assertIsNotNone(conclusion_table)
            self.assertEqual([20, 80], conclusion_table["column_width_weights"])
            self.assertTrue(any("**" in row[1] for row in conclusion_table["rows"]))
            metadata_lock_table = self._find_table(payload["sections"], "元数据锁信息")
            self.assertIsNotNone(metadata_lock_table)
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


def _gauss_sql_first_result() -> dict:
    return {
        "meta": {"db_type": "gaussdb", "db_host": "10.0.0.9", "db_port": 8000, "db_name": "postgres", "collect_time": "2026-03-12T00:30:05+08:00"},
        "os": _gauss_os_payload(),
        "db": _gauss_db_payload(),
    }

def _gauss_os_payload() -> dict:
    return {
        "system_info": {"hostname": "gauss-host", "os": "linux", "arch": "amd64", "cpu_cores": 8, "file_descriptor_usage_percent": 1.0, "mysql_fd_usage_percent": 0.0},
        "cpu": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 55.0, "user_percent": 20.0, "system_percent": 10.0, "idle_percent": 45.0, "iowait_percent": 1.0, "nice_percent": 0.0}]},
        "memory": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 62.0, "swap_usage_percent": 10.0, "meminfo": {"MemTotal": 17179869184, "SwapTotal": 4294967296, "SwapFree": 3758096384}}]},
        "filesystem": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "mountpoints": [{"mountpoint": "/", "device": "/dev/root", "fstype": "xfs", "usage_percent": 70.0, "inodes_usage_percent": 20.0, "read_only": False}]}]},
        "disk_io": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "total_iops": 123.0, "total_throughput_kbps": 2048.0, "avg_latency_ms": 1.5, "devices": []}]},
        "network": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "total_rate_bytes_per_sec": 2048.0, "total_rx_bytes_per_sec": 1024.0, "total_tx_bytes_per_sec": 1024.0, "error_drop_per_sec": 0.0, "interfaces": []}]},
        "process": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "load_avg_1": 0.9, "running_processes": 2, "blocked_processes": 0, "total_processes": 128, "context_switches": 2048}]},
    }

def _gauss_db_payload() -> dict:
    return {
        "basic_info": _domain([_item("CheckDBConnection", "数据库连接", "abnormal", "database connection failed"), _item("CheckGaussVer", "GaussDB 版本", "normal", "version ok")], {"version": "505.2.1.SPC1000", "checkdbconnection_status": "abnormal", "checkgaussver_status": "normal", "pguser": "root"}),
        "cluster": _domain([_item("CheckClusterState", "集群状态", "normal", "cluster ok", {"rows": [{"node_name": "dn_6001", "node_type": "D", "node_host": "10.0.0.9", "node_port": 8000}]}), _item("CheckReadonlyMode", "只读模式", "normal", "off", {"readonly": "off"})], {"checkclusterstate_status": "normal", "checkreadonlymode_status": "normal"}),
        "config_check": _domain([_item("CheckGUCValue", "GUC 值检查", "normal", "锁资源预算值 1024000", {"max_connections": 1000, "max_prepared_transactions": 1000, "max_locks_per_transaction": 512, "computed_value": 1024000}), _item("CheckDBParams", "数据库参数", "abnormal", "parameter drift", {"rows": [{"name": "max_connections", "setting": "1000"}, {"name": "work_mem", "setting": "16384"}]})], {"checkdbparams_status": "abnormal", "checkgucvalue_status": "normal"}),
        "connection": _domain([_item("CheckCurConnCount", "当前连接数", "normal", "10/100 connections", {"current_connections": 10, "max_connections": 100, "usage_percent": 10.0}), _item("CheckCursorNum", "游标数量", "abnormal", "cursor leak")], {"checkcurconncount_status": "normal", "checkcursornum_status": "abnormal"}),
        "storage": _domain([_item("CheckTableSpace", "表空间", "normal", "tablespace collected", {"rows": [{"tablespace": "pg_default", "location": ""}]}), _item("CheckSysTable", "系统表检查", "normal", "system tables", {"tables": [{"schema": "pg_catalog", "table_name": "pg_class", "pages": 85, "rows": 1616}]}), _item("CheckKeyDBTableSize", "大表检查", "normal", "database size", {"rows": [{"database": "postgres", "size_bytes": 1024}]})], {"checktablespace_status": "normal", "checksystable_status": "normal", "checkkeydbtablesize_status": "normal"}),
        "transactions": _domain([_item("CheckLockNum", "锁数量", "abnormal", "lock hotspot"), _item("CheckIdleSession", "空闲会话", "normal", "ok")], {"checklocknum_status": "abnormal", "checkidlesession_status": "normal"}),
        "performance": _domain([_item("CheckDBStat", "数据库运行状态", "normal", "db stat", {"rows": [{"database": "postgres", "numbackends": 10, "xact_commit": 100, "xact_rollback": 1, "deadlocks": 0}]}), _item("CheckBPHitRatio", "Buffer 命中率", "normal", "hit ratio", {"rows": [{"database": "postgres", "hit_ratio": "0.99"}]})], {"checkdbstat_status": "normal", "checkbphitratio_status": "normal"}),
        "sql_analysis": _sql_analysis_payload(),
        "security": {"summary": {}, "items": [], "count": 0, "visible_count": 0},
        "sql_raw_index": {"count": 2, "items": [{"domain": "basic_info", "item": "CheckDBConnection", "label": "数据库连接", "row_count": 1, "result_file": "sql/CheckDBConnection.json"}]},
    }

def _sql_analysis_payload() -> dict:
    payload = _domain([_item("CheckReturnType", "自定义函数", "normal", "ok")], {"checkreturntype_status": "normal", "no_index_table_count": 0, "no_primary_key_table_count": 2, "no_statistics_table_count": 7})
    payload.update({
        "no_index_summary": {"items": []},
        "no_primary_key_summary": {"items": [{"owner": "app", "total_table_count": 100, "no_pk_count": 2, "percentage": 2.0}]},
        "no_primary_key_detail": {"items": [{"owner": "app", "table_name": "order_log"}]},
        "no_statistics_summary": {"items": [{"tableowner": "rdsAdmin", "total_table_count": 239, "table_no_stat": 7, "percentage": 2.9288}]},
        "no_statistics_detail": {"items": [{"schemaname": "snapshot", "tableowner": "rdsAdmin", "tablename": "snap_pdb_info"}]},
    })
    return payload


def _domain(items: list[dict], summary: dict) -> dict:
    summary = {**summary, "visible_items": items}
    return {"summary": summary, "items": items, "count": len(items), "visible_count": len(items)}


def _item(name: str, label: str, status: str, summary: str, details: dict | None = None) -> dict:
    return {"item": name, "label": label, "normalized_status": status, "summary": summary, "details": details or {}}


if __name__ == "__main__":
    unittest.main()
