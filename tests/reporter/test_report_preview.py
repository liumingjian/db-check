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
        self.assertIn("| 风险等级 | 风险标识 | 定义 | 建议响应时效 |", markdown)
        self.assertIn("| 检查维度 | 风险标识 | 关键发现 |", markdown)
        self.assertIn("| 风险标识 | 检查维度 | 风险描述 | 影响分析 | 整改建议 |", markdown)
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
        result = {
            "meta": {"db_type": "gaussdb", "db_host": "10.0.0.9", "db_port": 8000, "db_name": "postgres", "collect_time": "2026-03-12T00:30:05+08:00"},
            "os": {
                "system_info": {"hostname": "gauss-host", "os": "linux", "arch": "amd64", "cpu_cores": 8, "file_descriptor_usage_percent": 1.0, "mysql_fd_usage_percent": 0.0},
                "cpu": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 55.0, "user_percent": 20.0, "system_percent": 10.0, "idle_percent": 45.0, "iowait_percent": 1.0, "nice_percent": 0.0}]},
                "memory": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 62.0, "swap_usage_percent": 10.0, "meminfo": {"MemTotal": 17179869184, "SwapTotal": 4294967296, "SwapFree": 3758096384}}]},
                "filesystem": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "mountpoints": [{"mountpoint": "/", "device": "/dev/root", "fstype": "xfs", "usage_percent": 70.0, "inodes_usage_percent": 20.0, "read_only": False}]}]},
                "disk_io": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "total_iops": 123.0, "total_throughput_kbps": 2048.0, "avg_latency_ms": 1.5, "devices": []}]},
                "network": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "total_rate_bytes_per_sec": 2048.0, "total_rx_bytes_per_sec": 1024.0, "total_tx_bytes_per_sec": 1024.0, "error_drop_per_sec": 0.0, "interfaces": []}]},
                "process": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "load_avg_1": 0.9, "running_processes": 2, "blocked_processes": 0, "total_processes": 128, "context_switches": 2048}]},
            },
            "db": {
                "basic_info": {
                    "summary": {"version": "505.2.1.SPC1000", "gauss_user": "Ruby", "gausshome": "/data/cluster/usr/local/core/app", "gausslog": "/data/cluster/var/lib/log/Ruby"},
                    "items": [
                        {"item": "CheckDBConnection", "label": "数据库连接", "normalized_status": "abnormal", "summary": "database connection failed"},
                        {"item": "CheckOMMonitor", "label": "omMonitor 进程", "normalized_status": "normal", "summary": "om_monitor 进程正常，PID=7354", "details": {"pid": "7354"}},
                    ],
                    "visible_count": 2,
                },
                "cluster": {
                    "summary": {
                        "visible_items": [
                            {"item": "CheckClusterState", "label": "集群状态", "normalized_status": "normal", "summary": "cluster ok", "details": {"cluster_state": "Normal", "redistributing": "No", "balanced": "Yes", "nodes": [{"node": "192.168.1.157", "status": "Normal"}]}},
                            {"item": "CheckIntegrity", "label": "数据一致性", "normalized_status": "normal", "summary": "数据一致性检查正常，SHA256=abcd", "details": {"sha256": "abcd"}},
                        ]
                    },
                    "items": [
                        {"item": "CheckClusterState", "label": "集群状态", "normalized_status": "normal", "summary": "cluster ok", "details": {"cluster_state": "Normal", "redistributing": "No", "balanced": "Yes", "nodes": [{"node": "192.168.1.157", "status": "Normal"}]}},
                        {"item": "CheckIntegrity", "label": "数据一致性", "normalized_status": "normal", "summary": "数据一致性检查正常，SHA256=abcd", "details": {"sha256": "abcd"}},
                        {"item": "CheckCatchup", "label": "主备追赶", "normalized_status": "not_applicable", "summary": ""},
                    ],
                    "visible_count": 2,
                },
                "config_check": {
                    "summary": {
                        "checkgucvalue_details": {
                            "max_connections": 1000,
                            "max_prepared_transactions": 1000,
                            "max_locks_per_transaction": 512,
                            "computed_value": 1024000,
                            "configuration_reasonable": True,
                        },
                        "checkgucconsistent_details": {
                            "instance_count": 2,
                            "parameter_count": 1200,
                            "key_parameter_group_count": 2,
                            "key_inconsistent_parameter_count": 1,
                            "key_groups": [
                                {
                                    "title": "内存与连接参数",
                                    "parameters": [
                                        {
                                            "label": "最大连接数",
                                            "representative_value": "400",
                                            "consistent": False,
                                            "instance_values": [
                                                {"instance": "CN_5001", "value": "400"},
                                                {"instance": "DN_6001", "value": "3000"},
                                            ],
                                        }
                                    ],
                                },
                                {
                                    "title": "安全与审计参数",
                                    "parameters": [
                                        {
                                            "label": "SSL 开关",
                                            "representative_value": "on",
                                            "consistent": True,
                                            "instance_values": [
                                                {"instance": "CN_5001", "value": "on"},
                                                {"instance": "DN_6001", "value": "on"},
                                            ],
                                        }
                                    ],
                                },
                            ],
                            "key_inconsistencies": [
                                {
                                    "label": "最大连接数",
                                    "distinct_value_count": 2,
                                    "instance_values": [
                                        {"instance": "CN_5001", "value": "400"},
                                        {"instance": "DN_6001", "value": "3000"},
                                    ],
                                }
                            ],
                        },
                        "visible_items": [
                            {"item": "CheckGUCValue", "label": "GUC 值检查", "normalized_status": "normal", "summary": "锁资源预算值 1024000，guc参数配置合理", "details": {"max_connections": 1000, "max_prepared_transactions": 1000, "max_locks_per_transaction": 512, "computed_value": 1024000, "configuration_reasonable": True}},
                            {"item": "CheckGUCConsistent", "label": "GUC 一致性", "normalized_status": "abnormal", "summary": "已分析 2 类关键参数，发现 1 个关键参数存在差异"},
                            {"item": "CheckDBParams", "label": "数据库参数", "normalized_status": "abnormal", "summary": "parameter drift"},
                        ],
                    },
                    "items": [
                        {"item": "CheckGUCValue", "label": "GUC 值检查", "normalized_status": "normal", "summary": "锁资源预算值 1024000，guc参数配置合理", "details": {"max_connections": 1000, "max_prepared_transactions": 1000, "max_locks_per_transaction": 512, "computed_value": 1024000, "configuration_reasonable": True}},
                        {"item": "CheckGUCConsistent", "label": "GUC 一致性", "normalized_status": "abnormal", "summary": "已分析 2 类关键参数，发现 1 个关键参数存在差异"},
                        {"item": "CheckDBParams", "label": "数据库参数", "normalized_status": "abnormal", "summary": "parameter drift"},
                    ],
                    "visible_count": 3,
                },
                "connection": {"summary": {"visible_items": [{"label": "游标数量", "normalized_status": "abnormal", "summary": "cursor leak"}]}, "items": [{"label": "游标数量", "normalized_status": "abnormal", "summary": "cursor leak"}], "visible_count": 1},
                "storage": {
                    "summary": {"visible_items": [{"item": "CheckSysTable", "label": "系统表检查", "normalized_status": "normal", "summary": "已检查 2 张系统表", "details": {"table_count": 2, "tables": [{"instance": "DN_6001", "table_name": "pg_attribute", "size_bytes": 3022848, "row_count": 19086, "avg_width": 13}, {"instance": "DN_6001", "table_name": "pg_class", "size_bytes": 729088, "row_count": 1616, "avg_width": 9}]}}, {"item": "CheckKeyDBTableSize", "label": "大表检查", "normalized_status": "normal", "summary": "已分析 1 个数据库的大表分布", "details": {"database_count": 1, "table_count": 2, "databases": [{"database": "postgres", "size_value": 18, "size_unit": "GB"}], "tables": [{"table_name": "public.orders", "size_value": 8, "size_unit": "GB"}, {"table_name": "public.customer", "size_value": 4, "size_unit": "GB"}]}}]},
                    "items": [
                        {"item": "CheckSysTable", "label": "系统表检查", "normalized_status": "normal", "summary": "已检查 2 张系统表", "details": {"table_count": 2, "tables": [{"instance": "DN_6001", "table_name": "pg_attribute", "size_bytes": 3022848, "row_count": 19086, "avg_width": 13}, {"instance": "DN_6001", "table_name": "pg_class", "size_bytes": 729088, "row_count": 1616, "avg_width": 9}]}},
                        {"item": "CheckKeyDBTableSize", "label": "大表检查", "normalized_status": "normal", "summary": "已分析 1 个数据库的大表分布", "details": {"database_count": 1, "table_count": 2, "databases": [{"database": "postgres", "size_value": 18, "size_unit": "GB"}], "tables": [{"table_name": "public.orders", "size_value": 8, "size_unit": "GB"}, {"table_name": "public.customer", "size_value": 4, "size_unit": "GB"}]}},
                    ],
                    "visible_count": 2,
                },
                "performance": {
                    "summary": {"visible_items": [{"item": "CheckErrorInLog", "label": "运行日志", "normalized_status": "abnormal", "summary": "最近日志 ERROR 数量 3", "details": {"error_count": 3, "sample_lines": ["2026-03-12 ERROR sample-1", "2026-03-12 ERROR sample-2"]}}]},
                    "items": [{"item": "CheckErrorInLog", "label": "运行日志", "normalized_status": "abnormal", "summary": "最近日志 ERROR 数量 3", "details": {"error_count": 3, "sample_lines": ["2026-03-12 ERROR sample-1", "2026-03-12 ERROR sample-2"]}}],
                    "visible_count": 1,
                },
                "transactions": {"summary": {"visible_items": [{"label": "锁数量", "normalized_status": "abnormal", "summary": "lock hotspot"}]}, "items": [{"label": "锁数量", "normalized_status": "abnormal", "summary": "lock hotspot"}], "visible_count": 1},
                "sql_analysis": {
                    "summary": {"no_index_table_count": 0, "no_primary_key_table_count": 2, "no_statistics_table_count": 7},
                    "items": [{"item": "CheckReturnType", "label": "自定义函数", "normalized_status": "normal", "summary": "用户定义函数不包含非法返回类型"}],
                    "visible_count": 1,
                    "no_index_summary": {"items": []},
                    "no_primary_key_summary": {"items": [{"owner": "app", "total_table_count": 100, "no_pk_count": 2, "percentage": 2.0}]},
                    "no_primary_key_detail": {"items": [{"owner": "app", "table_name": "order_log"}, {"owner": "app", "table_name": "audit_log"}]},
                    "no_statistics_summary": {"items": [{"tableowner": "rdsAdmin", "total_table_count": 239, "table_no_stat": 7, "percentage": 2.9288}]},
                    "no_statistics_detail": {"items": [{"schemaname": "snapshot", "tableowner": "rdsAdmin", "tablename": "snap_pdb_info"}]},
                },
            },
        }
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
        self.assertEqual(db_titles, ["2.2.1 基础可用性", "2.2.2 参数与配置", "2.2.3 对象与结构健康", "2.2.4 容量与数据治理", "2.2.5 运行日志"])
        config_tables = [table.title for table in report.sections[2].children[1].children[1].tables]
        self.assertEqual(config_tables, ["参数值检查", "参数一致性摘要", "内存与连接参数", "安全与审计参数", "参数差异明细", "参数与配置结论"])
        self.assertIn("gaussdb巡检报告", meta["change_log"][0]["change"])
        self.assertEqual("Cluster", meta["scope"]["architecture_role"])
        self.assertEqual("/data/cluster/usr/local/core/app", meta["scope"]["data_dir"])
        self.assertNotIn("主备追赶", markdown)

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
            conclusion_table = self._find_table(payload["sections"], "巡检结论摘要")
            self.assertIsNotNone(conclusion_table)
            self.assertEqual([20, 80], conclusion_table["column_width_weights"])
            self.assertTrue(any("**" in row[1] for row in conclusion_table["rows"]))
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
