from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path
import json

from reporter.cli.reporter_orchestrator import run

ROOT = Path(__file__).resolve().parents[2]


class ReporterOrchestratorTests(unittest.TestCase):
    def test_run_dir_generates_oracle_report(self) -> None:
        run_dir = self._prepare_oracle_run_dir()
        try:
            code = run(
                [
                    "--run-dir",
                    str(run_dir),
                    "--rule-file",
                    str(ROOT / "rules" / "oracle" / "rule.json"),
                    "--template-file",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "report-meta.json").exists())
            self.assertTrue((run_dir / "report-view.json").exists())
            self.assertTrue((run_dir / "report.docx").exists())
            report_view = json.loads((run_dir / "report-view.json").read_text(encoding="utf-8"))
            self.assertEqual(report_view["title"], "report.docx")
            summary_titles = [item["title"] for item in report_view["sections"][1]["children"]]
            self.assertEqual(
                summary_titles,
                ["1.1 巡检告警定义", "1.2 巡检范围", "1.3 综合健康评估", "1.4 风险发现与整改建议", "1.5 巡检结论"],
            )
            self.assertEqual(report_view["sections"][1]["title"], "第一章 巡检总结")
            conclusion_table = report_view["sections"][1]["children"][4]["tables"][0]
            self.assertEqual(conclusion_table["title"], "巡检结论摘要")
            self.assertEqual(conclusion_table["column_width_weights"], [20, 80])
            self.assertTrue(any("**" in row[1] for row in conclusion_table["rows"]))
            detail_titles = [item["title"] for item in report_view["sections"][2]["children"]]
            self.assertEqual(detail_titles, ["2.1 系统指标", "2.2 数据库指标"])
            system_section = report_view["sections"][2]["children"][0]
            system_child_titles = [item["title"] for item in system_section["children"]]
            self.assertEqual(
                system_child_titles,
                [
                    "2.1.1 CPU与调度",
                    "2.1.2 系统与进程",
                    "2.1.3 内存明细",
                    "2.1.4 文件系统明细",
                    "2.1.5 磁盘IO明细",
                    "2.1.6 网络接口明细",
                ],
            )
            db_child_titles = [item["title"] for item in report_view["sections"][2]["children"][1]["children"]]
            self.assertEqual(
                db_child_titles,
                [
                    "2.2.1 基础与配置",
                    "2.2.2 存储与日志",
                    "2.2.3 性能与会话",
                    "2.2.4 SQL 分析",
                    "2.2.5 安全与对象健康",
                    "2.2.6 备份与可恢复性",
                ],
            )
            system_rows = system_section["tables"][0]["rows"]
            labels = {row[0]: row[1] for row in system_rows}
            self.assertEqual(labels["Swap 使用率"], "12.50%")
            self.assertEqual(labels["运行队列"], "3")
            self.assertNotIn("数据库进程 fd 使用率", labels)
            filesystem_table = system_section["children"][3]["tables"][0]
            self.assertEqual(filesystem_table["column_width_weights"], [10, 15, 10, 13, 12, 12, 10, 10, 8])
            storage_section = report_view["sections"][2]["children"][1]["children"][1]
            storage_table_titles = [table["title"] for table in storage_section["tables"]]
            self.assertEqual(
                storage_table_titles,
                ["存储摘要", "表空间使用情况", "数据文件明细", "控制文件明细", "Redo日志明细", "待恢复数据文件", "表碎片分析", "无效对象", "不可用索引"],
            )
            for table in storage_section["tables"]:
                self.assertTrue(table["column_width_weights"], msg=f"missing widths for {table['title']}")
                self.assertEqual(len(table["column_width_weights"]), len(table["columns"]), msg=f"invalid widths for {table['title']}")
            performance_section = report_view["sections"][2]["children"][1]["children"][2]
            performance_titles = [table["title"] for table in performance_section["tables"]]
            self.assertEqual(
                performance_titles,
                [
                    "性能指标摘要",
                    "核心性能指标",
                    "实例效率",
                    "活跃会话明细",
                    "活跃会话概览",
                    "长事务",
                    "阻塞链",
                    "资源限制",
                    "Redo切换频率",
                    "表空间IO统计",
                    "Top等待事件",
                    "Latch统计",
                    "Time Model",
                    "UNDO表空间使用情况",
                    "UNDO统计",
                    "SGA Resize历史",
                ],
            )
            sql_section = report_view["sections"][2]["children"][1]["children"][3]
            sql_table_titles = [table["title"] for table in sql_section["tables"]]
            self.assertEqual(
                sql_table_titles,
                ["SQL 指标摘要", "Top SQL（按耗时）", "Top SQL（按逻辑读）", "Top SQL（按物理读）", "Top SQL（按执行次数）", "高解析SQL", "高版本SQL"],
            )
            backup_section = report_view["sections"][2]["children"][1]["children"][5]
            backup_titles = [table["title"] for table in backup_section["tables"]]
            self.assertEqual(
                backup_titles,
                ["备份摘要", "最近备份记录", "归档目的地", "归档目的地异常", "归档日志摘要", "恢复区使用情况"],
            )
            for table in _collect_tables(report_view["sections"]):
                if not table["title"]:
                    continue
                self.assertEqual(len(table["column_width_weights"]), len(table["columns"]), msg=f"width/column mismatch for {table['title']}")
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def test_run_dir_generates_docx_without_markdown_by_default(self) -> None:
        run_dir = self._prepare_run_dir()
        try:
            code = run(
                [
                    "--run-dir",
                    str(run_dir),
                    "--rule-file",
                    str(ROOT / "rules" / "mysql" / "rule.json"),
                    "--template-file",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "report-meta.json").exists())
            self.assertTrue((run_dir / "report-view.json").exists())
            self.assertTrue((run_dir / "report.docx").exists())
            self.assertFalse((run_dir / "report.md").exists())
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def test_run_dir_generates_markdown_when_requested(self) -> None:
        run_dir = self._prepare_run_dir()
        try:
            code = run(
                [
                    "--run-dir",
                    str(run_dir),
                    "--rule-file",
                    str(ROOT / "rules" / "mysql" / "rule.json"),
                    "--template-file",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                    "--out-md",
                    str(run_dir / "report.md"),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue((run_dir / "report.md").exists())
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def test_run_dir_detects_version_from_basic_info_version_vars(self) -> None:
        run_dir = self._prepare_run_dir()
        try:
            result_path = run_dir / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result["db"]["basic_info"].pop("version", None)
            result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            code = run(
                [
                    "--run-dir",
                    str(run_dir),
                    "--rule-file",
                    str(ROOT / "rules" / "mysql" / "rule.json"),
                    "--template-file",
                    str(ROOT / "reporter" / "templates" / "mysql-template.docx"),
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue((run_dir / "report.docx").exists())
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def _prepare_run_dir(self) -> Path:
        run_dir = Path(tempfile.mkdtemp())
        shutil.copy(ROOT / "contracts" / "manifest.sample.json", run_dir / "manifest.json")
        shutil.copy(ROOT / "contracts" / "result.sample.json", run_dir / "result.json")
        return run_dir

    def _prepare_oracle_run_dir(self) -> Path:
        run_dir = Path(tempfile.mkdtemp())
        manifest = {
            "schema_version": "1.0",
            "run_id": "oracle-10.0.0.8-20260312T004000Z",
            "db_type": "oracle",
            "start_time": "2026-03-12T00:40:00+08:00",
            "end_time": "2026-03-12T00:40:05+08:00",
            "exit_code": 0,
            "overall_status": "success",
            "module_stats": {
                "os": {"status": "success", "duration_ms": 10, "error": None},
                "db_basic": {"status": "success", "duration_ms": 10, "error": None},
                "db_storage": {"status": "success", "duration_ms": 10, "error": None},
                "db_perf": {"status": "success", "duration_ms": 10, "error": None},
                "db_security": {"status": "success", "duration_ms": 10, "error": None},
                "db_backup": {"status": "success", "duration_ms": 10, "error": None},
                "db_sql": {"status": "success", "duration_ms": 10, "error": None},
            },
            "artifacts": {"log": "collector.log", "result": "result.json", "summary": None, "report": None},
        }
        result = {
            "meta": {
                "schema_version": "2.0",
                "collector_version": "1.0.0",
                "db_type": "oracle",
                "db_host": "10.0.0.8",
                "db_port": 1521,
                "db_name": "ORCL",
                "collect_mode": "remote",
                "collect_time": "2026-03-12T00:40:05+08:00",
                "collect_duration_seconds": 5,
                "os_sample_count": 1,
                "timezone": "Asia/Shanghai",
            },
            "collect_config": {"sample_mode": "single", "sample_interval_seconds": None, "sample_period_seconds": None, "expected_samples": 1},
            "collect_window": {"window_start": "2026-03-12T00:40:00+08:00", "window_end": "2026-03-12T00:40:05+08:00", "duration_seconds": 5},
            "os": {
                "system_info": {"hostname": "oracle-host", "os": "linux", "arch": "amd64", "cpu_cores": 8, "file_descriptor_usage_percent": 1.2, "mysql_fd_usage_percent": 0.0},
                "cpu": {"samples": [{"timestamp": "2026-03-12T00:40:05+08:00", "usage_percent": 42.0, "user_percent": 20.0, "system_percent": 8.0, "idle_percent": 58.0, "iowait_percent": 1.0, "nice_percent": 0.5}]},
                "memory": {"samples": [{"timestamp": "2026-03-12T00:40:05+08:00", "usage_percent": 68.0, "swap_usage_percent": 12.5, "meminfo": {"MemTotal": 17179869184, "MemAvailable": 8589934592, "Buffers": 536870912, "Cached": 2147483648, "SwapTotal": 4294967296, "SwapFree": 3758096384, "HugePagesTotal": 0, "HugePagesFree": 0, "HugePageSize": 2097152}}]},
                "filesystem": {"samples": [{"timestamp": "2026-03-12T00:40:05+08:00", "mountpoints": [{"mountpoint": "/", "device": "/dev/mapper/root", "fstype": "xfs", "usage_percent": 71.0, "total_bytes": 214748364800, "used_bytes": 152900835328, "free_bytes": 61847529472, "inodes_usage_percent": 33.3, "read_only": False}]}]},
                "disk_io": {"samples": [{"timestamp": "2026-03-12T00:40:05+08:00", "total_iops": 188.5, "total_throughput_kbps": 4096.0, "avg_latency_ms": 1.25, "devices": [{"name": "sda", "throughput_kbps": 4096.0, "avg_latency_ms": 1.25}]}]},
                "network": {"samples": [{"timestamp": "2026-03-12T00:40:05+08:00", "total_rx_bytes_per_sec": 2048.0, "total_tx_bytes_per_sec": 1024.0, "total_rate_bytes_per_sec": 3072.0, "error_drop_per_sec": 0.5, "interfaces": [{"name": "eth0", "rx_bytes_per_sec": 2048.0, "tx_bytes_per_sec": 1024.0, "error_drop_per_sec": 0.5, "bytes_recv": 10485760, "bytes_sent": 5242880}]}]},
                "process": {"samples": [{"timestamp": "2026-03-12T00:40:05+08:00", "load_avg_1": 1.75, "running_processes": 3, "blocked_processes": 1, "total_processes": 245, "context_switches": 1024}]},
            },
                "db": {
                "basic_info": {"db_name": "ORCL", "instance_name": "orcl1", "version": "19.3.0.0.0", "dbid": 123456, "is_rac": True, "character_set": "AL32UTF8", "log_mode": "ARCHIVELOG"},
                "config_check": {"spfile": "/u01/app/oracle/product/19c/dbs/spfileORCL.ora"},
                "storage": {
                    "datafiles": {"items": [{"file_name": "/u02/oradata/system01.dbf"}], "count": 1},
                    "tablespace_usage": {"items": [{"tablespace_name": "USERS", "total_size_gb": 10, "used_size_gb": 8.5, "real_percent": 85.0}], "count": 1}
                },
                "backup": {
                    "archive_log_mode": "ARCHIVELOG",
                    "jobs": {"items": [{"session_key": 1, "input_type": "DB FULL", "status": "COMPLETED", "start_time": "2026-03-11T01:00:00+08:00", "end_time": "2026-03-11T02:00:00+08:00", "hours": 1.0}], "count": 1},
                    "archive_destinations": {"items": [{"dest_name": "LOG_ARCHIVE_DEST_1", "status": "VALID", "destination": "/arch", "target": "PRIMARY", "archiver": "ARCH", "error": ""}], "count": 1},
                    "archive_destination_errors": {"items": [{"dest_name": "LOG_ARCHIVE_DEST_2", "status": "ERROR", "destination": "/arch2", "error": "ORA-16038"}], "count": 1},
                    "archive_log_summary": {"items": [{"archive_count": 512, "archive_size_gb": 24.5, "oldest_archive_time": "2026-03-10 00:00:00", "newest_archive_time": "2026-03-12 00:30:00"}], "count": 1},
                    "recovery_area": {"items": [{"name": "/fra", "space_limit_gb": 300.0, "space_used_gb": 180.0, "space_reclaimable_gb": 20.0, "space_used_pct": 60.0, "number_of_files": 220}], "count": 1},
                },
                "performance": {
                    "metric_overview": {"items": [{"metric_name": "Host CPU Utilization (%)", "average_value": 45.2, "metric_unit": "%"}], "count": 1},
                    "active_sessions": {"items": [{"inst_id": 1, "active_sessions": 8}], "count": 1},
                    "active_session_details": {"items": [{"sid": 11, "serial": 12, "username": "SYSTEM", "sql_id": "1abcd", "event": "db file sequential read", "seconds_in_wait": 5}], "count": 1},
                    "long_transactions": {"items": [{"sid": 21, "serial": 4, "username": "APP", "sql_id": "2bcde", "event": "log file sync", "start_time": "2026-03-12 00:10:00", "duration_minutes": 25.5}], "count": 1},
                    "blocking_chains": {"items": [{"waiter_sid": 31, "waiter_username": "APP", "blocker_sid": 32, "blocker_username": "APP2", "wait_event": "enq: TX - row lock contention", "seconds_in_wait": 19}], "count": 1},
                    "resource_limits": {"items": [{"inst_id": 1, "resource_name": "processes", "current_utilization": 180, "max_utilization": 220, "limit_value": 500}], "count": 1},
                    "instance_efficiency": {"items": [{"db_block_gets": 2048, "consistent_gets": 8096, "db_block_reads_pct": 3.1, "db_block_writes_pct": 16.7}], "count": 1},
                    "redo_switch_daily": {"items": [{"switch_count": 12, "switch_date": "2026-03-12"}], "count": 1},
                    "tablespace_io_stats": {"items": [{"tablespace_name": "USERS", "file_name": "/u02/oradata/users01.dbf", "phyrds": 100, "phyblkrd": 200, "phywrts": 80, "phyblkwrt": 160}], "count": 1},
                    "wait_events": {"items": [{"event": "db file sequential read", "waits": 600, "waited_ms": 24000, "avg_wait_ms": 40}], "count": 1},
                    "latch_data": {"items": [{"name": "cache buffers chains", "gets": 100000, "misses": 12, "sleeps": 2}], "count": 1},
                    "time_model": {"items": [{"stat_name": "DB CPU", "seconds": 360.5}], "count": 1},
                    "undo_tablespace_usage": {"items": [{"tablespace_name": "UNDOTBS1", "total_size_gb": 80.0, "used_size_gb": 42.0, "active_size_gb": 11.0, "unexpired_size_gb": 19.0, "expired_size_gb": 12.0, "usage_percent": 52.5}], "count": 1},
                    "undo_stats": {"items": [{"begin_time": "2026-03-12 00:00:00", "end_time": "2026-03-12 00:10:00", "txncount": 320, "maxquerylen": 120, "ssolderrcnt": 1, "nospaceerrcnt": 0}], "count": 1},
                    "sga_resize_ops": {"items": [{"component": "DEFAULT buffer cache", "oper_type": "GROW", "oper_mode": "AUTO", "initial_size_mb": 4096, "target_size_mb": 5120, "final_size_mb": 5120, "start_time": "2026-03-11 23:00:00", "status": "COMPLETE"}], "count": 1},
                    "redo_nowait_pct": 99.7,
                },
                "sql_analysis": {
                    "top_sql_by_elapsed_time": {"items": [{"sql_text": "select * from dual", "elapsed_time_sec": 6.1, "cpu_time_sec": 2.3, "executions": 10}], "count": 1},
                    "top_sql_by_buffer_gets": {"items": [{"sql_text": "select * from orders", "buffer_gets": 10000, "disk_reads": 120, "executions": 20}], "count": 1},
                    "top_sql_by_disk_reads": {"items": [{"sql_text": "select * from customer", "disk_reads": 500, "buffer_gets": 2000, "executions": 15}], "count": 1},
                    "top_sql_by_executions": {"items": [{"sql_text": "select 1 from dual", "executions": 1000, "buffer_gets": 100, "disk_reads": 1}], "count": 1},
                    "high_parse_count_sql": {"items": [{"sql_id": "1abcd", "executions": 12000, "parse_calls": 11800, "version_count": 2, "sharable_mem": 8192, "sql_text": "select * from orders where id=:1"}], "count": 1},
                    "high_version_count_sql": {"items": [{"sql_id": "2bcde", "version_count": 88, "executions": 3000, "parse_calls": 600, "sharable_mem": 16384, "sql_text": "select * from customer where code=:1"}], "count": 1},
                    "sql_with_executions_ratio_pct": 91.5,
                    "memory_for_sql_with_executions_ratio_pct": 86.0,
                },
                "security": {
                    "disabled_constraints": {"items": [], "count": 0},
                    "disabled_triggers": {"items": [], "count": 0},
                    "dba_role_users": {"items": [{"grantee": "SYSTEM"}], "count": 1},
                    "expired_users": {"items": [], "count": 0},
                    "table_degree_gt_one": {"items": [], "count": 0},
                    "indexes_degree_gt_one": {"items": [], "count": 0}
                }
            }
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        (run_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return run_dir


def _collect_tables(sections: list[dict]) -> list[dict]:
    tables: list[dict] = []
    for section in sections:
        tables.extend(section.get("tables", []))
        tables.extend(_collect_tables(section.get("children", [])))
    return tables


if __name__ == "__main__":
    unittest.main()
