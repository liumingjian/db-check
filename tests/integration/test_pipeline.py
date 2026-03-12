from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[2]


class PipelineIntegrationTests(unittest.TestCase):
    def test_db_reporter_generates_gaussdb_artifacts_from_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env = dict(os.environ)
            env["GOCACHE"] = "/tmp/go-cache"
            run_dir = temp_path / "gauss-run"
            run_dir.mkdir()
            (run_dir / "manifest.json").write_text(json.dumps(_gauss_manifest(), ensure_ascii=False, indent=2), encoding="utf-8")
            (run_dir / "result.json").write_text(json.dumps(_gauss_result(), ensure_ascii=False, indent=2), encoding="utf-8")
            command = [
                "go",
                "run",
                str(ROOT / "reporter" / "cmd" / "db-reporter"),
                "--run-dir",
                str(run_dir),
                "--out-md",
                str(run_dir / "report.md"),
            ]
            completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "report-meta.json").exists())
            self.assertTrue((run_dir / "report-view.json").exists())
            self.assertTrue((run_dir / "report.docx").exists())
            report_meta = json.loads((run_dir / "report-meta.json").read_text(encoding="utf-8"))
            self.assertEqual("gaussdb巡检报告", report_meta["change_log"][0]["change"])
            report_view = json.loads((run_dir / "report-view.json").read_text(encoding="utf-8"))
            self.assertEqual(report_view["sections"][1]["title"], "第一章 巡检总结")
            db_children = [item["title"] for item in report_view["sections"][2]["children"][1]["children"]]
            self.assertEqual(
                db_children,
                ["2.2.1 基础可用性", "2.2.2 参数与配置", "2.2.3 对象与结构健康", "2.2.4 容量与数据治理", "2.2.5 运行日志"],
            )
            config_tables = [table["title"] for table in report_view["sections"][2]["children"][1]["children"][1]["tables"]]
            self.assertEqual(config_tables, ["参数值检查", "参数一致性摘要", "内存与连接参数", "安全与审计参数", "参数差异明细", "参数与配置结论"])

    def test_db_reporter_generates_oracle_artifacts_from_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env = dict(os.environ)
            env["GOCACHE"] = "/tmp/go-cache"
            run_dir = temp_path / "oracle-run"
            run_dir.mkdir()
            (run_dir / "manifest.json").write_text(json.dumps(_oracle_manifest(), ensure_ascii=False, indent=2), encoding="utf-8")
            (run_dir / "result.json").write_text(json.dumps(_oracle_result(), ensure_ascii=False, indent=2), encoding="utf-8")
            command = [
                "go",
                "run",
                str(ROOT / "reporter" / "cmd" / "db-reporter"),
                "--run-dir",
                str(run_dir),
                "--out-md",
                str(run_dir / "report.md"),
            ]
            completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "report-meta.json").exists())
            self.assertTrue((run_dir / "report-view.json").exists())
            self.assertTrue((run_dir / "report.md").exists())
            self.assertTrue((run_dir / "report.docx").exists())
            report_meta = json.loads((run_dir / "report-meta.json").read_text(encoding="utf-8"))
            self.assertEqual("oracle巡检报告", report_meta["change_log"][0]["change"])
            report_view = json.loads((run_dir / "report-view.json").read_text(encoding="utf-8"))
            self.assertEqual(
                [item["title"] for item in report_view["sections"][1]["children"]],
                ["1.1 巡检告警定义", "1.2 巡检范围", "1.3 综合健康评估", "1.4 风险发现与整改建议", "1.5 巡检结论"],
            )
            conclusion_table = report_view["sections"][1]["children"][4]["tables"][0]
            self.assertEqual(conclusion_table["title"], "巡检结论摘要")
            self.assertTrue(any("**" in row[1] for row in conclusion_table["rows"]))
            db_tables = report_view["sections"][2]["children"][1]["children"][2]["tables"]
            db_table_titles = [table["title"] for table in db_tables]
            self.assertIn("核心性能指标", db_table_titles)
            self.assertIn("UNDO表空间使用情况", db_table_titles)
            self.assertIn("SGA Resize历史", db_table_titles)

    def test_db_reporter_generates_formal_artifacts_from_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env = dict(os.environ)
            env["GOCACHE"] = "/tmp/go-cache"
            run_dir = temp_path / "sample-run"
            run_dir.mkdir()
            (run_dir / "manifest.json").write_text((ROOT / "contracts" / "manifest.sample.json").read_text(encoding="utf-8"), encoding="utf-8")
            (run_dir / "result.json").write_text((ROOT / "contracts" / "result.sample.json").read_text(encoding="utf-8"), encoding="utf-8")
            command = [
                "go",
                "run",
                str(ROOT / "reporter" / "cmd" / "db-reporter"),
                "--run-dir",
                str(run_dir),
                "--out-md",
                str(run_dir / "report.md"),
            ]
            completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "report-meta.json").exists())
            self.assertTrue((run_dir / "report-view.json").exists())
            self.assertTrue((run_dir / "report.md").exists())
            self.assertTrue((run_dir / "report.docx").exists())

    def test_collector_os_only_creates_run_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env = dict(os.environ)
            env["GOCACHE"] = "/tmp/go-cache"
            command = [
                "go",
                "run",
                str(ROOT / "collector" / "cmd" / "db-collector"),
                "--db-type",
                "mysql",
                "--os-only",
                "--output-dir",
                str(temp_path),
            ]
            completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            run_id = _extract_run_id(completed.stdout)
            run_dir = temp_path / run_id
            self.assertTrue((run_dir / "manifest.json").exists())
            self.assertTrue((run_dir / "result.json").exists())
            result = json.loads((run_dir / "result.json").read_text(encoding="utf-8"))
            cpu_sample = result["os"]["cpu"]["samples"][0]
            memory_sample = result["os"]["memory"]["samples"][0]
            disk_sample = result["os"]["disk_io"]["samples"][0]
            network_sample = result["os"]["network"]["samples"][0]
            process_sample = result["os"]["process"]["samples"][0]
            self.assertIn("user_percent", cpu_sample)
            self.assertIn("swap_usage_percent", memory_sample)
            self.assertIn("meminfo", memory_sample)
            self.assertIn("total_iops", disk_sample)
            self.assertIn("total_rate_bytes_per_sec", network_sample)
            self.assertIn("running_processes", process_sample)


def _extract_run_id(stdout: str) -> str:
    for line in stdout.splitlines():
        if line.startswith("run_id="):
            return line.split("=", 1)[1].strip()
    raise AssertionError(f"run_id missing from stdout: {stdout}")


def _oracle_manifest() -> dict:
    return {
        "schema_version": "1.0",
        "run_id": "oracle-127.0.0.1-20260312T003010Z",
        "db_type": "oracle",
        "start_time": "2026-03-12T00:30:00+08:00",
        "end_time": "2026-03-12T00:30:05+08:00",
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


def _gauss_manifest() -> dict:
    return {
        "schema_version": "1.0",
        "run_id": "gaussdb-127.0.0.1-20260312T003010Z",
        "db_type": "gaussdb",
        "start_time": "2026-03-12T00:30:00+08:00",
        "end_time": "2026-03-12T00:30:05+08:00",
        "exit_code": 0,
        "overall_status": "success",
        "module_stats": {
            "os": {"status": "success", "duration_ms": 10, "error": None},
            "db_basic": {"status": "success", "duration_ms": 10, "error": None},
        },
        "artifacts": {"log": "collector.log", "result": "result.json", "summary": None, "report": None},
    }


def _oracle_result() -> dict:
    return {
        "meta": {
            "schema_version": "2.0",
            "collector_version": "1.0.0",
            "db_type": "oracle",
            "db_host": "127.0.0.1",
            "db_port": 1521,
            "db_name": "ORCLCDB",
            "collect_mode": "remote",
            "collect_time": "2026-03-12T00:30:05+08:00",
            "collect_duration_seconds": 5,
            "os_sample_count": 1,
            "timezone": "Asia/Shanghai",
        },
        "collect_config": {
            "sample_mode": "single",
            "sample_interval_seconds": None,
            "sample_period_seconds": None,
            "expected_samples": 1,
        },
        "collect_window": {
            "window_start": "2026-03-12T00:30:00+08:00",
            "window_end": "2026-03-12T00:30:05+08:00",
            "duration_seconds": 5,
        },
        "os": {
            "system_info": {"hostname": "oracle-host", "os": "linux", "arch": "amd64", "cpu_cores": 8, "file_descriptor_usage_percent": 1.0, "mysql_fd_usage_percent": 0.0},
            "cpu": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 48.0, "user_percent": 22.0, "system_percent": 8.0, "idle_percent": 52.0, "nice_percent": 1.0, "iowait_percent": 1.0}]},
            "memory": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 65.0, "swap_usage_percent": 10.0, "meminfo": {"MemTotal": 17179869184, "MemAvailable": 8589934592, "SwapTotal": 4294967296, "SwapFree": 3865470566}}]},
            "filesystem": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "mountpoints": [{"mountpoint": "/", "device": "/dev/root", "fstype": "xfs", "usage_percent": 70.0, "inodes_usage_percent": 22.0, "read_only": False}]}]},
            "disk_io": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "total_iops": 180.0, "total_throughput_kbps": 4096.0, "avg_latency_ms": 1.3, "devices": []}]},
            "network": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "interfaces": [], "total_rate_bytes_per_sec": 3072.0, "total_rx_bytes_per_sec": 2048.0, "total_tx_bytes_per_sec": 1024.0, "error_drop_per_sec": 0.0}]},
            "process": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "load_avg_1": 0.5, "running_processes": 2, "blocked_processes": 0, "total_processes": 128, "context_switches": 2048, "go_routines": 0}]},
        },
        "db": {
            "basic_info": {"db_name": "ORCLCDB", "instance_name": "ORCLCDB", "version": "19.0.0.0.0", "dbid": 123456, "is_rac": False, "character_set": "AL32UTF8", "log_mode": "ARCHIVELOG"},
            "config_check": {"spfile": "/opt/oracle/product/19c/dbhome_1/dbs/spfileORCLCDB.ora"},
            "storage": {
                "datafiles": {"items": [{"file_name": "/opt/oracle/oradata/ORCLCDB/system01.dbf"}], "count": 1},
                "tablespace_usage": {"items": [{"tablespace_name": "USERS", "total_size_gb": 10, "used_size_gb": 6, "real_percent": 60.0}], "count": 1},
                "recover_files": {"items": [], "count": 0},
                "invalid_objects": {"items": [], "count": 0},
                "invalid_indexes": {"items": [], "count": 0},
            },
            "backup": {
                "archive_log_mode": "ARCHIVELOG",
                "jobs": {"items": [{"session_key": 1, "input_type": "DB FULL", "status": "COMPLETED", "start_time": "2026-03-11T01:00:00+08:00", "end_time": "2026-03-11T02:00:00+08:00", "hours": 1.0}], "count": 1},
                "archive_destinations": {"items": [{"dest_name": "LOG_ARCHIVE_DEST_1", "status": "VALID", "destination": "/arch", "target": "PRIMARY", "archiver": "ARCH", "error": ""}], "count": 1},
                "archive_destination_errors": {"items": [], "count": 0},
                "archive_log_summary": {"items": [{"archive_count": 256, "archive_size_gb": 12.5, "oldest_archive_time": "2026-03-10 00:00:00", "newest_archive_time": "2026-03-12 00:00:00"}], "count": 1},
                "recovery_area": {"items": [{"name": "/fra", "space_limit_gb": 200.0, "space_used_gb": 82.0, "space_reclaimable_gb": 18.0, "space_used_pct": 41.0, "number_of_files": 120}], "count": 1},
            },
            "performance": {
                "metric_overview": {"items": [{"metric_name": "Host CPU Utilization (%)", "average_value": 32.5, "metric_unit": "%"}], "count": 1},
                "active_sessions": {"items": [{"inst_id": 1, "active_sessions": 4}], "count": 1},
                "active_session_details": {"items": [{"sid": 11, "serial": 1, "username": "SYSTEM", "sql_id": "abcd", "event": "db file sequential read", "seconds_in_wait": 3}], "count": 1},
                "long_transactions": {"items": [{"sid": 12, "serial": 2, "username": "APP", "sql_id": "bcde", "event": "log file sync", "start_time": "2026-03-12 00:10:00", "duration_minutes": 15.2}], "count": 1},
                "blocking_chains": {"items": [{"waiter_sid": 21, "waiter_username": "APP", "blocker_sid": 22, "blocker_username": "APP2", "wait_event": "enq: TX - row lock contention", "seconds_in_wait": 18}], "count": 1},
                "resource_limits": {"items": [{"inst_id": 1, "resource_name": "processes", "current_utilization": 120, "max_utilization": 135, "limit_value": 300}], "count": 1},
                "instance_efficiency": {"items": [{"db_block_gets": 1024, "consistent_gets": 4096, "db_block_reads_pct": 2.3, "db_block_writes_pct": 14.1}], "count": 1},
                "redo_switch_daily": {"items": [{"switch_count": 5, "switch_date": "2026-03-12"}], "count": 1},
                "tablespace_io_stats": {"items": [{"tablespace_name": "USERS", "file_name": "/opt/oracle/oradata/ORCLCDB/users01.dbf", "phyrds": 100, "phyblkrd": 200, "phywrts": 30, "phyblkwrt": 60}], "count": 1},
                "wait_events": {"items": [{"event": "db file sequential read", "waits": 100, "waited_ms": 2000, "avg_wait_ms": 20}], "count": 1},
                "latch_data": {"items": [{"name": "cache buffers chains", "gets": 1000, "misses": 8, "sleeps": 1}], "count": 1},
                "time_model": {"items": [{"stat_name": "DB CPU", "seconds": 123.4}], "count": 1},
                "undo_tablespace_usage": {"items": [{"tablespace_name": "UNDOTBS1", "total_size_gb": 40.0, "used_size_gb": 12.0, "active_size_gb": 4.0, "unexpired_size_gb": 5.0, "expired_size_gb": 3.0, "usage_percent": 30.0}], "count": 1},
                "undo_stats": {"items": [{"begin_time": "2026-03-12 00:00:00", "end_time": "2026-03-12 00:10:00", "txncount": 120, "maxquerylen": 45, "ssolderrcnt": 0, "nospaceerrcnt": 0}], "count": 1},
                "sga_resize_ops": {"items": [{"component": "DEFAULT buffer cache", "oper_type": "GROW", "oper_mode": "AUTO", "initial_size_mb": 1024, "target_size_mb": 1536, "final_size_mb": 1536, "start_time": "2026-03-11 23:00:00", "status": "COMPLETE"}], "count": 1},
                "redo_nowait_pct": 99.8,
            },
            "sql_analysis": {
                "top_sql_by_elapsed_time": {"items": [{"sql_text": "select * from dual", "elapsed_time_sec": 6.1, "cpu_time_sec": 2.3, "executions": 10}], "count": 1},
                "top_sql_by_buffer_gets": {"items": [], "count": 0},
                "top_sql_by_disk_reads": {"items": [], "count": 0},
                "top_sql_by_executions": {"items": [{"sql_text": "select 1 from dual", "executions": 1000, "buffer_gets": 100, "disk_reads": 1}], "count": 1},
                "high_parse_count_sql": {"items": [{"sql_id": "1abcd", "executions": 12000, "parse_calls": 11800, "version_count": 2, "sharable_mem": 8192, "sql_text": "select * from orders where id=:1"}], "count": 1},
                "high_version_count_sql": {"items": [{"sql_id": "2bcde", "version_count": 88, "executions": 3000, "parse_calls": 600, "sharable_mem": 16384, "sql_text": "select * from customer where code=:1"}], "count": 1},
                "sql_with_executions_ratio_pct": 92.1,
                "memory_for_sql_with_executions_ratio_pct": 87.6,
            },
            "security": {
                "disabled_constraints": {"items": [], "count": 0},
                "disabled_triggers": {"items": [], "count": 0},
                "dba_role_users": {"items": [{"grantee": "SYSTEM"}], "count": 1},
                "expired_users": {"items": [], "count": 0},
                "table_degree_gt_one": {"items": [], "count": 0},
                "indexes_degree_gt_one": {"items": [], "count": 0},
            },
        },
    }


def _gauss_result() -> dict:
    return {
        "meta": {
            "schema_version": "2.0",
            "collector_version": "1.1.0",
            "db_type": "gaussdb",
            "db_host": "127.0.0.1",
            "db_port": 8000,
            "db_name": "postgres",
            "collect_time": "2026-03-12T00:30:05+08:00",
            "timezone": "Asia/Shanghai",
        },
        "collect_config": {
            "sample_mode": "single",
            "sample_interval_seconds": None,
            "sample_period_seconds": None,
            "expected_samples": 1,
        },
        "collect_window": {
            "window_start": "2026-03-12T00:30:00+08:00",
            "window_end": "2026-03-12T00:30:05+08:00",
            "duration_seconds": 5,
        },
        "os": {
            "system_info": {"hostname": "gauss-host", "os": "linux", "arch": "amd64", "cpu_cores": 8, "file_descriptor_usage_percent": 1.0, "mysql_fd_usage_percent": 0.0},
            "cpu": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 48.0, "user_percent": 22.0, "system_percent": 8.0, "idle_percent": 52.0, "nice_percent": 1.0, "iowait_percent": 1.0}]},
            "memory": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "usage_percent": 65.0, "swap_usage_percent": 10.0, "meminfo": {"MemTotal": 17179869184, "MemAvailable": 8589934592, "SwapTotal": 4294967296, "SwapFree": 3865470566}}]},
            "filesystem": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "mountpoints": [{"mountpoint": "/", "device": "/dev/root", "fstype": "xfs", "usage_percent": 70.0, "inodes_usage_percent": 22.0, "read_only": False}]}]},
            "disk_io": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "total_iops": 180.0, "total_throughput_kbps": 4096.0, "avg_latency_ms": 1.3, "devices": []}]},
            "network": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "interfaces": [], "total_rate_bytes_per_sec": 3072.0, "total_rx_bytes_per_sec": 2048.0, "total_tx_bytes_per_sec": 1024.0, "error_drop_per_sec": 0.0}]},
            "process": {"samples": [{"timestamp": "2026-03-12T00:30:05+08:00", "load_avg_1": 0.5, "running_processes": 2, "blocked_processes": 0, "total_processes": 128, "context_switches": 2048, "go_routines": 0}]},
        },
            "db": {
                "basic_info": {
                    "summary": {
                        "version": "505.2.1.SPC1000",
                    "gaussdb_version": "gaussdb (GaussDB Kernel 505.2.1.SPC1000 build demo)",
                    "gsql_version": "gsql (GaussDB Kernel 505.2.1.SPC1000 build demo)",
                    "gs_check_version": "gs_check (GaussDB Kernel om 505.2.1.SPC1000 build demo)",
                    "checkdbconnection_status": "abnormal",
                    "checkommonitor_status": "normal",
                    "checkgaussver_status": "normal",
                    "gauss_user": "Ruby",
                    "gauss_env_file": "~/gauss_env_file",
                    "gausshome": "/data/cluster/usr/local/core/app",
                    "gausslog": "/data/cluster/var/lib/log/Ruby",
                        "pguser": "rdsAdmin",
                        "pghost": "/data/cluster/temp"
                    },
                    "items": [
                        {"item": "CheckDBConnection", "label": "数据库连接", "normalized_status": "abnormal", "summary": "database connection failed"},
                        {"item": "CheckOMMonitor", "label": "omMonitor 进程", "normalized_status": "normal", "summary": "om_monitor 进程正常，PID=7354", "details": {"pid": "7354"}},
                    ],
                    "count": 2,
                    "visible_count": 2
                },
                "cluster": {
                    "summary": {"checkclusterstate_status": "normal", "checkintegrity_status": "normal", "checkcatchup_status": "not_applicable", "visible_items": [{"item": "CheckClusterState", "label": "集群状态", "normalized_status": "normal", "summary": "cluster ok", "details": {"cluster_state": "Normal", "redistributing": "No", "balanced": "Yes", "nodes": [{"node": "192.168.1.157", "status": "Normal"}]}}, {"item": "CheckIntegrity", "label": "数据一致性", "normalized_status": "normal", "summary": "数据一致性检查正常，SHA256=abcd", "details": {"sha256": "abcd"}}]},
                    "items": [{"item": "CheckClusterState", "label": "集群状态", "normalized_status": "normal", "summary": "cluster ok", "details": {"cluster_state": "Normal", "redistributing": "No", "balanced": "Yes", "nodes": [{"node": "192.168.1.157", "status": "Normal"}]}}, {"item": "CheckIntegrity", "label": "数据一致性", "normalized_status": "normal", "summary": "数据一致性检查正常，SHA256=abcd", "details": {"sha256": "abcd"}}],
                    "count": 2,
                    "visible_count": 2
                },
                "config_check": {
                    "summary": {
                        "checkdbparams_status": "abnormal",
                        "checkgucvalue_status": "normal",
                        "checkgucconsistent_status": "abnormal",
                        "checkgucvalue_details": {"max_connections": 1000, "max_prepared_transactions": 1000, "max_locks_per_transaction": 512, "computed_value": 1024000, "configuration_reasonable": True},
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
                    "count": 3,
                    "visible_count": 3,
                },
                "connection": {"summary": {"visible_items": [{"label": "游标数量", "normalized_status": "abnormal", "summary": "cursor leak"}]}, "items": [{"label": "游标数量", "normalized_status": "abnormal", "summary": "cursor leak"}], "count": 1, "visible_count": 1},
                "storage": {"summary": {"checksystable_status": "normal", "checkkeydbtablesize_status": "normal", "visible_items": [{"item": "CheckSysTable", "label": "系统表检查", "normalized_status": "normal", "summary": "已检查 2 张系统表", "details": {"table_count": 2, "tables": [{"instance": "DN_6001", "table_name": "pg_attribute", "size_bytes": 3022848, "row_count": 19086, "avg_width": 13}, {"instance": "DN_6001", "table_name": "pg_class", "size_bytes": 729088, "row_count": 1616, "avg_width": 9}]}}, {"item": "CheckKeyDBTableSize", "label": "大表检查", "normalized_status": "normal", "summary": "已分析 1 个数据库的大表分布", "details": {"database_count": 1, "table_count": 2, "databases": [{"database": "postgres", "size_value": 18, "size_unit": "GB"}], "tables": [{"table_name": "public.orders", "size_value": 8, "size_unit": "GB"}, {"table_name": "public.customer", "size_value": 4, "size_unit": "GB"}]}}]}, "items": [{"item": "CheckSysTable", "label": "系统表检查", "normalized_status": "normal", "summary": "已检查 2 张系统表", "details": {"table_count": 2, "tables": [{"instance": "DN_6001", "table_name": "pg_attribute", "size_bytes": 3022848, "row_count": 19086, "avg_width": 13}, {"instance": "DN_6001", "table_name": "pg_class", "size_bytes": 729088, "row_count": 1616, "avg_width": 9}]}}, {"item": "CheckKeyDBTableSize", "label": "大表检查", "normalized_status": "normal", "summary": "已分析 1 个数据库的大表分布", "details": {"database_count": 1, "table_count": 2, "databases": [{"database": "postgres", "size_value": 18, "size_unit": "GB"}], "tables": [{"table_name": "public.orders", "size_value": 8, "size_unit": "GB"}, {"table_name": "public.customer", "size_value": 4, "size_unit": "GB"}]}}], "count": 2, "visible_count": 2},
                "performance": {"summary": {"checkerrorinlog_status": "abnormal", "visible_items": [{"item": "CheckErrorInLog", "label": "运行日志", "normalized_status": "abnormal", "summary": "最近日志 ERROR 数量 3", "details": {"error_count": 3, "sample_lines": ["2026-03-12 ERROR sample-1", "2026-03-12 ERROR sample-2"]}}]}, "items": [{"item": "CheckErrorInLog", "label": "运行日志", "normalized_status": "abnormal", "summary": "最近日志 ERROR 数量 3", "details": {"error_count": 3, "sample_lines": ["2026-03-12 ERROR sample-1", "2026-03-12 ERROR sample-2"]}}], "count": 1, "visible_count": 1},
                "transactions": {"summary": {"visible_items": [{"label": "锁数量", "normalized_status": "abnormal", "summary": "lock hotspot"}]}, "items": [{"label": "锁数量", "normalized_status": "abnormal", "summary": "lock hotspot"}], "count": 1, "visible_count": 1},
                "sql_analysis": {"summary": {"checkreturntype_status": "normal", "no_index_table_count": 0, "no_primary_key_table_count": 2, "no_statistics_table_count": 7}, "items": [{"item": "CheckReturnType", "label": "自定义函数", "normalized_status": "normal", "summary": "用户定义函数不包含非法返回类型"}], "count": 1, "visible_count": 1, "no_index_summary": {"items": []}, "no_primary_key_summary": {"items": [{"owner": "app", "total_table_count": 100, "no_pk_count": 2, "percentage": 2.0}]}, "no_primary_key_detail": {"items": [{"owner": "app", "table_name": "order_log"}, {"owner": "app", "table_name": "audit_log"}]}, "no_statistics_summary": {"items": [{"tableowner": "rdsAdmin", "total_table_count": 239, "table_no_stat": 7, "percentage": 2.9288}]}, "no_statistics_detail": {"items": [{"schemaname": "snapshot", "tableowner": "rdsAdmin", "tablename": "snap_pdb_info"}]}},
            "security": {"summary": {}, "items": [], "count": 0, "visible_count": 0},
            "gs_check_raw_index": {"items": [], "count": 0}
        },
    }


if __name__ == "__main__":
    unittest.main()
