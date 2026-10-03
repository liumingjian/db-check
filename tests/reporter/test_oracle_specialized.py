import unittest

from reporter.content.oracle_report_builder import build_oracle_report_view


class OracleSpecializedReportTests(unittest.TestCase):
    def test_report_exposes_per_instance_parameters_and_storage_redundancy(self):
        result = {"db": {"rac": {
            "instances": {"items": [{"inst_id": 2, "instance_name": "DB2", "host_name": "node2", "status": "OPEN", "database_status": "ACTIVE", "active_state": "NORMAL"}]},
            "parameters": {"items": [{"inst_id": 2, "name": "sga_target", "value": "2147483648", "isdefault": "FALSE"}]},
        }, "asm": {"diskgroups": {"items": [{"name": "DATA", "state": "CONNECTED", "redundancy": "NORMAL", "total_mb": 1000, "free_mb": 300, "used_pct": 70, "usable_file_mb": 100, "required_mirror_free_mb": 100, "offline_disks": 0}]}},
            "host_checks": {"clusterware": {"output": "ora.DB.db ONLINE ONLINE node2"}}}}
        pending = list(build_oracle_report_view(result, {}, {}).sections)
        while pending:
            section = pending.pop()
            if section.title == "2.2.7 Data Guard、ASM 与 RAC":
                break
            pending.extend(section.children)
        else:
            self.fail("specialized evidence missing")
        tables = {table.title: table for table in section.tables}
        self.assertEqual(tables["RAC 当前生效参数"].rows, (("2", "sga_target", "2147483648", "FALSE"),))
        self.assertEqual(tables["ASM 磁盘组"].rows, (("DATA", "CONNECTED", "NORMAL", "1000", "300", "70", "100", "100", "0"),))
        self.assertIn("停机节点", "".join(section.paragraphs))
        self.assertIn("ora.DB.db ONLINE ONLINE node2", str(tables["主机服务命令结果"].rows))
