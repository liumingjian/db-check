from __future__ import annotations

import unittest

from reporter.content.oracle_report_builder import build_oracle_report_view


class OracleTopologyReportTests(unittest.TestCase):
    def test_opening_reports_version_verification_without_claiming_smoke_pass(self) -> None:
        for family in ("11gR2", "12c", "19c", "21c", "23ai", "unknown"):
            with self.subTest(family=family):
                opening = build_oracle_report_view(
                    {"db": {"version_info": {"family": family}}}, {}, {},
                ).sections[0]
                verification = dict(opening.tables[0].rows)["版本验证级别"]
                if family in ("11gR2", "19c"):
                    self.assertIn("端到端 smoke 待验证", verification)
                elif family in ("12c", "21c", "23ai"):
                    self.assertIn("未经过容器验证", verification)
                else:
                    self.assertIn("版本未识别", verification)
                self.assertNotIn("验证通过", verification)
                self.assertIn("不采集 AWR、ASH、ADDM", "".join(opening.paragraphs))

    def test_report_opens_with_detected_topology_and_every_visible_pdb(self) -> None:
        result = {"db": {
            "version_info": {"version": "19.3.0.0.0", "major": 19, "family": "19c"},
            "deployment_topology": {
                "is_rac": True, "is_cdb": True, "is_asm": True,
                "database_role": "PHYSICAL STANDBY", "role": "standby",
                "connected_container": "CDB$ROOT", "inspection_scope": "connected_container",
                "pdb_list_state": "collected",
                "pdbs": {"items": [
                    {"name": "PDB$SEED", "open_mode": "READ ONLY", "size_bytes": 104857600},
                    {"name": "APP", "open_mode": "MOUNTED", "size_bytes": 209715200},
                ]},
            },
        }}
        view = build_oracle_report_view(result, {}, {})
        opening = view.sections[0]
        self.assertEqual(opening.title, "部署形态")
        rows = dict(opening.tables[0].rows)
        self.assertEqual(rows["数据库版本"], "19c / 19.3.0.0.0")
        self.assertEqual(rows["实例部署"], "RAC")
        self.assertEqual(rows["数据库角色"], "PHYSICAL STANDBY")
        self.assertEqual(rows["当前容器"], "CDB$ROOT")
        self.assertIn("仅巡检当前连接容器", "".join(opening.paragraphs))
        self.assertEqual(opening.tables[1].rows, (
            ("PDB$SEED", "READ ONLY", "100.00"), ("APP", "MOUNTED", "200.00"),
        ))

    def test_missing_topology_and_incomplete_pdb_list_are_explicit(self) -> None:
        opening = build_oracle_report_view({}, {}, {}).sections[0]
        self.assertEqual(dict(opening.tables[0].rows)["实例部署"], "未知")
        result = {"db": {"deployment_topology": {
            "is_cdb": True, "connected_container": "APP", "pdb_list_state": "not_collected",
            "pdbs": {"items": [{"name": "APP", "open_mode": "READ WRITE", "size_bytes": 1048576}]},
        }}}
        opening = build_oracle_report_view(result, {}, {}).sections[0]
        self.assertEqual(opening.tables[1].status, "not_collected")
        self.assertIn("CDB$ROOT", opening.tables[1].note)
        self.assertIn("APP", opening.tables[1].rows[0])
