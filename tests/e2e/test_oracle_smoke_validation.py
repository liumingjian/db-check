"""Smoke rejects SQL/privilege failures before accepting generated reports."""

import json
import tempfile
import unittest
from pathlib import Path

from validate_oracle_smoke import validate_artifacts


class OracleSmokeValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.run_dir = Path(self.temp.name)
        self.db = {
            "version_info": {"family": "19c"},
            "deployment_topology": {"role": "primary", "is_rac": False, "is_asm": False, "is_cdb": True},
            "inspection_account": {"object_probes": [{"object": "SYS.V_$INSTANCE", "readable": True}]},
            "collection_availability": {},
        }

    def validate(self):
        for name, value in (("result.json", {"db": self.db}), ("summary.json", {}), ("report-view.json", {})):
            (self.run_dir / name).write_text(json.dumps(value), encoding="utf-8")
        validate_artifacts(self.run_dir, "19c")

    def test_sql_error_rejects_smoke_even_when_pipeline_created_artifacts(self):
        self.db["collect_errors"] = ["oracle.backup.jobs: ORA-00904: invalid identifier"]
        with self.assertRaisesRegex(ValueError, "database query errors"):
            self.validate()

    def test_unavailable_query_without_collect_error_still_rejects_smoke(self):
        self.db["collection_availability"] = {
            "db.storage.temp_usage": {"readable": False, "error_code": "ORA-00942"},
        }
        with self.assertRaisesRegex(ValueError, "unexpected unavailable dataset"):
            self.validate()

    def test_allowed_pdb_visibility_path_does_not_hide_sql_failure(self):
        self.db["collection_availability"] = {
            "db.deployment_topology.pdbs": {
                "readable": False, "error_code": "ORA-01031",
                "reason": "insufficient privileges", "remediation": "Grant access",
            },
        }
        with self.assertRaisesRegex(ValueError, "unexpected unavailable dataset"):
            self.validate()

    def test_privilege_probe_failure_rejects_smoke(self):
        self.db["inspection_account"]["object_probes"][0]["readable"] = False
        with self.assertRaisesRegex(ValueError, "privilege pre-check incomplete or failed"):
            self.validate()
