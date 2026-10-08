"""Create matching Collector ZIPs for browser-to-report acceptance checks."""
from __future__ import annotations

import copy
import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests" / "integration"))
from test_pipeline_awr import _oracle_manifest, _oracle_result
from test_pipeline_wdr import _gaussdb_manifest, _gaussdb_result


def create_fixtures(destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    oracle = copy.deepcopy(_oracle_result())
    oracle["meta"]["db_name"] = "ORACC"
    oracle["db"]["basic_info"].update(db_name="ORACC", dbid=2668322570)
    fixtures = [
        ("oracle.zip", _oracle_manifest(), oracle),
        ("gaussdb.zip", _gaussdb_manifest(), _gaussdb_result()),
        ("mysql.zip", json.loads((ROOT / "contracts" / "manifest.sample.json").read_text()),
         json.loads((ROOT / "contracts" / "result.sample.json").read_text())),
    ]
    for name, manifest, result in fixtures:
        with zipfile.ZipFile(destination / name, "w") as archive:
            archive.writestr("manifest.json", json.dumps(manifest))
            archive.writestr("result.json", json.dumps(result))
    (destination / "broken.zip").write_text("invalid ZIP")
    for source, name in [
        ("awrrpt_1_19321_19322.html", "awr.html"),
        ("wdr_cluster.html", "wdr-one.html"),
        ("wdr_cluster.html", "wdr-two.htm"),
    ]:
        (destination / name).write_bytes((ROOT / "resources" / source).read_bytes())


if __name__ == "__main__":
    create_fixtures(Path(sys.argv[1]))
