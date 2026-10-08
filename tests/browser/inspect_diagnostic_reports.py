"""Verify diagnostic-derived content in reports downloaded by the browser suite."""
from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys
from xml.etree import ElementTree
from zipfile import ZipFile


def report_texts(path: Path) -> dict[str, str]:
    with ZipFile(path) as reports:
        documents = {}
        for name in reports.namelist():
            if not name.endswith(".docx"):
                continue
            with ZipFile(BytesIO(reports.read(name))) as document:
                tree = ElementTree.fromstring(document.read("word/document.xml"))
                documents[name.split("/")[0]] = " ".join(tree.itertext())
        return documents


def require_awr(text: str) -> None:
    for marker in ("SQL ordered by Elapsed Time", "0bckxtm13v9sd", "Concurrency", "7.55", "39394"):
        assert marker in text, f"Missing AWR-derived content: {marker}"
    assert "WDR 性能洞察" not in text


def require_wdr(text: str, expected_sources: int = 2) -> None:
    for marker in ("WDR 性能洞察", "dn_6001_6002_6003", "2026-03-13 09:36:45"):
        assert marker in text, f"Missing WDR-derived content: {marker}"
    assert text.count("Summary + Detail") == expected_sources, "Every selected WDR must contribute a source row"
    assert "SQL ordered by Elapsed Time" not in text


def inspect_downloads(directory: Path) -> None:
    plain = report_texts(directory / "no-attachment.zip")
    assert set(plain) == {"oracle.zip"}
    assert "SQL ordered by Elapsed Time" not in plain["oracle.zip"]
    require_awr(report_texts(directory / "one-awr.zip")["oracle.zip"])
    require_wdr(report_texts(directory / "multiple-wdr.zip")["gaussdb.zip"])
    mixed = report_texts(directory / "mixed-batch.zip")
    assert set(mixed) == {"oracle.zip", "gaussdb.zip", "mysql.zip"}
    require_awr(mixed["oracle.zip"])
    require_wdr(mixed["gaussdb.zip"])
    assert "WDR 性能洞察" not in mixed["mysql.zip"]
    assert "SQL ordered by Elapsed Time" not in mixed["mysql.zip"]
    print("All downloaded documents contain the expected item-specific diagnostic content.")


def inspect_recovery_downloads(directory: Path) -> None:
    scenarios = ("malformed-awr", "malformed-wdr", "wrong-type-awr", "wrong-type-wdr",
                 "awr-name-mismatch", "awr-dbid-mismatch", "wdr-name-mismatch")
    for scenario in scenarios:
        documents = report_texts(directory / f"{scenario}.zip")
        assert set(documents) == {"oracle.zip", "gaussdb.zip"}, scenario
        require_awr(documents["oracle.zip"])
        require_wdr(documents["gaussdb.zip"], expected_sources=1)
    removed = report_texts(directory / "removed-optional.zip")["oracle.zip"]
    assert "SQL ordered by Elapsed Time" not in removed
    require_awr(report_texts(directory / "stale-response-corrected.zip")["oracle.zip"])
    print("All corrected downloads contain the expected diagnostic content; removing the optional attachment excludes it.")


def inspect_guidance_download(directory: Path) -> None:
    documents = report_texts(directory / "guidance-mixed.zip")
    assert set(documents) == {"oracle.zip", "gaussdb.zip", "mysql.zip"}
    require_awr(documents["oracle.zip"])
    require_wdr(documents["gaussdb.zip"], expected_sources=1)
    assert "WDR 性能洞察" not in documents["mysql.zip"]
    assert "SQL ordered by Elapsed Time" not in documents["mysql.zip"]
    print("Manual-check guidance permits a downloaded mixed batch containing each item's diagnostic content.")


if __name__ == "__main__":
    if "--guidance" in sys.argv:
        inspect_guidance_download(Path(sys.argv[1]))
    elif "--recovery" in sys.argv:
        inspect_recovery_downloads(Path(sys.argv[1]))
    else:
        inspect_downloads(Path(sys.argv[1]))
