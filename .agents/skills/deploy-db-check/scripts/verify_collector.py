#!/usr/bin/env python3
"""Check the deployed collector catalogue and packages without changing data."""

import argparse
import hashlib
from pathlib import Path
import sqlite3
import sys
import zipfile


def verify(data_dir: Path) -> None:
    database = (data_dir / "platform.db").resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(database, uri=True) as connection:
        versions = connection.execute(
            "SELECT version FROM releases WHERE status = 'latest'"
        ).fetchall()
        if len(versions) != 1:
            raise ValueError("Expected one recommended collector release; found " + str(len(versions)))
        version = versions[0][0]
        packages = connection.execute(
            "SELECT platform, file_name, size, sha256 FROM release_packages WHERE version = ?",
            (version,),
        ).fetchall()
    expected = {"linux-amd64", "linux-arm64", "windows-amd64", "windows-arm64"}
    if {row[0] for row in packages} != expected:
        raise ValueError("Recommended collector must contain all four supported platforms")
    release_dir = (data_dir / "releases" / version).resolve()
    for platform, filename, size, checksum in packages:
        path = (release_dir / filename).resolve()
        if path.parent != release_dir:
            raise ValueError("Package path escapes its release directory")
        if path.stat().st_size != size:
            raise ValueError("Package size mismatch: " + platform)
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if digest != checksum:
            raise ValueError("Package checksum mismatch: " + platform)
        with zipfile.ZipFile(path) as archive:
            binary = "db-collector.exe" if platform.startswith("windows-") else "db-collector"
            if not any(
                Path(info.filename).name == binary
                and not info.is_dir()
                and info.file_size > 0
                for info in archive.infolist()
            ):
                raise ValueError("Collector binary missing: " + platform)
            if archive.testzip() is not None:
                raise ValueError("Corrupt ZIP: " + platform)
        print("PASS " + version + " " + platform)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    args = parser.parse_args()
    try:
        verify(args.data_dir)
    except (OSError, sqlite3.Error, ValueError, zipfile.BadZipFile) as error:
        print("FAIL: " + str(error), file=sys.stderr)
        sys.exit(1)
