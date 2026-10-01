#!/usr/bin/env bash
# Publishes the collector release tagged on HEAD to the platform (ADR 0002).
# A person runs it until the CI phase; CI will call this same script.
#
#   DBCHECK_PUBLISH_TOKEN=<token> scripts/publish_release.sh --url https://dbcheck.example.com
#
# It refuses unless HEAD is exactly on a vX.Y.Z or vX.Y.Z-rcN tag equal to the
# collector's built-in version, then builds the four .zip release packages
# with scripts/build_release_packages.sh and posts them. README.md documents
# the details. Flags: --url, --dist-dir (see reporter/cmd/publish-release).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="$(mktemp -d)"
trap 'rm -rf "$BIN_DIR"' EXIT
(cd "$ROOT_DIR" && GOCACHE="${GOCACHE:-/tmp/go-cache}" go build -o "$BIN_DIR/publish-release" ./reporter/cmd/publish-release)
"$BIN_DIR/publish-release" --repo "$ROOT_DIR" "$@"
