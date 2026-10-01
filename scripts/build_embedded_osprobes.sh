#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ASSET_ROOT="$ROOT_DIR/collector/internal/osprobeassets/bin"
PROBES=(linux-amd64 linux-arm64)
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

build_probe() {
  local platform="$1"
  CGO_ENABLED=0 GOOS="${platform%-*}" GOARCH="${platform#*-}" GOCACHE=/tmp/go-cache \
    go build -trimpath -ldflags='-s -w' -o "$TMP_DIR/$platform" "$ROOT_DIR/collector/cmd/db-osprobe"
}

# db-osprobe links collector/internal/osinfo, which embeds these very assets.
# Every probe is built against empty placeholders, so no probe carries an
# earlier build's probes: the assets, and the collector embedding them, are
# the same on every build of the same source, and a fresh checkout builds.
for platform in "${PROBES[@]}"; do
  mkdir -p "$ASSET_ROOT/$platform"
  : > "$ASSET_ROOT/$platform/db-osprobe.gz"
done
for platform in "${PROBES[@]}"; do
  build_probe "$platform"
done
# -n leaves the file name and mtime out of the gzip header.
for platform in "${PROBES[@]}"; do
  gzip -n -c "$TMP_DIR/$platform" > "$ASSET_ROOT/$platform/db-osprobe.gz"
done
