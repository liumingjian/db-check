#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ASSET_ROOT="$ROOT_DIR/collector/internal/osprobeassets/bin"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

build_probe() {
  local goos="$1"
  local goarch="$2"
  local target_dir="$ASSET_ROOT/${goos}-${goarch}"
  local raw_bin="$TMP_DIR/db-osprobe-${goos}-${goarch}"
  mkdir -p "$target_dir"
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" GOCACHE=/tmp/go-cache \
    go build -trimpath -ldflags='-s -w' -o "$raw_bin" "$ROOT_DIR/collector/cmd/db-osprobe"
  gzip -c "$raw_bin" > "$target_dir/db-osprobe.gz"
}

build_probe linux amd64
build_probe linux arm64
