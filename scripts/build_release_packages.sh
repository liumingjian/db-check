#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="${DIST_DIR:-$ROOT_DIR/dist}"
# VERSION names the release packages, db-collector-<version>-<os>-<arch>.zip.
# It defaults to the collector's built-in version.
VERSION="${VERSION:-$(sed -n 's/^[[:space:]]*Version[[:space:]]*=[[:space:]]*"\(.*\)"$/\1/p' "$ROOT_DIR/collector/internal/cli/config.go")}"
# Every file in a package carries this time, the HEAD commit's by default.
# Together with -trimpath and zip -X it makes the packages reproducible, so
# re-publishing a tag sends identical files and stays idempotent.
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git -C "$ROOT_DIR" log -1 --format=%ct 2>/dev/null || date +%s)}"
PLATFORMS=(
  "linux amd64"
  "linux arm64"
  "windows amd64"
  "windows arm64"
)

log() {
  printf '[INFO] %s\n' "$*"
}

archive_platform() {
  local pkg_dir="$1"
  local archive_path="$pkg_dir.zip"
  local stamp
  rm -f "$archive_path"
  # BSD date takes -r <epoch>, GNU date -d @<epoch>.
  stamp="$(TZ=UTC date -r "$SOURCE_DATE_EPOCH" +%Y%m%d%H%M.%S 2>/dev/null || TZ=UTC date -d "@$SOURCE_DATE_EPOCH" +%Y%m%d%H%M.%S)"
  find "$pkg_dir" -exec env TZ=UTC touch -h -t "$stamp" {} +
  (cd "$DIST_DIR" && find "$(basename "$pkg_dir")" | LC_ALL=C sort | TZ=UTC zip -X -q -@ "$archive_path")
  log "archive written: $archive_path"
}

# write_guide assembles GUIDE.md from collector/guide. It joins the common/,
# db/, and <goos>/ parts in file-name order, then fills the placeholders.
# collector/guide/README.md says what goes in each part.
write_guide() {
  local target="$1"
  local goos="$2"
  local guide_dir="$ROOT_DIR/collector/guide"
  local binary="db-collector" collector="./db-collector" shell="bash"
  if [[ "$goos" == "windows" ]]; then
    binary="db-collector.exe"
    collector='.\\db-collector.exe'
    shell="powershell"
  fi
  local part
  for part in "$guide_dir"/common/*.md "$guide_dir"/db/*.md "$guide_dir/$goos"/*.md; do
    printf '%s\t%s\n' "$(basename "$part")" "$part"
  done | LC_ALL=C sort | cut -f2 | while IFS= read -r part; do
    cat "$part"
    printf '\n'
  done | sed \
    -e "s|{{VERSION}}|$VERSION|g" \
    -e "s|{{PACKAGE}}|$(basename "$target")|g" \
    -e "s|{{BINARY}}|$binary|g" \
    -e "s|{{COLLECTOR}}|$collector|g" \
    -e "s|{{SHELL}}|$shell|g" > "$target/GUIDE.md"
  if grep -q '{{' "$target/GUIDE.md"; then
    printf '[ERROR] unfilled placeholder in %s/GUIDE.md\n' "$target" >&2
    exit 1
  fi
  mkdir -p "$target/oracle"
  cp "$ROOT_DIR/scripts/oracle/create_inspection_account.sql" "$target/oracle/"
}

build_platform() {
  local goos="$1"
  local goarch="$2"
  local pkg_dir="$DIST_DIR/db-collector-$VERSION-$goos-$goarch"
  local exe_suffix=""
  log "package started: $goos/$goarch"
  if [[ "$goos" == "windows" ]]; then
    exe_suffix=".exe"
  fi
  rm -rf "$pkg_dir"
  mkdir -p "$pkg_dir"
  log "build db-collector: $goos/$goarch"
  GOOS="$goos" GOARCH="$goarch" GOCACHE=/tmp/go-cache go build -trimpath -o "$pkg_dir/db-collector$exe_suffix" "$ROOT_DIR/collector/cmd/db-collector"
  log "write guide: $goos/$goarch"
  write_guide "$pkg_dir" "$goos"
  archive_platform "$pkg_dir"
  log "package finished: $goos/$goarch"
}

main() {
  local item
  if [[ -z "$VERSION" ]]; then
    printf '[ERROR] cannot read the collector version from collector/internal/cli/config.go; set VERSION\n' >&2
    exit 1
  fi
  log "release started: $VERSION -> $DIST_DIR"
  mkdir -p "$DIST_DIR"
  DIST_DIR="$(cd "$DIST_DIR" && pwd)"
  log "build embedded os probes"
  "$ROOT_DIR/scripts/build_embedded_osprobes.sh"
  for item in "${PLATFORMS[@]}"; do
    build_platform ${item}
  done
  log "release finished: $DIST_DIR"
}

main "$@"
