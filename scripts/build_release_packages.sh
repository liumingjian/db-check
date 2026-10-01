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

write_quickstart() {
  local target="$1"
  local exe_suffix="$2"
  cat > "$target/QUICKSTART.md" <<EOF
# Quick Start

## 1. 执行采集

\`\`\`bash
./db-collector$exe_suffix --db-type mysql --db-host 127.0.0.1 --db-port 3306 --db-username root --db-password rootpwd --dbname dbcheck
\`\`\`

Oracle 示例：

\`\`\`bash
./db-collector$exe_suffix --db-type oracle --db-host 127.0.0.1 --db-port 1521 --db-username system --db-password oraclepwd --dbname ORCL
\`\`\`

GaussDB 示例：

\`\`\`bash
./db-collector$exe_suffix --db-type gaussdb --db-host 10.0.0.10 --db-port 8000 --db-username root --db-password secret --dbname postgres
\`\`\`

说明：
- 默认输出目录为当前目录下的 \`./runs\`
- Oracle 路径下 \`--dbname\` 表示 SID/实例名
- GaussDB 路径下 \`--db-host/--db-port/--db-username/--db-password/--dbname\` 用于 openGauss SQL-first 直连采集
- GaussDB 路径下 \`--gauss-user\` 和 \`--gauss-env-file\` 已废弃，传入后会被显式忽略
- 未提供 OS 参数时不会采集 OS 指标；如需远程 OS 采集，可追加 \`--os-host/--os-port/--os-username/--os-password\`
- 如需本机 OS 采集，可追加 \`--local\`

## 2. 上传生成报告

采集完成后，\`run\` 目录中会包含：
- \`collector.log\`
- \`result.json\`
- \`manifest.json\`

将该 \`run\` 目录压缩成 ZIP 后，在 db-check Web 页面上传 ZIP 生成 Word 报告。
EOF
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
  log "write quickstart: $goos/$goarch"
  write_quickstart "$pkg_dir" "$exe_suffix"
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
