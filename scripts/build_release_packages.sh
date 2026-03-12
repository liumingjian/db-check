#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="${DIST_DIR:-$ROOT_DIR/dist}"
PLATFORMS=(
  "linux amd64"
  "linux arm64"
  "windows amd64"
  "darwin arm64"
)

copy_runtime_modules() {
  local target="$1"
  mkdir -p "$target"
  cp -R "$ROOT_DIR/analyzer" "$target/"
  cp -R "$ROOT_DIR/reporter" "$target/"
  cp -R "$ROOT_DIR/tasks" "$target/"
  cp -R "$ROOT_DIR/contracts" "$target/"
  find "$target" \( -name '__pycache__' -o -name '*.pyc' \) -prune -exec rm -rf {} +
}

write_quickstart() {
  local target="$1"
  local python_hint="$2"
  local exe_suffix="$3"
  cat > "$target/QUICKSTART.md" <<EOF
# Quick Start

## 1. 安装 Python 依赖

\`\`\`bash
$python_hint -m pip install -r runtime/requirements.txt
\`\`\`

## 2. 执行采集

\`\`\`bash
./db-collector$exe_suffix --db-type mysql --db-host 127.0.0.1 --db-port 3306 --db-username root --db-password rootpwd --dbname dbcheck --output-dir ./runs
\`\`\`

Oracle 示例：

\`\`\`bash
./db-collector$exe_suffix --db-type oracle --db-host 127.0.0.1 --db-port 1521 --db-username system --db-password oraclepwd --dbname ORCL --output-dir ./runs
\`\`\`

GaussDB 示例：

\`\`\`bash
./db-collector$exe_suffix --db-type gaussdb --db-host 10.0.0.10 --db-port 8000 --db-username root --db-password secret --dbname postgres --gauss-user Ruby --gauss-env-file ~/gauss_env_file --output-dir ./runs
\`\`\`

说明：
- Oracle 路径下 \`--dbname\` 表示 SID/实例名
- GaussDB 路径下 \`--gauss-user\` 和 \`--gauss-env-file\` 用于主机侧执行 \`gs_check\`
- GaussDB 路径下 \`--db-host/--db-port/--db-username/--db-password/--dbname\` 同时用于 openGauss SQL 直连采集
- 如需远程 OS 采集，可追加 \`--os-host/--os-port/--os-username/--os-password\`

## 3. 生成 Word 报告

\`\`\`bash
./db-reporter$exe_suffix --run-dir ./runs/<run_id>
\`\`\`

生成完成后，\`run\` 目录中会包含：
- \`result.json\`
- \`summary.json\`
- \`report-meta.json\`
- \`report-view.json\`
- \`report.docx\`
EOF
}

build_platform() {
  local goos="$1"
  local goarch="$2"
  local pkg_dir="$DIST_DIR/db-check-$goos-$goarch"
  local exe_suffix=""
  local python_hint="python3"
  if [[ "$goos" == "windows" ]]; then
    exe_suffix=".exe"
    python_hint="python"
  fi
  rm -rf "$pkg_dir"
  mkdir -p "$pkg_dir/assets/rules/mysql" "$pkg_dir/assets/templates" "$pkg_dir/runtime"
  mkdir -p "$pkg_dir/assets/rules/oracle" "$pkg_dir/assets/rules/gaussdb"
  GOOS="$goos" GOARCH="$goarch" GOCACHE=/tmp/go-cache go build -o "$pkg_dir/db-collector$exe_suffix" "$ROOT_DIR/collector/cmd/db-collector"
  GOOS="$goos" GOARCH="$goarch" GOCACHE=/tmp/go-cache go build -o "$pkg_dir/db-reporter$exe_suffix" "$ROOT_DIR/reporter/cmd/db-reporter"
  cp "$ROOT_DIR/rules/mysql/rule.json" "$pkg_dir/assets/rules/mysql/rule.json"
  cp "$ROOT_DIR/rules/oracle/rule.json" "$pkg_dir/assets/rules/oracle/rule.json"
  cp "$ROOT_DIR/rules/gaussdb/rule.json" "$pkg_dir/assets/rules/gaussdb/rule.json"
  cp "$ROOT_DIR/reporter/templates/mysql-template.docx" "$pkg_dir/assets/templates/mysql-template.docx"
  cp "$ROOT_DIR/reporter/cli/reporter_orchestrator.py" "$pkg_dir/runtime/reporter_orchestrator.py"
  cp "$ROOT_DIR/requirements.txt" "$pkg_dir/runtime/requirements.txt"
  copy_runtime_modules "$pkg_dir/runtime/python_modules"
  write_quickstart "$pkg_dir" "$python_hint" "$exe_suffix"
}

main() {
  local item
  mkdir -p "$DIST_DIR"
  "$ROOT_DIR/scripts/build_embedded_osprobes.sh"
  for item in "${PLATFORMS[@]}"; do
    build_platform ${item}
  done
}

main "$@"
