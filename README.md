# db-check

`db-check` 是一个面向企业内网数据库巡检场景的双入口工具链，用于完成数据库与主机指标采集、规则分析和 Word 巡检报告生成。

当前主要入口包括：
- `db-collector`：采集数据库与 OS 指标，产出标准 `run` 目录
- `db-reporter`：基于 `run` 目录生成 `summary.json`、`report-meta.json`、`report-view.json` 和最终 `report.docx`
- `db-web`（可选）：Web 报告生成服务。上传 ZIP 采集包 → 后端执行 reporter pipeline → WebSocket 推送日志/进度 → 下载结果 ZIP

当前正式实现覆盖：
- MySQL `5.6 / 5.7 / 8.0`
- Oracle `11g / 19c`
- GaussDB `505.2.1.SPC1000`

当前版本里，MySQL、Oracle 与 GaussDB 都已经打通以下完整链路：
- `db-collector` 采集数据库 + OS 指标，生成标准 `run` 目录
- `db-reporter` 自动识别 `db_type`，生成 `summary.json`、`report-meta.json`、`report-view.json` 和 `report.docx`
- 第一章“巡检总结”使用统一模板，关键指标会在 Word 报告中加粗高亮显示

## 核心能力

- 采集 MySQL / Oracle / GaussDB 与 OS 关键巡检指标，输出标准化 contracts 产物
- 基于规则自动分析风险，生成结构化 `summary.json`
- 基于统一 `ReportView` 和 Word 模板生成正式巡检报告
- 远程 OS 采集通过 SSH 下发临时 `db-osprobe` 二进制执行，避免依赖目标机 `sar/free/vmstat/iostat`
- GaussDB 数据库指标通过“SSH 执行系统自带 `gs_check` + openGauss Go 驱动 SQL 采集”组合获取，并在 `run_dir/gs_check/`、`run_dir/sql/` 保留原始输出
- 支持 Linux / macOS / Windows 多平台发布包构建
- 支持 MySQL / Oracle 的 Docker 多版本 e2e 验证，保证采集、分析、报告链路一致

## 适用场景

- 客户内网离线巡检
- 实施工程师现场采集与回传
- 分析工程师基于采集结果生成标准报告
- 开发团队持续迭代采集、规则和报告能力

## Quick Start

本项目推荐三种使用方式：
- 方式一：编译运行
  - 适合本地验证、联调、交付前测试
- 方式二：源码运行
  - 适合开发、调试和排查问题
- 方式三：Web 报告服务（`db-web` + `web/` 前端）
  - 适合一键上传 ZIP 采集包并生成报告（带 WS 日志/进度）

无论采用哪种方式，最终用户只需要完成两步：
1. 使用 `db-collector` 采集指标，生成 `run` 目录
2. 使用 `db-reporter` 基于 `run` 目录生成 Word 报告

---

## 方式一：编译运行

### 1. 环境要求

- Go `1.24+`
- Python `3.10+`
- 当前 Shell 可正常执行 `python3`
- 如果需要运行 e2e：需要 Docker 与 Docker Compose

### 2. 初始化 Python 虚拟环境

```bash
python3 -m venv .venv
source .venv/bin/activate
make init-python
```

说明：
- `db-reporter` 会调用 Python 运行时完成分析、元数据和报告渲染
- 在源码仓库中，所有 `python3` 命令都应在已激活的 `.venv` 中执行

### 3. 编译两个入口程序

```bash
make build
```

默认输出目录：
- `bin/`

### 4. 执行采集

MySQL 示例：

```bash
./bin/db-collector \
  --db-type mysql \
  --db-host 127.0.0.1 \
  --db-port 3306 \
  --db-username root \
  --db-password rootpwd \
  --dbname mysql \
  --output-dir ./runs
```

Oracle 示例：

```bash
./bin/db-collector \
  --db-type oracle \
  --db-host 127.0.0.1 \
  --db-port 1521 \
  --db-username system \
  --db-password oraclepwd \
  --dbname ORCL \
  --output-dir ./runs
```

GaussDB 示例：

```bash
./bin/db-collector \
  --db-type gaussdb \
  --db-host 10.250.0.157 \
  --db-port 8000 \
  --db-username root \
  --db-password Gauss_246 \
  --dbname postgres \
  --gauss-user Ruby \
  --gauss-env-file ~/gauss_env_file \
  --os-host 10.250.0.157 \
  --os-port 22 \
  --os-username root \
  --os-password ATT@2022 \
  --output-dir ./runs
```

如需同时采集远程主机 OS（Linux over SSH），增加 SSH 参数。当前远程 OS 采集会通过 SSH 自动上传临时 helper 二进制执行，不依赖目标机预装 `sar/free/vmstat/iostat`：

```bash
./bin/db-collector \
  --db-type mysql \
  --db-host 10.250.0.24 \
  --db-port 33306 \
  --db-username root \
  --db-password ATT@2022 \
  --dbname mysql \
  --os-host 10.250.0.24 \
  --os-port 22 \
  --os-username root \
  --os-password ATT@2022 \
  --output-dir ./runs
```

Oracle + 远程 OS 示例：

```bash
./bin/db-collector \
  --db-type oracle \
  --db-host 10.250.0.222 \
  --db-port 1522 \
  --db-username system \
  --db-password 123456aB \
  --dbname xe \
  --os-host 10.250.0.222 \
  --os-port 22 \
  --os-username root \
  --os-password ATT@2022 \
  --output-dir ./runs
```

GaussDB 路径补充说明：
- `--gauss-user` 用于切换到实际安装 GaussDB 的系统用户后执行 `gs_check`
- `--gauss-env-file` 用于 `source` GaussDB 环境文件后再执行 `gs_check`
- `--db-host/--db-port/--db-username/--db-password/--dbname` 同时用于 openGauss 直连 SQL 采集与报告元数据
- GaussDB 的 `gs_check` 原始输出落在 `run_dir/gs_check/`，SQL 原始查询与结果落在 `run_dir/sql/`

执行成功后，终端会打印：
- `run_id=...`
- `manifest=...`
- `result=...`

例如：
```text
run_id=mysql-127.0.0.1-20260311T120000Z
manifest=./runs/mysql-127.0.0.1-20260311T120000Z/manifest.json
result=./runs/mysql-127.0.0.1-20260311T120000Z/result.json
```

### 5. 生成 Word 报告

```bash
./bin/db-reporter \
  --python-bin "$VIRTUAL_ENV/bin/python3" \
  --run-dir ./runs/<run_id>
```

如果需要同时导出 Markdown：

```bash
./bin/db-reporter \
  --python-bin "$VIRTUAL_ENV/bin/python3" \
  --run-dir ./runs/<run_id> \
  --out-md ./runs/<run_id>/report.md
```

### 6. 验证产物

报告生成成功后，`./runs/<run_id>/` 目录中应至少包含：
- `collector.log`
- `manifest.json`
- `result.json`
- `gs_check/`（仅 GaussDB）
- `sql/`（GaussDB 结构化 SQL 原始输出）
- `summary.json`
- `report-meta.json`
- `report-view.json`
- `report.docx`

按需导出时还会包含：
- `report.md`

---

## 方式二：源码运行

### 1. 环境要求

- Go `1.24+`
- Python `3.10+`
- 已激活 `.venv`

### 2. 初始化 Python 虚拟环境

```bash
python3 -m venv .venv
source .venv/bin/activate
make init-python
```

### 3. 直接运行采集端

```bash
GOCACHE=/tmp/go-cache go run ./collector/cmd/db-collector \
  --db-type mysql \
  --db-host 127.0.0.1 \
  --db-port 3306 \
  --db-username root \
  --db-password rootpwd \
  --dbname mysql \
  --output-dir ./runs
```

### 4. 直接运行报告端

```bash
GOCACHE=/tmp/go-cache go run ./reporter/cmd/db-reporter \
  --python-bin "$VIRTUAL_ENV/bin/python3" \
  --run-dir ./runs/<run_id>
```

如果需要同时导出 Markdown：

```bash
GOCACHE=/tmp/go-cache go run ./reporter/cmd/db-reporter \
  --python-bin "$VIRTUAL_ENV/bin/python3" \
  --run-dir ./runs/<run_id> \
  --out-md ./runs/<run_id>/report.md
```

### 5. 适用说明

源码运行方式适合：
- 调试采集逻辑
- 调试规则判定
- 调试报告内容和模板渲染
- 在不构建发布包的前提下快速验证完整链路

---

## 方式三：Web 报告服务（db-web + web/）

`db-web` 提供一个 Web 入口：上传 ZIP 采集包，服务端执行 `db-reporter` 对应的 reporter pipeline，并通过 WebSocket 推送日志/进度，最终下载结果 ZIP（只包含成功项的 `report.docx`）。

### 1. 环境要求

- Go `1.24+`
- Python `3.10+`（需安装 `requirements.txt` 依赖；推荐使用 `.venv`）
- Node.js（建议 `20+`，用于 `web/`）

### 2. 使用 PM2 一键启动（推荐）

PM2 会同时管理：
- `dbcheck-api`：后端 `db-web`（默认 `127.0.0.1:8080`）
- `dbcheck-web`：前端 Next dev server（默认 `:3000`）

说明：
- **PM2 只用于管理 Web 服务前后端**（`db-web` + `web/`），不管理 `db-collector` / `db-reporter` 这类一次性 CLI 任务。

准备依赖（如已准备可跳过）：

```bash
python3 -m venv .venv
source .venv/bin/activate
make init-python

make web-install
```

启动（dev）：

```bash
make pm2-start
```

查看日志/状态：

```bash
make pm2-status
make pm2-logs
```

最小链路验证（可选）：

```bash
make pm2-smoke
```

启动（production，需先 build）：

```bash
make build-db-web
make web-build
make pm2-start-prod
```

说明：
- `ecosystem.config.cjs` 内置了本地联调默认值：`DBCHECK_DATA_DIR=/tmp/dbcheck-data`、`ALLOWED_ORIGINS=http://127.0.0.1:3000,http://localhost:3000`、`DBCHECK_API_TOKEN=secret` 等；如需覆盖，可在 `pm2 start` 前设置同名环境变量，并用 `make pm2-restart` 刷新 env。
- 若用局域网地址打开前端（例如 `http://192.168.x.x:3000`），请显式设置 `NEXT_PUBLIC_API_BASE`（或在生成页手动填写 API Base），避免请求被推断到访问者本机 `127.0.0.1:8080`。

### 3. 准备输入 ZIP

要求：
- 每个上传 ZIP 里必须且只能包含 1 份采集产物（ZIP 内只能有一个 `manifest.json`）
- 最小需要包含：`manifest.json` + `result.json`

示例（从已有 `run` 目录打包）：

```bash
RUN_ID="mysql-127.0.0.1-20260311T120000Z"
cd runs/"$RUN_ID"
zip -r /tmp/mysql-run.zip manifest.json result.json
```

示例（快速体验：使用仓库内置 MySQL e2e 产物，无需真实数据库）：

```bash
RUN_DIR="$(find tests/e2e/runs -maxdepth 4 -path '*/mysql-8.0/*/manifest.json' -print | sort | tail -n 1 | xargs -I{} dirname {})"
zip -j /tmp/mysql-e2e.zip "$RUN_DIR/manifest.json" "$RUN_DIR/result.json"
```

### 4. 启动后端（db-web）

后端需要 3 个环境变量：
- `DBCHECK_DATA_DIR`：任务落盘目录（例如 `/tmp/dbcheck-data`）
- `ALLOWED_ORIGINS`：前端 Origin 白名单（逗号分隔）。支持三种写法：
  - 完整 Origin：`http://127.0.0.1:3000`（推荐）
  - Host（含端口）：`127.0.0.1:3000` / `localhost:3000`（更宽松，适合本地联调）
  - `*`：允许任意 Origin（仅建议本地联调临时使用；生产环境不要用）
- `DBCHECK_API_TOKEN`：固定 Bearer token（前端会提示输入）

说明：
- 当 `ALLOWED_ORIGINS` 配置里包含 `localhost` / `127.0.0.1`（带端口）时，`db-web` 会自动放行同端口的本机局域网地址（例如 `http://192.168.x.x:3000`），避免你用 Next dev server 的 Network 地址打开前端时触发 CORS。

```bash
source .venv/bin/activate

export DBCHECK_DATA_DIR=/tmp/dbcheck-data
export ALLOWED_ORIGINS=http://127.0.0.1:3000,http://localhost:3000
# 本地快速联调（不建议在生产环境使用）：
# export ALLOWED_ORIGINS=*
export DBCHECK_API_TOKEN=secret

go run ./reporter/cmd/db-web --addr 127.0.0.1:8080 --python-bin "$VIRTUAL_ENV/bin/python3"
```

### 5. 启动前端（web/）

前端通过 `NEXT_PUBLIC_API_BASE`（完整 Origin）指向后端（推荐）：

```bash
cd web
npm install
NEXT_PUBLIC_API_BASE=http://127.0.0.1:8080 npm run dev
```

访问：`http://127.0.0.1:3000`

说明：
- 如果你忘了设置 `NEXT_PUBLIC_API_BASE`，前端会在运行时做一个本地开发推断：当页面在 `:3000` 时，默认后端为 `:8080`；否则默认同源。
- 也可以在生成页手动填写“后端 API 地址”，无需重启前端。

### 6. 手动验证流程（MySQL）

1. 页面选择 MySQL
2. 上传 `/tmp/mysql-run.zip`（或 `/tmp/mysql-e2e.zip`）
3. 点击“生成报告”，在生成页输入 Token（与 `DBCHECK_API_TOKEN` 一致）
4. 观察 WS 日志与进度，完成后点击下载，得到 `reports-<task_id>.zip`

### 7. 仅用 curl 验证 HTTP（可选）

```bash
API_BASE="http://127.0.0.1:8080"
TOKEN="secret"

curl -sS -X POST \
  -H "Authorization: Bearer ${TOKEN}" \
  -F "zips=@/tmp/mysql-e2e.zip;type=application/zip" \
  "${API_BASE}/api/reports/generate"

# 轮询状态（done 后会出现 download_url）
curl -sS -H "Authorization: Bearer ${TOKEN}" \
  "${API_BASE}/api/reports/status/<task_id>"

# 下载结果
curl -sS -H "Authorization: Bearer ${TOKEN}" \
  -o reports.zip \
  "${API_BASE}/api/reports/download/<task_id>"
```

接口定义见：`docs/openapi/dbcheck-web.yaml`

### 8. 常见报错排查（TypeError: Failed to fetch）

如果前端日志里出现 `TypeError: Failed to fetch`，通常是以下原因之一：
1. 后端不可达或 `NEXT_PUBLIC_API_BASE` 配错（例如前端指向了错误的端口）
2. `ALLOWED_ORIGINS` 未包含当前页面的 `window.location.origin`（注意 `127.0.0.1` 与 `localhost` 属于不同 Origin）
3. 使用 Next dev server 的 Network 地址打开前端（例如 `http://192.168.x.x:3000`），但后端只放行了 `localhost/127.0.0.1`（可在 `ALLOWED_ORIGINS` 中显式加入该 Origin，或本地临时用 `*`）
4. HTTPS 页面调用 HTTP API 触发浏览器 Mixed Content 拦截

可以用预检请求快速定位是否是 CORS 配置问题（返回 204 且包含 `Access-Control-Allow-Origin` 为正常）：

```bash
API_BASE="http://127.0.0.1:8080"
curl -i -sS -X OPTIONS \
  -H "Origin: http://127.0.0.1:3000" \
  -H "Access-Control-Request-Method: POST" \
  "${API_BASE}/api/reports/generate"
```

---

## 发布包构建

如果需要构建多平台交付包：

```bash
make release
```

默认输出目录：
- `dist/`

生成后目录形态类似：

```text
dist/
├── db-check-darwin-arm64/
├── db-check-linux-amd64/
├── db-check-linux-arm64/
└── db-check-windows-amd64/
```

每个发布包目录中都包含：
- `db-collector`
- `db-reporter`
- `assets/`
- `runtime/`
- `QUICKSTART.md`

其中：
- `assets/rules/mysql/rule.json`
- `assets/rules/oracle/rule.json`
- `assets/rules/gaussdb/rule.json`
- `assets/templates/mysql-template.docx`

都会随发布包一起交付。

## E2E 覆盖

当前 Docker e2e 覆盖以下数据库版本：
- MySQL `5.6 / 5.7 / 8.0`
- Oracle `11g / 19c`

GaussDB 当前不承诺 Docker e2e。原因是 `gs_check`、`gauss_env_file`、数据库安装用户和工具链环境都依赖正式安装形态，当前以真实环境回归为准。

执行方式：

```bash
source .venv/bin/activate
tests/e2e/run_docker_e2e.sh --mysql-version 5.6 --mysql-version 5.7 --mysql-version 8.0
tests/e2e/run_docker_e2e.sh --db-type oracle --oracle-version 11g
tests/e2e/run_docker_e2e.sh --db-type oracle --oracle-version 19c
```

## run 目录说明

`db-collector` 每次执行都会生成一个独立的 `run` 目录，命名规则为：

```text
<db_type>-<host>-<yyyymmddThhmmssZ>
```

典型结构如下：

```text
runs/<run_id>/
├── collector.log
├── gs_check/               # 仅 GaussDB，保留每个 CheckItem 的原始 stdout 与 index.json
├── manifest.json
├── result.json
├── sql/                    # 仅 GaussDB，保留原始 SQL 与查询结果
├── summary.json
├── report-meta.json
├── report-view.json
├── report.docx
└── report.md                # 可选
```

各文件职责：
- `collector.log`：采集执行日志
- `gs_check/`：GaussDB 原始 `gs_check` 输出缓存，便于复核和排障
- `sql/`：GaussDB 原始 SQL 查询与结果缓存，便于复核和排障
- `manifest.json`：本次运行的执行态描述
- `result.json`：原始采集结果
- `summary.json`：规则分析结果
- `report-meta.json`：报告元数据
- `report-view.json`：报告内容视图模型
- `report.docx`：最终交付报告
- `report.md`：可选的人类可读导出物

## 项目目录结构

```text
.
├── README.md                # 项目入口文档
├── collector/               # Go 采集端
├── analyzer/                # Python 分析端
├── reporter/                # 报告生成与 Word 渲染
├── contracts/               # contracts schema 与样例
├── rules/                   # 检查规则
├── scripts/                 # 开发与构建辅助脚本
├── tests/                   # 单测、集成测试、e2e
├── docs/                    # 全局文档中心
├── dist/                    # 多平台发布包
├── bin/                     # 本地编译产物
└── tmp/                     # 其他临时产物
```

## Make 入口

仓库级高频操作已统一收敛到 `Makefile`：

```bash
make help
```

Makefile 的职责边界：
- 用于环境初始化、构建、测试、发布、清理
- 不作为正式运行入口的再包装层

常用目标：
- `make init-python`
- `make build`
- `make test-reporter`
- `make test-integration`
- `make test-e2e`
- `make release`
- `make clean`

运行时入口保持为：
- 编译模式：直接执行 `./bin/db-collector`、`./bin/db-reporter`
- 源码模式：直接执行 `go run ./collector/cmd/db-collector`、`go run ./reporter/cmd/db-reporter`

## 文档导航

如果你是不同角色，建议按下面顺序阅读：

### 客户或实施人员

1. [docs/README.md](/Users/lmj/projects/ai-project/db-check/docs/README.md)
2. [业务全景与实现流程.md](/Users/lmj/projects/ai-project/db-check/docs/architecture/业务全景与实现流程.md)
3. [模板说明](/Users/lmj/projects/ai-project/db-check/docs/templates/README.md)

### 架构师或方案评审人员

1. [业务全景与实现流程.md](/Users/lmj/projects/ai-project/db-check/docs/architecture/业务全景与实现流程.md)
2. [最小架构规范.md](/Users/lmj/projects/ai-project/db-check/docs/architecture/最小架构规范.md)
3. [冻结契约说明.md](/Users/lmj/projects/ai-project/db-check/docs/specs/冻结契约说明.md)
4. [db-check contracts PRD.md](/Users/lmj/projects/ai-project/db-check/docs/specs/db-check-contracts-prd.md)

### 开发者

1. [docs/README.md](/Users/lmj/projects/ai-project/db-check/docs/README.md)
2. [开发规范.md](/Users/lmj/projects/ai-project/db-check/docs/specs/开发规范.md)
3. [业务全景与实现流程.md](/Users/lmj/projects/ai-project/db-check/docs/architecture/业务全景与实现流程.md)
4. [tests/e2e](/Users/lmj/projects/ai-project/db-check/tests/e2e)

## 常见问题

### 1. `db-reporter` 提示找不到 `python3`

使用 `--python-bin` 显式指定解释器路径，例如：

```bash
--python-bin "$VIRTUAL_ENV/bin/python3"
```

### 2. `db-reporter` 提示缺少 `jsonschema` 或 `python-docx`

在已激活的虚拟环境中执行：

```bash
make init-python
```

### 3. `run` 目录不完整，无法生成报告

`db-reporter` 至少要求 `run` 目录中存在：
- `manifest.json`
- `result.json`

### 4. MySQL 版本无法自动识别

优先检查 `result.json` 中是否存在：
- `db.basic_info.version`
- `db.basic_info.version_vars.version`
- `db.basic_info.summary.version`
- `db.basic_info.summary.gaussdb_version`

如果采集结果本身缺失，再显式传入：

```bash
--mysql-version <version>
```

### 5. Oracle 的 `--dbname` 表示什么

Oracle 路径下，`--dbname` 表示 `SID/实例名`，不是 `service name`。

### 6. 如何执行完整端到端测试

```bash
source .venv/bin/activate
tests/e2e/run_docker_e2e.sh
```

### 7. GaussDB 为什么需要 `--gauss-user` 和 `--gauss-env-file`

GaussDB 当前同时使用两条数据库采集链路：
- 通过 SSH 切换到安装用户并加载环境后执行系统自带 `gs_check`
- 通过 openGauss Go 驱动直连数据库执行 SQL 采集

因此 `--gauss-user` 和 `--gauss-env-file` 仍然必需，用于保证 `gs_check`、`gaussdb`、`gsql` 等命令可用；而 `--db-host/--db-port/--db-username/--db-password/--dbname` 则用于 SQL 直连采集。
