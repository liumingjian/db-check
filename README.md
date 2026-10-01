# db-check

`db-check` 是一个面向企业内网数据库巡检场景的工具链，用于完成数据库与主机指标采集、规则分析和 Word 巡检报告生成。

当前主要入口包括：
- `db-collector`：客户侧唯一可执行采集器，采集数据库与可选 OS 指标，产出标准 `run` 目录
- `db-web`：Web 报告生成服务。上传 ZIP 采集包 → 后端执行 reporter pipeline → WebSocket 推送日志/进度 → 下载结果 ZIP

当前正式实现覆盖：
- MySQL `5.6 / 5.7 / 8.0`
- Oracle `11g / 19c`
- GaussDB `505.2.1.SPC1000`

当前版本里，MySQL、Oracle 与 GaussDB 都已经打通以下完整链路：
- `db-collector` 采集数据库指标；只有显式提供 `--local`、`--os-only` 或远程 OS 参数时才采集 OS 指标
- Web 上传采集 ZIP 后自动识别 `db_type`，生成 `summary.json`、`report-meta.json`、`report-view.json` 和 `report.docx`
- 第一章“巡检总结”使用统一模板，关键指标会在 Word 报告中加粗高亮显示

## 核心能力

- 采集 MySQL / Oracle / GaussDB 与 OS 关键巡检指标，输出标准化 contracts 产物
- 基于规则自动分析风险，生成结构化 `summary.json`
- 基于统一 `ReportView` 和 Word 模板生成正式巡检报告
- 远程 OS 采集通过 SSH 下发临时 `db-osprobe` 二进制执行，避免依赖目标机 `sar/free/vmstat/iostat`
- GaussDB 数据库指标默认通过 openGauss Go 驱动 SQL-first 采集，并在 `run_dir/sql/` 保留原始 SQL 与结果
- 支持 Linux / Windows（x86_64 与 ARM64）多平台发布包构建
- 支持 MySQL / Oracle 的 Docker 多版本 e2e 验证，保证采集、分析、报告链路一致

## 适用场景

- 客户内网离线巡检
- 实施工程师现场采集与回传
- 分析工程师基于采集结果生成标准报告
- 开发团队持续迭代采集、规则和报告能力

## Quick Start

本项目推荐三种使用方式：
- 方式一：编译运行
  - 适合客户侧离线采集
- 方式二：源码运行
  - 适合开发、调试和排查问题
- 方式三：Web 报告服务（`db-web` + `web/` 前端）
  - 适合一键上传 ZIP 采集包并生成报告（带 WS 日志/进度）

无论采用哪种方式，最终用户只需要完成两步：
1. 使用 `db-collector` 采集指标，生成 `run` 目录
2. 将 `run` 目录压缩成 ZIP，在 Web 页面上传生成 Word 报告

---

## 方式一：编译运行

### 1. 环境要求

- Go `1.24+`

### 2. 编译采集器

```bash
make build
```

默认输出目录：
- `bin/`

### 3. 执行采集

MySQL 示例：

```bash
./bin/db-collector \
  --db-type mysql \
  --db-host 127.0.0.1 \
  --db-port 3306 \
  --db-username root \
  --db-password rootpwd \
  --dbname mysql
```

Oracle 示例：

```bash
./bin/db-collector \
  --db-type oracle \
  --db-host 127.0.0.1 \
  --db-port 1521 \
  --db-username system \
  --db-password oraclepwd \
  --dbname ORCL
```

GaussDB 示例：

```bash
./bin/db-collector \
  --db-type gaussdb \
  --db-host 10.250.0.157 \
  --db-port 8000 \
  --db-username root \
  --db-password Gauss_246 \
  --dbname postgres
```

默认输出目录是当前目录下的 `./runs`。未提供 OS 参数时不会采集 OS 指标，报告里的 OS 检查会保持“未评估”语义。

如需同时采集远程主机 OS（Linux over SSH），显式增加 SSH 参数。当前远程 OS 采集会通过 SSH 自动上传临时 helper 二进制执行，不依赖目标机预装 `sar/free/vmstat/iostat`：

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
  --os-password ATT@2022
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
  --os-password ATT@2022
```

GaussDB 路径补充说明：
- `--db-host/--db-port/--db-username/--db-password/--dbname` 用于 openGauss SQL 直连采集与报告元数据
- `--gauss-user` 和 `--gauss-env-file` 已废弃；SQL-first 采集不会使用主机侧 `gs_check`
- GaussDB 的 SQL 原始查询与结果落在 `run_dir/sql/`

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

### 4. 上传生成 Word 报告

采集成功后，`./runs/<run_id>/` 目录中应至少包含：
- `collector.log`
- `manifest.json`
- `result.json`
- `sql/`（GaussDB 结构化 SQL 原始输出）

将该 `run` 目录压缩成 ZIP，然后在 Web 页面上传生成 Word 报告。

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
  --dbname mysql
```

### 4. 上传生成报告

采集完成后，将 `./runs/<run_id>/` 压缩成 ZIP，在 Web 页面上传生成报告。源码内的 reporter pipeline 仍用于 `db-web` 后端实现和自动化测试，不作为客户侧命令入口。

### 5. 适用说明

源码运行方式适合：
- 调试采集逻辑
- 调试规则判定
- 调试 Web 报告生成链路
- 在不构建发布包的前提下快速验证完整链路

---

## 方式三：Web 报告服务（db-web + web/）

`db-web` 提供一个 Web 入口：上传 ZIP 采集包，服务端执行内部 reporter pipeline，并通过 WebSocket 推送日志/进度，最终下载结果 ZIP（只包含成功项的 `report.docx`）。

### 1. 环境要求

- Go `1.24+`
- Python `3.10+`（需安装 `requirements.txt` 依赖；推荐使用 `.venv`）
- Node.js（建议 `20+`，用于 `web/`）

### 2. 使用 PM2 一键启动（推荐）

PM2 会同时管理：
- `dbcheck-api`：后端 `db-web`（默认 `127.0.0.1:8080`）
- `dbcheck-web`：前端 Next dev server（默认 `:3000`）

说明：
- **PM2 只用于管理 Web 服务前后端**（`db-web` + `web/`），不管理 `db-collector` 这类一次性 CLI 任务。

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

最小链路验证（可选，需要一个已启用且已改过临时密码的账号）：

```bash
DBCHECK_SMOKE_USERNAME=<用户名> DBCHECK_SMOKE_PASSWORD=<密码> make pm2-smoke
```

启动（production，需先 build）：

```bash
make build-db-web
make web-build
make pm2-start-prod
```

说明：
- `ecosystem.config.cjs` 会自动读取仓库根目录 `.env`（不覆盖已 export 的同名变量），并内置本地联调默认值：`DBCHECK_DATA_DIR=/tmp/dbcheck-data`、`ALLOWED_ORIGINS=http://127.0.0.1:3000,http://localhost:3000` 等；如需覆盖，可修改 `.env` 或在 `pm2 start` 前设置同名环境变量，并用 `make pm2-restart` 刷新 env。
- 若用局域网地址打开前端（例如 `http://192.168.x.x:3000`），前端会默认请求同一主机的 `:8080` 后端；如后端在其他主机，可显式设置 `NEXT_PUBLIC_API_BASE` 或在生成页手动填写 API Base。

远程 Linux 部署示例：

```bash
cat > .env <<'EOF'
DBCHECK_ADDR=0.0.0.0:18080
DBCHECK_DATA_DIR=/tmp/dbcheck-data
ALLOWED_ORIGINS=*
EOF

make pm2-restart
```

当前页面在 `:3000` 时，前端默认把 API 推断为同一主机的后端端口；该端口会从 `DBCHECK_ADDR` 自动派生。上例中访问 `http://10.250.0.222:3000` 时，前端会自动请求 `http://10.250.0.222:18080`。内网自测可用 `ALLOWED_ORIGINS=*` 避免频繁换 IP 时反复改 CORS；若要收紧白名单，再改成具体 Origin 列表。

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

后端需要 2 个环境变量（共享令牌 `DBCHECK_API_TOKEN` 已停用：仍设置时会被忽略，启动日志给出告警；每个用户用自己的账号登录）：
- `DBCHECK_DATA_DIR`：任务落盘目录（例如 `/tmp/dbcheck-data`）
- `ALLOWED_ORIGINS`：前端 Origin 白名单（逗号分隔）。支持三种写法：
  - 完整 Origin：`http://127.0.0.1:3000`（推荐）
  - Host（含端口）：`127.0.0.1:3000` / `localhost:3000`（更宽松，适合本地联调）
  - `*`：允许任意 Origin（仅建议本地联调临时使用；生产环境不要用）

说明：
- 当 `ALLOWED_ORIGINS` 配置里包含 `localhost` / `127.0.0.1`（带端口）时，`db-web` 会自动放行同端口的本机局域网地址（例如 `http://192.168.x.x:3000`），避免你用 Next dev server 的 Network 地址打开前端时触发 CORS。

```bash
source .venv/bin/activate

export DBCHECK_DATA_DIR=/tmp/dbcheck-data
export ALLOWED_ORIGINS=http://127.0.0.1:3000,http://localhost:3000
# 本地快速联调（不建议在生产环境使用）：
# export ALLOWED_ORIGINS=*

# 首次使用：创建首个管理员，打印一次临时密码（首次登录需修改）
go run ./reporter/cmd/db-web admin create --username admin

go run ./reporter/cmd/db-web --addr 127.0.0.1:8080 --python-bin "$VIRTUAL_ENV/bin/python3"
```

### 5. 启动前端（web/）

前端用构建期开关 `NEXT_PUBLIC_API_MODE` 选择数据来源：`mock`（默认，数据存于浏览器 localStorage，可在用户菜单“重置 Mock 数据”）或 `real`（连接 db-web）。两者之间没有自动回退。real 模式通过 `NEXT_PUBLIC_API_BASE`（完整 Origin）指向后端（推荐）：

```bash
cd web
npm install
NEXT_PUBLIC_API_MODE=real NEXT_PUBLIC_API_BASE=http://127.0.0.1:8080 npm run dev
```

访问：`http://127.0.0.1:3000`

说明：
- 如果你忘了设置 `NEXT_PUBLIC_API_BASE`，前端会在运行时做一个本地开发推断：当页面在 `:3000` 时，默认后端为 `:8080`；否则默认同源。

### 6. 手动验证流程（MySQL）

1. 页面选择 MySQL
2. 上传 `/tmp/mysql-run.zip`（或 `/tmp/mysql-e2e.zip`）
3. 用自己的账号登录（首个管理员由 `db-web admin create` 创建，首次登录需修改临时密码），然后点击“生成报告”
4. 观察 WS 日志与进度，完成后点击下载，得到 `reports-<task_id>.zip`

### 7. 仅用 curl 验证 HTTP（可选）

```bash
API_BASE="http://127.0.0.1:8080"
# 用已启用且已改过临时密码的账号登录，取会话令牌
TOKEN="$(curl -sS -X POST -H "Content-Type: application/json" \
  -d '{"username":"<用户名>","password":"<密码>"}' \
  "${API_BASE}/api/auth/sign-in" | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')"

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

生成页会先做一次 API 探测；失败日志会显示当前 API 地址与页面 Origin，优先按这两个值核对后端监听地址和 `ALLOWED_ORIGINS`。

可以用预检请求快速定位是否是 CORS 配置问题（返回 204 且包含 `Access-Control-Allow-Origin` 为正常）：

```bash
API_BASE="http://127.0.0.1:8080"
curl -i -sS -X OPTIONS \
  -H "Origin: http://127.0.0.1:3000" \
  -H "Access-Control-Request-Method: POST" \
  "${API_BASE}/api/reports/generate"
```

### 9. 发布采集器版本（publish script）

采集器版本只能从 git tag 发布（ADR 0002）。CI 阶段上线前由人工在发布机上运行 `scripts/publish_release.sh`，CI 之后调用的也是同一个脚本。

前置条件：
- 服务端已设置 `DBCHECK_PUBLISH_TOKEN`（未设置时发布接口不可用），发布机持有同一个值
- 发布机有 Go、`git`、`zip`，在仓库根目录的干净检出上运行（已跟踪文件不能有未提交修改）
- HEAD 恰好位于 `vX.Y.Z` 或 `vX.Y.Z-rcN` tag 上，且 tag 去掉 `v` 后与采集器内置版本（`collector/internal/cli/config.go` 的 `Version`，即 `db-collector --version`）完全一致。发布预发布版时，先把 `Version` 改为 `X.Y.Z-rcN` 再打 tag
- 发布说明取自 tag 注释（`git tag -a`）；没有注释时，读取 `CHANGELOG.md` 中 `## [X.Y.Z]` 一节（文件可选）；两者都没有则拒绝发布

```bash
git tag -a v1.2.0 -m "- 新增 xxx
- 修复 yyy"        # 打在要发布的提交上
export DBCHECK_PUBLISH_TOKEN='<与服务端一致>'
scripts/publish_release.sh --url https://dbcheck.example.com
```

脚本依次：
1. 校验 tag 与内置版本、工作区是否干净、发布说明；任一不满足即报错退出，不会构建
2. 调用 `scripts/build_release_packages.sh` 构建四个发布包：`dist/db-collector-<version>-{linux,windows}-{amd64,arm64}.zip`（`--dist-dir` 可改输出目录）
3. 计算 SHA256，连同版本、tag、commit、发布说明、支持的数据库类型（脚本内维护：mysql、oracle、gaussdb，见 `reporter/internal/publish`）一起提交到 `POST /api/ci/releases`

结果：`vX.Y.Z` 发布为「最新」，原最新版转为「已弃用」；`vX.Y.Z-rcN` 发布为「预发布」，仅管理员可见。发布包是可复现构建（同一提交重建出的 zip 逐字节相同），因此同一 tag 重复运行是幂等的：内容一致时提示 `already published ... nothing changed` 并成功退出；同一版本内容不同（例如 tag 被移动）时服务端返回 409，脚本失败。

---

## 发布包构建

如果需要构建多平台交付包：

```bash
make release
```

默认输出目录：
- `dist/`

包名带采集器内置版本（可用 `VERSION=` 覆盖），生成后目录形态类似：

```text
dist/
├── db-collector-1.2.0-linux-amd64/
├── db-collector-1.2.0-linux-amd64.zip
├── db-collector-1.2.0-linux-arm64.zip
├── db-collector-1.2.0-windows-amd64.zip
└── db-collector-1.2.0-windows-arm64.zip
```

构建需要 `zip` 命令。发布到平台请用 `scripts/publish_release.sh`（见「方式三」第 9 节）。

每个发布包目录中都包含：
- `db-collector`
- `QUICKSTART.md`

报告生成不随客户侧采集包发布，统一通过 db-check Web 上传采集 ZIP 完成。

## E2E 覆盖

当前 Docker e2e 覆盖以下数据库版本：
- MySQL `5.6 / 5.7 / 8.0`
- Oracle `11g / 19c`

GaussDB 当前不承诺 Docker e2e。原因是可用镜像、内核版本和 openGauss 兼容行为与正式安装形态存在差异，当前以真实环境回归为准。

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
├── manifest.json
├── result.json
└── sql/                    # 仅 GaussDB，保留原始 SQL 与查询结果
```

各文件职责：
- `collector.log`：采集执行日志
- `sql/`：GaussDB 原始 SQL 查询与结果缓存，便于复核和排障
- `manifest.json`：本次运行的执行态描述
- `result.json`：原始采集结果

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
- 编译模式：直接执行 `./bin/db-collector`
- 源码模式：直接执行 `go run ./collector/cmd/db-collector`

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

### 1. Web 报告服务提示缺少 `jsonschema` 或 `python-docx`

在已激活的虚拟环境中执行：

```bash
make init-python
```

### 2. `run` 目录不完整，无法生成报告

Web 上传的采集 ZIP 至少要求 `run` 目录中存在：
- `manifest.json`
- `result.json`

### 3. MySQL 版本无法自动识别

优先检查 `result.json` 中是否存在：
- `db.basic_info.version`
- `db.basic_info.version_vars.version`
- `db.basic_info.summary.version`
- `db.basic_info.summary.gaussdb_version`

如果采集结果本身缺失，需要先修正采集结果，再重新上传生成报告。

### 4. Oracle 的 `--dbname` 表示什么

Oracle 路径下，`--dbname` 表示 `SID/实例名`，不是 `service name`。

### 6. 如何执行完整端到端测试

```bash
source .venv/bin/activate
tests/e2e/run_docker_e2e.sh
```

### 7. GaussDB 为什么不再需要 `--gauss-user` 和 `--gauss-env-file`

GaussDB 当前默认使用 openGauss Go 驱动直连数据库执行 SQL-first 采集。`--gauss-user` 和 `--gauss-env-file` 仍可被解析，但只作为废弃参数记录，不参与采集。
