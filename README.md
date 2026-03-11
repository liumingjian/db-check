# db-check

`db-check` 是一个面向企业内网数据库巡检场景的双入口工具链，用于完成 MySQL 指标采集、规则分析和 Word 巡检报告生成。

当前正式入口只有两个：
- `db-collector`：采集数据库与 OS 指标，产出标准 `run` 目录
- `db-reporter`：基于 `run` 目录生成 `summary.json`、`report-meta.json`、`report-view.json` 和最终 `report.docx`

当前主实现覆盖 MySQL `5.6 / 5.7 / 8.0`。

## 核心能力

- 采集 MySQL 与 OS 关键巡检指标，输出标准化 contracts 产物
- 基于规则自动分析风险，生成结构化 `summary.json`
- 基于统一 `ReportView` 和 Word 模板生成正式巡检报告
- 支持 Linux / macOS / Windows 多平台发布包构建
- 支持 Docker 多版本 e2e 验证，保证采集、分析、报告链路一致

## 适用场景

- 客户内网离线巡检
- 实施工程师现场采集与回传
- 分析工程师基于采集结果生成标准报告
- 开发团队持续迭代采集、规则和报告能力

## Quick Start

本项目推荐两种使用方式：
- 方式一：编译运行
  - 适合本地验证、联调、交付前测试
- 方式二：源码运行
  - 适合开发、调试和排查问题

无论采用哪种方式，最终用户只需要完成两步：
1. 使用 `db-collector` 采集指标，生成 `run` 目录
2. 使用 `db-reporter` 基于 `run` 目录生成 Word 报告

---

## 方式一：编译运行

### 1. 环境要求

- Go `1.21+`
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

如需同时采集远程主机 OS（Linux over SSH），增加 SSH 参数：

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
- `summary.json`
- `report-meta.json`
- `report-view.json`
- `report.docx`

按需导出时还会包含：
- `report.md`

---

## 方式二：源码运行

### 1. 环境要求

- Go `1.21+`
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
├── summary.json
├── report-meta.json
├── report-view.json
├── report.docx
└── report.md                # 可选
```

各文件职责：
- `collector.log`：采集执行日志
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

如果采集结果本身缺失，再显式传入：

```bash
--mysql-version <version>
```

### 5. 如何执行完整端到端测试

```bash
source .venv/bin/activate
tests/e2e/run_docker_e2e.sh
```
