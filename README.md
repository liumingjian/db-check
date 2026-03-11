# db-check

`db-check` 是一个面向数据库巡检场景的分层式工具链，当前主实现覆盖 MySQL 5.6 / 5.7 / 8.0，并提供从采集、分析、报告内容生成到模板化 Word 输出的完整闭环。

## 当前主链路

```text
db-collector
-> collector.log + manifest.json + result.json
-> db-analyzer + rule.json
-> summary.json
-> generate_report_meta.py
-> report-meta.json
-> db_report_preview.py
-> report.md + report-view.json
-> render_template_docx.py + mysql-template.docx
-> report.docx
```

## 文档入口

- [docs/README.md](/Users/lmj/projects/ai-project/db-check/docs/README.md)
  - 统一文档导航，适合新手和管理者先看
- [业务全景与实现流程.md](/Users/lmj/projects/ai-project/db-check/docs/architecture/业务全景与实现流程.md)
  - 详细业务流程、数据流、时序图、E2E 闭环说明
- [bin](/Users/lmj/projects/ai-project/db-check/bin)
  - 可执行入口与本地构建产物
- [contracts](/Users/lmj/projects/ai-project/db-check/contracts)
  - 冻结契约、Schema、样例 JSON
- [rules/mysql/rule.json](/Users/lmj/projects/ai-project/db-check/rules/mysql/rule.json)
  - MySQL 检查规则定义
- [reporter/templates](/Users/lmj/projects/ai-project/db-check/reporter/templates)
  - 运行时模板资产与样例数据
- [tests/e2e](/Users/lmj/projects/ai-project/db-check/tests/e2e)
  - Docker 多版本端到端测试与场景注入

## 环境初始化

所有 Python 命令必须在虚拟环境中运行：

```bash
python3 -m venv .venv
source .venv/bin/activate
scripts/init_python_env.sh
```

## 常用命令

### Collector

```bash
./bin/db-collector \
  --db-type mysql \
  --db-host 127.0.0.1 \
  --db-port 3306 \
  --db-username root \
  --db-password rootpwd \
  --dbname dbcheck \
  --output-dir ./runs
```

### Analyzer

```bash
source .venv/bin/activate
python3 analyzer/cli/db_analyzer.py \
  --manifest /path/to/manifest.json \
  --result /path/to/result.json \
  --rule rules/mysql/rule.json \
  --strict-schema \
  --out /path/to/summary.json
```

### 生成 Markdown 报告与报告视图

```bash
source .venv/bin/activate
python3 reporter/cli/generate_report_meta.py \
  --result /path/to/result.json \
  --summary /path/to/summary.json \
  --mysql-version 8.0 \
  --out /path/to/report-meta.json

python3 reporter/cli/db_report_preview.py \
  --result /path/to/result.json \
  --summary /path/to/summary.json \
  --meta /path/to/report-meta.json \
  --out-md /path/to/report.md \
  --out-json /path/to/report-view.json
```

### 生成最终 Word 报告

```bash
source .venv/bin/activate
python3 reporter/cli/render_template_docx.py \
  --report-md /path/to/report.md \
  --report-view /path/to/report-view.json \
  --template reporter/templates/mysql-template.docx \
  --out /path/to/report.docx
```

### Contracts 校验

```bash
source .venv/bin/activate
python3 tasks/validate_frozen_contracts.py \
  --manifest /path/to/manifest.json \
  --result /path/to/result.json \
  --summary /path/to/summary.json \
  --rule rules/mysql/rule.json \
  --strict-schema
```

### Docker E2E

```bash
source .venv/bin/activate
tests/e2e/run_docker_e2e.sh
```
