# 文档导航

`docs/` 是当前项目的全局文档中心，面向三类读者：
- 客户使用者：只关心怎么采集、怎么生成 Word 报告
- 架构师：关注分层边界、contracts、规则和整条交付链路
- 开发者：关注内部实现、测试和后续演进

## Quick Start

如果你刚接触项目，先看根 [README.md](/Users/lmj/projects/ai-project/db-check/README.md)。

根 README 已经提供两套完整的上手路径：
1. 编译运行
2. 源码运行

当前客户主路径仍然只有两个入口：
- `db-collector`
- `db-reporter --run-dir ...`

## 建议阅读顺序

### 客户或实施人员

1. [README.md](/Users/lmj/projects/ai-project/db-check/README.md)
2. [业务全景与实现流程](/Users/lmj/projects/ai-project/db-check/docs/architecture/业务全景与实现流程.md)
3. [模板说明](/Users/lmj/projects/ai-project/db-check/docs/templates/README.md)

### 如果你负责架构或方案评审

1. [业务全景与实现流程](/Users/lmj/projects/ai-project/db-check/docs/architecture/业务全景与实现流程.md)
2. [最小架构规范](/Users/lmj/projects/ai-project/db-check/docs/architecture/最小架构规范.md)
3. [冻结契约说明](/Users/lmj/projects/ai-project/db-check/docs/specs/冻结契约说明.md)
4. [db-check contracts PRD](/Users/lmj/projects/ai-project/db-check/docs/specs/db-check-contracts-prd.md)

### 如果你负责报告与交付物

1. [报告内容设计](/Users/lmj/projects/ai-project/db-check/docs/reporting/报告内容设计.md)
2. [报告模板设计要点](/Users/lmj/projects/ai-project/db-check/docs/reporting/报告模板设计要点.md)
3. [template-mysql](/Users/lmj/projects/ai-project/db-check/docs/reporting/template-mysql.md)
4. [模板说明](/Users/lmj/projects/ai-project/db-check/docs/templates/README.md)

### 如果你负责开发与测试

1. [README.md](/Users/lmj/projects/ai-project/db-check/README.md)
2. [开发规范](/Users/lmj/projects/ai-project/db-check/docs/specs/开发规范.md)
3. [业务全景与实现流程](/Users/lmj/projects/ai-project/db-check/docs/architecture/业务全景与实现流程.md)
4. [tests/e2e/README.md](/Users/lmj/projects/ai-project/db-check/tests/e2e/README.md)

## 目录说明

- [architecture](/Users/lmj/projects/ai-project/db-check/docs/architecture)
  - 客户视角流程、内部实现链路、运行时序
- [specs](/Users/lmj/projects/ai-project/db-check/docs/specs)
  - PRD、开发规范、冻结契约等规范性文档
- [reporting](/Users/lmj/projects/ai-project/db-check/docs/reporting)
  - 报告内容设计、模板设计与表现基准
- [templates](/Users/lmj/projects/ai-project/db-check/docs/templates/README.md)
  - 参考模板、历史模板与运行时模板边界说明
- [governance](/Users/lmj/projects/ai-project/db-check/docs/governance)
  - 目录治理和文档治理规范
