# 数据库巡检平台 - 企业级产品需求文档（PRD v2.0, Contracts 驱动）

> 文档定位：高层需求文档（企业级 19 章模板）  
> 适用范围：DB-Check v2.0（MVP: MySQL）  
> 文档状态：已批准（替代旧链路 PRD）  
> 最后更新：2026-03-05

---

## 模板结构总览

1. 产品概述（Product Overview）  
2. 目标用户与画像（Target Users & Personas）  
3. 核心问题与机会（Problem Statement）  
4. 产品目标与成功指标（Goals & Metrics）  
5. 功能需求（Functional Requirements）  
6. 非功能需求（Non-Functional Requirements）  
7. 用户故事与用例（User Stories & Use Cases）  
8. 信息架构（Information Architecture）  
9. 数据模型设计（Data Model）  
10. API 设计（API Design）  
11. UI/UX 规范（UI/UX Specification）  
12. 技术栈与架构（Tech Stack & Architecture）  
13. 项目结构约定（Project Structure Conventions）  
14. 安全需求（Security Requirements）  
15. 测试策略（Testing Strategy）  
16. 部署与运维（Deployment & Operations）  
17. 分阶段交付计划（Phased Delivery Plan）  
18. 风险与缓解（Risks & Mitigations）  
19. 附录（Appendix）

---

## 1. 产品概述（Product Overview）

### 1.1 一句话描述
DB-Check 是面向数据库运维工程师的离线巡检工具套件，通过三层契约链路实现采集、分析、报告解耦与可审计交付。

### 1.2 产品背景
- 项目代号：DB-Check
- 当前版本：v2.0（Contracts 驱动）
- 旧版本状态：v1.x 单 JSON 链路已废弃并在 v2.0 移除
- 业务背景：保持原有客户内网离线巡检场景，不引入 Web 平台依赖

### 1.3 核心价值主张
- 保留原有采集器架构优势：Go 单二进制、低依赖、内网可落地
- 引入 contracts 三层契约：执行态、事实层、分析层职责分离
- 提升可追溯性：任何报告结论可回放到 `manifest/result/summary`

### 1.4 项目范围
- In Scope（v2.0 MVP）
- MySQL 巡检主链路：`db-collector -> analyzer -> db-reporter`
- 三层契约与 schema 门禁
- 旧参数兼容映射（优先兼容）
- Out of Scope（v2.0 MVP）
- Oracle 实际交付（仅路线图保留）
- Web UI、实时告警、自动调优执行

### 1.5 典型工作流程
1. 工程师在客户内网运行 `db-collector`，生成 `collector.log + manifest.json + result.json`。  
2. 将 `runs/<run_id>/` 目录离线传回公司环境。  
3. 运行 Analyzer CLI，生成 `summary.json`。  
4. 运行 `db-reporter`，基于 `result + summary + 模板` 生成 `report.docx`。

---

## 2. 目标用户与画像（Target Users & Personas）

### 2.1 用户角色定义
- 现场工程师：负责采集执行与离线回传
- 分析/报告工程师：负责分析与报告生成（同一角色可兼任）
- DBA/技术主管：审阅报告并制定优化方案
- 客户方运维：接收报告并跟踪整改

### 2.2 角色诉求
- 现场工程师：命令稳定、失败可定位、产物可追溯
- 分析/报告工程师：规则判定稳定、报告生成可复现
- 管理者：指标口径统一、风险判定一致

---

## 3. 核心问题与机会（Problem Statement）

### 3.1 当前痛点
- 旧链路“执行态仅在日志中可见”，审计与回放成本高
- 单 JSON 同时承载事实与结论，职责边界模糊
- 判定责任分散导致口径不一致

### 3.2 机会点
- contracts 提供冻结契约、schema、校验脚本，可作为统一执行标准
- 分层后可独立演进：采集、分析、报告相互解耦

---

## 4. 产品目标与成功指标（Goals & Metrics）

### 4.1 业务目标
- v2.0 建立 MySQL 可交付主链路，替代旧单 JSON 模式
- 将判定职责统一收敛到 Analyzer

### 4.2 KPI（按三层契约口径）
| 指标 | 目标值 | 统计口径 |
|---|---:|---|
| 单库采集耗时 | < 10 分钟 | `manifest.start_time/end_time` |
| 采集成功率 | > 95% | `manifest.module_stats` |
| 分析成功率 | 100%（有效输入） | `summary` 生成成功次数 |
| 报告生成成功率 | 100%（有效输入） | `report.docx` 产出率 |
| 契约校验通过率 | 100% | CI `--strict-schema` |

---

## 5. 功能需求（Functional Requirements）

### 5.1 功能总览
- 模块A（Collector）：沿用原架构与技术选型，输出执行态与事实层
- 模块B（Analyzer）：独立执行判定与缺失语义映射
- 模块C（Reporter）：仅消费 `result + summary` 生成 Word 报告
- 模块D（Contracts Gate）：schema 与一致性门禁

### 5.2 Collector（沿用原架构）
- 保持 Go 主体架构、本地/远程模式、gopsutil、go-ora+sqlplus 设计思想
- v2.0 MVP 实际交付范围仅 MySQL；Oracle 仅路线图提及
- `--os-only` 保留为旁路模式，不进入主链路契约
- 主链路输出产物必须为：`collector.log`、`manifest.json`、`result.json`

### 5.3 Analyzer（新增主责任组件）
- 输入：`manifest.json + result.json + rule.json`
- 输出：`summary.json`
- 负责：阈值判定、缺失语义映射、失败摘要生成、计数守恒
- Reporter 不再承担阈值判定职责

### 5.4 Reporter（职责收敛）
- 输入：`result.json + summary.json + template.docx`
- 输出：`report.docx`
- 负责：排版、章节组织、风险展示
- 不负责：规则计算与风险判定

### 5.5 缺失语义映射（冻结）
- `failed -> summary.unevaluated_items`
- `skipped -> summary.unevaluated_items`
- `not_applicable -> summary.na_items`

---

## 6. 非功能需求（Non-Functional Requirements）

### 6.1 性能
- 单库采集 < 10 分钟
- 报告生成 < 30 秒

### 6.2 可靠性
- 单项采集失败不阻断整体流程
- 退出码语义冻结：`0/10/20/30`

### 6.3 可审计性
- 每次巡检必须形成可回放运行目录
- 任何风险项必须可追溯到规则、原始事实与执行态

### 6.4 一致性
- 主链路数据必须通过 contracts schema 与一致性校验

---

## 7. 用户故事与用例（User Stories & Use Cases）

### 7.1 用户故事（MVP: MySQL）
- US-001 采集：作为现场工程师，我希望一次采集得到完整运行目录，便于离线回传与复核。  
- US-002 分析：作为分析工程师，我希望 Analyzer 统一产出风险结论，避免人工口径差异。  
- US-003 报告：作为报告工程师，我希望报告仅依赖 `result+summary`，提高可复现性。

### 7.2 端到端用例
1. `db-collector` 采集并写入 `runs/<run_id>/`。  
2. Analyzer CLI 读取同目录产物并写入 `summary.json`。  
3. `db-reporter` 生成 `report.docx`。  
4. 所有产物通过 CI 合同门禁后进入交付。

---

## 8. 信息架构（Information Architecture）

### 8.1 命令结构
```text
db-collector
├── remote/local 采集
└── --os-only（旁路模式）

python3 analyzer/cli/db_analyzer.py
└── manifest + result + rule -> summary

db-reporter
└── result + summary + template -> report
```

### 8.2 数据流向
```text
Collector -> manifest.json
          -> result.json
          -> collector.log
                  |
                  v
              Analyzer -> summary.json
                  |
                  v
          Reporter -> report.docx
```

### 8.3 运行目录（强制）
```text
runs/<run_id>/
  collector.log
  manifest.json
  result.json
  summary.json
  report.docx
```

---

## 9. 数据模型设计（Data Model）

### 9.1 三层契约摘要
- `manifest.json`：执行态（状态、退出码、产物索引）
- `result.json`：事实层（仅采集事实）
- `summary.json`：分析层（风险结论、缺失映射、失败摘要）

### 9.2 字段与 schema 来源
- 冻结说明：[`docs/specs/冻结契约说明.md`](/Users/lmj/projects/ai-project/db-check/docs/specs/冻结契约说明.md)
- MVP 架构：[`docs/architecture/最小架构规范.md`](/Users/lmj/projects/ai-project/db-check/docs/architecture/最小架构规范.md)
- Schema：
- [`contracts/schemas/manifest.schema.json`](contracts/schemas/manifest.schema.json)
- [`contracts/schemas/result.schema.json`](contracts/schemas/result.schema.json)
- [`contracts/schemas/summary.schema.json`](contracts/schemas/summary.schema.json)
- [`contracts/schemas/rule.schema.json`](contracts/schemas/rule.schema.json)

### 9.3 退出码语义（冻结）
- `0`：成功
- `10`：部分成功（可继续分析）
- `20`：采集失败（需 failure 摘要）
- `30`：前置失败（需 failure 摘要）

---

## 10. API 设计（API Design）

### 10.1 Collector CLI
```bash
db-collector --db-type mysql [连接参数] [采集参数]
```

- run_id 规则：`<db_type>-<host>-<yyyymmddThhmmssZ>`（全局唯一）
- 兼容策略：旧参数优先兼容，映射到新契约字段
- 示例映射：
| 旧参数 | 新口径 |
|---|---|
| `--os-collect-interval` | `collect_config.sample_interval_seconds` |
| `--os-collect-duration` | `collect_config.sample_period_seconds` |

### 10.2 Analyzer CLI
```bash
python3 analyzer/cli/db_analyzer.py --manifest <manifest.json> --result <result.json> --rule <rule.json> --out <summary.json>
```

### 10.3 Reporter CLI
```bash
db-reporter --run-dir <runs/<run_id>>
```

### 10.4 规则治理
- 规则 JSON 为单一真源
- 运行输入遵循 `rule.schema.json`
- Reporter 禁止直接执行规则判定

### 10.5 时间字段例外（显式偏离记录）
- 合同主规范仍为 contracts；但 v2.0 业务解释层采用“本地时区优先”原则
- 具体要求：
- 时间字段仍建议 RFC3339 表达
- 报表展示、文件命名、默认解析以采集机本地时区语义为准
- 该例外需在版本与风险章节持续追踪，后续版本可收敛

---

## 11. UI/UX 规范（UI/UX Specification）

### 11.1 CLI 输出分层
- `collector.log`：完整执行日志
- stderr：人类可读摘要（阶段、成功/失败统计、产物路径）
- 结构化产物：`manifest/result/summary`

### 11.2 报告输出
- 报告输入为 `result+summary`
- 保持 Word 模板生成规范与章节布局

---

## 12. 技术栈与架构（Tech Stack & Architecture）

### 12.1 技术选型
- Collector：Go（保留原架构）
- Analyzer：Python（契约判定与映射）
- Reporter：Python（报告渲染）
- Rule：JSON Schema 约束

### 12.2 职责边界
- Collector：采集与产物编排，不做最终风险判定
- Analyzer：唯一判定责任方
- Reporter：纯展示层

---

## 13. 项目结构约定（Project Structure Conventions）

```text
db-check/
├── collector/
├── analyzer/
├── reporter/
├── contracts/
│   ├── schemas/
│   ├── contract-freeze.md
│   └── 最小架构规范.md
├── tasks/
└── runs/
```

- 保留 monorepo 思路
- 增加 analyzer 目录作为一等组件

---

## 14. 安全需求（Security Requirements）

- 保留旧安全基线：凭据不落盘、日志脱敏、最小权限
- 契约文件不得包含凭据与密钥
- CI 门禁必须校验 contracts 结构一致性

---

## 15. 测试策略（Testing Strategy）

### 15.1 门禁原则
- CI 强制执行 strict schema 校验（不可降级）
- 未通过 contracts 校验禁止发布

### 15.2 核心测试
- 缺失语义映射一致性：`failed/skipped/not_applicable`
- 失败摘要一致性：`exit_code=20/30` 与 `summary.failure` 对齐
- 计数守恒：`total = normal + warning + critical + unevaluated + not_applicable`
- CLI 责任边界：三命令职责不重叠

### 15.3 版本矩阵（MVP）
- 仅 MySQL：5.7 / 8.0 / 8.4
- Oracle 在路线图阶段定义，不纳入 v2.0 发布门槛

---

## 16. 部署与运维（Deployment & Operations）

- 维持离线部署背景：客户内网采集、公司环境分析与报告
- 主链路采用分段执行，不要求在线联动
- 运维对象以运行目录为最小审计单元

---

## 17. 分阶段交付计划（Phased Delivery Plan）

### 17.1 v2.0（单阶段 MySQL MVP）
- 交付三层契约主链路
- 完成 Analyzer 判定闭环
- 完成 strict schema CI 门禁
- 完成 MySQL 版本矩阵验证

### 17.2 路线图（非 v2.0 交付范围）
- Oracle 链路纳入后续版本规划
- 保留 go-ora + sqlplus 降级设计思想

---

## 18. 风险与缓解（Risks & Mitigations）

| 风险 | 影响 | 缓解措施 |
|---|---|---|
| 旧链路移除导致使用习惯冲击 | 中 | 文档明确 v2.0 起废弃并移除，提供迁移说明 |
| 规则迁移到 JSON 的学习成本 | 中 | 提供 schema 示例与校验脚本，统一发布口径 |
| 时间字段本地时区优先例外导致理解偏差 | 中 | 在版本策略中显式记录偏离，统一解释与回溯口径 |
| 多组件分段执行增加操作步骤 | 低 | 统一运行目录规范与命令模板 |

---

## 19. 附录（Appendix）

### 19.1 术语表
- Collector：采集组件
- Analyzer：分析判定组件
- Reporter：报告生成组件
- Contracts：冻结契约与 schema 规范集合
- Run Directory：一次巡检的完整产物目录

### 19.2 版本与兼容策略
- v2.0 起旧链路（单 JSON + reporter 判定）立即移除
- 旧数据不兼容，需迁移转换后导入新链路
- 时间字段本地时区优先为 v2.0 显式例外策略，后续版本可收敛

### 19.3 参考链接
- [冻结契约说明.md](/Users/lmj/projects/ai-project/db-check/docs/specs/冻结契约说明.md)
- [最小架构规范.md](/Users/lmj/projects/ai-project/db-check/docs/architecture/最小架构规范.md)
- [contracts/schemas/manifest.schema.json](contracts/schemas/manifest.schema.json)
- [contracts/schemas/result.schema.json](contracts/schemas/result.schema.json)
- [contracts/schemas/summary.schema.json](contracts/schemas/summary.schema.json)
- [contracts/schemas/rule.schema.json](contracts/schemas/rule.schema.json)

### 19.4 变更记录
| 日期 | 版本 | 变更内容 |
|---|---|---|
| 2026-03-05 | v2.0 | 新建 contracts 驱动 PRD，替代旧链路定义 |
