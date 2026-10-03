# Oracle 支持范围与使用说明

Collector 自动识别版本并选择查询，工程师无需声明版本。当前版本分支支持 11gR2、12c、19c、21c、23ai，18c 归入 12c 系列。11gR1 不在声明范围内。

| 版本 | 实现范围 | 本实现验证状态 |
| --- | --- | --- |
| 11gR2 | 非 CDB；补丁历史使用 DBA_REGISTRY_HISTORY；告警日志不采集 | 2026-10-03 Mac 容器端到端 smoke 通过，单机 primary、非 ASM |
| 12c，含 18c | 探测 CDB、当前容器与可见 PDB；SQL 补丁与诊断告警视图 | 未经过容器验证 |
| 19c | 同 12c，并使用探测出的部署形态选择专用检查 | 2026-10-03 Mac 容器端到端 smoke 通过，单机 primary、非 ASM、连接 CDB$ROOT |
| 21c | 同 12c | 未经过容器验证 |
| 23ai | 同 12c | 未经过容器验证 |

版本分支、源代码审查和已编写的 fixture 测试不能证明目标数据库 SQL 已成功运行。报告开头显示同一验证级别。权限不足、版本能力缺失和主机命令不可用都会形成覆盖缺口，不能解读为正常。

上述 smoke 不覆盖 RAC、ASM 或 standby 容器。其状态与适用性分支由 fixture 和单元测试验证；生产部署仍需按实际拓扑核对。19c smoke 只巡检连接的 CDB$ROOT，没有切换到其他 PDB。

## 连接与巡检账号

```bash
# 现有 SID 参数保持兼容
./db-collector --db-type oracle --db-host DB_HOST --db-port 1521 \
  --db-username DBCHECK --db-password 'PASSWORD' --dbname ORCL

# service name 可连接 PDB
./db-collector --db-type oracle --db-host DB_HOST --db-port 1521 \
  --db-username DBCHECK --db-password 'PASSWORD' --oracle-service-name ORCLPDB1

# SYS 自动启用 SYSDBA；其他具有该权限的账号显式加 --oracle-sysdba
./db-collector --db-type oracle --db-host DB_HOST --db-port 1521 \
  --db-username SYS --db-password 'PASSWORD' --oracle-service-name ORCLCDB
```

`--dbname` 表示 SID，与 `--oracle-service-name` 必须二选一。用户名 SYS 的识别不区分大小写，两种连接方式均支持 `--oracle-sysdba`。此变更没有添加 Wallet、TNS、SSL 或 SSH 数据库隧道支持。客户 DBA 应先创建 [只读巡检账号](oracle-inspection-account.md)，授权脚本是 `scripts/oracle/create_inspection_account.sql`。SYSDBA 是兼容已有账号的连接选项，不是巡检账号的必要权限。

## 容器与部署形态

报告开头显示版本、验证级别、单机或 RAC、CDB、主备角色、ASM、当前容器以及可见 PDB 的状态和大小。Collector 始终在当前连接容器查询，不切换其他 PDB。CDB$ROOT 的实例状态视图可能反映整个实例，不能当作单个 PDB 的负载。普通账号具有 `SYS.V_$PDBS` 读取权限仍可能受 `CONTAINER_DATA` 过滤。请 DBA 核对容器可见性并补充完整清单，或在 CDB$ROOT 使用 SYSDBA 采集来确认完整性。普通账号或连接 PDB 的清单保留已见记录，并标记覆盖不完整。看到 PDB 清单不代表已经巡检清单中的每个 PDB。

Data Guard 专用检查仅评估探测为 standby 的数据库，primary 的专用延迟、缺口与保护模式检查为不适用；primary 的归档目的地错误仍由常规恢复检查评估。V$ARCHIVE_GAP 仅显示当前阻塞缺口，单次 DATUM_TIME 不证明统计持续更新。ASM 检查仅在检测到 ASM 时运行，同时保留考虑冗余后的 USABLE_FILE_MB。RAC 参数按 inst_id 读取；GV$INSTANCE 只列出正在运行的实例，停机节点需结合 clusterware 的预期资源清单确认。

## 主机工具与许可边界

Listener 和 RAC clusterware 检查需要数据库主机访问。Collector 在数据库主机运行时使用 `--local`；远程使用 `--os-host`、`--os-port`、`--os-username`、`--os-password` 或 `--os-ssh-key-path`。访问账号必须具备 Oracle/Grid 软件属主所需的命令权限和 PATH。只连数据库、主机不正确、工具不在 PATH、命令超时或无有效输出时，这些检查显示未采集，并提供手工操作建议。命令返回明确的 TNS/CRS 故障输出时保留真实输出和执行结果，由规则判断风险。

Collector 执行 `lsnrctl status` 检查默认 listener，其他 listener 由客户 DBA 手工检查。RAC 执行 `crsctl check crs` 和 `crsctl status resource -t`，输出中的 OFFLINE 等异常需要对照计划停用资源判断。OS 数据来自所配置的主机；测试中的独立 os-target 不是 Oracle 主机，因此其 Oracle 工具未采集是预期限制。

遵循 [ADR 0004](adr/0004-collector-skips-licensed-oracle-diagnostics.md)，Collector 不查询 `dba_hist_*`、ASH、ADDM，也没有启用这些采集的开关。AWR 只作为工程师另行上传的输入。TDE 检查记录钱包状态与加密表空间数量，不读取密钥；未启用 TDE 的告警需要客户按安全政策和许可证条件评估。数据库链接仅通过可见的固定用户名提示可能保存凭据，不查询 SYS.LINK$、不读取密码，也不能完整证明密码存在。

SQL 补丁清单按最后成功操作判断，成功回滚会移除已安装记录；它不是 Oracle Home 二进制补丁清单，也不证明补丁最新。11g 历史缺少操作状态，当前 SQL 补丁清单和失败状态显示未采集，请客户 DBA 提供 `opatch lsinventory`。

## 阈值来源

容量沿用现有规则的 >80% 警告、>90% 严重；RMAN 成功备份年龄 >48 小时警告、>168 小时严重。没有成功备份会触发严重告警。Data Guard 延迟使用 >30 秒警告、>60 秒严重的保守调查基线。最大性能保护模式与安全策略的风险分级见参考阈值对比，需客户 DBA 按批准的备份计划、RPO 和安全要求判断。

[空间与告警规则](oracle-space-alert.md) 说明临时空间真实块大小、autoextend 上限、SYSAUX >80/>90、最大段和回收站阈值，以及七天内最新 200 条 ORA 告警限制。[性能规则](oracle-performance-judgments.md) 列出等待、latch、资源、时间模型、UNDO 和并行度阈值及累计样本限制。[参考阈值对比](oracle-threshold-reference.md) 记录 EasyDBA 与 DBCheck 的固定版本、可比指标及本项目基线。没有同指标参考值的检查不声称双来源一致。不提供客户阈值覆盖或数字健康分。

## Mac 验证记录

2026-10-03 确认工具实际运行在 Darwin/arm64 Mac 上，测试均在该 Mac 执行环境运行，未在服务器执行。集成分支为 `codex/oracle-spec-48`，smoke 源码提交为 `fc5771a15700a8ebb071f58f86738ad311ec224a`。复用 `/Users/lmj/projects/ai-project/devs/db-check/.venv`，没有新建 Python 环境。

| 检查 | 结果 |
| --- | --- |
| `make test-go` | 全部 Go 测试通过 |
| Analyzer | 38 项通过 |
| Reporter | 50 项通过，其中 2 项因缺少本地 node-level WDR fixture 跳过 |
| Integration | 6 项通过，包括 Oracle 与上传 AWR 链路 |
| Oracle smoke validator 单元测试 | 4 项通过 |
| Oracle 11gR2 Docker smoke | 退出码 0，SQL 错误 0，严格产物验收通过 |
| Oracle 19c Docker smoke | 退出码 0，SQL 错误 0，严格产物验收通过 |

11gR2 的 70 项检查为正常 33、警告 10、严重 9、未采集 5、不适用 13、权限不足 0。预期未采集项为 11g 告警日志与 SQL 补丁状态能力，以及测试 OS 主机缺少 Oracle listener 工具。日志保存在执行机 `/tmp/db-check-spec-48/smoke-acceptance.log`，产物为 `tests/e2e/runs/20261003T212242/oracle-11g/oracle-127.0.0.1-20261003T132258Z/`。这些本地产物与日志不是发布包内容。

19c 的 70 项检查为正常 34、警告 13、严重 9、未采集 2、不适用 12、权限不足 0。预期未采集项为测试 OS 主机缺少 listener 工具，以及普通 CDB$ROOT 账号不能证明完整 PDB 可见性。产物为 `tests/e2e/runs/20261003T212242/oracle-19c/oracle-127.0.0.1-20261003T132954Z/`，日志与 11gR2 同文件。两版合并运行退出码为 0，分别通过严格产物验收。

以下命令用于复现验证。两个 Oracle 版本均已通过 #56 的 smoke 验收。

```bash
git switch codex/oracle-spec-48
source /Users/lmj/projects/ai-project/devs/db-check/.venv/bin/activate
uv run --active --no-project python -m unittest discover -s tests/analyzer -p 'test_*.py'
uv run --active --no-project python -m unittest discover -s tests/reporter -p 'test_*.py'
uv run --active --no-project python -m unittest discover -s tests/integration -p 'test_*.py'
uv run --active --no-project python -m unittest discover -s tests/e2e -p 'test_*.py'
make test-go
tests/e2e/run_docker_e2e.sh --db-type oracle --oracle-version 11g
tests/e2e/run_docker_e2e.sh --db-type oracle --oracle-version 19c
```

E2E 脚本在隔离的测试项目中启动、初始化并清理容器，不应指向客户环境。11g 镜像需要 amd64 支持，19c 镜像为 arm64。脚本调用 Oracle 产物验收，数据库 SQL 错误不能以报告生成成功代替通过。预期缺口仅包括 11g 告警/补丁能力、Oracle 主机工具不可用、PDB 可见范围，以及有明确说明的空性能样本。需要保留每个版本的提交 SHA、命令退出码、终端日志和 artifacts 路径。人工检查生成的 Word 报告开头部署形态/PDB 与末尾缺口建议，核对 result.json 的 SQL 证据及 summary.json 的检查状态。修复测试或 SQL 失败后重新运行对应版本，两个版本分别通过才满足 #56 的 smoke 验收。
