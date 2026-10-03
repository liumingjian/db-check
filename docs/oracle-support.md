# Oracle 支持范围与使用说明

Collector 自动识别版本并选择查询，工程师无需声明版本。当前版本分支支持 11gR2、12c、19c、21c、23ai，18c 归入 12c 系列。11gR1 不在声明范围内。

| 版本 | 实现范围 | 本实现验证状态 |
| --- | --- | --- |
| 11gR2 | 非 CDB；补丁历史使用 DBA_REGISTRY_HISTORY；告警日志不采集 | 已有容器环境，远程 Mac 端到端 smoke 待验证 |
| 12c，含 18c | 探测 CDB、当前容器与可见 PDB；SQL 补丁与诊断告警视图 | 未经过容器验证 |
| 19c | 同 12c，并使用探测出的部署形态选择专用检查 | 已有容器环境，远程 Mac 端到端 smoke 待验证 |
| 21c | 同 12c | 未经过容器验证 |
| 23ai | 同 12c | 未经过容器验证 |

版本分支、源代码审查和已编写的 fixture 测试不能证明目标数据库 SQL 已成功运行。报告开头显示同一验证级别。权限不足、版本能力缺失和主机命令不可用都会形成覆盖缺口，不能解读为正常。

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

报告开头显示版本、验证级别、单机或 RAC、CDB、主备角色、ASM、当前容器以及可见 PDB 的状态和大小。Collector 始终在当前连接容器查询，不切换其他 PDB。CDB$ROOT 的实例状态视图可能反映整个实例，不能当作单个 PDB 的负载。完整 PDB 清单需要连接 CDB$ROOT 并有 `SYS.V_$PDBS` 读取权限；连接 PDB 的清单仅表示当前账号可见范围，报告明确标记不完整。看到 PDB 清单不代表已经巡检清单中的每个 PDB。

Data Guard 专用检查仅评估探测为 standby 的数据库，primary 的专用延迟、缺口与保护模式检查为不适用；primary 的归档目的地错误仍由常规恢复检查评估。V$ARCHIVE_GAP 仅显示当前阻塞缺口，单次 DATUM_TIME 不证明统计持续更新。ASM 检查仅在检测到 ASM 时运行，同时保留考虑冗余后的 USABLE_FILE_MB。RAC 参数按 inst_id 读取；GV$INSTANCE 只列出正在运行的实例，停机节点需结合 clusterware 的预期资源清单确认。

## 主机工具与许可边界

Listener 和 RAC clusterware 检查需要数据库主机访问。Collector 在数据库主机运行时使用 `--local`；远程使用 `--os-host`、`--os-port`、`--os-username`、`--os-password` 或 `--os-ssh-key-path`。访问账号必须具备 Oracle/Grid 软件属主所需的命令权限和 PATH。只连数据库、主机不正确、工具不在 PATH 或命令失败时，这些检查显示未采集，并提供手工操作建议。

Collector 执行 `lsnrctl status` 检查默认 listener，其他 listener 由客户 DBA 手工检查。RAC 执行 `crsctl check crs` 和 `crsctl status resource -t`，输出中的 OFFLINE 等异常需要对照计划停用资源判断。OS 数据来自所配置的主机；测试中的独立 os-target 不是 Oracle 主机，因此其 Oracle 工具未采集是预期限制。

遵循 [ADR 0004](adr/0004-collector-skips-licensed-oracle-diagnostics.md)，Collector 不查询 `dba_hist_*`、ASH、ADDM，也没有启用这些采集的开关。AWR 只作为工程师另行上传的输入。TDE 检查记录钱包状态与加密表空间数量，不读取密钥；未启用 TDE 的告警需要客户按安全政策和许可证条件评估。数据库链接仅通过可见的固定用户名提示可能保存凭据，不查询 SYS.LINK$、不读取密码，也不能完整证明密码存在。

SQL 补丁清单按最后成功操作判断，成功回滚会移除已安装记录；它不是 Oracle Home 二进制补丁清单，也不证明补丁最新。11g 历史缺少操作状态，当前 SQL 补丁清单和失败状态显示未采集，请客户 DBA 提供 `opatch lsinventory`。

## 阈值来源

容量沿用现有规则的 >80% 警告、>90% 严重；RMAN 成功备份年龄 >48 小时警告、>168 小时严重。没有成功备份会触发严重告警。Data Guard 延迟 >60/>300 秒、最大性能保护模式提示以及安全策略检查是项目默认调查基线，需客户 DBA 按批准的备份计划、RPO 和安全要求判断。

[空间与告警规则](oracle-space-alert.md) 说明临时空间真实块大小、autoextend 上限、SYSAUX >85/>90、最大段和回收站阈值，以及七天内最新 200 条 ORA 告警限制。[性能规则](oracle-performance-judgments.md) 列出等待、latch、资源、时间模型、UNDO 和并行度阈值及累计样本限制。已核实的参考值与本项目自行选择的值均在上述文档中区分。EasyDBA 与 DBCheck 双来源阈值核对结果尚待记录，在完成前不能声称所有新阈值均取两个参考平台中更保守的值。不提供客户阈值覆盖或数字健康分。

## 远程 Mac 验证计划

Issue #48 要求所有执行代码的测试通过指定的远程 Mac executor 运行。当前没有确认执行机，也没有本实现的 11gR2/19c smoke 通过证据。以下是待执行命令，不能视为执行记录。先在已确认的远程 Mac 检出集成分支，并进入该检出目录。复用现有适用 Python 环境，例如原项目的 `.venv`，不要为了 worktree 另建环境。

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
