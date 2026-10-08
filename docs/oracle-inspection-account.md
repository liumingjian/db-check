# Oracle 巡检账号与覆盖缺口

由客户 DBA 使用 SQL*Plus 连接到需要巡检的数据库容器，然后执行：

```sql
SET DEFINE OFF
VARIABLE inspection_account VARCHAR2(30)
VARIABLE inspection_password VARCHAR2(128)
EXEC :inspection_account := 'DBCHECK';
-- Set the password bind in this trusted session without saving or spooling it.
EXEC :inspection_password := 'ReplaceWithYourPassword';
@scripts/oracle/create_inspection_account.sql
```

脚本读取绑定变量，不使用 SQL*Plus 文本替换。DDL 前验证账号以字母开头，只含 ASCII 字母、数字、下划线、`#` 或 `$`，最长 30 字符；密码为 8–128 位相同字符集。账号通过 `DBMS_ASSERT` 引用。请在可信 SQL*Plus 会话设置变量，关闭 SQL 日志并避免保存密码；执行结束清空密码变量。脚本创建账号，授予 `CREATE SESSION`、`SELECT_CATALOG_ROLE` 与当前状态视图的 `SELECT` 权限。未授予 DBA、SYSDBA、写权限或 SELECT ANY DICTIONARY。补充视图在旧版本不存在时会跳过；其他授权错误会终止脚本。

`SELECT_CATALOG_ROLE` 是 Oracle 管理的目录角色，其可读对象可能包括需要额外许可证的诊断视图。Collector 按 ADR 0004 仅查询自身的非授权诊断功能 SQL，不查询 AWR、ASH、ADDM，也没有开启这些采集的开关。需要更严格对象范围的客户可由 DBA 根据报告中的 `required_objects` 逐项授权。

11gR2 在普通非 CDB 数据库执行。12c 及以上在实际巡检连接容器执行本地授权；PDB 使用本地账号名，CDB$ROOT 使用符合 `COMMON_USER_PREFIX` 的通用账号名，默认例如 `C##DBCHECK`。脚本只授予当前容器权限。普通账号读取 `SYS.V_$PDBS` 成功仍可能因 `CONTAINER_DATA` 只看到部分 PDB。请 DBA 核对容器可见性并补充完整清单，或在 CDB$ROOT 使用 SYSDBA 采集来确认清单完整。Collector 保留已见 PDB，同时将无法证明完整性的清单标记为覆盖缺口。授权不会自动赋予其他 PDB 的访问权限，Collector 只巡检当前连接容器。

Collector 先识别版本，再执行零行 SELECT 权限预检，随后采集部署形态与指标。预检验证实际视图访问，不根据角色名称推断权限。每个正式查询仍记录读取成功或失败，成功的空结果与失败后保留的空占位数据有不同含义。

报告末尾列出权限不足与未采集的检查。请客户 DBA 按错误码确认视图存在，在报告标明的连接容器执行只读授权；重新使用原连接参数采集并生成报告。ORA-00942 也可能意味着版本不支持对象，需先确认视图存在。连接中断、版本能力缺失及未提供主机访问等情况标记为未采集，不能按正常处理。

`db.collection_availability` 以完整 dataset JSON 路径为键，值包含 `readable`、`required_objects`；失败时增加 `error_code`、`reason`、`remediation`。规则缺口保存在 `unevaluated_items`，`reason_type` 为 `insufficient_privilege` 或 `not_collected`，带检查名称、维度和操作建议。两个可选子计数属于 `counts.unevaluated`，不重复计入总数；存在缺口时整体风险最低为中风险。
