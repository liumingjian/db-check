## 采集 Oracle

### 创建巡检账号

采集器使用只读的巡检账号连接 Oracle。如果客户已经有巡检账号，跳过这一步。

请客户 DBA 在安装包目录中启动 SQL*Plus，连接要巡检的数据库，然后执行以下语句。执行前把 `DBCHECK` 和 `ChangeMe` 换成实际的账号和密码：

```sql
SET DEFINE OFF
VARIABLE inspection_account VARCHAR2(30)
VARIABLE inspection_password VARCHAR2(128)
EXEC :inspection_account := 'DBCHECK';
EXEC :inspection_password := 'ChangeMe';
@oracle/create_inspection_account.sql
```

如果数据库是 12c 或更高版本，在要巡检的容器中执行。在 `CDB$ROOT` 中创建账号时，账号名要带 `C##` 前缀，例如 `C##DBCHECK`。

### 执行采集

复制以下命令，按下表替换示例值，然后执行：

```{{SHELL}}
{{COLLECTOR}} --db-type oracle --db-host 10.0.0.10 --db-port 1521 --db-username DBCHECK --db-password 'ChangeMe' --dbname ORCL
```

| 参数 | 示例值 | 替换为 |
| --- | --- | --- |
| `--db-host` | `10.0.0.10` | 数据库地址。在数据库主机上运行时，填 `127.0.0.1`。 |
| `--db-port` | `1521` | 监听端口。端口是 1521 时，可以删除这个参数。 |
| `--db-username` | `DBCHECK` | 巡检账号。 |
| `--db-password` | `'ChangeMe'` | 巡检账号的密码。保留两侧的单引号。 |
| `--dbname` | `ORCL` | 实例的 SID。 |

要通过服务名连接，例如连接一个 PDB，把 `--dbname ORCL` 换成 `--oracle-service-name PDB1`。这两个参数只能使用一个。

要同时采集主机指标，在命令末尾加上「采集主机指标」一节中的参数。

采集结束时，命令输出一行 `run_id=...`。等号后面的值是本次结果目录的名称。
