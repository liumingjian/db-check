## 采集 GaussDB

复制以下命令，按下表替换示例值，然后执行：

```{{SHELL}}
{{COLLECTOR}} --db-type gaussdb --db-host 10.0.0.10 --db-port 8000 --db-username dbcheck --db-password 'ChangeMe' --dbname postgres
```

| 参数 | 示例值 | 替换为 |
| --- | --- | --- |
| `--db-host` | `10.0.0.10` | 数据库地址。在数据库主机上运行时，填 `127.0.0.1`。 |
| `--db-port` | `8000` | 数据库端口。端口是 8000 时，可以删除这个参数。 |
| `--db-username` | `dbcheck` | 客户 DBA 提供的账号。 |
| `--db-password` | `'ChangeMe'` | 这个账号的密码。保留两侧的单引号。 |
| `--dbname` | `postgres` | 连接时使用的库名。 |

不要使用 `--gauss-user` 和 `--gauss-env-file`。这两个参数已经废弃，采集器会忽略它们。

要同时采集主机指标，在命令末尾加上「采集主机指标」一节中的参数。

采集结束时，命令输出一行 `run_id=...`。等号后面的值是本次结果目录的名称。
