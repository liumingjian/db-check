## 采集主机指标

采集命令默认只采集数据库。要把主机的 CPU、内存和磁盘等指标写进报告，在采集命令末尾加上以下参数之一：

- 如果采集器运行在数据库主机上，加 `--local`。
- 如果采集器运行在另一台主机上，加 SSH 参数。采集器通过 SSH 登录数据库主机读取指标：

```bash
--os-host 10.0.0.10 --os-username root --os-password 'ChangeMe'
```

SSH 端口不是 22 时，加上 `--os-port`。要用私钥登录，把 `--os-password` 换成 `--os-ssh-key-path`，值为私钥文件的路径。如果不填 `--os-username` 和 `--os-password`，采集器使用数据库账号和密码登录 SSH。

`--local` 和 SSH 参数不能同时使用。
