## 解压安装包并确认版本

1. 把 `{{PACKAGE}}.zip` 复制到数据库主机。如果不能在数据库主机上运行采集器，复制到一台能连通数据库的 Linux 主机。
2. 解压安装包，然后进入解压后的目录：

```bash
unzip {{PACKAGE}}.zip
cd {{PACKAGE}}
```

3. 给采集器加上执行权限：

```bash
chmod +x db-collector
```

4. 查看版本：

```bash
{{COLLECTOR}} --version
```

输出应为 `{{VERSION}}`。

如果主机上没有 `unzip` 命令，在其他电脑上解压，再把解压后的目录复制到主机。

本说明中的命令都在解压后的目录中执行。
