## 解压安装包并确认版本

1. 把 `{{PACKAGE}}.zip` 复制到数据库主机。如果不能在数据库主机上运行采集器，复制到一台能连通数据库的 Windows 主机。
2. 右键单击 ZIP 文件，选择 **全部解压缩**。
3. 在解压出的文件中，打开 `{{BINARY}}` 所在的文件夹。
4. 按住 Shift 键，右键单击文件夹空白处，选择 **在此处打开 PowerShell 窗口**。Windows 11 上选择 **在终端中打开**。
5. 在 PowerShell 中查看版本：

```powershell
{{COLLECTOR}} --version
```

输出应为 `{{VERSION}}`。

本说明中的命令都在 PowerShell 中执行。不要使用命令提示符 `cmd`，因为 `cmd` 不识别命令中的单引号。
