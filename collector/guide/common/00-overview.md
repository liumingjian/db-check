# db-collector {{VERSION}} 使用说明

本说明适用于安装包 `{{PACKAGE}}.zip`。

采集器读取数据库和主机的诊断信息，把结果写入一个目录。把这个目录压缩成 ZIP 并上传到 db-check 平台后，平台生成 Word 巡检报告。一个采集器支持 MySQL、Oracle 和 GaussDB 三种数据库。

按以下顺序操作：

1. 解压安装包并确认版本。
2. 按数据库类型执行采集。
3. 把采集结果压缩成 ZIP。
4. 上传 ZIP 并生成报告。

安装包包含以下文件：

- `{{BINARY}}`：采集器。
- `GUIDE.md`：本说明。
- `oracle/create_inspection_account.sql`：创建 Oracle 巡检账号的脚本，只在巡检 Oracle 时使用。
