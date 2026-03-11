# 模板说明

本目录存放的是参考模板和历史模板，不直接参与程序运行。

## 目录职责

- `docs/templates/reference/`
  - 参考模板、历史模板、原始样式来源

## 运行时模板资产在哪里

程序运行时真正会被读取的模板资产不在 `docs/`，而在：

- [reporter/templates/mysql-template.docx](/Users/lmj/projects/ai-project/db-check/reporter/templates/mysql-template.docx)
- [reporter/templates/report-meta.sample.json](/Users/lmj/projects/ai-project/db-check/reporter/templates/report-meta.sample.json)

这样设计的原因是：

- `docs/` 用于阅读和交付说明
- `reporter/templates/` 用于程序运行时消费
