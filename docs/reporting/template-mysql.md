**文档控制**

**文档信息**

|  |  |
| --- | --- |
| **文档属性** | **内容** |
| 文档名称 | 四川中行MySQL巡检报告 |
| 巡检时间 | 2026/02/26 17:31:49 |
| 出具日期 | 2026/02/28 |
| 巡检人员 | 刘明建 |
| 版本 | v1.0 |

**修改记录**

|  |  |  |  |
| --- | --- | --- | --- |
| **日期** | **作者** | **版本** | **修改记录** |
| 2025/02/26 | 刘明建 | v1.0 | 模版创建 |
|  |  |  |  |
|  |  |  |  |

**审阅记录**

|  |  |  |  |
| --- | --- | --- | --- |
| **姓名** | **职位** | **联系方式** | **邮箱** |
| 周海波 | 数据库技术经理 | 13570391044 | haibo.zhou@antute.com.cn |

目录

文档控制 2

文档信息 2

修改记录 2

审阅记录 2

第一章 巡检总结 5

1.1 巡检告警定义 5

1.2 巡检范围 5

1.3 巡检总结 5

1.4 风险与建议 5

第二章 巡检明细 7

2.1 系统指标 7

2.2 MySQL基础信息 7

2.2.1 实例基础信息 7

2.2.2 主从集群状态 7

2.2.3 mysql关键参数 7

2.2.4 数据库容量 8

2.2.5 数据库用户 8

2.2.6 数据库对象数量 8

2.2.7 运行线程信息 8

2.2.8 数据库文件信息 8

2.2.9 数据库状态信息 8

2.3 数据库性能检查 10

2.3.1 InnoDB详细信息 10

2.3.2 InnoDB锁等待 10

2.3.3 元数据锁信息 10

2.3.4 连接数检查 10

2.3.5 占用空间top 10的表 11

2.3.6 占用空间top 10的索引 11

2.3.7 没有主键或唯一键的表 11

2.3.8 非Innodb引擎的数据对象 11

2.3.9 单张表超过6个索引的对象 11

2.3.10 联合索引的字段个数大于4的对象 11

2.3.11 单张表字段个数大于50的对象 11

2.3.12 物理IO top 10的表 11

2.3.13 数据库内存分布top 10 12

2.3.14 慢SQL top10 12

2.3.15 全表扫描的SQL top10 12

2.3.16 全表扫描的表top10 12

2.3.17 使用临时表的SQL top10 12

2.3.18 行操作次数top10 12

2.3.19 未使用的索引 13

2.3.20 自增值使用率top10 13

2.3.21 冗余索引 13

2.4 数据库备份 13

2.4.1 备份策略 13

2.4.2 备份集可用性检查 13

# 巡检总结

## 巡检告警定义

|  |  |  |  |
| --- | --- | --- | --- |
| **风险等级** | **标识** | **定义** | **建议响应时效** |
| 高风险 | 🔴 | 经检查发现的最高级别告警，基于问题严重性、时间紧迫性、影响范围等综合判断，可能影响业务连续性或数据安全，建议立即处理。 | 24小时内 |
| 中风险 | 🟡 | 经检查发现的中等级别告警，一般为仍处在发展变化中且尚未转化为高风险的问题，建议关注发展趋势并尽快安排处理。 | 1~2周内 |
| 低风险 | 🔵 | 经检查未发现严重问题，但存在不符合最佳实践或可优化的空间，建议结合实际情况在合适窗口处理。 | 1~3个月内 |
| 正常 | 🟢 | 经检查该项指标符合行业最佳实践标准，数据库及操作系统运行良好，无需处理。 | 持续保持 |

## 巡检范围

|  |  |
| --- | --- |
| **项目** | **内容** |
| 巡检对象 |  |
| 实例清单 |  |
| 数据库版本 |  |
| 架构角色 |  |

## 综合健康评估

|  |  |  |
| --- | --- | --- |
| **检查维度** | **风险** | **关键发现** |
| 操作系统资源 | 🟢 | CPU平均使用率 15%，内存使用率 62%，磁盘IO正常，各项资源充足。 |
| 数据库可用性 | 🟢 | 主从复制正常，延迟 0 秒，GTID 一致，实例持续运行 100 天无异常重启。 |
| 数据库性能 | 🟡 | 存在慢SQL（最大耗时 21.05s），全表扫描表 2 个，临时表使用需关注。 |
| 安全配置 | 🟡 | root 使用 mysql\_native\_password 认证，app\_user 授权 host 为 %（全网段开放）。 |
| 备份与恢复 | 🔵 | 备份策略已配置（物理备份每天1次+NBU远程），但备份集可用性验证脚本待补充。 |
| 参数配置 | 🟢 | 双一配置正常（sync\_binlog=1, innodb\_flush\_log\_at\_trx\_commit=1），关键参数符合最佳实践。 |
| 对象与索引 | 🔵 | 存在 130 个未使用索引和 10 个冗余索引，2 张表字段超过 50 个。 |
| 容量规划 | 🟢 | 数据总量 2.58 GB，连接数使用率 2%，自增值使用率正常，容量充足。 |
| 操作系统资源 | 🟢 | CPU平均使用率 15%，内存使用率 62%，磁盘IO正常，各项资源充足。 |

## 风险发现与整改建议

以下仅列出存在风险的检查项，按风险等级从高到低排序。评估为"正常"的检查项不在此表列出。

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **风险等级** | **检查维度** | **风险描述** | **影响分析** | **整改建议** |
| 🟡 | 性能 | 慢SQL最大耗时达21.05秒，共28次执行，平均耗时751ms | 高峰期可能导致连接堆积、用户响应超时，严重时引发雪崩效应 | 1. 对TOP 10慢SQL添加合适索引；2. 优化全表扫描查询，具体SQL及优化方案见4.8节 |
| 🔵 | 安全 | root账户使用mysql\_native\_password认证插件 | MySQL 8.0默认推荐caching\_sha2\_password，旧插件安全性较低 | 升级认证插件：ALTER USER 'root'@'localhost' IDENTIFIED WITH caching\_sha2\_password BY '\*\*\*'; |
| 🔵 | 索引 | 存在130个自实例启动以来未使用的索引 | 冗余索引占用磁盘空间，降低INSERT/UPDATE/DELETE性能，增加DDL耗时 | 确认业务无使用后执行清理 |

## 巡检结论

本次巡检共检查 **8** 个维度、42个检查项。其中发现高风险0 项、中风险 3项、低风险 4项。

数据库整体运行状况良好，主从复制正常，关键参数配置符合最佳实践。主要风险集中在 **SQL性能优化** 和 **安全配置加固** 两个方面，建议在未来1~2周内优先处理中风险项。低风险项建议纳入日常运维优化计划，在合适的变更窗口逐步完成整改。

本次巡检未发现影响业务连续性的高风险问题。

# 巡检明细

## 系统指标

## MySQL基础信息

### 实例基础信息

|  |  |
| --- | --- |
| **参数名称** | **当前值** |
| 实例地址 | 192.168.1.101 |
| 端口 | 3307 |
| MySQL版本 | 8.0.32 |
| 运行时间 | 100 days |
| 数据目录 | /data/mysql/3307 |

### 主从集群状态

|  |  |
| --- | --- |
| **参数名称** | **当前值** |
| role | Slave |
| Master\_Host | 192.168.1.100 |
| Master\_Port | 3307 |
| Slave\_IO\_Running | Yes |
| Slave\_SQL\_Running | Yes |
| Seconds\_Behind\_Master | 0 |

### mysql关键参数

|  |  |
| --- | --- |
| **参数名称** | **当前值** |
| binlog\_format | ROW |
| binlog\_row\_image | FULL |
| character\_set\_server | utf8mb4 |
| enforce\_gtid\_consistency | ON |
| expire\_logs\_days | 7 |
| explicit\_defaults\_for\_timestamp | ON |
| gtid\_mode | ON |
| innodb\_buffer\_pool\_size | 8589934592 |
| innodb\_flush\_log\_at\_trx\_commit | 1 |
| innodb\_flush\_method | O\_DIRECT |
| innodb\_io\_capacity | 15000 |
| innodb\_io\_capacity\_max | 20000 |
| innodb\_thread\_concurrency | 0 |
| log\_bin\_trust\_function\_creators | ON |
| lower\_case\_table\_names | 1 |
| max\_allowed\_packet | 33554432 |
| max\_connections | 3000 |
| max\_user\_connections | 0 |
| open\_files\_limit | 65535 |
| sync\_binlog | 1 |

### 数据库容量

|  |  |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- | --- |
| **库名** | **字符集** | **排序规则** | **表大小（GB）** | **索引大小（GB）** | **数据库总大小（GB）** | **extent 碎片大小（GB）** |
| cms\_v2\_prod | utf8 | utf8\_general\_ci | 2.34 | 0.02 | 2.37 | 0.60 |
| dataview | utf8mb4 | utf8mb4\_general\_ci | 0.21 | 0.00 | 0.21 | 0.02 |

### 数据库用户

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **用户名** | **授权范围** | **ADMIN\_OPTION** | **密码认证插件** | **用户状态（是否被锁）** |
| root | localhost | Y | mysql\_native\_password | N |
| app\_user | % | N | mysql\_native\_password | N |

### 数据库对象数量

|  |  |  |
| --- | --- | --- |
| **数据库** | **对象类型** | **对象数量** |
| cms\_v2\_prod | BASE TABLE | 14 |
| cms\_v2\_prod | INDEX (BTREE) | 46 |
| cms\_v2\_prod | PROCEDURE | 2 |

### 运行线程信息

|  |  |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- | --- |
| **ID** | **USER** | **HOST** | **DB** | **COMMAND** | **TIME** | **STATE** |
| 508375 | user1 | 2406:440:600::91af:10513 | db1 | Sleep | 23 |  |
| 508380 | i\_dbchk | 10.133.63.16:36373 |  | Query | 0 | executing |

### 数据库文件信息

### 数据库状态信息

关注一些重点指标，比如 Created\_tmp\_disk\_tables/Created\_tmp\_tables 比值>10 时，临时表使用存在性能问题。

|  |  |
| --- | --- |
| **参数名称** | **当前值** |
| Handler\_external\_lock | 15581359 |
| Innodb\_row\_lock\_current\_waits | 0 |
| Innodb\_row\_lock\_time | 5 |
| Innodb\_row\_lock\_time\_avg | 5 |
| Innodb\_row\_lock\_time\_max | 5 |
| Innodb\_row\_lock\_waits | 1 |
| Key\_blocks\_not\_flushed | 0 |
| Key\_blocks\_unused | 6696 |
| Key\_blocks\_used | 8 |
| Locked\_connects | 0 |
| Performance\_schema\_locker\_lost | 0 |
| Questions | 16251217 |
| Table\_locks\_immediate | 694201 |
| Table\_locks\_waited | 0 |
| Threads\_connected | 32 |
| Threads\_created | 59 |
| Threads\_running | 2 |
| Uptime | 2160175 |

## 数据库性能检查

### InnoDB详细信息

InnoDB 详细信息，查询 InnoDB 存储引擎的运行时信息，包括死锁的详细信息。

|  |
| --- |
| **InnoDB Status** |
| =====================================  2025-06-09 17:47:01 0xfffb85e9e730 INNODB MONITOR OUTPUT  =====================================  ----------------------------  END OF INNODB MONITOR OUTPUT  ============================ |

### InnoDB锁等待

数据库中正在发生的锁等待详细情况。

|  |  |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- | --- |
| **lock\_id** | **lock\_trx\_id** | **lock\_mode** | **lock\_type** | **lock\_table** | **lock\_index** | **lock\_data** |
| 35918577:1:3:2 | 35918577 | X | RECORD | test.t1 | PRIMARY | 1 |

### 元数据锁信息

|  |  |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- | --- |
| **proc\_id** | **obj\_type** | **obj\_sch** | **obj\_name** | **lck\_type** | **lck\_dur** | **lck\_stat** |
| 12345 | TABLE | testdb | t1 | SHARED\_WRITE | TRANSACTION | GRANTED |

说明：

proc\_id: 会话ID（连接/线程在 processlist 里的编号）

obj\_type: 锁对象类型（如 TABLE）

obj\_sch: 对象所属库名

obj\_name: 对象名（表名等）

lck\_type: 锁类型（如 SHARED\_READ、EXCLUSIVE）

lck\_dur: 锁持续范围（如 TRANSACTION）

lck\_stat: 锁状态（如 GRANTED、PENDING）

### 连接数检查

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **当前连接数** | **最大连接数** | **连接数利用率** | **当前运行线程数量** | **最大连接错误量** |
| 60 | 3000 | 2.00% | 2 | 317 |

### 占用空间top 10的表

|  |  |  |
| --- | --- | --- |
| **table\_schema** | **table\_name** | **table\_size\_mb** |
| cms\_v2\_prod | t\_log\_operation | 1525.86 |
| cms\_v2\_prod | t\_sys\_resource | 461.56 |
| cms\_v2\_prod | t\_sys\_job\_detail | 201.00 |

### 占用空间top 10的索引

|  |  |  |  |
| --- | --- | --- | --- |
| **table\_schema** | **table\_name** | **index\_name** | **index\_size\_mb** |
| cms\_v2\_prod | t\_sys\_job\_detail | idx\_execute\_time | 92.00 |

### 没有主键或唯一键的表

执行时间超过 5 秒的事务列表。长事务会带来锁资源占用、undo 日志空间膨胀等问题。

|  |  |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- | --- |
| **trx\_id** | **user** | **host** | **db** | **time** | **state** | **info** |

### 非Innodb引擎的数据对象

建表建议索引个数最好不要超过 5 个。如果是核心查询表可适当增加。索引不是越多越好，要综合考虑查询及写入。

|  |  |  |
| --- | --- | --- |
| **table\_schema** | **table\_name** | **index\_count** |
| cms\_v2\_prod | t\_log\_operation | 7 |

### 单张表超过6个索引的对象

建表时联合索引建议字段个数最好不要超过 4 个。如果业务需要也可以支持，或者使用 hash 索引替代。

|  |  |  |  |
| --- | --- | --- | --- |
| **table\_schema** | **table\_name** | **index\_name** | **column\_count** |

### 联合索引的字段个数大于4的对象

建议表设计时字段不要超过 50 个，如果业务上太多可以通过切表的设计。

|  |  |  |
| --- | --- | --- |
| **table\_schema** | **table\_name** | **columns** |
| cms\_v2\_prod | t\_acq\_agent | 61 |
| cms\_v2\_prod | t\_config\_inst\_alter\_relation | 54 |

### 单张表字段个数大于50的对象

表必须有主键，推荐使用自增 id 并且用 bigint 数据类型为准。无主键在主从延迟时可能会导致严重的延迟。

|  |  |
| --- | --- |
| **table\_schema** | **table\_name** |

### 物理IO top 10的表

发生物理 IO 比较高的表，说明无法在内存中完成，可以重点优化内存结构。

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **object\_schema** | **object\_name** | **count\_read** | **count\_write** | **count\_fetch** |
| cms\_v2\_prod | t\_acq\_agent | 10165 | 68603417 | 638531102 |

### 数据库内存分布top 10

|  |  |  |
| --- | --- | --- |
| **event\_name** | **current\_alloc** | **high\_alloc** |
| memory/innodb/buf\_buf\_pool | 8.14 GB | 8.14 GB |
| memory/innodb/os0file | 65.55 MB | 65.68 MB |
| memory/performance\_schema/events\_statements\_summary\_by\_digest | 40.28 MB | 40.28 MB |

### 慢SQL top10

查询耗时比较高（默认记录执行大于 3 秒或者没有走索引的情况）的数据库交互。以查询最大耗时倒序排序。

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **db** | **exec\_count** | **max\_latency** | **avg\_latency** | **sql\_text** |
| cms\_v2\_prod | 28 | 21.05 s | 751.72 ms | SELECT ... FROM ... |

### 全表扫描的SQL top10

### 全表扫描的表top10

发生过全表扫描的表及其扫描次数和延迟。建议为高频全表扫描建立合适的索引。

|  |  |  |  |
| --- | --- | --- | --- |
| **object\_schema** | **object\_name** | **rows\_full\_scanned** | **latency** |
| cmdb\_prod | sys\_privilege | 37314123 | 30.33 s |
| csim | t\_message | 9345248 | 17.20 s |

### 使用临时表的SQL top10

使用临时表的SQL也是重点性能指标之一，我们重点关注使用到磁盘临时表的SQL。另外大量使用tmp的表容易导致内存碎片。

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **db** | **exec\_count** | **count\_pct** | **last\_seen** | **sql\_text** |
| cms\_v2\_prod | 28 | 67 | 2025-05-26 10:35:39 | SELECT COUNT(?) FROM ... |

### 行操作次数top10

行操作top30的数据库对象。以查询总耗时倒序排序，主要有查询总耗时、insert总行数、update总行数、delete总行数。

|  |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- |
| **table\_schema** | **table\_name** | **select\_latency** | **rows\_inserted** | **rows\_updated** | **rows\_deleted** |
| nacos\_config | permissions | 18.50 s | 0 | 0 | 0 |

### 未使用的索引

以下是实例启动以来从事未使用的索引。索引会影响数据库写入效率，建议清理。

|  |  |  |
| --- | --- | --- |
| **object\_schema** | **object\_name** | **index\_name** |
| cms\_v2\_prod | t\_sys\_user\_20240620 | deleted |
| cms\_v2\_prod | t\_sys\_menu | menu\_pid |

### 自增值使用率top10

自增值达到上限后数据库将无法写入数据。

|  |  |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- | --- |
| **table\_schema** | **table\_name** | **column\_name** | **data\_type** | **max\_value** | **auto\_increment** | **auto\_increment\_ratio** |
| cms\_v2\_prod | t\_sys\_user | user\_id | bigint | 9223372036854775807 | 1075590294160412673 | 0.1166 |

### 冗余索引

冗余索引会影响数据库写入效率，建议清理。

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **tb\_schema** | **tb\_name** | **redundant\_index** | **redundant\_index\_columns** | **sql\_drop\_index** |
| cms\_v2\_prod | t\_config\_alter\_notice\_relation | alter\_tpl\_id | alter\_tpl\_id | ALTER TABLE `cms\_v2\_prod`.`t\_config\_alter\_notice\_relation` DROP INDEX `alter\_tpl\_id` |

## 数据库备份

### 备份策略

本地备份：xtrabackup物理备份每天1次，保留3天；rsync实时备份增量binlog，保留7天。

远程备份：NBU备份（每2天拷贝一次本地物理备份集；每周全量拷贝一次本地binlog备份文件，每天增量拷贝一次binlog备份文件。）

### 备份集可用性检查

巡检最近 7 次备份。物理备份（xtrabackup --check）或逻辑备份（mysqlcheck）的完整性验证。

巡检脚本待补充，暂时人工填写。

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **备份主机** | **备份类型** | **备份检查时间** | **备份集可用状态** | **本次备份耗时** |
