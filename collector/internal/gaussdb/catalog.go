package gaussdb

type itemSpec struct {
	Name   string
	Domain string
	Label  string
}

var itemCatalog = [...]itemSpec{
	{Name: "CheckClusterState", Domain: "cluster", Label: "集群状态"},
	{Name: "CheckGaussVer", Domain: "basic_info", Label: "GaussDB 版本"},
	{Name: "CheckDBConnection", Domain: "basic_info", Label: "数据库连接"},
	{Name: "CheckIntegrity", Domain: "cluster", Label: "数据一致性"},
	{Name: "CheckOMMonitor", Domain: "basic_info", Label: "omMonitor 进程"},
	{Name: "CheckReadonlyMode", Domain: "cluster", Label: "只读模式"},
	{Name: "CheckDBParams", Domain: "config_check", Label: "数据库参数"},
	{Name: "CheckGUCValue", Domain: "config_check", Label: "GUC 值检查"},
	{Name: "CheckGUCConsistent", Domain: "config_check", Label: "GUC 一致性"},
	{Name: "CheckTableSpace", Domain: "storage", Label: "表空间"},
	{Name: "CheckHashIndex", Domain: "storage", Label: "Hash 索引"},
	{Name: "CheckDilateSysTab", Domain: "storage", Label: "系统表膨胀"},
	{Name: "CheckSysTable", Domain: "storage", Label: "系统表检查"},
	{Name: "CheckKeyDBTableSize", Domain: "storage", Label: "大表检查"},
	{Name: "CheckCurConnCount", Domain: "connection", Label: "当前连接数"},
	{Name: "CheckCursorNum", Domain: "connection", Label: "游标数量"},
	{Name: "CheckPoolerNum", Domain: "connection", Label: "连接池数量"},
	{Name: "CheckLockNum", Domain: "transactions", Label: "锁数量"},
	{Name: "CheckIdleSession", Domain: "transactions", Label: "空闲会话"},
	{Name: "CheckPgPreparedXacts", Domain: "transactions", Label: "预备事务"},
	{Name: "CheckWorkloadTrx", Domain: "transactions", Label: "长事务"},
	{Name: "CheckDBStat", Domain: "performance", Label: "数据库运行状态"},
	{Name: "CheckBPHitRatio", Domain: "performance", Label: "Buffer 命中率"},
	{Name: "CheckErrorInLog", Domain: "performance", Label: "运行日志"},
	{Name: "CheckReturnType", Domain: "sql_analysis", Label: "自定义函数"},
	{Name: "CheckPgxcRedistb", Domain: "cluster", Label: "分布表"},
	{Name: "CheckNodeGroupName", Domain: "cluster", Label: "节点组"},
	{Name: "CheckCatchup", Domain: "cluster", Label: "主备追赶"},
	{Name: "CheckDnSync", Domain: "cluster", Label: "DN 同步状态"},
	{Name: "CheckDnWait", Domain: "cluster", Label: "DN 等待状态"},
}

var domainOrder = [...]string{
	"basic_info",
	"cluster",
	"config_check",
	"connection",
	"storage",
	"performance",
	"transactions",
	"sql_analysis",
	"security",
}
