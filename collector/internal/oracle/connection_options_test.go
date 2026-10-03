package oracle

import (
	"dbcheck/collector/internal/cli"
	"testing"

	go_ora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/configurations"
)

func TestServiceNameConnectionTarget(t *testing.T) {
	connection, err := go_ora.ParseConfig(buildDSN(cli.Config{
		DBHost: "10.0.0.1", DBPort: 1521,
		DBUsername: "system", DBPassword: "secret",
		OracleServiceName: " ORCLPDB1.example.com ",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if connection.ServiceName != "ORCLPDB1.example.com" || connection.SID != "" {
		t.Fatalf("unexpected service-name target: service=%q SID=%q", connection.ServiceName, connection.SID)
	}
	if connection.DBAPrivilege != configurations.NONE {
		t.Fatalf("ordinary account unexpectedly uses DBA privileges: %v", connection.DBAPrivilege)
	}
}

func TestOracleSYSDBAConnection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		user   string
		sysdba bool
	}{
		{name: "explicit SYSDBA", user: "inspector", sysdba: true},
		{name: "SYS implies SYSDBA", user: "SYS"},
		{name: "SYS ignores case", user: "sys"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connection, err := go_ora.ParseConfig(buildDSN(cli.Config{
				DBHost: "10.0.0.1", DBPort: 1521, DBName: "ORCL",
				DBUsername: tc.user, DBPassword: "secret", OracleSYSDBA: tc.sysdba,
			}))
			if err != nil {
				t.Fatal(err)
			}
			if connection.DBAPrivilege != configurations.SYSDBA || connection.SID != "ORCL" {
				t.Fatalf("expected SYSDBA on SID ORCL, got privilege=%v SID=%q", connection.DBAPrivilege, connection.SID)
			}
		})
	}
}
