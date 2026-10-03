package oracle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"dbcheck/collector/internal/cli"
)

func TestDeploymentDetectsVersionFamiliesAndStandalone11g(t *testing.T) {
	for _, tc := range []struct {
		version, family string
		major           int
		multitenant     bool
	}{
		{"11.2.0.4.0", "11gR2", 11, false},
		{"12.2.0.1.0", "12c", 12, true},
		{"18.0.0.0.0", "12c", 18, true},
		{"19.3.0.0.0", "19c", 19, true},
		{"21.3.0.0.0", "21c", 21, true},
		{"23.4.0.0.0", "23ai", 23, true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			c := deploymentCollector(t, deploymentFixture{version: tc.version, rac: "FALSE", role: "PRIMARY", cdb: "NO", asm: "NO"})
			version, topology := c.collectDeployment(context.Background())
			if version["major"] != tc.major || version["family"] != tc.family || version["version"] != tc.version {
				t.Fatalf("unexpected version: %#v", version)
			}
			caps := version["capabilities"].(map[string]any)
			if caps["multitenant"] != tc.multitenant || caps["diagnostic_alert"] != tc.multitenant {
				t.Fatalf("unexpected capabilities: %#v", caps)
			}
			if topology["is_rac"] != false || topology["is_cdb"] != false || topology["is_asm"] != false || topology["role"] != "primary" {
				t.Fatalf("unexpected topology: %#v", topology)
			}
			if len(c.errors) != 0 {
				t.Fatalf("unexpected probing errors: %v", c.errors)
			}
		})
	}
}

type deploymentFixture struct {
	version, rac, role, cdb, asm, container string
	pdbs                                    [][]driver.Value
	fail                                    bool
	missingCDB                              bool
	queries                                 *[]string
	pdbError                                bool
}

func TestDeploymentDetectsRACStandbyASMAndListsPDBs(t *testing.T) {
	c := deploymentCollector(t, deploymentFixture{
		version: "19.3.0.0.0", rac: "TRUE", role: "PHYSICAL STANDBY", cdb: "YES", asm: "YES", container: "CDB$ROOT",
		pdbs: [][]driver.Value{{int64(2), "PDB$SEED", "READ ONLY", "NO", int64(104857600)}, {int64(3), "APP", "MOUNTED", "NO", int64(209715200)}},
	})
	_, topology := c.collectDeployment(context.Background())
	if topology["is_rac"] != true || topology["is_cdb"] != true || topology["is_asm"] != true || topology["role"] != "standby" || topology["database_role"] != "PHYSICAL STANDBY" {
		t.Fatalf("unexpected topology: %#v", topology)
	}
	if topology["connected_container"] != "CDB$ROOT" || topology["inspection_scope"] != "connected_container" || topology["pdb_list_state"] != "collected" {
		t.Fatalf("unexpected scope: %#v", topology)
	}
	pdbs := topology["pdbs"].(map[string]any)["items"].([]map[string]any)
	if len(pdbs) != 2 || pdbs[1]["name"] != "APP" || pdbs[1]["open_mode"] != "MOUNTED" || pdbs[1]["size_bytes"] != int64(209715200) {
		t.Fatalf("unexpected PDBs: %#v", pdbs)
	}
}

func TestDeploymentMarksPDBScopedListIncomplete(t *testing.T) {
	c := deploymentCollector(t, deploymentFixture{version: "23.4.0.0.0", rac: "FALSE", role: "PRIMARY", cdb: "YES", asm: "NO", container: "APP"})
	_, topology := c.collectDeployment(context.Background())
	if topology["pdb_list_state"] != "not_collected" || topology["pdb_list_remediation"] == "" {
		t.Fatalf("PDB visibility gap must be explicit: %#v", topology)
	}
}

func TestDeploymentDoesNotTreatFailedProbesAsStandalonePrimary(t *testing.T) {
	c := deploymentCollector(t, deploymentFixture{fail: true})
	version, topology := c.collectDeployment(context.Background())
	if version["family"] != "unknown" || topology["is_rac"] != nil || topology["is_cdb"] != nil || topology["is_asm"] != nil || topology["role"] != "unknown" {
		t.Fatalf("failed probes must remain unknown: %#v %#v", version, topology)
	}
	if len(c.errors) == 0 {
		t.Fatal("probe errors must be recorded")
	}
}

func TestDeploymentPreservesUnknownCDBWhenOnlyThatProbeFails(t *testing.T) {
	c := deploymentCollector(t, deploymentFixture{version: "19.3.0.0.0", rac: "FALSE", role: "PRIMARY", asm: "NO", missingCDB: true})
	version, topology := c.collectDeployment(context.Background())
	if version["family"] != "19c" || topology["is_cdb"] != nil || topology["pdb_list_state"] != "not_collected" {
		t.Fatalf("missing CDB privilege must not imply non-CDB: %#v %#v", version, topology)
	}
}

func deploymentCollector(t *testing.T, fixture deploymentFixture) *metricsCollector {
	t.Helper()
	db := sql.OpenDB(deploymentConnector{fixture})
	t.Cleanup(func() { db.Close() })
	return newMetricsCollector(db, cli.Config{})
}

type deploymentConnector struct{ fixture deploymentFixture }

func (c deploymentConnector) Connect(context.Context) (driver.Conn, error) {
	return deploymentConn{c.fixture}, nil
}
func (c deploymentConnector) Driver() driver.Driver { return deploymentDriver{} }

type deploymentDriver struct{}

func (deploymentDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type deploymentConn struct{ fixture deploymentFixture }

func (deploymentConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (deploymentConn) Close() error                        { return nil }
func (deploymentConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (c deploymentConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	f := c.fixture
	if f.queries != nil {
		*f.queries = append(*f.queries, query)
	}
	if f.fail {
		return nil, errors.New("ORA-00942: table or view does not exist")
	}
	value := ""
	switch {
	case strings.Contains(query, "v$instance"):
		value = f.version
	case strings.Contains(query, "cluster_database"):
		value = f.rac
	case strings.Contains(query, "database_role"):
		value = f.role
	case strings.Contains(query, "SELECT cdb"):
		if f.missingCDB {
			return nil, errors.New("ORA-01031: insufficient privileges")
		}
		value = f.cdb
	case strings.Contains(query, "SYS_CONTEXT"):
		value = f.container
	case strings.Contains(query, "v$pdbs"):
		return &deploymentRows{columns: []string{"con_id", "name", "open_mode", "restricted", "size_bytes"}, values: f.pdbs, failAfterRows: f.pdbError}, nil
	case strings.Contains(query, "v$datafile"):
		value = f.asm
	default:
		return nil, errors.New("unexpected probe")
	}
	return &deploymentRows{columns: []string{"value"}, values: [][]driver.Value{{value}}}, nil
}

type deploymentRows struct {
	columns       []string
	values        [][]driver.Value
	failAfterRows bool
}

func (r *deploymentRows) Columns() []string { return r.columns }
func (r *deploymentRows) Close() error      { return nil }
func (r *deploymentRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		if r.failAfterRows {
			return errors.New("ORA-03113: connection lost while reading rows")
		}
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
	return nil
}
