package oracle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"dbcheck/collector/internal/cli"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestOrdinaryRootAccountDoesNotClaimCompletePDBVisibility(t *testing.T) {
	c := deploymentCollector(t, deploymentFixture{version: "19.3", rac: "FALSE", role: "PRIMARY", cdb: "YES", asm: "NO", container: "CDB$ROOT", pdbs: [][]driver.Value{{int64(3), "APP", "READ WRITE", "NO", int64(1)}}})
	_, topology := c.collectDeployment(context.Background())
	if topology["pdb_list_state"] != "not_collected" || !strings.Contains(topology["pdb_list_remediation"].(string), "CONTAINER_DATA") {
		t.Fatalf("filtered PDB list declared complete: %#v", topology)
	}
}

type lagConnector struct{}

func (lagConnector) Connect(context.Context) (driver.Conn, error) { return lagConn{}, nil }
func (lagConnector) Driver() driver.Driver                        { return deploymentDriver{} }

type lagConn struct{ deploymentConn }

func (lagConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "v$dataguard_stats") {
		return &deploymentRows{columns: []string{"name", "value"}, values: [][]driver.Value{{"apply lag", "+00 00:00:01"}}, failAfterRows: true}, nil
	}
	return nil, errors.New("unsupported fixture")
}
func TestPartialLagReadRetainsUnavailableEvidence(t *testing.T) {
	db := sql.OpenDB(lagConnector{})
	defer db.Close()
	c := newMetricsCollector(db, cli.Config{})
	c.collectDataGuard(context.Background(), map[string]any{"role": "standby"})
	if c.availability["db.data_guard.apply_lag_seconds"].(map[string]any)["readable"] != false {
		t.Fatal("partial lag evidence read as healthy")
	}
}

type blockedHostRunner struct{ stopped chan struct{} }

func (r *blockedHostRunner) Run(string) (string, error) { <-r.stopped; return "", errors.New("closed") }
func (r *blockedHostRunner) Close() error {
	select {
	case <-r.stopped:
	default:
		close(r.stopped)
	}
	return nil
}
func TestHostCommandHonorsCollectionCancellation(t *testing.T) {
	runner := &blockedHostRunner{stopped: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := runHostCommand(ctx, runner, "lsnrctl status")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing cancellation: %v", err)
	}
}

type failedHostRunner struct{ output string }

func (r failedHostRunner) Run(string) (string, error) { return r.output, errors.New("command failed") }
func (failedHostRunner) Close() error                 { return nil }

func TestListenerFailureIsEvidenceButMissingExecutableIsGap(t *testing.T) {
	c := newMetricsCollector(nil, cli.Config{})
	evidence := c.collectHostCommand(context.Background(), failedHostRunner{"TNS-12541: no listener"}, "listener", "lsnrctl status", "Ask DBA")
	if evidence["command_succeeded"] != false || evidence["output"] != "TNS-12541: no listener" {
		t.Fatalf("observed failure lost: %#v", evidence)
	}
	evidence = c.collectHostCommand(context.Background(), failedHostRunner{"lsnrctl: command not found"}, "listener", "lsnrctl status", "Ask DBA")
	if evidence != nil || c.availability["db.host_checks.listener"].(map[string]any)["readable"] != false {
		t.Fatal("missing command must remain not collected")
	}
}
