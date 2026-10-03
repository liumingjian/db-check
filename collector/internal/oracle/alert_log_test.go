package oracle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"dbcheck/collector/internal/cli"
)

func TestAlertCollectionSummarizesEveryCodeAndDeduplicatesPerRecord(t *testing.T) {
	db := sql.OpenDB(alertConnector{rows: [][]driver.Value{
		{"2026-10-03", "ORA-00600 ORA-00600 ORA-07445 ORA-01555 ORA-04031 ORA-00060"},
		{"2026-10-02", "ORA-00600"},
	}})
	defer db.Close()
	c := newMetricsCollector(db, cli.Config{})
	payload := c.collectAlertLog(context.Background(), map[string]any{"major": 19})
	if payload["critical_count"] != 5 || payload["warning_count"] != 1 || payload["matching_records"] != 2 {
		t.Fatalf("unexpected alert summary: %#v", payload)
	}
	items := payload["errors"].(map[string]any)["items"].([]map[string]any)
	if len(items) != 5 {
		t.Fatalf("expected five distinct codes: %#v", items)
	}
	for _, item := range items {
		if item["error_code"] == "ORA-00600" && (item["count"] != 2 || item["latest_time"] != "2026-10-03") {
			t.Fatalf("unexpected internal-error summary: %#v", item)
		}
	}
}

func TestAlertCollectionSkips11gAndPreservesDeniedRead(t *testing.T) {
	for _, major := range []int{11, 19} {
		db := sql.OpenDB(alertConnector{err: errors.New("ORA-01031: insufficient privileges")})
		c := newMetricsCollector(db, cli.Config{})
		c.collectAlertLog(context.Background(), map[string]any{"major": major})
		evidence := c.availability["db.alert_log"].(map[string]any)
		if evidence["readable"] != false || evidence["remediation"] == "" {
			t.Fatalf("gap missing: %#v", evidence)
		}
		if major == 11 && len(c.errors) != 0 {
			t.Fatal("11g must skip querying alerts")
		}
		if major == 19 && evidence["error_code"] != "ORA-01031" {
			t.Fatalf("permission loss hidden: %#v", evidence)
		}
		db.Close()
	}
}

type alertConnector struct {
	rows [][]driver.Value
	err  error
}

func (c alertConnector) Connect(context.Context) (driver.Conn, error) {
	return alertConn{fixture: c}, nil
}
func (c alertConnector) Driver() driver.Driver { return deploymentDriver{} }

type alertConn struct {
	deploymentConn
	fixture alertConnector
}

func (c alertConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.fixture.err != nil {
		return nil, c.fixture.err
	}
	return &deploymentRows{columns: []string{"originating_timestamp", "message_text"}, values: c.fixture.rows}, nil
}
