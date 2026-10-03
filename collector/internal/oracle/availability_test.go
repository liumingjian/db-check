package oracle

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
)

func TestFailedProbeRetainsUnreadableEvidence(t *testing.T) {
	c := deploymentCollector(t, deploymentFixture{fail: true})
	c.collectDeployment(context.Background())
	item := c.availability["db.version_info"].(map[string]any)
	if item["readable"] != false || item["error_code"] != "ORA-00942" || item["remediation"] == "" {
		t.Fatalf("failed query must carry actionable evidence: %#v", item)
	}
}

func TestPrivilegePrecheckPrecedesTopologyAndMetrics(t *testing.T) {
	queries := []string{}
	c := deploymentCollector(t, deploymentFixture{version: "19.3", queries: &queries})
	c.collectAll(context.Background())
	if len(queries) < 3 || !strings.Contains(queries[0], "version FROM") || !strings.Contains(queries[1], "WHERE 1=0") {
		t.Fatalf("version discovery must precede actual-read privilege probes: %v", queries)
	}
	roleIndex, lastProbe := -1, -1
	for i, query := range queries {
		if strings.Contains(query, "WHERE 1=0") {
			lastProbe = i
		}
		if strings.Contains(query, "database_role FROM") {
			roleIndex = i
		}
	}
	if roleIndex <= lastProbe {
		t.Fatalf("privilege precheck must finish before topology collection: %v", queries)
	}
}

func TestSuccessfulEmptyRowsAreReadable(t *testing.T) {
	c := deploymentCollector(t, deploymentFixture{version: "19.3", cdb: "YES", container: "CDB$ROOT"})
	c.collectDeployment(context.Background())
	item := c.availability["db.deployment_topology.pdbs"].(map[string]any)
	if item["readable"] != true {
		t.Fatalf("empty successful result must remain readable: %#v", item)
	}
}

func TestPartialRowsDoNotCountAsCollected(t *testing.T) {
	c := deploymentCollector(t, deploymentFixture{version: "19.3", cdb: "YES", container: "CDB$ROOT", pdbError: true,
		pdbs: [][]driver.Value{{int64(3), "APP", "READ WRITE", "NO", int64(1024)}}})
	_, topology := c.collectDeployment(context.Background())
	item := c.availability["db.deployment_topology.pdbs"].(map[string]any)
	if item["readable"] != false || item["error_code"] != "ORA-03113" || topology["pdb_list_state"] != "not_collected" {
		t.Fatalf("partial rows must not be judged complete: %#v %#v", item, topology)
	}
}
