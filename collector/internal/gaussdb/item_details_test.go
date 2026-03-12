package gaussdb

import "testing"

func TestParseClusterStateDetailsExtractsNodes(t *testing.T) {
	details := parseClusterStateDetails("Cluster status:\ncluster_state         : Normal\nredistributing        : No\nbalanced              : Yes\n192.168.1.157         : Normal")
	if details["cluster_state"] != "Normal" {
		t.Fatalf("unexpected cluster state: %#v", details)
	}
	if details["node_count"] != 1 {
		t.Fatalf("unexpected node count: %#v", details["node_count"])
	}
}

func TestParseErrorInLogDetailsExtractsCountAndSamples(t *testing.T) {
	parsed := parsedOutput{
		Summary: "Number of ERROR in log is 3<NEW_LINE_SEPARATOR>2026-03-12 ERROR sample-1<NEW_LINE_SEPARATOR>2026-03-12 ERROR sample-2",
	}
	details := parseErrorInLogDetails(parsed)
	if details["error_count"] != 3 {
		t.Fatalf("unexpected error count: %#v", details["error_count"])
	}
	samples, ok := details["sample_lines"].([]string)
	if !ok || len(samples) != 2 {
		t.Fatalf("unexpected samples: %#v", details["sample_lines"])
	}
}
