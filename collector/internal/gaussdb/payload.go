package gaussdb

import "strings"

type itemRecord struct {
	Item             string
	Domain           string
	Label            string
	Status           string
	NormalizedStatus string
	Summary          string
	Details          map[string]any
	RawFile          string
	Command          string
	DurationMS       int64
}

type gaussConfig struct {
	GaussUser    string
	GaussEnvFile string
}

func buildPayload(metadata map[string]any, records []itemRecord, collectErrors []string) map[string]any {
	payload := map[string]any{
		"basic_info": buildDomain("basic_info", metadata, records),
		"cluster":    buildDomain("cluster", map[string]any{}, records),
		"config_check": buildDomain(
			"config_check",
			map[string]any{},
			records,
		),
		"connection":   buildDomain("connection", map[string]any{}, records),
		"storage":      buildDomain("storage", map[string]any{}, records),
		"performance":  buildDomain("performance", map[string]any{}, records),
		"transactions": buildDomain("transactions", map[string]any{}, records),
		"sql_analysis": buildDomain("sql_analysis", map[string]any{}, records),
		"security":     buildDomain("security", map[string]any{}, records),
		"gs_check_raw_index": map[string]any{
			"items": toRawIndex(records),
			"count": len(records),
		},
	}
	if len(collectErrors) > 0 {
		payload["collect_errors"] = collectErrors
	}
	return payload
}

func buildPayloadWithSQL(metadata map[string]any, records []itemRecord, collectErrors []string, sqlResult sqlCollectionResult) map[string]any {
	payload := buildPayload(metadata, records, collectErrors)
	for domain, extra := range sqlResult.Domains {
		mergeDomainExtra(payload, domain, extra)
	}
	if len(sqlResult.RawIndex) > 0 {
		payload["sql_raw_index"] = map[string]any{
			"items": sqlResult.RawIndex,
			"count": len(sqlResult.RawIndex),
		}
	}
	if len(sqlResult.Errors) > 0 {
		payload["collect_errors"] = appendStringSlices(payload["collect_errors"], sqlResult.Errors)
	}
	return payload
}

func buildDomain(name string, summary map[string]any, records []itemRecord) map[string]any {
	items := make([]map[string]any, 0, len(records))
	for _, record := range records {
		if record.Domain != name {
			continue
		}
		items = append(items, map[string]any{
			"item":              record.Item,
			"label":             record.Label,
			"status":            record.Status,
			"normalized_status": record.NormalizedStatus,
			"summary":           record.Summary,
			"details":           record.Details,
			"raw_file":          record.RawFile,
			"command":           record.Command,
			"duration_ms":       record.DurationMS,
		})
	}
	return map[string]any{
		"summary":       enrichSummary(summary, items),
		"items":         items,
		"count":         len(items),
		"visible_count": countVisible(items),
	}
}

func enrichSummary(summary map[string]any, items []map[string]any) map[string]any {
	cloned := make(map[string]any, len(summary)+4)
	for key, value := range summary {
		cloned[key] = value
	}
	cloned["normal_count"] = countByStatus(items, "normal")
	cloned["abnormal_count"] = countByStatus(items, "abnormal")
	cloned["not_applicable_count"] = countByStatus(items, "not_applicable")
	cloned["visible_items"] = visibleItems(items)
	attachItemProjections(cloned, items)
	return cloned
}

func attachItemProjections(summary map[string]any, items []map[string]any) {
	for _, item := range items {
		name := strings.ToLower(asString(item["item"]))
		if name == "" {
			continue
		}
		summary[name+"_status"] = item["normalized_status"]
		summary[name+"_raw_status"] = item["status"]
		summary[name+"_summary"] = item["summary"]
		if details, ok := item["details"].(map[string]any); ok && len(details) > 0 {
			summary[name+"_details"] = details
		}
	}
}

func toRawIndex(records []itemRecord) []map[string]any {
	items := make([]map[string]any, 0, len(records))
	for _, record := range records {
		items = append(items, map[string]any{
			"item":              record.Item,
			"domain":            record.Domain,
			"label":             record.Label,
			"status":            record.Status,
			"normalized_status": record.NormalizedStatus,
			"summary":           record.Summary,
			"details":           record.Details,
			"raw_file":          record.RawFile,
			"command":           record.Command,
			"duration_ms":       record.DurationMS,
		})
	}
	return items
}

func countVisible(items []map[string]any) int {
	count := 0
	for _, item := range items {
		if strings.TrimSpace(asString(item["normalized_status"])) == "not_applicable" {
			continue
		}
		count++
	}
	return count
}

func visibleItems(items []map[string]any) []map[string]any {
	visible := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(asString(item["normalized_status"])) == "not_applicable" {
			continue
		}
		visible = append(visible, item)
	}
	return visible
}

func countByStatus(items []map[string]any, target string) int {
	count := 0
	for _, item := range items {
		if strings.TrimSpace(asString(item["normalized_status"])) == target {
			count++
		}
	}
	return count
}

func asString(value any) string {
	text, _ := value.(string)
	return text
}

func mergeDomainExtra(payload map[string]any, domain string, extra sqlDomainExtra) {
	current, ok := payload[domain].(map[string]any)
	if !ok {
		return
	}
	for key, value := range extra.Fields {
		current[key] = value
	}
	summary, ok := current["summary"].(map[string]any)
	if !ok {
		summary = map[string]any{}
		current["summary"] = summary
	}
	for key, value := range extra.Summary {
		summary[key] = value
	}
}

func appendStringSlices(existing any, values []string) []string {
	items := make([]string, 0, len(values))
	if current, ok := existing.([]string); ok {
		items = append(items, current...)
	}
	items = append(items, values...)
	return items
}
