package gaussdb

import (
	"context"
	"database/sql"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

type sqlDomainExtra struct {
	Summary map[string]any
	Fields  map[string]any
}

type sqlCollectionResult struct {
	Metadata map[string]any
	Records  []itemRecord
	Domains  map[string]sqlDomainExtra
	RawIndex []map[string]any
	Errors   []string
}

func (c Collector) collectSQLFirstArtifacts(ctx context.Context, cfg cli.Config, runDir string, writer core.ArtifactWriter) (sqlCollectionResult, error) {
	db, err := openDB(ctx, cfg)
	if err != nil {
		return sqlCollectionResult{}, err
	}
	defer db.Close()

	result := newSQLCollectionResult()
	c.collectIgnoredOptionNotice(cfg, &result)
	c.collectSQLChecks(ctx, db, cfg, runDir, writer, &result)
	collectSQLAnalysis(ctx, db, cfg, runDir, writer, &result)
	indexPath := filepath.Join(runDir, "sql", "index.json")
	if err := writer.WriteJSON(indexPath, map[string]any{"items": result.RawIndex, "count": len(result.RawIndex)}); err != nil {
		return sqlCollectionResult{}, queryError("sql.index", err)
	}
	return result, nil
}

func newSQLCollectionResult() sqlCollectionResult {
	return sqlCollectionResult{
		Metadata: map[string]any{},
		Records:  make([]itemRecord, 0, len(itemCatalog)),
		Domains:  map[string]sqlDomainExtra{},
		RawIndex: []map[string]any{},
		Errors:   []string{},
	}
}

func (c Collector) collectIgnoredOptionNotice(cfg cli.Config, result *sqlCollectionResult) {
	ignored := ignoredGaussShellOptions(cfg)
	if len(ignored) == 0 {
		return
	}
	result.Metadata["ignored_options"] = strings.Join(ignored, ",")
	c.reportCheck("INFO", "gaussdb_check_finished", "GaussDB option ignored", itemSpec{Name: "GaussShellOptions", Domain: "basic_info"}, 0, 0, "")
}

func (c Collector) collectSQLChecks(
	ctx context.Context,
	db *sql.DB,
	cfg cli.Config,
	runDir string,
	writer core.ArtifactWriter,
	result *sqlCollectionResult,
) {
	for _, item := range itemCatalog {
		spec, ok := sqlCheckByName[item.Name]
		if !ok {
			result.Records = append(result.Records, notApplicableRecord(item))
			continue
		}
		record := c.collectSQLCheck(ctx, db, cfg, runDir, writer, item, result, spec)
		result.Records = append(result.Records, record)
	}
}

func (c Collector) collectSQLCheck(
	ctx context.Context,
	db *sql.DB,
	cfg cli.Config,
	runDir string,
	writer core.ArtifactWriter,
	item itemSpec,
	result *sqlCollectionResult,
	spec sqlCheckSpec,
) itemRecord {
	started := c.now()
	c.reportCheck("INFO", "gaussdb_check_started", "GaussDB check started", item, 0, 0, "")
	rows, err := writeAndQueryRows(ctx, db, cfg, runDir, writer, spec.Name, spec.Query)
	duration := c.now().Sub(started).Milliseconds()
	if err != nil {
		record := failedSQLRecord(item, spec.Query, duration, err)
		result.Errors = append(result.Errors, record.Summary)
		c.reportSQLFailure(item, duration, err)
		return record
	}
	record := spec.Project(item, rows, duration)
	record.RawFile = filepath.ToSlash(filepath.Join("sql", spec.Name+".json"))
	record.Command = spec.Query
	c.applyMetadataProjection(spec.Name, rows, result.Metadata)
	result.RawIndex = append(result.RawIndex, rawSQLIndex(spec.Name, item, rows))
	c.reportCheck("INFO", "gaussdb_check_finished", "GaussDB check finished", item, duration, len(rows), "")
	return record
}

func collectSQLAnalysis(
	ctx context.Context,
	db *sql.DB,
	cfg cli.Config,
	runDir string,
	writer core.ArtifactWriter,
	result *sqlCollectionResult,
) {
	for _, spec := range sqlQueryCatalog {
		recordErr := collectSQLQuery(ctx, db, cfg, runDir, writer, spec, result)
		if recordErr != nil {
			result.Errors = append(result.Errors, recordErr.Error())
		}
	}
}

func writeAndQueryRows(ctx context.Context, db *sql.DB, cfg cli.Config, runDir string, writer core.ArtifactWriter, name string, query string) ([]map[string]any, error) {
	sqlPath := filepath.Join(runDir, "sql", name+".sql")
	if err := writer.WriteText(sqlPath, query+"\n"); err != nil {
		return nil, queryError(name+".write_sql", err)
	}
	rows, err := queryRows(ctx, db, cfg.SQLTimeoutSeconds, query)
	if err != nil {
		return nil, queryError(name, err)
	}
	if err := writeSQLResult(runDir, writer, name, rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func writeSQLResult(runDir string, writer core.ArtifactWriter, name string, rows []map[string]any) error {
	resultPath := filepath.Join(runDir, "sql", name+".json")
	if err := writer.WriteJSON(resultPath, rowsPayload(rows)); err != nil {
		return queryError(name+".write_json", err)
	}
	return nil
}

func collectSQLQuery(
	ctx context.Context,
	db *sql.DB,
	cfg cli.Config,
	runDir string,
	writer core.ArtifactWriter,
	spec sqlQuerySpec,
	result *sqlCollectionResult,
) error {
	rows, err := writeAndQueryRows(ctx, db, cfg, runDir, writer, spec.Name, spec.Query)
	if err != nil {
		return err
	}
	recordSQLResult(spec, rows, result)
	item := itemSpec{Name: spec.Name, Domain: spec.Domain, Label: spec.Label}
	result.RawIndex = append(result.RawIndex, rawSQLIndex(spec.Name, item, rows))
	return nil
}

func rawSQLIndex(name string, item itemSpec, rows []map[string]any) map[string]any {
	return map[string]any{
		"item":        name,
		"domain":      item.Domain,
		"label":       item.Label,
		"sql_file":    filepath.ToSlash(filepath.Join("sql", name+".sql")),
		"result_file": filepath.ToSlash(filepath.Join("sql", name+".json")),
		"row_count":   len(rows),
	}
}

func recordSQLResult(spec sqlQuerySpec, rows []map[string]any, result *sqlCollectionResult) {
	extra := result.Domains[spec.Domain]
	if extra.Fields == nil {
		extra.Fields = map[string]any{}
	}
	if extra.Summary == nil {
		extra.Summary = map[string]any{}
	}
	key := sqlPayloadKey(spec.Name)
	extra.Fields[key] = rowsPayload(rows)
	applySummaryProjection(spec.Name, rows, extra.Summary)
	result.Domains[spec.Domain] = extra
}

func sqlPayloadKey(name string) string {
	switch name {
	case "NoIndexSummary":
		return "no_index_summary"
	case "NoPrimaryKeySummary":
		return "no_primary_key_summary"
	case "NoPrimaryKeyDetail":
		return "no_primary_key_detail"
	case "NoStatisticsSummary":
		return "no_statistics_summary"
	case "NoStatisticsDetail":
		return "no_statistics_detail"
	default:
		panic(fmt.Sprintf("unknown sql payload key for %s", name))
	}
}

func applySummaryProjection(name string, rows []map[string]any, summary map[string]any) {
	switch name {
	case "NoIndexSummary":
		summary["no_index_owner_count"] = len(rows)
		summary["no_index_table_count"] = sumInt(rows, "no_index_count")
	case "NoPrimaryKeySummary":
		summary["no_primary_key_owner_count"] = len(rows)
		summary["no_primary_key_table_count"] = sumInt(rows, "no_pk_count")
	case "NoPrimaryKeyDetail":
		summary["no_primary_key_detail_count"] = len(rows)
	case "NoStatisticsSummary":
		summary["no_statistics_owner_count"] = len(rows)
		summary["no_statistics_table_count"] = sumInt(rows, "table_no_stat")
	case "NoStatisticsDetail":
		summary["no_statistics_detail_count"] = len(rows)
	}
}

func (c Collector) applyMetadataProjection(name string, rows []map[string]any, metadata map[string]any) {
	if len(rows) == 0 {
		return
	}
	switch name {
	case "CheckGaussVer":
		version := strings.TrimSpace(formatAny(rows[0]["version"]))
		if version != "" {
			metadata["gaussdb_version"] = version
			metadata["gsql_version"] = "not_applicable: SQL-first mode"
			metadata["gs_check_version"] = "not_applicable: SQL-first mode"
			metadata["version"] = extractKernelVersion(version)
		}
	case "CheckDBConnection":
		version := strings.TrimSpace(formatAny(rows[0]["version"]))
		if version != "" && metadata["gaussdb_version"] == nil {
			metadata["gaussdb_version"] = version
			metadata["version"] = extractKernelVersion(version)
		}
		metadata["pguser"] = formatAny(rows[0]["user"])
	}
}

func (c Collector) reportSQLFailure(item itemSpec, duration int64, err error) {
	event := "gaussdb_check_failed"
	message := "GaussDB check failed"
	if isTimeoutError(err) {
		event = "gaussdb_check_timeout"
		message = "GaussDB check timeout"
	}
	c.reportCheck("ERROR", event, message, item, duration, 0, err.Error())
}

func (c Collector) reportCheck(level string, event string, message string, item itemSpec, duration int64, rowCount int, err string) {
	if c.Progress == nil {
		return
	}
	tokens := []string{"item=" + item.Name, "domain=" + item.Domain}
	if duration > 0 {
		tokens = append(tokens, fmt.Sprintf("duration_ms=%d", duration))
	}
	if rowCount > 0 {
		tokens = append(tokens, fmt.Sprintf("row_count=%d", rowCount))
	}
	c.Progress(core.ProgressEvent{
		Level: level, Event: event, Message: message, RunID: c.RunID,
		Step: 5, Total: 11, Tokens: tokens, Error: err,
	})
}

func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "deadline exceeded")
}

func sumInt(rows []map[string]any, key string) int {
	total := 0
	for _, row := range rows {
		switch value := row[key].(type) {
		case int:
			total += value
		case int32:
			total += int(value)
		case int64:
			total += int(value)
		case float64:
			total += int(value)
		}
	}
	return total
}
