package gaussdb

import (
	"context"
	"database/sql"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"fmt"
	"path/filepath"
)

type sqlDomainExtra struct {
	Summary map[string]any
	Fields  map[string]any
}

type sqlCollectionResult struct {
	Domains  map[string]sqlDomainExtra
	RawIndex []map[string]any
	Errors   []string
}

func collectSQLArtifacts(ctx context.Context, cfg cli.Config, runDir string, writer core.ArtifactWriter) (sqlCollectionResult, error) {
	db, err := openDB(ctx, cfg)
	if err != nil {
		return sqlCollectionResult{}, err
	}
	defer db.Close()

	result := sqlCollectionResult{Domains: map[string]sqlDomainExtra{}, RawIndex: []map[string]any{}, Errors: []string{}}
	for _, spec := range sqlQueryCatalog {
		recordErr := collectSQLQuery(ctx, db, cfg, runDir, writer, spec, &result)
		if recordErr != nil {
			result.Errors = append(result.Errors, recordErr.Error())
		}
	}
	indexPath := filepath.Join(runDir, "sql", "index.json")
	if err := writer.WriteJSON(indexPath, map[string]any{"items": result.RawIndex, "count": len(result.RawIndex)}); err != nil {
		return sqlCollectionResult{}, queryError("sql.index", err)
	}
	return result, nil
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
	sqlPath := filepath.Join(runDir, "sql", spec.Name+".sql")
	if err := writer.WriteText(sqlPath, spec.Query+"\n"); err != nil {
		return queryError(spec.Name+".write_sql", err)
	}
	rows, err := queryRows(ctx, db, cfg.SQLTimeoutSeconds, spec.Query)
	if err != nil {
		return queryError(spec.Name, err)
	}
	resultPath := filepath.Join(runDir, "sql", spec.Name+".json")
	if err := writer.WriteJSON(resultPath, rowsPayload(rows)); err != nil {
		return queryError(spec.Name+".write_json", err)
	}
	recordSQLResult(spec, rows, result)
	result.RawIndex = append(result.RawIndex, map[string]any{
		"item":        spec.Name,
		"domain":      spec.Domain,
		"label":       spec.Label,
		"sql_file":    filepath.ToSlash(filepath.Join("sql", spec.Name+".sql")),
		"result_file": filepath.ToSlash(filepath.Join("sql", spec.Name+".json")),
		"row_count":   len(rows),
	})
	return nil
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
