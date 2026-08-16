package store

import (
	"context"
	"database/sql"
	"fmt"
)

// GetStats РІРѕР·РІСЂР°С‰Р°РµС‚ СЃС‚Р°С‚РёСЃС‚РёРєСѓ РёРЅРґРµРєСЃР°
func (db *DB) GetStats(ctx context.Context) (*Stats, error) {
	stats := &Stats{}

	if err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) AS total_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'SQL') AS sql_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'H') AS h_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'PAS') AS pas_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'INC') AS inc_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'JS') AS js_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'XML') AS xml_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'SMF') AS smf_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'DFM') AS dfm_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'TPR') AS tpr_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'RPT') AS rpt_files
		FROM files
	`).Scan(
		&stats.TotalFiles,
		&stats.SQLFiles,
		&stats.HFiles,
		&stats.PASFiles,
		&stats.INCFiles,
		&stats.JSFiles,
		&stats.XMLFiles,
		&stats.SMFFiles,
		&stats.DFMFiles,
		&stats.TPRFiles,
		&stats.RPTFiles,
	); err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	aggregates := []struct {
		query  string
		target *int
		name   string
	}{
		{`SELECT COUNT(*) FROM sql_procedures`, &stats.Procedures, "sql procedures"},
		{`SELECT COUNT(*) FROM sql_tables`, &stats.Tables, "sql tables"},
		{`SELECT COUNT(*) FROM sql_columns`, &stats.Columns, "sql columns"},
		{`SELECT COUNT(*) FROM pas_units`, &stats.Units, "pas units"},
		{`SELECT COUNT(*) FROM pas_classes`, &stats.Classes, "pas classes"},
		{`SELECT COUNT(*) FROM pas_methods`, &stats.Methods, "pas methods"},
		{`SELECT COUNT(*) FROM pas_fields`, &stats.PASFields, "pas fields"},
		{`SELECT COUNT(*) FROM js_functions`, &stats.JSFunctions, "js functions"},
		{`SELECT COUNT(*) FROM smf_instruments`, &stats.SMFInstruments, "smf instruments"},
		{`SELECT COUNT(*) FROM dfm_forms`, &stats.Forms, "dfm forms"},
		{`SELECT COUNT(*) FROM h_files_defines`, &stats.Defines, "h defines"},
		{`SELECT COUNT(*) FROM report_forms`, &stats.ReportForms, "report forms"},
		{`SELECT COUNT(*) FROM report_fields`, &stats.ReportFields, "report fields"},
		{`SELECT COUNT(*) FROM report_params`, &stats.ReportParams, "report params"},
		{`SELECT COUNT(*) FROM vb_functions`, &stats.VBFunctions, "vb functions"},
		{`SELECT COUNT(*) FROM api_business_objects`, &stats.APIBusinessObjects, "api business objects"},
		{`SELECT COUNT(*) FROM api_contracts`, &stats.APIContracts, "api contracts"},
		{`SELECT COUNT(*) FROM api_contract_params`, &stats.APIContractParams, "api contract params"},
		{`SELECT COUNT(*) FROM api_contract_tables`, &stats.APIContractTables, "api contract tables"},
		{`SELECT COUNT(*) FROM api_contract_table_fields`, &stats.APIContractFields, "api contract table fields"},
		{`SELECT COUNT(*) FROM api_business_object_params`, &stats.APIBusinessParams, "api business params"},
		{`SELECT COUNT(*) FROM api_business_object_tables`, &stats.APIBusinessTables, "api business tables"},
		{`SELECT COUNT(*) FROM sql_index_definitions`, &stats.SQLTableIndexes, "sql table indexes"},
		{`SELECT COUNT(*) FROM api_business_object_table_indexes`, &stats.APITableIndexes, "api business table indexes"},
		{`SELECT COUNT(*) FROM api_macro_invocations`, &stats.APIMacros, "api macro invocations"},
		{`SELECT COUNT(*) FROM query_fragments`, &stats.QueryFragments, "query fragments"},
		{`SELECT COUNT(*) FROM relations`, &stats.Relations, "relations"},
	}

	for _, aggregate := range aggregates {
		if err := db.QueryRowContext(ctx, aggregate.query).Scan(aggregate.target); err != nil {
			return nil, fmt.Errorf("failed to get %s count: %w", aggregate.name, err)
		}
	}

	var finishedAt sql.NullTime
	var status sql.NullString
	var errorsCount sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT id, started_at, finished_at, status, errors_count
		FROM scan_runs
		ORDER BY started_at DESC, id DESC
		LIMIT 1
	`).Scan(&stats.LastScanID, &stats.LastScanStarted, &finishedAt, &status, &errorsCount); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get last scan info: %w", err)
		}
		return stats, nil
	}

	if finishedAt.Valid {
		stats.LastScanFinished = finishedAt.Time
	}
	if status.Valid {
		stats.LastScanStatus = status.String
	}
	if errorsCount.Valid {
		stats.Errors = int(errorsCount.Int64)
	}

	return stats, nil
}
