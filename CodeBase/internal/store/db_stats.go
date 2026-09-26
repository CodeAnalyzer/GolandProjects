package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// GetStats возвращает статистику индекса. Сначала читает сохранённый снапшот
// (обновляется по завершении init/update); если снапшота нет — считает живым
// подсчётом и сохраняет результат для последующих вызовов. Ошибка чтения
// снапшота деградирует в живой подсчёт.
// lsaGeneration — активное поколение LSA (из sidecar-state модели): при непустом
// значении счётчики spec_vocab/spec_embeddings считаются по этому поколению,
// при пустом — по всем поколениям (фолбэк при недоступном state).
func (db *DB) GetStats(ctx context.Context, lsaGeneration string) (*Stats, error) {
	snapshot, storedGeneration, ok, err := db.LoadStatsSnapshot(ctx)
	if err == nil && ok && storedGeneration == lsaGeneration {
		return snapshot, nil
	}

	stats, computeErr := db.computeStats(ctx, lsaGeneration)
	if computeErr != nil {
		return nil, computeErr
	}
	// best-effort: сохраняем снапшот, чтобы следующий вызов с тем же поколением
	// читал его без пересчёта.
	_ = db.SaveStatsSnapshot(ctx, stats, lsaGeneration)
	return stats, nil
}

// RefreshStatsSnapshot пересчитывает статистику живым подсчётом и перезаписывает
// снапшот. Вызывается по завершении init/update.
func (db *DB) RefreshStatsSnapshot(ctx context.Context, lsaGeneration string) error {
	stats, err := db.computeStats(ctx, lsaGeneration)
	if err != nil {
		return err
	}
	return db.SaveStatsSnapshot(ctx, stats, lsaGeneration)
}

// SaveStatsSnapshot перезаписывает единственную строку снапшота вместе с
// поколением LSA, для которого он посчитан.
func (db *DB) SaveStatsSnapshot(ctx context.Context, stats *Stats, lsaGeneration string) error {
	payload, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("failed to marshal stats snapshot: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO stats_snapshot (id, payload, lsa_generation, created_at)
		VALUES (1, $1, $2, NOW())
		ON CONFLICT (id) DO UPDATE SET
			payload = EXCLUDED.payload,
			lsa_generation = EXCLUDED.lsa_generation,
			created_at = NOW()
	`, payload, lsaGeneration); err != nil {
		return fmt.Errorf("failed to save stats snapshot: %w", err)
	}
	return nil
}

// LoadStatsSnapshot читает снапшот и поколение, для которого он посчитан.
// ok=false означает, что снапшота нет.
func (db *DB) LoadStatsSnapshot(ctx context.Context) (*Stats, string, bool, error) {
	var payload []byte
	var lsaGeneration string
	err := db.QueryRowContext(ctx, `SELECT payload, lsa_generation FROM stats_snapshot WHERE id = 1`).Scan(&payload, &lsaGeneration)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", false, nil
		}
		return nil, "", false, fmt.Errorf("failed to load stats snapshot: %w", err)
	}
	var stats Stats
	if err := json.Unmarshal(payload, &stats); err != nil {
		return nil, "", false, fmt.Errorf("failed to decode stats snapshot: %w", err)
	}
	return &stats, lsaGeneration, true, nil
}

func (db *DB) computeStats(ctx context.Context, lsaGeneration string) (*Stats, error) {
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
			COUNT(*) FILTER (WHERE UPPER(extension) = 'RPT') AS rpt_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'MD') AS md_files,
			COUNT(*) FILTER (WHERE UPPER(extension) IN ('YAML','YML')) AS yaml_files
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
		&stats.MDFiles,
		&stats.YAMLFiles,
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
		{`SELECT COUNT(*) FROM spec_configs`, &stats.SpecConfigs, "spec configs"},
		{`SELECT COUNT(*) FROM spec_capabilities`, &stats.SpecCapabilities, "spec capabilities"},
		{`SELECT COUNT(*) FROM spec_requirements`, &stats.SpecRequirements, "spec requirements"},
		{`SELECT COUNT(*) FROM spec_scenarios`, &stats.SpecScenarios, "spec scenarios"},
		{`SELECT COUNT(*) FROM spec_usecases`, &stats.SpecUsecases, "spec usecases"},
		{`SELECT COUNT(*) FROM spec_changes`, &stats.SpecChanges, "spec changes"},
		{`SELECT COUNT(*) FROM spec_change_delta`, &stats.SpecChangeDeltas, "spec change deltas"},
		{`SELECT COUNT(*) FROM spec_code_mentions`, &stats.SpecCodeMentions, "spec code mentions"},
	}

	for _, aggregate := range aggregates {
		if err := db.QueryRowContext(ctx, aggregate.query).Scan(aggregate.target); err != nil {
			return nil, fmt.Errorf("failed to get %s count: %w", aggregate.name, err)
		}
	}

	// Полнотекстовый слой спек: счётчики LSA generation-осведомлённы.
	// Поколения согласованы между spec_vocab и spec_embeddings по конструкции
	// публикации, поэтому число поколений берётся из spec_vocab.
	if lsaGeneration != "" {
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM spec_vocab WHERE generation = $1`, lsaGeneration,
		).Scan(&stats.SpecVocabTerms); err != nil {
			return nil, fmt.Errorf("failed to get spec vocab terms count: %w", err)
		}
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM spec_embeddings WHERE generation = $1`, lsaGeneration,
		).Scan(&stats.SpecEmbeddings); err != nil {
			return nil, fmt.Errorf("failed to get spec embeddings count: %w", err)
		}
	} else {
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM spec_vocab`,
		).Scan(&stats.SpecVocabTerms); err != nil {
			return nil, fmt.Errorf("failed to get spec vocab terms count: %w", err)
		}
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM spec_embeddings`,
		).Scan(&stats.SpecEmbeddings); err != nil {
			return nil, fmt.Errorf("failed to get spec embeddings count: %w", err)
		}
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT generation) FROM spec_vocab`,
	).Scan(&stats.SpecLSAGenerations); err != nil {
		return nil, fmt.Errorf("failed to get spec lsa generations count: %w", err)
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
