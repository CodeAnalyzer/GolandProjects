package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
)

func (db *DB) ResolveSpecMentionSourceIDs(ctx context.Context) error {
	queries := []string{
		`UPDATE spec_code_mentions m
		 SET source_id = COALESCE((
			 SELECT r.id FROM spec_requirements r
			 WHERE r.file_id = m.file_id
			   AND (m.line_number BETWEEN r.line_start AND r.line_end
			        OR POSITION(LOWER(m.mention_name) IN LOWER(r.body_text)) > 0)
			 ORDER BY CASE WHEN m.line_number BETWEEN r.line_start AND r.line_end THEN 0 ELSE 1 END, r.req_order
			 LIMIT 1
		 ), 0)
		 WHERE m.source_type = 'spec_requirement' AND m.source_id = 0`,
		`UPDATE spec_code_mentions m
		 SET source_id = COALESCE((
			 SELECT s.id FROM spec_scenarios s
			 WHERE s.file_id = m.file_id
			   AND (m.line_number BETWEEN s.line_start AND s.line_end
			        OR POSITION(LOWER(m.mention_name) IN LOWER(COALESCE(s.given_text, '') || ' ' || COALESCE(s.when_text, '') || ' ' || COALESCE(s.then_text, ''))) > 0)
			 ORDER BY CASE WHEN m.line_number BETWEEN s.line_start AND s.line_end THEN 0 ELSE 1 END, s.scn_order
			 LIMIT 1
		 ), 0)
		 WHERE m.source_type = 'spec_scenario' AND m.source_id = 0`,
	}
	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("resolve spec mention source ids: %w", err)
		}
	}
	return nil
}

// LoadAllSpecCodeMentions загружает все spec_code_mentions для постпроцессинга.
func (db *DB) LoadAllSpecCodeMentions(ctx context.Context) ([]*model.SpecCodeMention, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, file_id, source_type, source_id, mention_name, mention_kind, line_number
		FROM spec_code_mentions
		ORDER BY id
	`)
	if err != nil {
		return nil, fmt.Errorf("load spec_code_mentions: %w", err)
	}
	defer rows.Close()

	var result []*model.SpecCodeMention
	for rows.Next() {
		var r model.SpecCodeMention
		if err := rows.Scan(&r.ID, &r.FileID, &r.SourceType, &r.SourceID, &r.MentionName, &r.MentionKind, &r.LineNumber); err != nil {
			return nil, err
		}
		result = append(result, &r)
	}
	return result, rows.Err()
}

// DeleteSpecReferenceRelations удаляет все relations с type='references_code'
// где source_type начинается с 'spec_'.
func (db *DB) DeleteSpecReferenceRelations(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `
		DELETE FROM relations
		WHERE relation_type = 'references_code'
		AND source_type IN ('spec_capability', 'spec_requirement', 'spec_scenario', 'spec_usecase')
	`)
	if err != nil {
		return fmt.Errorf("delete spec references_code relations: %w", err)
	}
	return nil
}

// FindDFMFormIDsByNames возвращает map[lower(name)]id для пакетного резолва.
func (db *DB) FindDFMFormIDsByNames(ctx context.Context, names []string) (map[string]int64, error) {
	if len(names) == 0 {
		return map[string]int64{}, nil
	}
	placeholders := make([]string, len(names))
	args := make([]interface{}, len(names))
	for i, name := range names {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = strings.ToLower(name)
	}
	query := fmt.Sprintf(`
		SELECT LOWER(form_name), MAX(id) as id
		FROM dfm_forms
		WHERE LOWER(form_name) IN (%s)
		GROUP BY LOWER(form_name)
	`, strings.Join(placeholders, ","))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find dfm_forms by names: %w", err)
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var name string
		var id int64
		if err := rows.Scan(&name, &id); err != nil {
			return nil, err
		}
		result[name] = id
	}
	return result, rows.Err()
}

// FindSMFInstrumentIDsByNames возвращает map[lower(name)]id.
func (db *DB) FindSMFInstrumentIDsByNames(ctx context.Context, names []string) (map[string]int64, error) {
	if len(names) == 0 {
		return map[string]int64{}, nil
	}
	placeholders := make([]string, len(names))
	args := make([]interface{}, len(names))
	for i, name := range names {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = strings.ToLower(name)
	}
	query := fmt.Sprintf(`
		SELECT LOWER(instrument_name), MAX(id) as id
		FROM smf_instruments
		WHERE LOWER(instrument_name) IN (%s)
		GROUP BY LOWER(instrument_name)
	`, strings.Join(placeholders, ","))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find smf_instruments by names: %w", err)
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var name string
		var id int64
		if err := rows.Scan(&name, &id); err != nil {
			return nil, err
		}
		result[name] = id
	}
	return result, rows.Err()
}

// FindPASMethodIDsByNames возвращает map[lower(name)]id.
func (db *DB) FindPASMethodIDsByNames(ctx context.Context, names []string) (map[string]int64, error) {
	if len(names) == 0 {
		return map[string]int64{}, nil
	}
	placeholders := make([]string, len(names))
	args := make([]interface{}, len(names))
	for i, name := range names {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = strings.ToLower(name)
	}
	query := fmt.Sprintf(`
		SELECT LOWER(method_name), MAX(id) as id
		FROM pas_methods
		WHERE LOWER(method_name) IN (%s)
		GROUP BY LOWER(method_name)
	`, strings.Join(placeholders, ","))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find pas_methods by names: %w", err)
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var name string
		var id int64
		if err := rows.Scan(&name, &id); err != nil {
			return nil, err
		}
		result[name] = id
	}
	return result, rows.Err()
}

// FindAPIContractIDsByNames возвращает map[lower(name)]id.
func (db *DB) FindAPIContractIDsByNames(ctx context.Context, names []string) (map[string]int64, error) {
	if len(names) == 0 {
		return map[string]int64{}, nil
	}
	placeholders := make([]string, len(names))
	args := make([]interface{}, len(names))
	for i, name := range names {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = strings.ToLower(name)
	}
	query := fmt.Sprintf(`
		SELECT LOWER(contract_name), MAX(id) as id
		FROM api_contracts
		WHERE LOWER(contract_name) IN (%s)
		GROUP BY LOWER(contract_name)
	`, strings.Join(placeholders, ","))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find api_contracts by names: %w", err)
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var name string
		var id int64
		if err := rows.Scan(&name, &id); err != nil {
			return nil, err
		}
		result[name] = id
	}
	return result, rows.Err()
}
