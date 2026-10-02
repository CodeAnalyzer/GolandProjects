package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"

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

// LoadAllSpecCodeMentions загружает все spec_code_mentions для постпроцессинга
// вместе с продуктом файла спеки (контекст приоритетного резолва).
func (db *DB) LoadAllSpecCodeMentions(ctx context.Context) ([]*model.SpecCodeMention, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT m.id, m.file_id, m.source_type, m.source_id, m.mention_name, m.mention_kind, m.line_number,
		       COALESCE(f.ds_product_id, 0)
		FROM spec_code_mentions m
		JOIN files f ON f.id = m.file_id
		ORDER BY m.id
	`)
	if err != nil {
		return nil, fmt.Errorf("load spec_code_mentions: %w", err)
	}
	defer rows.Close()

	var result []*model.SpecCodeMention
	for rows.Next() {
		var r model.SpecCodeMention
		if err := rows.Scan(&r.ID, &r.FileID, &r.SourceType, &r.SourceID, &r.MentionName, &r.MentionKind, &r.LineNumber, &r.ProductID); err != nil {
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

// normalizeLookupNames приводит имена к нижнему регистру, триммит и убирает дубли.
func normalizeLookupNames(names []string) []string {
	normalized := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, key)
	}
	return normalized
}

// lookupEntityIDsByNames — общий пакетный резолв имён сущности в id.
// Приоритет кандидатов: не-генерируемый источник (не UPLOAD/.t01) → источник
// указанного продукта → самый свежий по id. Нулевой productID валиден —
// продуктовое предпочтение не применяется, приоритет не-копий сохраняется.
// table/column передаются только из фиксированных мест вызова (не из ввода пользователя).
func (db *DB) lookupEntityIDsByNames(ctx context.Context, table, column string, names []string, productID int64) (map[string]int64, error) {
	normalized := normalizeLookupNames(names)
	result := make(map[string]int64, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}
	query := `
		SELECT DISTINCT ON (name_key) name_key, id
		FROM (
			SELECT LOWER(e.` + column + `) AS name_key, e.id AS id,
			       (NOT f.is_generated) AS not_generated,
			       COALESCE(f.ds_product_id = $2, FALSE) AS product_match
			FROM ` + table + ` e
			JOIN files f ON f.id = e.file_id
			WHERE LOWER(e.` + column + `) = ANY($1)
		) c
		ORDER BY name_key, not_generated DESC, product_match DESC, id DESC
	`
	rows, err := db.QueryContext(ctx, query, pq.Array(normalized), productID)
	if err != nil {
		return nil, fmt.Errorf("find %s by names: %w", table, err)
	}
	defer rows.Close()
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

// FindDFMFormIDsByNames возвращает map[lower(name)]id для пакетного резолва.
func (db *DB) FindDFMFormIDsByNames(ctx context.Context, names []string, productID int64) (map[string]int64, error) {
	return db.lookupEntityIDsByNames(ctx, "dfm_forms", "form_name", names, productID)
}

// FindSMFInstrumentIDsByNames возвращает map[lower(name)]id.
func (db *DB) FindSMFInstrumentIDsByNames(ctx context.Context, names []string, productID int64) (map[string]int64, error) {
	return db.lookupEntityIDsByNames(ctx, "smf_instruments", "instrument_name", names, productID)
}

// FindPASMethodIDsByNames возвращает map[lower(name)]id.
func (db *DB) FindPASMethodIDsByNames(ctx context.Context, names []string, productID int64) (map[string]int64, error) {
	return db.lookupEntityIDsByNames(ctx, "pas_methods", "method_name", names, productID)
}

// FindAPIContractIDsByNames возвращает map[lower(name)]id.
func (db *DB) FindAPIContractIDsByNames(ctx context.Context, names []string, productID int64) (map[string]int64, error) {
	return db.lookupEntityIDsByNames(ctx, "api_contracts", "contract_name", names, productID)
}

// FindReportFormIDsByNames возвращает map[lower(report_name)]id для пакетного резолва.
func (db *DB) FindReportFormIDsByNames(ctx context.Context, names []string, productID int64) (map[string]int64, error) {
	return db.lookupEntityIDsByNames(ctx, "report_forms", "report_name", names, productID)
}

// FindAPIContractIDsByTableNames возвращает мультикарту lower(table_name) →
// id контрактов-владельцев (DISTINCT contract_id) из api_contract_tables.
// Контракты-копии (is_generated) фильтруются, если среди владельцев таблицы
// есть хотя бы один контракт-не-копия; иначе возвращаются все владельцы.
func (db *DB) FindAPIContractIDsByTableNames(ctx context.Context, tableNames []string) (map[string][]int64, error) {
	normalized := normalizeLookupNames(tableNames)
	if len(normalized) == 0 {
		return map[string][]int64{}, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT c.name_key, array_agg(DISTINCT c.contract_id) AS contract_ids
		FROM (
			SELECT LOWER(t.table_name) AS name_key, t.contract_id AS contract_id,
			       (NOT f.is_generated) AS not_generated
			FROM api_contract_tables t
			JOIN api_contracts c ON c.id = t.contract_id
			JOIN files f ON f.id = c.file_id
			WHERE LOWER(t.table_name) = ANY($1)
		) c
		JOIN (
			SELECT name_key, BOOL_OR(not_generated) AS has_canonical
			FROM (
				SELECT LOWER(t.table_name) AS name_key, (NOT f.is_generated) AS not_generated
				FROM api_contract_tables t
				JOIN api_contracts c ON c.id = t.contract_id
				JOIN files f ON f.id = c.file_id
				WHERE LOWER(t.table_name) = ANY($1)
			) cc
			GROUP BY name_key
		) pref ON pref.name_key = c.name_key
		WHERE (NOT pref.has_canonical) OR c.not_generated
		GROUP BY c.name_key
	`, pq.Array(normalized))
	if err != nil {
		return nil, fmt.Errorf("find api_contracts by table names: %w", err)
	}
	defer rows.Close()
	result := map[string][]int64{}
	for rows.Next() {
		var name string
		var ids pq.Int64Array
		if err := rows.Scan(&name, &ids); err != nil {
			return nil, err
		}
		result[name] = []int64(ids)
	}
	return result, rows.Err()
}
