package review

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/lib/pq"
)

type indexedFile struct {
	ID          int64
	Path        string
	RelPath     string
	DsProductID int64
}

type tableIndexCandidate struct {
	Name   string
	Fields []string
}

func (r *Runner) getIndexedFile(path string) (*indexedFile, error) {
	variants := []string{path, filepath.ToSlash(path), strings.ReplaceAll(path, "/", `\`)}
	for _, candidate := range variants {
		var item indexedFile
		var dsProduct sql.NullInt64
		err := r.db.QueryRow(`
			SELECT id, path, rel_path, ds_product_id
			FROM files
			WHERE LOWER(path) = LOWER($1) OR LOWER(rel_path) = LOWER($1)
			ORDER BY id DESC
			LIMIT 1
		`, candidate).Scan(&item.ID, &item.Path, &item.RelPath, &dsProduct)
		if err == nil {
			if dsProduct.Valid {
				item.DsProductID = dsProduct.Int64
			}
			item.Path = normalizePath(item.Path)
			return &item, nil
		}
		if err != sql.ErrNoRows {
			return nil, err
		}
	}
	return nil, sql.ErrNoRows
}

func (r *Runner) lookupTableProductID(tableName string) (int64, error) {
	var productID int64
	err := r.db.QueryRow(`
		SELECT f.ds_product_id
		FROM sql_tables t
		JOIN files f ON f.id = t.file_id
		WHERE LOWER(t.table_name) = LOWER($1)
		  AND t.context = 'create'
		  AND f.ds_product_id IS NOT NULL
		ORDER BY t.id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName)).Scan(&productID)
	if err == nil {
		return productID, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	err = r.db.QueryRow(`
		SELECT f.ds_product_id
		FROM sql_tables t
		JOIN files f ON f.id = t.file_id
		WHERE LOWER(t.table_name) = LOWER($1)
		  AND t.context = 'dfm_embedded'
		  AND f.ds_product_id IS NOT NULL
		ORDER BY t.id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName)).Scan(&productID)
	if err != nil {
		return 0, err
	}
	return productID, nil
}

func (r *Runner) lookupProcedureProductID(procName string) (int64, error) {
	var productID int64
	err := r.db.QueryRow(`
		SELECT f.ds_product_id
		FROM sql_procedures p
		JOIN files f ON f.id = p.file_id
		WHERE LOWER(p.proc_name) = LOWER($1)
		  AND f.ds_product_id IS NOT NULL
		ORDER BY p.id DESC
		LIMIT 1
	`, strings.TrimSpace(procName)).Scan(&productID)
	if err != nil {
		return 0, err
	}
	return productID, nil
}

func (r *Runner) lookupProcedureCreateFiles(procName string) ([]int64, error) {
	rows, err := r.db.Query(`
		SELECT DISTINCT f.id
		FROM sql_procedures p
		JOIN files f ON f.id = p.file_id
		WHERE LOWER(p.proc_name) = LOWER($1)
		  AND LOWER(f.path) NOT LIKE '%/upload/%'
		  AND LOWER(f.path) NOT LIKE '%\upload\%'
		  AND LOWER(f.path) NOT LIKE '%.t01'
		ORDER BY f.id
	`, strings.TrimSpace(procName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *Runner) lookupIndexExists(tableName, indexName string) (bool, error) {
	normalizedTable := strings.TrimSpace(tableName)
	normalizedIndex := strings.TrimSpace(indexName)
	if normalizedTable == "" || normalizedIndex == "" {
		return false, nil
	}

	var exists bool
	err := r.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM sql_index_definitions i
			WHERE LOWER(i.table_name) = LOWER($1)
			  AND LOWER(i.index_name) = LOWER($2)
		)
	`, normalizedTable, normalizedIndex).Scan(&exists)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	err = r.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM api_business_object_table_indexes i
			JOIN api_business_object_tables t ON t.id = i.business_table_id
			WHERE LOWER(t.table_name) = LOWER($1)
			  AND LOWER(i.index_name) = LOWER($2)
		)
	`, normalizedTable, normalizedIndex).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (r *Runner) lookupTableIndexCandidates(tableName string) ([]tableIndexCandidate, error) {
	normalizedTable := strings.TrimSpace(tableName)
	if normalizedTable == "" {
		return nil, nil
	}

	type aggregate struct {
		name       string
		fieldsByNo map[int]string
	}

	items := make(map[string]*aggregate)
	order := make([]string, 0)

	consumeRows := func(query string) error {
		rows, err := r.db.Query(query, normalizedTable)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var indexID int64
			var indexName string
			var fieldName sql.NullString
			var fieldOrder sql.NullInt64
			if err := rows.Scan(&indexID, &indexName, &fieldName, &fieldOrder); err != nil {
				return err
			}

			key := fmt.Sprintf("%d:%s", indexID, normalizeIdentifier(indexName))
			agg, exists := items[key]
			if !exists {
				agg = &aggregate{name: strings.TrimSpace(indexName), fieldsByNo: map[int]string{}}
				items[key] = agg
				order = append(order, key)
			}

			if fieldName.Valid {
				field := normalizeIdentifier(fieldName.String)
				if field != "" {
					no := len(agg.fieldsByNo) + 1
					if fieldOrder.Valid && fieldOrder.Int64 > 0 {
						no = int(fieldOrder.Int64)
					}
					agg.fieldsByNo[no] = field
				}
			}
		}
		return rows.Err()
	}

	if err := consumeRows(`
		SELECT i.id, i.index_name, f.field_name, f.field_order
		FROM sql_index_definitions i
		LEFT JOIN sql_index_definition_fields f ON f.table_index_id = i.id
		WHERE LOWER(i.table_name) = LOWER($1)
		ORDER BY i.id, f.field_order, f.id
	`); err != nil {
		return nil, err
	}

	if err := consumeRows(`
		SELECT i.id, i.index_name, f.field_name, f.field_order
		FROM api_business_object_table_indexes i
		JOIN api_business_object_tables t ON t.id = i.business_table_id
		LEFT JOIN api_business_object_table_index_fields f ON f.table_index_id = i.id
		WHERE LOWER(t.table_name) = LOWER($1)
		ORDER BY i.id, f.field_order, f.id
	`); err != nil {
		return nil, err
	}

	result := make([]tableIndexCandidate, 0, len(order))
	for _, key := range order {
		agg := items[key]
		if agg == nil {
			continue
		}
		fields := make([]string, 0, len(agg.fieldsByNo))
		for i := 1; i <= len(agg.fieldsByNo); i++ {
			if field, exists := agg.fieldsByNo[i]; exists {
				fields = append(fields, field)
			}
		}
		result = append(result, tableIndexCandidate{Name: agg.name, Fields: fields})
	}

	return result, nil
}

func (r *Runner) findAPITableNames(names []string) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	normalized := make([]string, 0, len(names))
	seen := map[string]struct{}{}
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
	if len(normalized) == 0 {
		return result, nil
	}

	load := func(query string) error {
		rows, err := r.db.Query(query, pq.Array(normalized))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			result[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
		}
		return rows.Err()
	}

	if err := load(`SELECT LOWER(table_name) FROM api_business_object_tables WHERE LOWER(table_name) = ANY($1)`); err != nil {
		return nil, err
	}
	if err := load(`SELECT LOWER(table_name) FROM api_contract_tables WHERE LOWER(table_name) = ANY($1)`); err != nil {
		return nil, err
	}
	return result, nil
}
