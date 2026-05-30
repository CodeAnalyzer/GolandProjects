package review

import (
	"database/sql"
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
