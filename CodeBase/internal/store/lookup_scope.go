package store

import (
	"context"
	"fmt"

	"github.com/lib/pq"
)

// LookupScope — scope выбора копии сущности в FindLatest*-lookup'ах: продукт
// и ветка файла-источника. Нулевые значения означают «любой продукт» и
// «default-ветка»: ось ветки вырождается, выбор идёт по цепочке
// not_generated → branch_match → product_match → default_branch → id DESC
// (см. db_lookup_sql.go, db_lookup_pas.go, db_lookup_dfm.go, api_store.go).
// При ненулевом ProductID множество кандидатов дополнительно ограничено
// тремя ярусами (D4 среза branch-aware-lookup): ветка источника →
// default-ветка продукта (или файлы без ветки) → другие продукты; копии
// остальных веток того же продукта исключаются из кандидатов.
type LookupScope struct {
	ProductID int64 // 0 = любой продукт
	BranchID  int64 // 0 = default-ветка / глобально
}

// entityFileScopeTables — белый список таблиц для EntityFileScopes:
// таблица сущности → колонка файла.
var entityFileScopeTables = map[string]bool{
	"files":           true,
	"sql_procedures":  true,
	"query_fragments": true,
	"js_functions":    true,
	"pas_classes":     true,
	"pas_methods":     true,
	"pas_fields":      true,
	"api_contracts":   true,
}

// EntityFileScopes возвращает scope (продукт, ветка) файла для сущностей
// таблицы entityTable по их id. Для "files" id — это сам file_id. Таблица
// обязана входить в белый список entityFileScopeTables.
func (db *DB) EntityFileScopes(ctx context.Context, entityTable string, ids []int64) (map[int64]LookupScope, error) {
	if !entityFileScopeTables[entityTable] {
		return nil, fmt.Errorf("entity file scopes: table %q is not allowed", entityTable)
	}
	result := make(map[int64]LookupScope, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	unique := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return result, nil
	}

	query := `
		SELECT e.id, COALESCE(f.ds_product_id, 0), COALESCE(f.ds_product_branch_id, 0)
		FROM ` + entityTable + ` e
		JOIN files f ON f.id = e.file_id
		WHERE e.id = ANY($1)`
	if entityTable == "files" {
		query = `
		SELECT id, COALESCE(ds_product_id, 0), COALESCE(ds_product_branch_id, 0)
		FROM files
		WHERE id = ANY($1)`
	}
	rows, err := db.QueryContext(ctx, query, pq.Array(unique))
	if err != nil {
		return nil, fmt.Errorf("entity file scopes (%s): %w", entityTable, err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var scope LookupScope
		if err := rows.Scan(&id, &scope.ProductID, &scope.BranchID); err != nil {
			return nil, err
		}
		result[id] = scope
	}
	return result, rows.Err()
}
