package store

import (
	"context"
	"fmt"
	"sort"

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

// ProductBranchPairs — плоские пары (product_id, branch_id) разрешённых
// областей review для unnest-предикатов: карта productID → []branchID,
// развёрнутая в два массива одинаковой длины. Пара с branch_id = 0
// означает «любая ветка продукта» (product-scope, branch_id NULL у файла).
// Нулевое значение (пустые массивы) — области не заданы: предикат области
// не применяется, lookup деградирует до прежнего глобального поведения.
type ProductBranchPairs struct {
	Products []int64
	Branches []int64
}

// IsEmpty сообщает, заданы ли пары областей (false — области применяются).
func (p ProductBranchPairs) IsEmpty() bool { return len(p.Products) == 0 }

// NewProductBranchPairs разворачивает карту областей в плоские пары для
// unnest. Порядок детерминирован (product_id по возрастанию) — одинаковые
// пары дают одинаковые параметры запроса.
func NewProductBranchPairs(areas map[int64][]int64) ProductBranchPairs {
	if len(areas) == 0 {
		return ProductBranchPairs{}
	}
	products := make([]int64, 0, len(areas))
	for pid := range areas {
		products = append(products, pid)
	}
	sort.Slice(products, func(i, j int) bool { return products[i] < products[j] })
	pairs := ProductBranchPairs{
		Products: make([]int64, 0, len(products)),
		Branches: make([]int64, 0, len(products)),
	}
	for _, pid := range products {
		for _, bid := range areas[pid] {
			pairs.Products = append(pairs.Products, pid)
			pairs.Branches = append(pairs.Branches, bid)
		}
	}
	return pairs
}

// ProductBranchInfo — строка ds_products_branch, необходимая для построения
// областей видимости review (детект EXT-продуктов по ключу prj/*).
type ProductBranchInfo struct {
	BranchID  int64
	ProductID int64
	BranchKey string
	IsDefault bool
}

// ListDSProductBranches возвращает все релизные линии всех продуктов.
func (db *DB) ListDSProductBranches(ctx context.Context) ([]ProductBranchInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, ds_product_id, branch_key, is_default
		FROM ds_products_branch
		ORDER BY ds_product_id, id
	`)
	if err != nil {
		return nil, fmt.Errorf("list ds product branches: %w", err)
	}
	defer rows.Close()
	result := make([]ProductBranchInfo, 0, 16)
	for rows.Next() {
		var item ProductBranchInfo
		if err := rows.Scan(&item.BranchID, &item.ProductID, &item.BranchKey, &item.IsDefault); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// ListDSProductIDs возвращает id всех продуктов индекса — включая продукты
// без единой релизной линии: их областью видимости является product-scope.
func (db *DB) ListDSProductIDs(ctx context.Context) ([]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT id FROM ds_products ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list ds products: %w", err)
	}
	defer rows.Close()
	result := make([]int64, 0, 16)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
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
