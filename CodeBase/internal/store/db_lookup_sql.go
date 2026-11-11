package store

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// FindLatestSQLProcedureIDsByNames возвращает map нижнего имени процедуры -> id.
// Кандидат выбирается по приоритету: не-генерируемый источник (не UPLOAD/.t01),
// совпадение ветки scope, совпадение продукта scope, default-ветка (is_default),
// затем самый свежий по id. Нулевой scope валиден — продуктово-веточные
// предпочтения не применяются, приоритет не-копий и default-ветки сохраняется.
// При ненулевом ProductID множество кандидатов ограничено ярусами scope
// (scopeTierFilter): ветка источника → default-ветка продукта → другие
// продукты; копии остальных веток того же продукта исключаются.
func (db *DB) FindLatestSQLProcedureIDsByNames(ctx context.Context, procNames []string, scope LookupScope) (map[string]int64, error) {
	normalized := make([]string, 0, len(procNames))
	seen := make(map[string]struct{}, len(procNames))
	for _, procName := range procNames {
		key := strings.ToLower(strings.TrimSpace(procName))
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, key)
	}
	result := make(map[string]int64, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT ON (proc_key) proc_key, id
		FROM (
			SELECT LOWER(p.proc_name) AS proc_key, p.id AS id,
			       (NOT f.is_generated) AS not_generated,
			       COALESCE(f.ds_product_branch_id = $3, FALSE) AS branch_match,
			       COALESCE(f.ds_product_id = $2, FALSE) AS product_match,
			       COALESCE(br.is_default, FALSE) AS default_branch
			FROM sql_procedures p
			JOIN files f ON f.id = p.file_id
			LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
			WHERE LOWER(p.proc_name) = ANY($1)
			  AND (
				    ($3 <> 0 AND f.ds_product_branch_id = $3)
				 OR (f.ds_product_id = $2 AND (br.id IS NULL OR br.is_default))
				 OR ($2 = 0 OR COALESCE(f.ds_product_id, 0) <> $2)
			  )
		) AS procedures
		ORDER BY proc_key, not_generated DESC, branch_match DESC, product_match DESC, default_branch DESC, id DESC
	`, pq.Array(normalized), scope.ProductID, scope.BranchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var procName string
		var id int64
		if err := rows.Scan(&procName, &id); err != nil {
			return nil, err
		}
		result[strings.ToLower(strings.TrimSpace(procName))] = id
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindSQLProcedureIDsByFile возвращает id процедур файла по имени.
func (db *DB) FindSQLProcedureIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, proc_name
		FROM sql_procedures
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var procName string
		if err := rows.Scan(&id, &procName); err != nil {
			return nil, err
		}
		key := strings.ToLower(strings.TrimSpace(procName))
		if key == "" {
			continue
		}
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindLatestSQLTableIDsByNames возвращает map нижнего имени таблицы -> последний id.
// Один SQL-запрос вместо N вызовов скалярного lookup.
// Приоритет кандидатов как в FindLatestSQLProcedureIDsByNames:
// не-генерируемый источник → ветка scope → продукт scope → default-ветка → id.
func (db *DB) FindLatestSQLTableIDsByNames(ctx context.Context, tableNames []string, scope LookupScope) (map[string]int64, error) {
	normalized := make([]string, 0, len(tableNames))
	seen := make(map[string]struct{})
	for _, name := range tableNames {
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
	result := make(map[string]int64, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT ON (table_key) table_key, id
		FROM (
			SELECT LOWER(t.table_name) AS table_key, t.id AS id,
			       (NOT f.is_generated) AS not_generated,
			       COALESCE(f.ds_product_branch_id = $3, FALSE) AS branch_match,
			       COALESCE(f.ds_product_id = $2, FALSE) AS product_match,
			       COALESCE(br.is_default, FALSE) AS default_branch
			FROM sql_tables t
			JOIN files f ON f.id = t.file_id
			LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
			WHERE LOWER(t.table_name) = ANY($1)
			  AND (
				    ($3 <> 0 AND f.ds_product_branch_id = $3)
				 OR (f.ds_product_id = $2 AND (br.id IS NULL OR br.is_default))
				 OR ($2 = 0 OR COALESCE(f.ds_product_id, 0) <> $2)
			  )
		) AS tables
		ORDER BY table_key, not_generated DESC, branch_match DESC, product_match DESC, default_branch DESC, id DESC
	`, pq.Array(normalized), scope.ProductID, scope.BranchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var tableKey string
		var id int64
		if err := rows.Scan(&tableKey, &id); err != nil {
			return nil, err
		}
		result[tableKey] = id
	}
	return result, rows.Err()
}

// FindLatestSQLColumnDefinitionType возвращает тип колонки таблицы. При
// ненулевом контексте определение выбирается по приоритету value-режима
// review (design D3): определение из указанного файла, затем разрешённая
// область (unnest-пары product/branch; для своего продукта это продукт и
// ветка файла, для чужих — разрешённая ветка/область EXT), затем продукт
// scope, default-ветка, затем самое свежее глобальное (id DESC). Пустые
// pairs вырождают областной тир — поведение до скоупинга. Нулевой контекст
// сохраняет прежнее поведение — глобальный latest-wins (с предпочтением
// default-ветки).
func (db *DB) FindLatestSQLColumnDefinitionType(ctx context.Context, tableName string, columnName string, fileID int64, scope LookupScope, pairs ProductBranchPairs) (string, error) {
	var dataType string
	err := db.QueryRowContext(ctx, `
		SELECT scd.data_type
		FROM sql_column_definitions scd
		JOIN files f ON f.id = scd.file_id
		LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
		WHERE LOWER(scd.table_name) = LOWER($1)
		  AND LOWER(scd.column_name) = LOWER($2)
		  AND TRIM(COALESCE(scd.data_type, '')) <> ''
		  AND scd.data_type <> 'DSUNKNOWN'
		ORDER BY (scd.file_id = $3) DESC,
		         EXISTS (
		             SELECT 1 FROM unnest($6::bigint[], $7::bigint[]) AS pb(pid, bid)
		             WHERE pb.pid = f.ds_product_id
		               AND (pb.bid = 0 OR pb.bid = f.ds_product_branch_id)
		         ) DESC,
		         (COALESCE(f.ds_product_branch_id = $5, FALSE)) DESC,
		         (COALESCE(f.ds_product_id = $4, FALSE)) DESC,
		         (COALESCE(br.is_default, FALSE)) DESC,
		         scd.id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName), strings.TrimSpace(columnName), fileID, scope.ProductID, scope.BranchID,
		pq.Array(pairs.Products), pq.Array(pairs.Branches)).Scan(&dataType)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(dataType), nil
}

// FindAPIColumnDefinitionType ищет тип колонки в API-контрактах и business objects.
// Используется как fallback, когда тип не найден в sql_column_definitions (например, для ptable).
// При ненулевом productID применяется приоритет value-режима review: разрешённая
// область (unnest-пары), продукт scope, default-ветка, затем самый свежий (id DESC).
// Тиры синхронизированы с FindLatestSQLColumnDefinitionType (без тира файла —
// определения API не живут в SQL-файле review). Пустые pairs вырождают
// областной тир; нулевой productID даёт глобальный выбор с предпочтением
// default-ветки.
func (db *DB) FindAPIColumnDefinitionType(ctx context.Context, tableName string, columnName string, productID int64, pairs ProductBranchPairs) (string, error) {
	var dataType string
	areaExpr := `
	             EXISTS (
	                 SELECT 1 FROM unnest($4::bigint[], $5::bigint[]) AS pb(pid, bid)
	                 WHERE pb.pid = fl.ds_product_id
	                   AND (pb.bid = 0 OR pb.bid = fl.ds_product_branch_id)
	             )`
	err := db.QueryRowContext(ctx, `
		SELECT type_name FROM (
			SELECT f.type_name AS type_name, f.id AS id,
			       COALESCE(fl.ds_product_id = $3, FALSE) AS product_match,
			       `+areaExpr+` AS area_match,
			       COALESCE(br.is_default, FALSE) AS default_branch
			FROM api_contract_table_fields f
			JOIN api_contract_tables t ON t.id = f.contract_table_id
			JOIN api_contracts c ON c.id = t.contract_id
			JOIN files fl ON fl.id = c.file_id
			LEFT JOIN ds_products_branch br ON br.id = fl.ds_product_branch_id
			WHERE LOWER(t.table_name) = LOWER($1)
			  AND LOWER(f.field_name) = LOWER($2)
			  AND TRIM(COALESCE(f.type_name, '')) <> ''
			UNION ALL
			SELECT f.type_name AS type_name, f.id AS id,
			       COALESCE(fl.ds_product_id = $3, FALSE) AS product_match,
			       `+areaExpr+` AS area_match,
			       COALESCE(br.is_default, FALSE) AS default_branch
			FROM api_business_object_table_fields f
			JOIN api_business_object_tables t ON t.id = f.business_table_id
			JOIN files fl ON fl.id = t.file_id
			LEFT JOIN ds_products_branch br ON br.id = fl.ds_product_branch_id
			WHERE LOWER(t.table_name) = LOWER($1)
			  AND LOWER(f.field_name) = LOWER($2)
			  AND TRIM(COALESCE(f.type_name, '')) <> ''
		) combined
		ORDER BY area_match DESC, product_match DESC, default_branch DESC, id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName), strings.TrimSpace(columnName), productID,
		pq.Array(pairs.Products), pq.Array(pairs.Branches)).Scan(&dataType)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(dataType), nil
}

// BatchFindColumnDefinitionTypes возвращает карту "table|col" -> data_type для всех
// колонок указанных таблиц одним запросом. Используется для предзагрузки кэша типов
// перед запуском правил review, чтобы избежать тысяч отдельных DB-запросов.
// Приоритет синхронизирован с ленивым FindLatestSQLColumnDefinitionType
// (value-режим design D3): определение из указанного файла, разрешённая
// область (unnest-пары product/branch), ветка файла, продукт, default-ветка,
// самое свежее. Пустые pairs вырождают областной тир (поведение до скоупинга).
func (db *DB) BatchFindColumnDefinitionTypes(ctx context.Context, tableNames []string, fileID int64, scope LookupScope, pairs ProductBranchPairs) (map[string]string, error) {
	if len(tableNames) == 0 {
		return map[string]string{}, nil
	}
	lowerNames := make([]string, len(tableNames))
	for i, t := range tableNames {
		lowerNames[i] = strings.ToLower(strings.TrimSpace(t))
	}
	rows, err := db.QueryContext(ctx, `
		SELECT LOWER(scd.table_name), LOWER(scd.column_name), scd.data_type
		FROM sql_column_definitions scd
		JOIN files f ON f.id = scd.file_id
		LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
		WHERE LOWER(scd.table_name) = ANY($1)
		  AND TRIM(COALESCE(scd.data_type, '')) <> ''
		  AND scd.data_type <> 'DSUNKNOWN'
		ORDER BY (scd.file_id = $2) DESC,
		         EXISTS (
		             SELECT 1 FROM unnest($5::bigint[], $6::bigint[]) AS pb(pid, bid)
		             WHERE pb.pid = f.ds_product_id
		               AND (pb.bid = 0 OR pb.bid = f.ds_product_branch_id)
		         ) DESC,
		         (COALESCE(f.ds_product_branch_id = $4, FALSE)) DESC,
		         (COALESCE(f.ds_product_id = $3, FALSE)) DESC,
		         (COALESCE(br.is_default, FALSE)) DESC,
		         scd.id DESC
	`, pq.Array(lowerNames), fileID, scope.ProductID, scope.BranchID,
		pq.Array(pairs.Products), pq.Array(pairs.Branches))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var tbl, col, dtype string
		if err := rows.Scan(&tbl, &col, &dtype); err != nil {
			return nil, err
		}
		key := tbl + "|" + col
		if _, exists := result[key]; !exists {
			result[key] = strings.TrimSpace(dtype)
		}
	}
	return result, rows.Err()
}

// FindSQLTableIDsByFileAndLine возвращает id таблиц файла по имени, контексту и строке.
func (db *DB) FindSQLTableIDsByFileAndLine(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, table_name, context, line_number
		FROM sql_tables
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var tableName string
		var context string
		var lineNumber int
		if err := rows.Scan(&id, &tableName, &context, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildSQLTableLookupKey(tableName, context, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (db *DB) FindSQLColumnDefinitionIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, table_name, column_name, line_number, column_order
		FROM sql_column_definitions
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var tableName string
		var columnName string
		var lineNumber int
		var columnOrder int
		if err := rows.Scan(&id, &tableName, &columnName, &lineNumber, &columnOrder); err != nil {
			return nil, err
		}
		key := BuildSQLColumnDefinitionLookupKey(tableName, columnName, lineNumber, columnOrder)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

// FindSQLIndexDefinitionIDsByFile возвращает id SQL-индексов по file_id.
func (db *DB) FindSQLIndexDefinitionIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, table_name, index_name, line_number
		FROM sql_index_definitions
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var tableName string
		var indexName string
		var lineNumber int
		if err := rows.Scan(&id, &tableName, &indexName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildSQLIndexDefinitionLookupKey(tableName, indexName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

// FindQueryFragmentIDsByFileAndHash возвращает id query fragments файла по hash/context/line.
func (db *DB) FindQueryFragmentIDsByFileAndHash(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, COALESCE(query_hash, ''), context, line_number
		FROM query_fragments
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var queryHash string
		var context string
		var lineNumber int
		if err := rows.Scan(&id, &queryHash, &context, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildQueryFragmentLookupKey(queryHash, context, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// BatchLookupProcedureParams возвращает map: LOWER(proc_name) -> []SQLParam
// для всех указанных имён процедур одним запросом. Приоритет кандидата
// (value-режим review, design D3): не-генерируемый источник (не UPLOAD/.t01),
// разрешённая область (unnest-пары product/branch; для своего продукта это
// продукт и ветка файла, для чужих — разрешённая ветка/область EXT), продукт
// scope, default-ветка, затем самый свежий по id. Пустые pairs вырождают
// областной тир (поведение до скоупинга).
func (db *DB) BatchLookupProcedureParams(ctx context.Context, names []string, scope LookupScope, pairs ProductBranchPairs) (map[string][]model.SQLParam, error) {
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
	result := make(map[string][]model.SQLParam, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT ON (proc_key) proc_key, parameters
		FROM (
			SELECT LOWER(p.proc_name) AS proc_key, p.parameters, p.id AS id,
			       (NOT f.is_generated) AS not_generated,
			       EXISTS (
			           SELECT 1 FROM unnest($2::bigint[], $3::bigint[]) AS pb(pid, bid)
			           WHERE pb.pid = f.ds_product_id
			             AND (pb.bid = 0 OR pb.bid = f.ds_product_branch_id)
			       ) AS area_match,
			       COALESCE(f.ds_product_id = $4, FALSE) AS product_match,
			       COALESCE(br.is_default, FALSE) AS default_branch
			FROM sql_procedures p
			JOIN files f ON f.id = p.file_id
			LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
			WHERE LOWER(p.proc_name) = ANY($1)
		) AS procedures
		ORDER BY proc_key, not_generated DESC, area_match DESC, product_match DESC, default_branch DESC, id DESC
	`, pq.Array(normalized),
		pq.Array(pairs.Products), pq.Array(pairs.Branches), scope.ProductID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var procKey string
		var paramsJSON []byte
		if err := rows.Scan(&procKey, &paramsJSON); err != nil {
			return nil, err
		}
		if len(paramsJSON) > 0 {
			var params []model.SQLParam
			if err := json.Unmarshal(paramsJSON, &params); err != nil {
				return nil, err
			}
			if len(params) > 0 {
				result[procKey] = params
			}
		}
	}
	return result, rows.Err()
}

// ProcedureScopeInfo — результат identity-lookup процедуры для review:
// продукт-владелец имени (свой продукт проверяется раньше чужого — любая
// копия в продукте scope перекрывает чужие объявления) и существование
// копии в разрешённой области владельца (режим existence: для своего
// продукта это ветка файла, для чужого — разрешённая ветка/область EXT).
type ProcedureScopeInfo struct {
	ProductID    int64
	ExistsInArea bool
}

// BatchLookupProcedureProductIDs возвращает map: LOWER(proc_name) ->
// ProcedureScopeInfo для всех указанных имён процедур. Владелец выбирается
// по приоритету identity-режима (design D3): свой продукт (scope.ProductID,
// любая ветка, включая генерируемые копии) → не-генерируемый источник →
// самый свежий по id. ExistsInArea = true, если у владельца есть копия в
// разрешённой области (unnest-пары); пустые pairs дают ExistsInArea = false
// для всех имён — вызовы без областей должны передавать непустые пары либо
// трактовать false как «существование неизвестно».
func (db *DB) BatchLookupProcedureProductIDs(ctx context.Context, names []string, scope LookupScope, pairs ProductBranchPairs) (map[string]ProcedureScopeInfo, error) {
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
	result := make(map[string]ProcedureScopeInfo, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT LOWER(p.proc_name) AS proc_key,
		       COALESCE(f.ds_product_id, 0) AS product_id,
		       (NOT f.is_generated) AS not_generated,
		       p.id AS id,
		       EXISTS (
		           SELECT 1 FROM unnest($2::bigint[], $3::bigint[]) AS pb(pid, bid)
		           WHERE pb.pid = f.ds_product_id
		             AND (pb.bid = 0 OR pb.bid = f.ds_product_branch_id)
		       ) AS in_area
		FROM sql_procedures p
		JOIN files f ON f.id = p.file_id
		WHERE LOWER(p.proc_name) = ANY($1)
		  AND f.ds_product_id IS NOT NULL
	`, pq.Array(normalized),
		pq.Array(pairs.Products), pq.Array(pairs.Branches))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type procCopy struct {
		productID    int64
		notGenerated bool
		id           int64
		inArea       bool
	}
	copiesByName := make(map[string][]procCopy, len(normalized))
	for rows.Next() {
		var procKey string
		var copy procCopy
		if err := rows.Scan(&procKey, &copy.productID, &copy.notGenerated, &copy.id, &copy.inArea); err != nil {
			return nil, err
		}
		copiesByName[procKey] = append(copiesByName[procKey], copy)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for procKey, copies := range copiesByName {
		owner := copies[0]
		for _, c := range copies[1:] {
			ownerBetter := func(a, b procCopy) bool {
				ownA := a.productID == scope.ProductID && scope.ProductID != 0
				ownB := b.productID == scope.ProductID && scope.ProductID != 0
				if ownA != ownB {
					return ownA
				}
				if a.notGenerated != b.notGenerated {
					return a.notGenerated
				}
				return a.id > b.id
			}
			if ownerBetter(c, owner) {
				owner = c
			}
		}
		exists := false
		for _, c := range copies {
			if c.productID == owner.productID && c.inArea {
				exists = true
				break
			}
		}
		result[procKey] = ProcedureScopeInfo{ProductID: owner.productID, ExistsInArea: exists}
	}
	return result, nil
}

// BatchLookupTableProductIDs возвращает map: LOWER(table_name) -> set of ds_product_id
// для всех указанных таблиц. Использует 3 источника (как lookupTableProductIDs):
// 1) sql_tables WHERE context IN ('create','select_into')
// 2) sql_column_definitions WHERE definition_kind IN ('create','select_into')
// 3) sql_tables WHERE context = 'dfm_embedded' (fallback)
// Источники 2 и 3 опрашиваются только для имён, не найденных в предыдущих.
func (db *DB) BatchLookupTableProductIDs(ctx context.Context, names []string) (map[string]map[int64]struct{}, error) {
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
	result := make(map[string]map[int64]struct{}, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}

	scanRows := func(query string, args []string) error {
		rows, err := db.QueryContext(ctx, query, pq.Array(args))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var tableKey string
			var productID int64
			if err := rows.Scan(&tableKey, &productID); err != nil {
				return err
			}
			if _, ok := result[tableKey]; !ok {
				result[tableKey] = make(map[int64]struct{})
			}
			result[tableKey][productID] = struct{}{}
		}
		return rows.Err()
	}

	if err := scanRows(`
		SELECT DISTINCT LOWER(t.table_name), f.ds_product_id
		FROM sql_tables t
		JOIN files f ON f.id = t.file_id
		WHERE LOWER(t.table_name) = ANY($1)
		  AND t.context IN ('create', 'select_into')
		  AND f.ds_product_id IS NOT NULL
	`, normalized); err != nil {
		return nil, err
	}

	missing := make([]string, 0, len(normalized))
	for _, name := range normalized {
		if len(result[name]) == 0 {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		if err := scanRows(`
			SELECT DISTINCT LOWER(scd.table_name), f.ds_product_id
			FROM sql_column_definitions scd
			JOIN files f ON f.id = scd.file_id
			WHERE LOWER(scd.table_name) = ANY($1)
			  AND scd.definition_kind IN ('create', 'select_into')
			  AND f.ds_product_id IS NOT NULL
		`, missing); err != nil {
			return nil, err
		}
	}

	missing = missing[:0]
	for _, name := range normalized {
		if len(result[name]) == 0 {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		if err := scanRows(`
			SELECT DISTINCT LOWER(t.table_name), f.ds_product_id
			FROM sql_tables t
			JOIN files f ON f.id = t.file_id
			WHERE LOWER(t.table_name) = ANY($1)
			  AND t.context = 'dfm_embedded'
			  AND f.ds_product_id IS NOT NULL
		`, missing); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// BatchLookupTableCreators возвращает map: LOWER(table_name) -> true для всех
// указанных таблиц, имеющих источник создания в индексе: DDL-скрипт
// (sql_tables.context IN ('create','select_into')), SELECT ... INTO
// (sql_column_definitions.definition_kind='select_into'), объявление в
// DSArchitect (api_contract_tables, api_business_object_tables).
// При непустых pairs существование подтверждается только источником в
// разрешённой области его продукта (режим existence, design D3/D4): файл
// источника джойнится с files и проверяется по unnest-парам product/branch
// (api_contract_tables — через api_contracts.file_id). Пустые pairs —
// прежнее глобальное поведение без фильтра по ветке.
// Предикат LOWER(table_name) = ANY дублируется в каждой ветви UNION ALL —
// проталкивание предиката сквозь UNION планировщиком не гарантируется, а
// внутриветвевой фильтр гарантирует использование индексов lower(table_name).
// Дедупликация имён выполняется Go-мапой (без DISTINCT).
func (db *DB) BatchLookupTableCreators(ctx context.Context, names []string, pairs ProductBranchPairs) (map[string]bool, error) {
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
	result := make(map[string]bool, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}

	// areaFilter — предикат принадлежности файла источника разрешённой
	// области (alias f). Подставляется только при непустых pairs.
	const areaFilter = ` AND EXISTS (
		       SELECT 1 FROM unnest($2::bigint[], $3::bigint[]) AS pb(pid, bid)
		       WHERE pb.pid = f.ds_product_id
		         AND (pb.bid = 0 OR pb.bid = f.ds_product_branch_id)
		   )`

	if pairs.IsEmpty() {
		rows, err := db.QueryContext(ctx, `
			SELECT LOWER(table_name) FROM sql_tables
			 WHERE context IN ('create','select_into') AND LOWER(table_name) = ANY($1)
			UNION ALL
			SELECT LOWER(scd.table_name) FROM sql_column_definitions scd
			 WHERE scd.definition_kind = 'select_into' AND LOWER(scd.table_name) = ANY($1)
			UNION ALL
			SELECT LOWER(table_name) FROM api_contract_tables
			 WHERE LOWER(table_name) = ANY($1)
			UNION ALL
			SELECT LOWER(table_name) FROM api_business_object_tables
			 WHERE LOWER(table_name) = ANY($1)
		`, pq.Array(normalized))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			result[name] = true
		}
		return result, rows.Err()
	}

	rows, err := db.QueryContext(ctx, `
		SELECT LOWER(t.table_name)
		FROM sql_tables t
		JOIN files f ON f.id = t.file_id
		WHERE t.context IN ('create','select_into') AND LOWER(t.table_name) = ANY($1)`+areaFilter+`
		UNION ALL
		SELECT LOWER(scd.table_name)
		FROM sql_column_definitions scd
		JOIN files f ON f.id = scd.file_id
		WHERE scd.definition_kind = 'select_into' AND LOWER(scd.table_name) = ANY($1)`+areaFilter+`
		UNION ALL
		SELECT LOWER(act.table_name)
		FROM api_contract_tables act
		JOIN api_contracts c ON c.id = act.contract_id
		JOIN files f ON f.id = c.file_id
		WHERE LOWER(act.table_name) = ANY($1)`+areaFilter+`
		UNION ALL
		SELECT LOWER(abt.table_name)
		FROM api_business_object_tables abt
		JOIN files f ON f.id = abt.file_id
		WHERE LOWER(abt.table_name) = ANY($1)`+areaFilter+`
	`, pq.Array(normalized),
		pq.Array(pairs.Products), pq.Array(pairs.Branches))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result[name] = true
	}
	return result, rows.Err()
}
