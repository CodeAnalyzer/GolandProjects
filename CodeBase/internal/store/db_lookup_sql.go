package store

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// FindLatestSQLProcedureIDByName возвращает последний id SQL процедуры по имени.
func (db *DB) FindLatestSQLProcedureIDByName(ctx context.Context, procName string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		SELECT id
		FROM sql_procedures
		WHERE LOWER(proc_name) = LOWER($1)
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(procName)).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (db *DB) FindLatestSQLProcedureIDsByNames(ctx context.Context, procNames []string) (map[string]int64, error) {
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
			SELECT LOWER(proc_name) AS proc_key, id
			FROM sql_procedures
			WHERE LOWER(proc_name) = ANY($1)
		) AS procedures
		ORDER BY proc_key, id DESC
	`, pq.Array(normalized))
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

// FindLatestSQLTableIDByName возвращает последний id SQL таблицы по имени.
func (db *DB) FindLatestSQLTableIDByName(ctx context.Context, tableName string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		SELECT id
		FROM sql_tables
		WHERE LOWER(table_name) = LOWER($1)
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName)).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindLatestSQLTableIDsByNames возвращает map нижнего имени таблицы -> последний id.
// Один SQL-запрос вместо N вызовов FindLatestSQLTableIDByName.
func (db *DB) FindLatestSQLTableIDsByNames(ctx context.Context, tableNames []string) (map[string]int64, error) {
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
			SELECT LOWER(table_name) AS table_key, id
			FROM sql_tables
			WHERE LOWER(table_name) = ANY($1)
		) AS tables
		ORDER BY table_key, id DESC
	`, pq.Array(normalized))
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

func (db *DB) FindLatestSQLColumnDefinitionType(ctx context.Context, tableName string, columnName string) (string, error) {
	var dataType string
	err := db.QueryRowContext(ctx, `
		SELECT data_type
		FROM sql_column_definitions
		WHERE LOWER(table_name) = LOWER($1)
		  AND LOWER(column_name) = LOWER($2)
		  AND TRIM(COALESCE(data_type, '')) <> ''
		  AND data_type <> 'DSUNKNOWN'
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName), strings.TrimSpace(columnName)).Scan(&dataType)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(dataType), nil
}

// FindAPIColumnDefinitionType ищет тип колонки в API-контрактах и business objects.
// Используется как fallback, когда тип не найден в sql_column_definitions (например, для ptable).
func (db *DB) FindAPIColumnDefinitionType(ctx context.Context, tableName string, columnName string) (string, error) {
	var dataType string
	err := db.QueryRowContext(ctx, `
		SELECT type_name FROM (
			SELECT f.type_name AS type_name, f.id AS id
			FROM api_contract_table_fields f
			JOIN api_contract_tables t ON t.id = f.contract_table_id
			WHERE LOWER(t.table_name) = LOWER($1)
			  AND LOWER(f.field_name) = LOWER($2)
			  AND TRIM(COALESCE(f.type_name, '')) <> ''
			UNION ALL
			SELECT f.type_name AS type_name, f.id AS id
			FROM api_business_object_table_fields f
			JOIN api_business_object_tables t ON t.id = f.business_table_id
			WHERE LOWER(t.table_name) = LOWER($1)
			  AND LOWER(f.field_name) = LOWER($2)
			  AND TRIM(COALESCE(f.type_name, '')) <> ''
		) combined
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName), strings.TrimSpace(columnName)).Scan(&dataType)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(dataType), nil
}

// BatchFindColumnDefinitionTypes возвращает карту "table|col" -> data_type для всех
// колонок указанных таблиц одним запросом. Используется для предзагрузки кэша типов
// перед запуском правил review, чтобы избежать тысяч отдельных DB-запросов.
func (db *DB) BatchFindColumnDefinitionTypes(ctx context.Context, tableNames []string) (map[string]string, error) {
	if len(tableNames) == 0 {
		return map[string]string{}, nil
	}
	lowerNames := make([]string, len(tableNames))
	for i, t := range tableNames {
		lowerNames[i] = strings.ToLower(strings.TrimSpace(t))
	}
	rows, err := db.QueryContext(ctx, `
		SELECT LOWER(table_name), LOWER(column_name), data_type
		FROM sql_column_definitions
		WHERE LOWER(table_name) = ANY($1)
		  AND TRIM(COALESCE(data_type, '')) <> ''
		  AND data_type <> 'DSUNKNOWN'
		ORDER BY id DESC
	`, pq.Array(lowerNames))
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
// для всех указанных имён процедур одним запросом. Берётся последняя по id запись
// для каждого имени (аналог ORDER BY id DESC LIMIT 1 в скалярной версии).
func (db *DB) BatchLookupProcedureParams(ctx context.Context, names []string) (map[string][]model.SQLParam, error) {
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
			SELECT LOWER(proc_name) AS proc_key, id, parameters
			FROM sql_procedures
			WHERE LOWER(proc_name) = ANY($1)
		) AS procedures
		ORDER BY proc_key, id DESC
	`, pq.Array(normalized))
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

// BatchLookupProcedureProductIDs возвращает map: LOWER(proc_name) -> ds_product_id
// для всех указанных имён процедур одним запросом.
func (db *DB) BatchLookupProcedureProductIDs(ctx context.Context, names []string) (map[string]int64, error) {
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
	result := make(map[string]int64, len(normalized))
	if len(normalized) == 0 {
		return result, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT ON (proc_key) proc_key, product_id
		FROM (
			SELECT LOWER(p.proc_name) AS proc_key, f.ds_product_id AS product_id, p.id
			FROM sql_procedures p
			JOIN files f ON f.id = p.file_id
			WHERE LOWER(p.proc_name) = ANY($1)
			  AND f.ds_product_id IS NOT NULL
		) AS procedures
		ORDER BY proc_key, id DESC
	`, pq.Array(normalized))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var procKey string
		var productID int64
		if err := rows.Scan(&procKey, &productID); err != nil {
			return nil, err
		}
		result[procKey] = productID
	}
	return result, rows.Err()
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
