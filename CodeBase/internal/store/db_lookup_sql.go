package store

import (
	"strings"

	"github.com/lib/pq"
)

// FindLatestSQLProcedureIDByName возвращает последний id SQL процедуры по имени.
func (db *DB) FindLatestSQLProcedureIDByName(procName string) (int64, error) {
	var id int64
	err := db.QueryRow(`
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

func (db *DB) FindLatestSQLProcedureIDsByNames(procNames []string) (map[string]int64, error) {
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

	rows, err := db.Query(`
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
func (db *DB) FindSQLProcedureIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
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
func (db *DB) FindLatestSQLTableIDByName(tableName string) (int64, error) {
	var id int64
	err := db.QueryRow(`
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
func (db *DB) FindLatestSQLTableIDsByNames(tableNames []string) (map[string]int64, error) {
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
	rows, err := db.Query(`
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

func (db *DB) FindLatestSQLColumnDefinitionType(tableName string, columnName string) (string, error) {
	var dataType string
	err := db.QueryRow(`
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

// FindSQLTableIDsByFileAndLine возвращает id таблиц файла по имени, контексту и строке.
func (db *DB) FindSQLTableIDsByFileAndLine(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
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

func (db *DB) FindSQLColumnDefinitionIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
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
func (db *DB) FindSQLIndexDefinitionIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
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
func (db *DB) FindQueryFragmentIDsByFileAndHash(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
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
