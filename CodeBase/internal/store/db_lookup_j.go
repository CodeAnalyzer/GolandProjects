package store

// FindJSFunctionIDsByFile возвращает id JS функций файла по имени и line_start.
func (db *DB) FindJSFunctionIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, function_name, line_start
		FROM js_functions
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
		var functionName string
		var lineStart int
		if err := rows.Scan(&id, &functionName, &lineStart); err != nil {
			return nil, err
		}
		key := BuildJSFunctionLookupKey(functionName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindJSFunctionIDByFileAndLine возвращает id JS функции, в диапазон которой попадает строка.
func (db *DB) FindJSFunctionIDByFileAndLine(fileID int64, lineNumber int) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM js_functions
		WHERE file_id = $1
		  AND line_start <= $2
		  AND line_end >= $2
		ORDER BY line_start DESC, id DESC
		LIMIT 1
	`, fileID, lineNumber).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (db *DB) FindJSConstantIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, constant_name, line_number
		FROM js_constants
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
		var constantName string
		var lineNumber int
		if err := rows.Scan(&id, &constantName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildJSConstantLookupKey(constantName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// FindLatestSMFInstrumentIDByFile возвращает последний id SMF инструмента файла.
func (db *DB) FindLatestSMFInstrumentIDByFile(fileID int64) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM smf_instruments
		WHERE file_id = $1
		ORDER BY id DESC
		LIMIT 1
	`, fileID).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}
