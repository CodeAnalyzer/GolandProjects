package store

// FindLatestDFMFormIDByClassName возвращает последний id DFM формы по имени класса формы.
func (db *DB) FindLatestDFMFormIDByClassName(className string) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM dfm_forms
		WHERE LOWER(form_class) = LOWER($1)
		ORDER BY id DESC
		LIMIT 1
	`, className).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindLatestDFMComponentIDByFormAndName возвращает последний id DFM-компонента по форме и имени компонента.
func (db *DB) FindLatestDFMComponentIDByFormAndName(formID int64, componentName string) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM dfm_components
		WHERE form_id = $1
		  AND LOWER(component_name) = LOWER($2)
		ORDER BY id DESC
		LIMIT 1
	`, formID, componentName).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindDFMComponentIDsByForm возвращает id компонентов формы по имени и line_start.
func (db *DB) FindDFMComponentIDsByForm(formID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, component_name, line_start
		FROM dfm_components
		WHERE form_id = $1
		ORDER BY id DESC
	`, formID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var componentName string
		var lineStart int
		if err := rows.Scan(&id, &componentName, &lineStart); err != nil {
			return nil, err
		}
		key := BuildDFMComponentLookupKey(componentName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindDFMFormIDsByFile возвращает id DFM форм файла по имени и line_start.
func (db *DB) FindDFMFormIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, form_name, line_start
		FROM dfm_forms
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
		var formName string
		var lineStart int
		if err := rows.Scan(&id, &formName, &lineStart); err != nil {
			return nil, err
		}
		key := BuildDFMFormLookupKey(formName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindDFMFormIDByFileAndLine возвращает id DFM формы, диапазон которой включает строку.
func (db *DB) FindDFMFormIDByFileAndLine(fileID int64, lineNumber int) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM dfm_forms
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
