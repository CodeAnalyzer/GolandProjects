package store

import "context"

// FindReportFormIDByFileAndLine возвращает id report form, диапазон которой включает строку.
func (db *DB) FindReportFormIDByFileAndLine(ctx context.Context, fileID int64, lineNumber int) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		SELECT id
		FROM report_forms
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

// FindReportFieldIDsByForm возвращает id report fields по форме.
func (db *DB) FindReportFieldIDsByForm(ctx context.Context, reportFormID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, field_name, line_number
		FROM report_fields
		WHERE report_form_id = $1
		ORDER BY id DESC
	`, reportFormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var fieldName string
		var lineNumber int
		if err := rows.Scan(&id, &fieldName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildReportFieldLookupKey(fieldName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

// FindReportParamIDsByForm возвращает id report params по форме.
func (db *DB) FindReportParamIDsByForm(ctx context.Context, reportFormID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, param_name, line_number
		FROM report_params
		WHERE report_form_id = $1
		ORDER BY id DESC
	`, reportFormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var paramName string
		var lineNumber int
		if err := rows.Scan(&id, &paramName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildReportParamLookupKey(paramName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

// FindVBFunctionIDsByForm возвращает id VB functions по форме.
func (db *DB) FindVBFunctionIDsByForm(ctx context.Context, reportFormID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, function_name, line_start
		FROM vb_functions
		WHERE report_form_id = $1
		ORDER BY id DESC
	`, reportFormID)
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
		key := BuildVBFunctionLookupKey(functionName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}
