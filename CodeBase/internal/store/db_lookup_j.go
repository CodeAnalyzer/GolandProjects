package store

import "context"

// FindJSFunctionIDsByFile РІРѕР·РІСЂР°С‰Р°РµС‚ id JS С„СѓРЅРєС†РёР№ С„Р°Р№Р»Р° РїРѕ РёРјРµРЅРё Рё line_start.
func (db *DB) FindJSFunctionIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
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

// FindJSFunctionIDByFileAndLine РІРѕР·РІСЂР°С‰Р°РµС‚ id JS С„СѓРЅРєС†РёРё, РІ РґРёР°РїР°Р·РѕРЅ РєРѕС‚РѕСЂРѕР№ РїРѕРїР°РґР°РµС‚ СЃС‚СЂРѕРєР°.
func (db *DB) FindJSFunctionIDByFileAndLine(ctx context.Context, fileID int64, lineNumber int) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
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

func (db *DB) FindJSConstantIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
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

// JSFuncRange РѕРїРёСЃС‹РІР°РµС‚ JS-С„СѓРЅРєС†РёСЋ СЃ РµС‘ line-РґРёР°РїР°Р·РѕРЅРѕРј РґР»СЏ Р»РѕРєР°Р»СЊРЅРѕРіРѕ СЂРµР·РѕР»РІР°.
type JSFuncRange struct {
	ID        int64
	LineStart int
	LineEnd   int
}

// FindJSFunctionIDRangesByFile РІРѕР·РІСЂР°С‰Р°РµС‚ РІСЃРµ JS-С„СѓРЅРєС†РёРё С„Р°Р№Р»Р° СЃ РёС… line-РґРёР°РїР°Р·РѕРЅР°РјРё.
// РћРґРёРЅ Р·Р°РїСЂРѕСЃ РІРјРµСЃС‚Рѕ N Р·Р°РїСЂРѕСЃРѕРІ FindJSFunctionIDByFileAndLine.
func (db *DB) FindJSFunctionIDRangesByFile(ctx context.Context, fileID int64) ([]JSFuncRange, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, line_start, line_end
		FROM js_functions
		WHERE file_id = $1
		ORDER BY line_start DESC, id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []JSFuncRange
	for rows.Next() {
		var r JSFuncRange
		if err := rows.Scan(&r.ID, &r.LineStart, &r.LineEnd); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// FindLatestSMFInstrumentIDByFile РІРѕР·РІСЂР°С‰Р°РµС‚ РїРѕСЃР»РµРґРЅРёР№ id SMF РёРЅСЃС‚СЂСѓРјРµРЅС‚Р° С„Р°Р№Р»Р°.
func (db *DB) FindLatestSMFInstrumentIDByFile(ctx context.Context, fileID int64) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
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
