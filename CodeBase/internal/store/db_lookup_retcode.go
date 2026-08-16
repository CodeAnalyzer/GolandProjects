package store

import (
	"context"
	"database/sql"
)

// RetCodeLookup вЂ” СЂРµР·СѓР»СЊС‚Р°С‚ РїРѕРёСЃРєР° РєРѕРґР° РІРѕР·РІСЂР°С‚Р° РІ ds_return_codes
type RetCodeLookup struct {
	RetCode  int64
	Message  string
	ProcName string
	ModuleID int
}

// LookupRetCode РёС‰РµС‚ РѕРїРёСЃР°РЅРёРµ РєРѕРґР° РІРѕР·РІСЂР°С‚Р° РІ С‚Р°Р±Р»РёС†Рµ ds_return_codes.
func (db *DB) LookupRetCode(ctx context.Context, retCode int64) (*RetCodeLookup, error) {
	row := db.QueryRowContext(ctx, 
		`SELECT ret_code, message, proc_name, module_id FROM ds_return_codes WHERE ret_code = $1`,
		retCode)

	var r RetCodeLookup
	var procName sql.NullString
	err := row.Scan(&r.RetCode, &r.Message, &procName, &r.ModuleID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if procName.Valid {
		r.ProcName = procName.String
	}
	return &r, nil
}

// LookupRetCodes вЂ” batch-lookup РґР»СЏ РЅРµСЃРєРѕР»СЊРєРёС… РєРѕРґРѕРІ РІРѕР·РІСЂР°С‚Р°.
// Р’РѕР·РІСЂР°С‰Р°РµС‚ map[retCode]*RetCodeLookup.
func (db *DB) LookupRetCodes(ctx context.Context, retCodes []int64) (map[int64]*RetCodeLookup, error) {
	result := make(map[int64]*RetCodeLookup)
	if len(retCodes) == 0 {
		return result, nil
	}

	// РЈРЅРёРєР°Р»СЊРЅС‹Рµ РєРѕРґС‹
	seen := make(map[int64]bool)
	codes := make([]int64, 0, len(retCodes))
	for _, c := range retCodes {
		if !seen[c] {
			seen[c] = true
			codes = append(codes, c)
		}
	}

	for _, code := range codes {
		r, err := db.LookupRetCode(ctx, code)
		if err != nil {
			return nil, err
		}
		if r != nil {
			result[code] = r
		}
	}
	return result, nil
}

// LookupRetCodeByMessage РёС‰РµС‚ РєРѕРґС‹ РІРѕР·РІСЂР°С‚Р° РїРѕ С„СЂР°РіРјРµРЅС‚Сѓ С‚РµРєСЃС‚Р° СЃРѕРѕР±С‰РµРЅРёСЏ (ILIKE).
// Р’РѕР·РІСЂР°С‰Р°РµС‚ СЃРїРёСЃРѕРє РІСЃРµС… СЃРѕРІРїР°РґРµРЅРёР№.
func (db *DB) LookupRetCodeByMessage(ctx context.Context, messagePattern string, limit int) ([]RetCodeLookup, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.QueryContext(ctx, 
		`SELECT ret_code, message, proc_name, module_id
		 FROM ds_return_codes
		 WHERE message ILIKE '%' || $1 || '%'
		 ORDER BY ret_code
		 LIMIT $2`,
		messagePattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []RetCodeLookup
	for rows.Next() {
		var r RetCodeLookup
		var procName sql.NullString
		if err := rows.Scan(&r.RetCode, &r.Message, &procName, &r.ModuleID); err != nil {
			return nil, err
		}
		if procName.Valid {
			r.ProcName = procName.String
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
