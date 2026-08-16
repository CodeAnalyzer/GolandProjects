package store

import (
	"context"
	"database/sql"
	"strings"
)

// FindLatestHFileIDByNameLike РІРѕР·РІСЂР°С‰Р°РµС‚ РїРѕСЃР»РµРґРЅРёР№ file id H-С„Р°Р№Р»Р° РїРѕ РёРјРµРЅРё include С‡РµСЂРµР· LIKE.
func (db *DB) FindLatestHFileIDByNameLike(ctx context.Context, fileName string) (int64, error) {
	normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(fileName), `\`, "/"))
	if normalized == "" {
		return 0, sql.ErrNoRows
	}

	if idx := strings.LastIndex(normalized, "/"); idx >= 0 {
		normalized = normalized[idx+1:]
	}

	var id int64
	err := db.QueryRowContext(ctx, `
		SELECT id
		FROM files
		WHERE LOWER(extension) = 'h'
		  AND (LOWER(path) LIKE $1 OR LOWER(rel_path) LIKE $1)
		ORDER BY id DESC
		LIMIT 1
	`, "%/"+normalized).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindHDefineIDsByFile РІРѕР·РІСЂР°С‰Р°РµС‚ id define-РѕРІ С„Р°Р№Р»Р° РїРѕ РёРјРµРЅРё Рё СЃС‚СЂРѕРєРµ.
func (db *DB) FindHDefineIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, define_name, line_number
		FROM h_files_defines
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
		var defineName string
		var lineNumber int
		if err := rows.Scan(&id, &defineName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildHDefineLookupKey(defineName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindHDefineExistsByName РїСЂРѕРІРµСЂСЏРµС‚, СЃСѓС‰РµСЃС‚РІСѓРµС‚ Р»Рё define СЃ СѓРєР°Р·Р°РЅРЅС‹Рј РёРјРµРЅРµРј РІ h_files_defines.
func (db *DB) FindHDefineExistsByName(ctx context.Context, defineName string) (bool, error) {
	normalized := strings.ToLower(strings.TrimSpace(defineName))
	if normalized == "" {
		return false, nil
	}
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM h_files_defines
			WHERE LOWER(define_name) = $1
		)
	`, normalized).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
