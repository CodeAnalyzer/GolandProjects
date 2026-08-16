package store

import (
	"context"
	"strings"

	"github.com/lib/pq"
)

// FindLatestDFMFormIDByClassName РІРѕР·РІСЂР°С‰Р°РµС‚ РїРѕСЃР»РµРґРЅРёР№ id DFM С„РѕСЂРјС‹ РїРѕ РёРјРµРЅРё РєР»Р°СЃСЃР° С„РѕСЂРјС‹.
func (db *DB) FindLatestDFMFormIDByClassName(ctx context.Context, className string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
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

// FindLatestDFMComponentIDByFormAndName РІРѕР·РІСЂР°С‰Р°РµС‚ РїРѕСЃР»РµРґРЅРёР№ id DFM-РєРѕРјРїРѕРЅРµРЅС‚Р° РїРѕ С„РѕСЂРјРµ Рё РёРјРµРЅРё РєРѕРјРїРѕРЅРµРЅС‚Р°.
func (db *DB) FindLatestDFMComponentIDByFormAndName(ctx context.Context, formID int64, componentName string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
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

// FindLatestDFMFormIDsByClassNames РІРѕР·РІСЂР°С‰Р°РµС‚ map РЅРёР¶РЅРµРіРѕ РёРјРµРЅРё РєР»Р°СЃСЃР° -> РїРѕСЃР»РµРґРЅРёР№ id DFM С„РѕСЂРјС‹.
// РћРґРёРЅ SQL-Р·Р°РїСЂРѕСЃ РІРјРµСЃС‚Рѕ N РІС‹Р·РѕРІРѕРІ FindLatestDFMFormIDByClassName.
func (db *DB) FindLatestDFMFormIDsByClassNames(ctx context.Context, classNames []string) (map[string]int64, error) {
	normalized := make([]string, 0, len(classNames))
	seen := make(map[string]struct{})
	for _, name := range classNames {
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
		SELECT DISTINCT ON (class_key) class_key, id
		FROM (
			SELECT LOWER(form_class) AS class_key, id
			FROM dfm_forms
			WHERE LOWER(form_class) = ANY($1)
		) AS forms
		ORDER BY class_key, id DESC
	`, pq.Array(normalized))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var classKey string
		var id int64
		if err := rows.Scan(&classKey, &id); err != nil {
			return nil, err
		}
		result[classKey] = id
	}
	return result, rows.Err()
}

// FindLatestDFMComponentIDsByFormAndNames РІРѕР·РІСЂР°С‰Р°РµС‚ map РЅРёР¶РЅРµРіРѕ РёРјРµРЅРё РєРѕРјРїРѕРЅРµРЅС‚Р° -> РїРѕСЃР»РµРґРЅРёР№ id
// РґР»СЏ Р·Р°РґР°РЅРЅРѕР№ С„РѕСЂРјС‹. РћРґРёРЅ SQL-Р·Р°РїСЂРѕСЃ РІРјРµСЃС‚Рѕ N РІС‹Р·РѕРІРѕРІ FindLatestDFMComponentIDByFormAndName.
func (db *DB) FindLatestDFMComponentIDsByFormAndNames(ctx context.Context, formID int64, componentNames []string) (map[string]int64, error) {
	normalized := make([]string, 0, len(componentNames))
	seen := make(map[string]struct{})
	for _, name := range componentNames {
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
		SELECT DISTINCT ON (comp_key) comp_key, id
		FROM (
			SELECT LOWER(component_name) AS comp_key, id
			FROM dfm_components
			WHERE form_id = $1
			  AND LOWER(component_name) = ANY($2)
		) AS components
		ORDER BY comp_key, id DESC
	`, formID, pq.Array(normalized))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var compKey string
		var id int64
		if err := rows.Scan(&compKey, &id); err != nil {
			return nil, err
		}
		result[compKey] = id
	}
	return result, rows.Err()
}

// FindDFMComponentIDsByForm РІРѕР·РІСЂР°С‰Р°РµС‚ id РєРѕРјРїРѕРЅРµРЅС‚РѕРІ С„РѕСЂРјС‹ РїРѕ РёРјРµРЅРё Рё line_start.
func (db *DB) FindDFMComponentIDsByForm(ctx context.Context, formID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
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

// FindDFMFormIDsByFile РІРѕР·РІСЂР°С‰Р°РµС‚ id DFM С„РѕСЂРј С„Р°Р№Р»Р° РїРѕ РёРјРµРЅРё Рё line_start.
func (db *DB) FindDFMFormIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
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

// FindDFMFormIDByFileAndLine РІРѕР·РІСЂР°С‰Р°РµС‚ id DFM С„РѕСЂРјС‹, РґРёР°РїР°Р·РѕРЅ РєРѕС‚РѕСЂРѕР№ РІРєР»СЋС‡Р°РµС‚ СЃС‚СЂРѕРєСѓ.
func (db *DB) FindDFMFormIDByFileAndLine(ctx context.Context, fileID int64, lineNumber int) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
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
