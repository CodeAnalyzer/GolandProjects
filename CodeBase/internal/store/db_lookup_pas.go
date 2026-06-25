package store

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
)

// FindLatestPASClassIDByName возвращает последний id класса по имени.
func (db *DB) FindLatestPASClassIDByName(className string) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM pas_classes
		WHERE LOWER(class_name) = LOWER($1)
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(className)).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindLatestPASClassIDsByNames возвращает map нижнего имени класса -> последний id.
// Один SQL-запрос вместо N вызовов FindLatestPASClassIDByName.
func (db *DB) FindLatestPASClassIDsByNames(classNames []string) (map[string]int64, error) {
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
	rows, err := db.Query(`
		SELECT DISTINCT ON (class_key) class_key, id
		FROM (
			SELECT LOWER(TRIM(class_name)) AS class_key, id
			FROM pas_classes
			WHERE LOWER(TRIM(class_name)) = ANY($1)
		) AS classes
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

// FindPASFieldDFMLinkCandidates возвращает PAS поля, которые можно связать с DFM-компонентами через уже привязанный класс.
func (db *DB) FindPASFieldDFMLinkCandidates() ([]PASFieldDFMLinkCandidate, error) {
	rows, err := db.Query(`
		SELECT pf.id, pf.field_name, pc.dfm_form_id
		FROM pas_fields pf
		JOIN pas_classes pc ON pf.class_id = pc.id
		WHERE pf.dfm_component_id IS NULL
		  AND pc.dfm_form_id IS NOT NULL
		ORDER BY pf.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]PASFieldDFMLinkCandidate, 0)
	for rows.Next() {
		var item PASFieldDFMLinkCandidate
		if err := rows.Scan(&item.FieldID, &item.FieldName, &item.DFMFormID); err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, rows.Err()
}

// FindPASUnitIDsByFile возвращает id PAS юнитов файла по имени и line_start.
func (db *DB) FindPASUnitIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, unit_name, line_start
		FROM pas_units
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
		var unitName string
		var lineStart int
		if err := rows.Scan(&id, &unitName, &lineStart); err != nil {
			return nil, err
		}
		key := BuildPASUnitLookupKey(unitName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindPASClassIDsByUnit возвращает id PAS классов юнита по имени и line_start.
func (db *DB) FindPASClassIDsByUnit(unitID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, class_name, line_start
		FROM pas_classes
		WHERE unit_id = $1
		ORDER BY id DESC
	`, unitID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var className string
		var lineStart int
		if err := rows.Scan(&id, &className, &lineStart); err != nil {
			return nil, err
		}
		key := BuildPASClassLookupKey(className, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindPASMethodIDsByUnit возвращает id PAS методов юнита по классу/имени/строке.
func (db *DB) FindPASMethodIDsByUnit(unitID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT pm.id, COALESCE(pc.class_name, ''), pm.method_name, pm.line_number
		FROM pas_methods pm
		LEFT JOIN pas_classes pc ON pm.class_id = pc.id
		WHERE pm.unit_id = $1
		ORDER BY pm.id DESC
	`, unitID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var className string
		var methodName string
		var lineNumber int
		if err := rows.Scan(&id, &className, &methodName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildPASMethodLookupKey(className, methodName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindPASFieldIDsByClassNames возвращает id PAS полей файла по классу/имени/строке.
func (db *DB) FindPASFieldIDsByUnit(unitID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT pf.id, COALESCE(pc.class_name, ''), pf.field_name, pf.line_number
		FROM pas_fields pf
		LEFT JOIN pas_classes pc ON pf.class_id = pc.id
		LEFT JOIN pas_methods pm ON 1 = 0
		WHERE pc.unit_id = $1 OR pf.class_id IS NULL
		ORDER BY pf.id DESC
	`, unitID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var className string
		var fieldName string
		var lineNumber int
		if err := rows.Scan(&id, &className, &fieldName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildPASFieldLookupKey(className, fieldName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// UpdatePASClassDFMForm обновляет dfm_form_id у PAS класса.
func (db *DB) UpdatePASClassDFMForm(classID int64, dfmFormID int64) error {
	_, err := db.Exec(`UPDATE pas_classes SET dfm_form_id = $1 WHERE id = $2`, NullableInt64(dfmFormID), classID)
	if err != nil {
		return fmt.Errorf("failed to update pas class dfm form: %w", err)
	}

	return nil
}

// UpdatePASFieldDFMComponent обновляет dfm_component_id у PAS поля.
func (db *DB) UpdatePASFieldDFMComponent(fieldID int64, dfmComponentID int64) error {
	_, err := db.Exec(`UPDATE pas_fields SET dfm_component_id = $1 WHERE id = $2`, NullableInt64(dfmComponentID), fieldID)
	if err != nil {
		return fmt.Errorf("failed to update pas field dfm component: %w", err)
	}

	return nil
}

type UnlinkedPASClass struct {
	ID        int64
	ClassName string
}

// FindUnlinkedPASClasses возвращает PAS классы без привязанной DFM формы.
func (db *DB) FindUnlinkedPASClasses() ([]UnlinkedPASClass, error) {
	rows, err := db.Query(`
		SELECT id, class_name
		FROM pas_classes
		WHERE dfm_form_id IS NULL
		ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]UnlinkedPASClass, 0)
	for rows.Next() {
		var item UnlinkedPASClass
		if err := rows.Scan(&item.ID, &item.ClassName); err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, rows.Err()
}

type PASFieldDFMLinkCandidate struct {
	FieldID   int64
	FieldName string
	DFMFormID int64
}

// UpdatePASMethodClass обновляет class_id у PAS метода.
func (db *DB) UpdatePASMethodClass(methodID int64, classID int64) error {
	_, err := db.Exec(`UPDATE pas_methods SET class_id = $1 WHERE id = $2`, classID, methodID)
	if err != nil {
		return fmt.Errorf("failed to update pas method class: %w", err)
	}
	return nil
}

// UpdatePASFieldClass обновляет class_id у PAS поля.
func (db *DB) UpdatePASFieldClass(fieldID int64, classID int64) error {
	_, err := db.Exec(`UPDATE pas_fields SET class_id = $1 WHERE id = $2`, classID, fieldID)
	if err != nil {
		return fmt.Errorf("failed to update pas field class: %w", err)
	}
	return nil
}
