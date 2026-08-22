package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

// FindLatestPASClassIDByName возвращает последний id класса по имени.
func (db *DB) FindLatestPASClassIDByName(ctx context.Context, className string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
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
func (db *DB) FindLatestPASClassIDsByNames(ctx context.Context, classNames []string) (map[string]int64, error) {
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
			SELECT LOWER(class_name) AS class_key, id
			FROM pas_classes
			WHERE LOWER(class_name) = ANY($1)
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
func (db *DB) FindPASFieldDFMLinkCandidates(ctx context.Context) ([]PASFieldDFMLinkCandidate, error) {
	rows, err := db.QueryContext(ctx, `
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
func (db *DB) FindPASUnitIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
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
func (db *DB) FindPASClassIDsByUnit(ctx context.Context, unitID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
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
func (db *DB) FindPASMethodIDsByUnit(ctx context.Context, unitID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
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
func (db *DB) FindPASFieldIDsByUnit(ctx context.Context, unitID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
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
func (db *DB) UpdatePASClassDFMForm(ctx context.Context, classID int64, dfmFormID int64) error {
	_, err := db.ExecContext(ctx, `UPDATE pas_classes SET dfm_form_id = $1 WHERE id = $2`, NullableInt64(dfmFormID), classID)
	if err != nil {
		return fmt.Errorf("failed to update pas class dfm form: %w", err)
	}

	return nil
}

// UpdatePASFieldDFMComponent обновляет dfm_component_id у PAS поля.
func (db *DB) UpdatePASFieldDFMComponent(ctx context.Context, fieldID int64, dfmComponentID int64) error {
	_, err := db.ExecContext(ctx, `UPDATE pas_fields SET dfm_component_id = $1 WHERE id = $2`, NullableInt64(dfmComponentID), fieldID)
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
func (db *DB) FindUnlinkedPASClasses(ctx context.Context) ([]UnlinkedPASClass, error) {
	rows, err := db.QueryContext(ctx, `
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
func (db *DB) UpdatePASMethodClass(ctx context.Context, methodID int64, classID int64) error {
	_, err := db.ExecContext(ctx, `UPDATE pas_methods SET class_id = $1 WHERE id = $2`, classID, methodID)
	if err != nil {
		return fmt.Errorf("failed to update pas method class: %w", err)
	}
	return nil
}

// UpdatePASFieldClass обновляет class_id у PAS поля.
func (db *DB) UpdatePASFieldClass(ctx context.Context, fieldID int64, classID int64) error {
	_, err := db.ExecContext(ctx, `UPDATE pas_fields SET class_id = $1 WHERE id = $2`, classID, fieldID)
	if err != nil {
		return fmt.Errorf("failed to update pas field class: %w", err)
	}
	return nil
}

// BatchUpdatePASClassDFMForm пакетно обновляет dfm_form_id у PAS классов.
// pairs — (classID, dfmFormID).
func (db *DB) BatchUpdatePASClassDFMForm(ctx context.Context, pairs []PASUpdatePair) error {
	if len(pairs) == 0 {
		return nil
	}
	const chunkSize = 1000
	for i := 0; i < len(pairs); i += chunkSize {
		end := i + chunkSize
		if end > len(pairs) {
			end = len(pairs)
		}
		chunk := pairs[i:end]
		ids := make([]int64, len(chunk))
		values := make([]int64, len(chunk))
		for j, p := range chunk {
			ids[j] = p.ID
			values[j] = p.ValueID
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE pas_classes c
			SET dfm_form_id = v.dfm_form_id
			FROM unnest($1::bigint[], $2::bigint[]) AS v(id, dfm_form_id)
			WHERE c.id = v.id
		`, pq.Array(ids), pq.Array(values)); err != nil {
			return fmt.Errorf("failed to batch update pas class dfm form: %w", err)
		}
	}
	return nil
}

// BatchUpdatePASMethodClass пакетно обновляет class_id у PAS методов.
func (db *DB) BatchUpdatePASMethodClass(ctx context.Context, pairs []PASUpdatePair) error {
	if len(pairs) == 0 {
		return nil
	}
	const chunkSize = 1000
	for i := 0; i < len(pairs); i += chunkSize {
		end := i + chunkSize
		if end > len(pairs) {
			end = len(pairs)
		}
		chunk := pairs[i:end]
		ids := make([]int64, len(chunk))
		classIDs := make([]int64, len(chunk))
		for j, p := range chunk {
			ids[j] = p.ID
			classIDs[j] = p.ValueID
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE pas_methods m
			SET class_id = v.class_id
			FROM unnest($1::bigint[], $2::bigint[]) AS v(id, class_id)
			WHERE m.id = v.id
		`, pq.Array(ids), pq.Array(classIDs)); err != nil {
			return fmt.Errorf("failed to batch update pas method class: %w", err)
		}
	}
	return nil
}

// BatchUpdatePASFieldClass пакетно обновляет class_id у PAS полей.
func (db *DB) BatchUpdatePASFieldClass(ctx context.Context, pairs []PASUpdatePair) error {
	if len(pairs) == 0 {
		return nil
	}
	const chunkSize = 1000
	for i := 0; i < len(pairs); i += chunkSize {
		end := i + chunkSize
		if end > len(pairs) {
			end = len(pairs)
		}
		chunk := pairs[i:end]
		ids := make([]int64, len(chunk))
		classIDs := make([]int64, len(chunk))
		for j, p := range chunk {
			ids[j] = p.ID
			classIDs[j] = p.ValueID
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE pas_fields f
			SET class_id = v.class_id
			FROM unnest($1::bigint[], $2::bigint[]) AS v(id, class_id)
			WHERE f.id = v.id
		`, pq.Array(ids), pq.Array(classIDs)); err != nil {
			return fmt.Errorf("failed to batch update pas field class: %w", err)
		}
	}
	return nil
}

// BatchUpdatePASFieldDFMComponent пакетно обновляет dfm_component_id у PAS полей.
func (db *DB) BatchUpdatePASFieldDFMComponent(ctx context.Context, pairs []PASUpdatePair) error {
	if len(pairs) == 0 {
		return nil
	}
	const chunkSize = 1000
	for i := 0; i < len(pairs); i += chunkSize {
		end := i + chunkSize
		if end > len(pairs) {
			end = len(pairs)
		}
		chunk := pairs[i:end]
		ids := make([]int64, len(chunk))
		componentIDs := make([]int64, len(chunk))
		for j, p := range chunk {
			ids[j] = p.ID
			componentIDs[j] = p.ValueID
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE pas_fields f
			SET dfm_component_id = v.component_id
			FROM unnest($1::bigint[], $2::bigint[]) AS v(id, component_id)
			WHERE f.id = v.id
		`, pq.Array(ids), pq.Array(componentIDs)); err != nil {
			return fmt.Errorf("failed to batch update pas field dfm component: %w", err)
		}
	}
	return nil
}

// PASUpdatePair — пара (ID, ValueID) для batch UPDATE.
type PASUpdatePair struct {
	ID      int64
	ValueID int64
}
