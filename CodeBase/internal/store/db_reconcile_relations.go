package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

// reconcileEntityBinding — связь типа сущности графа (relations.source_type /
// relations.target_type) с таблицей и способом достижения files.file_id.
type reconcileEntityBinding struct {
	entityType string
	table      string
	// viaReportForms: report_field/report_param/vb_function не имеют file_id
	// и достигают files через родительскую report_forms.
	viaReportForms bool
}

// reconcileEntityBindings покрывает все 17 типов model.RelationEntityTypes:
// 14 прямых file_id (9 не-spec + 5 spec_*, включая spec_change) и 3 через
// report_forms. Корректность покрытия проверяется unit-тестом
// TestReconcileEntityBindingsCoverRelationEntityTypes.
var reconcileEntityBindings = []reconcileEntityBinding{
	{entityType: "sql_procedure", table: "sql_procedures"},
	{entityType: "sql_table", table: "sql_tables"},
	{entityType: "pas_method", table: "pas_methods"},
	{entityType: "js_function", table: "js_functions"},
	{entityType: "query_fragment", table: "query_fragments"},
	{entityType: "dfm_form", table: "dfm_forms"},
	{entityType: "smf_instrument", table: "smf_instruments"},
	{entityType: "report_form", table: "report_forms"},
	{entityType: "api_contract", table: "api_contracts"},
	{entityType: "spec_capability", table: "spec_capabilities"},
	{entityType: "spec_requirement", table: "spec_requirements"},
	{entityType: "spec_scenario", table: "spec_scenarios"},
	{entityType: "spec_usecase", table: "spec_usecases"},
	{entityType: "spec_change", table: "spec_changes"},
	{entityType: "report_field", table: "report_fields", viaReportForms: true},
	{entityType: "report_param", table: "report_params", viaReportForms: true},
	{entityType: "vb_function", table: "vb_functions", viaReportForms: true},
}

// reconcileFileIDJoin строит условие принадлежности сущности старым поколениям
// файлов: прямые таблицы сравнивают e.file_id, косвенные — file_id родителя.
func (b reconcileEntityBinding) fileIDCondition() string {
	if !b.viaReportForms {
		return "e.file_id = ANY($1)"
	}
	return "rf.file_id = ANY($1)"
}

// GetOldFileGenerationIDs возвращает id старых поколений files: для
// изменённых путей — все id, кроме keep-id нового поколения; для удалённых
// путей — все id. Вызывается после вставки новых поколений и до каскадного
// удаления старых (change relations-incremental-consistency, D1).
func (db *DB) GetOldFileGenerationIDs(ctx context.Context, modifiedPaths []string, keepIDs map[string]int64, removedPaths []string) ([]int64, error) {
	allPaths := make([]string, 0, len(modifiedPaths)+len(removedPaths))
	allPaths = append(allPaths, modifiedPaths...)
	allPaths = append(allPaths, removedPaths...)
	if len(allPaths) == 0 {
		return nil, nil
	}

	result := make([]int64, 0, len(allPaths))
	const chunkSize = 500
	for i := 0; i < len(allPaths); i += chunkSize {
		end := i + chunkSize
		if end > len(allPaths) {
			end = len(allPaths)
		}
		rows, err := db.QueryContext(ctx, `
			SELECT id, path FROM files WHERE path = ANY($1)
		`, pq.Array(allPaths[i:end]))
		if err != nil {
			return nil, fmt.Errorf("failed to load file generations: %w", err)
		}
		for rows.Next() {
			var id int64
			var path string
			if err := rows.Scan(&id, &path); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan file generation: %w", err)
			}
			if keep, ok := keepIDs[path]; ok && keep == id {
				continue
			}
			result = append(result, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to iterate file generations: %w", err)
		}
		rows.Close()
	}
	return result, nil
}

// escapeLikeAll экранирует все спецсимволы LIKE (%/_/обратный слэш).
// В отличие от escapeLike из ds_info_store.go экранирует и подчёркивание:
// rootPath — литеральный префикс, а не поиск по маске.
func escapeLikeAll(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

// FindDuplicateFileGenerations находит пути с несколькими поколениями files
// (последствия падения в окне reconcile → удаление старых поколений).
// Возвращает map[path]keepID (последнее поколение) и map[path]oldIDs
// (не-последние поколения) в пределах корня обновления.
func (db *DB) FindDuplicateFileGenerations(ctx context.Context, rootPath string) (map[string]int64, map[string][]int64, error) {
	normalizedRoot := strings.ReplaceAll(strings.TrimSpace(rootPath), `\`, "/")
	normalizedRoot = strings.TrimSuffix(normalizedRoot, "/")

	rows, err := db.QueryContext(ctx, `
		SELECT path, MAX(id) AS keep_id
		FROM files
		WHERE path = $1 OR path LIKE $2
		GROUP BY path
		HAVING COUNT(*) > 1
	`, normalizedRoot, escapeLikeAll(normalizedRoot)+"/%")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find duplicate file generations: %w", err)
	}
	keepIDs := make(map[string]int64)
	dupPaths := make([]string, 0)
	for rows.Next() {
		var path string
		var keepID int64
		if err := rows.Scan(&path, &keepID); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("failed to scan duplicate file generation: %w", err)
		}
		keepIDs[path] = keepID
		dupPaths = append(dupPaths, path)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, fmt.Errorf("failed to iterate duplicate file generations: %w", err)
	}
	rows.Close()
	if len(dupPaths) == 0 {
		return keepIDs, nil, nil
	}

	oldIDsByPath := make(map[string][]int64, len(dupPaths))
	rows, err = db.QueryContext(ctx, `
		SELECT id, path FROM files WHERE path = ANY($1)
	`, pq.Array(dupPaths))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load duplicate file generations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var path string
		if err := rows.Scan(&id, &path); err != nil {
			return nil, nil, fmt.Errorf("failed to scan duplicate file generation row: %w", err)
		}
		if keepIDs[path] == id {
			continue
		}
		oldIDsByPath[path] = append(oldIDsByPath[path], id)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to iterate duplicate file generation rows: %w", err)
	}
	return keepIDs, oldIDsByPath, nil
}

// PurgeRelationSourcesByFileIDs удаляет рёбра, чьи source-сущности принадлежат
// старым поколениям файлов (их рёбра пересоздаются из нового содержимого при
// парсинге). Возвращает счётчики удалённых рёбер по source_type.
func (db *DB) PurgeRelationSourcesByFileIDs(ctx context.Context, fileIDs []int64) (map[string]int64, error) {
	return db.deleteRelationsByEntityFileIDs(ctx, fileIDs, "source")
}

// DeleteUnresolvedRelationTargets удаляет рёбра, чьи target-сущности
// принадлежат старым поколениям файлов и не были перенесены remap-ом.
// Критерий опирается на принадлежность target-сущности старому file_id, а не
// на NOT EXISTS по живой БД: старые строки на этом шаге ещё живы (каскад
// выполняется позже). Возвращает счётчики по target_type.
func (db *DB) DeleteUnresolvedRelationTargets(ctx context.Context, fileIDs []int64) (map[string]int64, error) {
	return db.deleteRelationsByEntityFileIDs(ctx, fileIDs, "target")
}

// deleteRelationsByEntityFileIDs — общая реализация purge источника и
// удаления неустранимых целей: DELETE ... USING по каждой таблице сущностей.
func (db *DB) deleteRelationsByEntityFileIDs(ctx context.Context, fileIDs []int64, side string) (map[string]int64, error) {
	counts := make(map[string]int64)
	if len(fileIDs) == 0 {
		return counts, nil
	}
	const chunkSize = 1000
	for _, binding := range reconcileEntityBindings {
		total := int64(0)
		for i := 0; i < len(fileIDs); i += chunkSize {
			end := i + chunkSize
			if end > len(fileIDs) {
				end = len(fileIDs)
			}
			query := binding.buildRelationEntityDelete(side)
			res, err := db.ExecContext(ctx, query, pq.Array(fileIDs[i:end]), binding.entityType)
			if err != nil {
				return nil, fmt.Errorf("failed to delete relations by %s %s: %w", side, binding.entityType, err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				return nil, fmt.Errorf("failed to read deleted rows count (%s %s): %w", side, binding.entityType, err)
			}
			total += n
		}
		if total > 0 {
			counts[binding.entityType] = total
		}
	}
	return counts, nil
}

func (b reconcileEntityBinding) buildRelationEntityDelete(side string) string {
	idColumn := "source_id"
	typeColumn := "source_type"
	if side == "target" {
		idColumn = "target_id"
		typeColumn = "target_type"
	}
	// PostgreSQL DELETE ... USING принимает список таблиц (не JOIN),
	// поэтому косвенные типы идут второй таблицей с условием в WHERE.
	var using string
	where := fmt.Sprintf("e.id = r.%s", idColumn)
	if b.viaReportForms {
		using = fmt.Sprintf("%s e, report_forms rf", b.table)
		where += " AND rf.id = e.report_form_id"
	} else {
		using = fmt.Sprintf("%s e", b.table)
	}
	return fmt.Sprintf("DELETE FROM relations r USING %s WHERE r.%s = $2 AND %s AND %s", using, typeColumn, where, b.fileIDCondition())
}

// remapTargetBinding — параметры target remap для межфайловых типов целей.
type remapTargetBinding struct {
	targetType string
	table      string
	// identityMatch — условие совпадения identity-ключа новой и старой строки
	// (без алиасов o/n; подставляется в запрос с алиасами old/new).
	identityMatch string
}

var remapTargetBindings = []remapTargetBinding{
	{targetType: "sql_procedure", table: "sql_procedures", identityMatch: "LOWER(n.proc_name) = LOWER(o.proc_name)"},
	{targetType: "sql_table", table: "sql_tables", identityMatch: "LOWER(n.table_name) = LOWER(o.table_name)"},
	{targetType: "api_contract", table: "api_contracts", identityMatch: "LOWER(n.contract_name) = LOWER(o.contract_name) AND LOWER(n.contract_kind) = LOWER(o.contract_kind)"},
}

// RemapRelationTargetsByFileIDs переносит цели рёбер на новые поколения
// сущностей того же файла по identity-ключу (см. change
// relations-incremental-consistency, D3): для старого id выбирается max(id)
// новой строки с тем же files.path и тем же ключом — семантика резолверов
// FindLatest... внутри одного path. Возвращает счётчики перенесённых рёбер по
// target_type.
func (db *DB) RemapRelationTargetsByFileIDs(ctx context.Context, fileIDs []int64) (map[string]int64, error) {
	counts := make(map[string]int64)
	if len(fileIDs) == 0 {
		return counts, nil
	}
	const chunkSize = 1000
	for _, binding := range remapTargetBindings {
		total := int64(0)
		for i := 0; i < len(fileIDs); i += chunkSize {
			end := i + chunkSize
			if end > len(fileIDs) {
				end = len(fileIDs)
			}
		query := fmt.Sprintf(`
			UPDATE relations r
			SET target_id = m.new_id
			FROM (
				-- Подзапрос намеренно не ограничен новым поколением:
				-- serial монотонен, поэтому max(id) при том же files.path —
				-- строка нового поколения, а фильтр new_id <> old_id
				-- отсекает само-совпадение со старой строкой (сущность
				-- удалена или переименована — max возвращает саму старую
				-- строку, ребро не переносится и уходит в
				-- deleted_unresolved).
				SELECT o.id AS old_id,
				       (SELECT MAX(n.id)
				          FROM %[1]s n
				          JOIN files fn ON fn.id = n.file_id
				         WHERE fn.path = fo.path AND %[2]s) AS new_id
				FROM %[1]s o
				JOIN files fo ON fo.id = o.file_id
				WHERE o.file_id = ANY($1)
			) m
			WHERE r.target_type = $2
			  AND r.target_id = m.old_id
			  AND m.new_id IS NOT NULL
			  AND m.new_id <> m.old_id
		`, binding.table, binding.identityMatch)
			res, err := db.ExecContext(ctx, query, pq.Array(fileIDs[i:end]), binding.targetType)
			if err != nil {
				return nil, fmt.Errorf("failed to remap relation targets %s: %w", binding.targetType, err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				return nil, fmt.Errorf("failed to read remapped rows count (%s): %w", binding.targetType, err)
			}
			total += n
		}
		if total > 0 {
			counts[binding.targetType] = total
		}
	}
	return counts, nil
}
