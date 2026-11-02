package indexer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// relationsReconcileResult — счётчики этапа согласования графа связей
// (change relations-incremental-consistency): purge рёбер по мёртвым
// источникам, remap межфайловых целей, удаление неустранимых целей.
type relationsReconcileResult struct {
	Purged             map[string]int64 // source_type -> удалено рёбер
	Remapped           map[string]int64 // target_type -> перенесено целей
	DeletedUnresolved  map[string]int64 // target_type -> удалено неустранимых
	OldFileGenerations int              // старых поколений files в этапе
	ElapsedMs          int64
}

func reconcileCounterTotal(counts map[string]int64) int64 {
	total := int64(0)
	for _, n := range counts {
		total += n
	}
	return total
}

// formatReconcileCounters — «type=n, ...» по возрастанию типа; пустая карта — «-».
func formatReconcileCounters(counts map[string]int64) string {
	if len(counts) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return strings.Join(parts, ", ")
}

// logReconcileResult пишет счётчики этапа в лог индексатора
// (indexer_errors_*.log), а не в консоль: печать поверх кадров
// прогресс-репортера (\r-спиннер) разрывает строку прогресса. Нулевой прогон
// сообщает нули (требование «Наблюдаемость согласования»).
func (idx *Indexer) logReconcileResult(label string, result *relationsReconcileResult) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s: old_file_generations=%d purged=%d remapped=%d deleted_unresolved=%d elapsed_ms=%d",
		label, result.OldFileGenerations,
		reconcileCounterTotal(result.Purged),
		reconcileCounterTotal(result.Remapped),
		reconcileCounterTotal(result.DeletedUnresolved),
		result.ElapsedMs)
	if len(result.Purged) > 0 {
		fmt.Fprintf(&sb, "\n  purge by source_type: %s", formatReconcileCounters(result.Purged))
	}
	if len(result.Remapped) > 0 {
		fmt.Fprintf(&sb, "\n  remap by target_type: %s", formatReconcileCounters(result.Remapped))
	}
	if len(result.DeletedUnresolved) > 0 {
		fmt.Fprintf(&sb, "\n  deleted_unresolved by target_type: %s", formatReconcileCounters(result.DeletedUnresolved))
	}
	idx.errorLogger.Print(sb.String())
}

// reconcileRelations — этап согласования графа между парсингом (workers) и
// каскадным удалением старых поколений files:
//  1. собрать старые id поколений изменённых/удалённых путей;
//  2. purge рёбер, чьи источники принадлежат старым поколениям;
//  3. remap межфайловых целей (sql_procedure/sql_table/api_contract) на новые
//     поколения того же файла по identity-ключу;
//  4. удалить рёбра с неустранимыми целями (сущность исчезла, путь изменён).
//
// No-op без изменённых и удалённых файлов не выполняет запросов к БД.
func (idx *Indexer) reconcileRelations(ctx context.Context, modifiedPaths []string, newFileIDs map[string]int64, removedPaths []string) (*relationsReconcileResult, error) {
	start := time.Now()
	result := &relationsReconcileResult{
		Purged:            make(map[string]int64),
		Remapped:          make(map[string]int64),
		DeletedUnresolved: make(map[string]int64),
	}
	defer func() {
		result.ElapsedMs = time.Since(start).Milliseconds()
		idx.logReconcileResult("Relations reconcile", result)
	}()

	if len(modifiedPaths) == 0 && len(removedPaths) == 0 {
		return result, nil
	}

	oldIDs, err := idx.db.GetOldFileGenerationIDs(ctx, modifiedPaths, newFileIDs, removedPaths)
	if err != nil {
		return result, err
	}
	result.OldFileGenerations = len(oldIDs)
	if len(oldIDs) == 0 {
		return result, nil
	}

	if result.Purged, err = idx.db.PurgeRelationSourcesByFileIDs(ctx, oldIDs); err != nil {
		return result, err
	}
	if result.Remapped, err = idx.db.RemapRelationTargetsByFileIDs(ctx, oldIDs); err != nil {
		return result, err
	}
	if result.DeletedUnresolved, err = idx.db.DeleteUnresolvedRelationTargets(ctx, oldIDs); err != nil {
		return result, err
	}
	return result, nil
}

// reconcileStaleFileGenerations — safety-net аварийного окна «reconcile →
// удаление старых поколений» (см. design Risks): если предыдущий прогон
// упал после согласования, но до удаления старых files-строк, дубли
// поколений остаются в БД навсегда (hash не изменился, путь не попадает в
// изменённые). В начале Update находим такие пути и доводим согласование
// до конца: purge/remap для не-последних поколений + их удаление.
func (idx *Indexer) reconcileStaleFileGenerations(ctx context.Context, rootPath string) {
	keepIDs, oldIDsByPath, err := idx.db.FindDuplicateFileGenerations(ctx, rootPath)
	if err != nil {
		idx.logError(rootPath, "Relations safety-net: error finding stale file generations: %v", err)
		return
	}
	if len(oldIDsByPath) == 0 {
		return
	}

	oldIDs := make([]int64, 0, len(oldIDsByPath))
	paths := make([]string, 0, len(oldIDsByPath))
	for path, ids := range oldIDsByPath {
		paths = append(paths, path)
		oldIDs = append(oldIDs, ids...)
	}

	purged, err := idx.db.PurgeRelationSourcesByFileIDs(ctx, oldIDs)
	if err != nil {
		idx.logError(rootPath, "Relations safety-net: error purging stale generations: %v", err)
		return
	}
	remapped, err := idx.db.RemapRelationTargetsByFileIDs(ctx, oldIDs)
	if err != nil {
		idx.logError(rootPath, "Relations safety-net: error remapping stale generations: %v", err)
		return
	}
	unresolved, err := idx.db.DeleteUnresolvedRelationTargets(ctx, oldIDs)
	if err != nil {
		idx.logError(rootPath, "Relations safety-net: error deleting unresolved targets: %v", err)
		return
	}
	if err := idx.db.DeleteFilesByPathsExcept(ctx, paths, keepIDs); err != nil {
		idx.logError(rootPath, "Relations safety-net: error deleting stale file rows: %v", err)
		return
	}

	result := &relationsReconcileResult{
		Purged:             purged,
		Remapped:           remapped,
		DeletedUnresolved:  unresolved,
		OldFileGenerations: len(oldIDs),
	}
	idx.errorLogger.Printf("Relations safety-net: stale_paths=%d removed_file_rows=%d", len(paths), len(oldIDs))
	idx.logReconcileResult("Relations safety-net reconcile", result)
}
