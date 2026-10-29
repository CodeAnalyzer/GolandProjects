package indexer

import (
	"context"
	"strings"

	"github.com/codebase/internal/store"
)

// collectEntityIDs возвращает уникальные ненулевые id элементов.
func collectEntityIDs[T any](items []T, idOf func(T) int64) []int64 {
	seen := make(map[int64]struct{}, len(items))
	result := make([]int64, 0, len(items))
	for _, item := range items {
		id := idOf(item)
		if id == 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

// groupByScope группирует элементы по scope (продукт, ветка) файла-источника.
// entityScopes — карта id сущности → scope (EntityFileScopes); отсутствующий
// id даёт нулевой scope (глобальный резолв без предпочтений).
func groupByScope[T any](items []T, entityScopes map[int64]store.LookupScope, idOf func(T) int64) map[store.LookupScope][]T {
	groups := make(map[store.LookupScope][]T)
	for _, item := range items {
		scope := entityScopes[idOf(item)]
		groups[scope] = append(groups[scope], item)
	}
	return groups
}

// resolveProcedureNames собирает уникальные имена процедур пачки элементов и
// резолвит их одним batch-запросом в scope группы.
func resolveProcedureNames[T any](ctx context.Context, db *store.DB, items []T, nameOf func(T) string, scope store.LookupScope) (map[string]int64, error) {
	procNameSet := make(map[string]struct{})
	for _, item := range items {
		name := strings.ToLower(strings.TrimSpace(nameOf(item)))
		if name != "" {
			procNameSet[name] = struct{}{}
		}
	}
	procNames := make([]string, 0, len(procNameSet))
	for name := range procNameSet {
		procNames = append(procNames, name)
	}
	return db.FindLatestSQLProcedureIDsByNames(ctx, procNames, scope)
}
