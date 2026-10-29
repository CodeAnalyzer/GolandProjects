package store

import (
	"fmt"
	"sort"
	"strings"
)

// ProfileMatchSQL строит SQL-предикат совпадения пары (продукт, ветка) с
// картой «product_id → branch_id» контекста профиля dsinfo. Используется в
// ORDER BY поверхностей, выбирающих копию сущности (ось profile_match).
//
// Пустая карта (профиль не задан или все линии не проиндексированы) даёт
// FALSE — ось вырождается, поведение идентично срезу branch-aware-lookup.
// Значения — int64 из БД, поэтому подстановка литералами безопасна.
func ProfileMatchSQL(productExpr, branchExpr string, branches map[int64]int64) string {
	if len(branches) == 0 {
		return "FALSE"
	}
	ids := make([]int64, 0, len(branches))
	for productID, branchID := range branches {
		if productID <= 0 || branchID <= 0 {
			continue
		}
		ids = append(ids, productID)
	}
	if len(ids) == 0 {
		return "FALSE"
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	pairs := make([]string, 0, len(ids))
	for _, productID := range ids {
		pairs = append(pairs, fmt.Sprintf("(%d, %d)", productID, branches[productID]))
	}
	return fmt.Sprintf("(%s, %s) IN (VALUES %s)", productExpr, branchExpr, strings.Join(pairs, ", "))
}
