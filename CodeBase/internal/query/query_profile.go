package query

import (
	"strings"

	"github.com/codebase/internal/store"
)

// ProfileContext — контекст профиля развёртывания (dsinfo) для выбора
// релизной ветки в запросах.
//
// Branches — карта product_id → branch_id выбранных линий (SQL-ось
// profile_match); Requested — product_name → все запрошенные линии профиля
// (для поля branch_requested и проверки fallback в результатах).
type ProfileContext struct {
	ProfileID int64
	Branches  map[int64]int64
	Requested map[string][]string
}

// Option настраивает Query при создании.
type Option func(*Query)

// WithProfile включает контекст профиля dsinfo: ось profile_match в выборе
// копии сущности и заполнение branch_requested. nil/пустой контекст —
// поведение без профиля (default-ветка).
func WithProfile(pc *ProfileContext) Option {
	return func(q *Query) {
		q.profile = pc
	}
}

// profileMatchSQL — SQL-выражение оси profile_match для пары
// (продукт, ветка) с защитой от NULL (COALESCE(..., FALSE)).
func (q *Query) profileMatchSQL(productExpr, branchExpr string) string {
	branches := map[int64]int64(nil)
	if q != nil && q.profile != nil {
		branches = q.profile.Branches
	}
	// COALESCE обязателен и без профиля: голый FALSE — константа, которую
	// PostgreSQL отвергает в ORDER BY («non-integer constant in ORDER BY»).
	return "COALESCE(" + store.ProfileMatchSQL(productExpr, branchExpr, branches) + ", FALSE)"
}

// profileBranches возвращает карту product_id → branch_id профиля (nil без
// контекста).
func (q *Query) profileBranches() map[int64]int64 {
	if q == nil || q.profile == nil {
		return nil
	}
	return q.profile.Branches
}

// branchRequested возвращает запрошенные линии профиля для продукта через
// запятую (пусто, если профиль не задан или продукт вне профиля).
func (q *Query) branchRequested(product string) string {
	if q == nil || q.profile == nil || product == "" {
		return ""
	}
	return strings.Join(q.profile.Requested[product], ", ")
}

// profileProductSet возвращает множество продуктов профиля (nil без
// контекста) — для приоритизации копий при схлопывании одинаковых имён.
func (q *Query) profileProductSet() map[string]bool {
	if q == nil || q.profile == nil || len(q.profile.Requested) == 0 {
		return nil
	}
	set := make(map[string]bool, len(q.profile.Requested))
	for name := range q.profile.Requested {
		set[name] = true
	}
	return set
}
