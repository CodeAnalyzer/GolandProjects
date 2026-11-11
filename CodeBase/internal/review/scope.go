package review

import (
	"context"
	"fmt"
	"strings"

	"github.com/codebase/internal/dsinfosvc"
	"github.com/codebase/internal/store"
)

// ReviewScope — единый объект области видимости review (design D1):
// продукт и ветка проверяемого файла плюс карта разрешённых областей
// productID → ветки. Для обычного продукта в карте одна ветка, для
// EXT-продуктов — объединение develop ∪ prj/<банк> (design D2). Ветка 0 в
// значении карты означает «любая ветка продукта» (product-scope — у файла
// branch_id NULL или продукт без default-линии). Строится один раз в
// RunSQLFileCtx до prewarm'ов и стартa параллельных правил; после этого
// только читается.
type ReviewScope struct {
	FileID    int64
	ProductID int64
	BranchID  int64 // 0 = product-scope (branch_id NULL у файла)
	// ProductBranch — разрешённые области продуктов (nil = области не
	// построены, lookup'ы деградируют до глобального поведения).
	ProductBranch map[int64][]int64
}

// Pairs возвращает плоские пары карты областей для unnest-предикатов SQL.
func (s ReviewScope) Pairs() store.ProductBranchPairs {
	return store.NewProductBranchPairs(s.ProductBranch)
}

// LookupScope — продуктовый scope для тира «продукт» value-режима.
func (s ReviewScope) LookupScope() store.LookupScope {
	return store.LookupScope{ProductID: s.ProductID, BranchID: s.BranchID}
}

// buildReviewScope строит область видимости по файлу и профилю dsinfo:
// список всех линий продуктов + разрешённый контекст профиля
// (явный id > CODEBASE_DSINFO_ID > активный > отсутствие). Ошибка профиля
// (несуществующий/некорректный id) — ошибка review без fallback.
func (r *Runner) buildReviewScope(ctx context.Context, file *indexedFile, dsinfoID int64) (ReviewScope, error) {
	scope := ReviewScope{FileID: file.ID, ProductID: file.DsProductID, BranchID: file.DsProductBranchID}
	productIDs, err := r.db.ListDSProductIDs(ctx)
	if err != nil {
		return scope, err
	}
	branches, err := r.db.ListDSProductBranches(ctx)
	if err != nil {
		return scope, err
	}
	analysis, err := dsinfosvc.ResolveAnalysisContext(ctx, r.db, dsinfoID)
	if err != nil {
		return scope, fmt.Errorf("resolve dsinfo profile: %w", err)
	}
	var profileBranches map[int64]int64
	if analysis != nil {
		profileBranches = analysis.Branches
	}
	scope.ProductBranch = buildProductBranchAreas(file.DsProductID, file.DsProductBranchID, productIDs, branches, profileBranches)
	return scope, nil
}

// isPrjBranchKey определяет ветку банка EXT-продукта по ключу prj/*
// (нормализация ключей линий единая — нижний регистр, см. internal/branchkey).
func isPrjBranchKey(branchKey string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(branchKey)), "prj/")
}

func containsInt64(values []int64, target int64) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

// buildProductBranchAreas строит карту разрешённых областей всех продуктов
// индекса (design D2):
//
//   - свой продукт: из файла (профиль не переопределяет). branch_id NULL →
//     {0} (product-scope, любая ветка); файл в prj-ветке EXT → {ветка файла,
//     default}; файл в default-ветке EXT → {default} ∪ prj; иначе → {ветка
//     файла};
//   - чужой продукт: линия профиля, при её отсутствии — default-ветку.
//     Чужой EXT: профильная prj-линия → {default, профильная prj}; иначе →
//     {default} ∪ prj;
//   - продукт без единой ветки (нет строк ds_products_branch) → {0}
//     (product-scope), чтобы отсутствие линий не вырождало область в пустую
//     (ложные findings).
//
// productLines — линии одного продукта, сгруппированные для построения
// областей: prj-ветки банков и default-линия.
type productLines struct {
	prj       []int64
	defaultID int64
}

func buildProductBranchAreas(ownProductID, ownBranchID int64, productIDs []int64, branches []store.ProductBranchInfo, profile map[int64]int64) map[int64][]int64 {
	byProduct := make(map[int64]*productLines)
	for _, b := range branches {
		lines := byProduct[b.ProductID]
		if lines == nil {
			lines = &productLines{}
			byProduct[b.ProductID] = lines
		}
		if b.IsDefault && lines.defaultID == 0 {
			lines.defaultID = b.BranchID
		}
		if isPrjBranchKey(b.BranchKey) {
			lines.prj = append(lines.prj, b.BranchID)
		}
	}

	areas := make(map[int64][]int64, len(byProduct)+len(productIDs))
	for productID, lines := range byProduct {
		if productID == ownProductID {
			areas[productID] = ownProductArea(ownBranchID, *lines)
			continue
		}
		resolved := int64(0)
		if profile != nil {
			resolved = profile[productID]
		}
		if resolved == 0 {
			resolved = lines.defaultID
		}
		areas[productID] = foreignProductArea(resolved, *lines)
	}
	// Продукты без единой линии (в том числе при пустом списке веток) —
	// product-scope; свой продукт без линий — область по ветке файла
	// (строка ветки могла быть удалена после индексации).
	for _, productID := range productIDs {
		if _, ok := byProduct[productID]; ok {
			continue
		}
		areas[productID] = []int64{0}
	}
	if _, ok := byProduct[ownProductID]; !ok {
		if ownBranchID == 0 {
			areas[ownProductID] = []int64{0}
		} else {
			areas[ownProductID] = []int64{ownBranchID}
		}
	}
	return areas
}

// ownProductArea — область своего продукта: файл авторитетен, профиль не
// применяется.
func ownProductArea(ownBranchID int64, lines productLines) []int64 {
	switch {
	case ownBranchID == 0:
		return []int64{0}
	case len(lines.prj) > 0 && containsInt64(lines.prj, ownBranchID):
		area := []int64{ownBranchID}
		if lines.defaultID != 0 && lines.defaultID != ownBranchID {
			area = append(area, lines.defaultID)
		}
		return area
	case len(lines.prj) > 0 && ownBranchID == lines.defaultID:
		area := make([]int64, 0, 1+len(lines.prj))
		if lines.defaultID != 0 {
			area = append(area, lines.defaultID)
		}
		area = append(area, lines.prj...)
		return area
	default:
		return []int64{ownBranchID}
	}
}

// foreignProductArea — область чужого продукта: линия профиля (resolved) с
// fallback на default; для EXT — объединение по модели поставки банка.
func foreignProductArea(resolved int64, lines productLines) []int64 {
	if len(lines.prj) == 0 {
		if resolved == 0 {
			return []int64{0}
		}
		return []int64{resolved}
	}
	if resolved != 0 && containsInt64(lines.prj, resolved) {
		area := []int64{resolved}
		if lines.defaultID != 0 && lines.defaultID != resolved {
			area = append(area, lines.defaultID)
		}
		return area
	}
	area := make([]int64, 0, 1+len(lines.prj))
	if lines.defaultID != 0 {
		area = append(area, lines.defaultID)
	}
	area = append(area, lines.prj...)
	return area
}
