package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/codebase/internal/branchkey"
)

// Статусы матчинга строки манифеста профиля на проиндексированные ветки.
const (
	// DsInfoMatchOK — продукт и линия проиндексированы.
	DsInfoMatchOK = "ok"
	// DsInfoMatchProductNotIndexed — продукта нет в индексе.
	DsInfoMatchProductNotIndexed = "product_not_indexed"
	// DsInfoMatchBranchNotIndexed — продукт есть, подходящей линии нет.
	DsInfoMatchBranchNotIndexed = "branch_not_indexed"
)

// DsInfoProductMatch — результат сопоставления строки манифеста профиля с
// индексом. Вычисляется на лету (снимок профиля не изменяется).
//
// RequestedBranchKey — branch_key манифеста (нормализованный при parse),
// MatchedBranchKey — фактический branch_key проиндексированной линии при
// статусе ok (в остальных статусах пуст).
type DsInfoProductMatch struct {
	LineNo             int    `json:"line_no"`
	RawName            string `json:"raw_name"`
	ProductName        string `json:"product_name"`
	RequestedBranchKey string `json:"branch_requested"`
	MatchedProductID   int64  `json:"-"`
	MatchedBranchID    int64  `json:"-"`
	MatchedBranchKey   string `json:"branch_used,omitempty"`
	MatchedBranchDefault bool `json:"-"` // выбранная линия — default-ветка продукта
	FallbackBranchID   int64  `json:"-"` // default-линия продукта при branch_not_indexed
	FallbackBranchKey  string `json:"-"` // branch_key этой линии
	Status             string `json:"status"`
}

// LoadDsInfoProductMatches сопоставляет манифест профиля с индексом:
// продукт — по точному product_name, линия — сначала точным branch_key,
// затем fallback по числовым токенам хвоста в пределах того же продукта
// (fa-contracts-11-01-7 ↔ sup/11-01-7). Точное совпадение имеет приоритет;
// при нескольких кандидатах fallback выбирается default-линия, затем
// больший id (порядок строк запроса).
func (db *DB) LoadDsInfoProductMatches(ctx context.Context, dsinfoID int64) ([]DsInfoProductMatch, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT p.line_no, p.raw_name, p.product_name, p.branch_key,
		       dp.id, b.id, b.branch_key, COALESCE(b.is_default, FALSE)
		FROM dsinfo_products p
		LEFT JOIN ds_products dp ON dp.product_name = p.product_name
		LEFT JOIN ds_products_branch b ON b.ds_product_id = dp.id
		WHERE p.dsinfo_id = $1
		ORDER BY p.line_no, COALESCE(b.is_default, FALSE) DESC, b.id DESC`,
		dsinfoID)
	if err != nil {
		return nil, fmt.Errorf("failed to load dsinfo product matches: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		productID int64
		branchID  int64
		branchKey string
		isDefault bool
	}
	type lineData struct {
		match      DsInfoProductMatch
		productID  int64
		hasProduct bool
		branches   []candidate
	}

	byLine := make(map[int]*lineData)
	var order []int
	for rows.Next() {
		var (
			lineNo                    int
			rawName, productName, key string
			productID, branchID       sql.NullInt64
			branchKey                 sql.NullString
			isDefault                 bool
		)
		if err := rows.Scan(&lineNo, &rawName, &productName, &key,
			&productID, &branchID, &branchKey, &isDefault); err != nil {
			return nil, err
		}
		ld, ok := byLine[lineNo]
		if !ok {
			ld = &lineData{match: DsInfoProductMatch{
				LineNo:             lineNo,
				RawName:            rawName,
				ProductName:        productName,
				RequestedBranchKey: key,
			}}
			byLine[lineNo] = ld
			order = append(order, lineNo)
		}
		if productID.Valid {
			ld.hasProduct = true
			if ld.productID == 0 {
				ld.productID = productID.Int64
			}
		}
		if branchID.Valid {
			ld.branches = append(ld.branches, candidate{
				productID: productID.Int64,
				branchID:  branchID.Int64,
				branchKey: branchKey.String,
				isDefault: isDefault,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	matches := make([]DsInfoProductMatch, 0, len(order))
	for _, lineNo := range order {
		ld := byLine[lineNo]
		m := ld.match
		if !ld.hasProduct {
			m.Status = DsInfoMatchProductNotIndexed
			matches = append(matches, m)
			continue
		}

		pick := -1
		for i := range ld.branches {
			if ld.branches[i].branchKey == m.RequestedBranchKey {
				pick = i
				break
			}
		}
		if pick < 0 {
			for i := range ld.branches {
				if branchkey.NumericTailMatches(m.RequestedBranchKey, ld.branches[i].branchKey) {
					pick = i
					break
				}
			}
		}
		if pick < 0 {
			m.Status = DsInfoMatchBranchNotIndexed
			m.MatchedProductID = ld.productID
			if len(ld.branches) > 0 {
				// Линия не проиндексирована: профиль всё равно выбирает
				// продукт, а внутри него — default-ветку (первая по порядку
				// запроса: is_default, затем больший id).
				m.FallbackBranchID = ld.branches[0].branchID
				m.FallbackBranchKey = ld.branches[0].branchKey
			}
			matches = append(matches, m)
			continue
		}

		m.Status = DsInfoMatchOK
		m.MatchedProductID = ld.branches[pick].productID
		m.MatchedBranchID = ld.branches[pick].branchID
		m.MatchedBranchKey = ld.branches[pick].branchKey
		m.MatchedBranchDefault = ld.branches[pick].isDefault
		matches = append(matches, m)
	}
	return matches, nil
}
