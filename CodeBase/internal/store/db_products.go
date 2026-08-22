package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func normalizeDSProductName(productName string) string {
	return strings.ToLower(strings.TrimSpace(productName))
}

// GetOrCreateDSProductIDByName возвращает id продукта по каноническому имени,
// создавая запись при отсутствии.
func (db *DB) GetOrCreateDSProductIDByName(ctx context.Context, productName string) (int64, error) {
	normalizedName := normalizeDSProductName(productName)
	if normalizedName == "" {
		return 0, fmt.Errorf("empty ds product name")
	}

	// ON CONFLICT DO NOTHING не трогает существующую строку (нет dead tuples),
	// но тогда RETURNING пуст — добираем id отдельным SELECT.
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO ds_products (product_name)
		VALUES ($1)
		ON CONFLICT (product_name) DO NOTHING
		RETURNING id
	`, normalizedName).Scan(&id)
	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return 0, fmt.Errorf("failed to get or create ds product id: %w", err)
	}

	err = db.QueryRowContext(ctx, `
		SELECT id FROM ds_products WHERE product_name = $1
	`, normalizedName).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to get ds product id after conflict: %w", err)
	}

	return id, nil
}
