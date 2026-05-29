package store

import (
	"fmt"
	"strings"
)

func normalizeDSProductName(productName string) string {
	return strings.ToLower(strings.TrimSpace(productName))
}

// GetOrCreateDSProductIDByName возвращает id продукта по каноническому имени,
// создавая запись при отсутствии.
func (db *DB) GetOrCreateDSProductIDByName(productName string) (int64, error) {
	normalizedName := normalizeDSProductName(productName)
	if normalizedName == "" {
		return 0, fmt.Errorf("empty ds product name")
	}

	var id int64
	err := db.QueryRow(`
		INSERT INTO ds_products (product_name)
		VALUES ($1)
		ON CONFLICT (product_name)
		DO UPDATE SET updated_at = NOW()
		RETURNING id
	`, normalizedName).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create ds product id: %w", err)
	}

	return id, nil
}
