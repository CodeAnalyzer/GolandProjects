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

// GetOrCreateDSProductIDByName РІРѕР·РІСЂР°С‰Р°РµС‚ id РїСЂРѕРґСѓРєС‚Р° РїРѕ РєР°РЅРѕРЅРёС‡РµСЃРєРѕРјСѓ РёРјРµРЅРё,
// СЃРѕР·РґР°РІР°СЏ Р·Р°РїРёСЃСЊ РїСЂРё РѕС‚СЃСѓС‚СЃС‚РІРёРё.
func (db *DB) GetOrCreateDSProductIDByName(ctx context.Context, productName string) (int64, error) {
	normalizedName := normalizeDSProductName(productName)
	if normalizedName == "" {
		return 0, fmt.Errorf("empty ds product name")
	}

	// ON CONFLICT DO NOTHING РЅРµ С‚СЂРѕРіР°РµС‚ СЃСѓС‰РµСЃС‚РІСѓСЋС‰СѓСЋ СЃС‚СЂРѕРєСѓ (РЅРµС‚ dead tuples),
	// РЅРѕ С‚РѕРіРґР° RETURNING РїСѓСЃС‚ вЂ” РґРѕР±РёСЂР°РµРј id РѕС‚РґРµР»СЊРЅС‹Рј SELECT.
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
