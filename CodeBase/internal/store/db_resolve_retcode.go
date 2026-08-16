package store

import (
	"context"
	"fmt"
)

// ResolveRetCodeConstants РѕР±РЅРѕРІР»СЏРµС‚ message РІ ds_return_codes,
// Р·Р°РјРµРЅСЏСЏ РёРјРµРЅР° РєРѕРЅСЃС‚Р°РЅС‚ РІРёРґР° LOC_RETCODE_<N> РЅР° РёС… Р·РЅР°С‡РµРЅРёСЏ РёР· h_files_defines.
// Р’С‹РїРѕР»РЅСЏРµС‚СЃСЏ РѕРґРЅРёРј SQL-Р·Р°РїСЂРѕСЃРѕРј UPDATE ... FROM.
func (db *DB) ResolveRetCodeConstants(ctx context.Context) (int, error) {
	// РћРґРёРЅ Р·Р°РїСЂРѕСЃ: UPDATE СЃ РїРѕРґР·Р°РїСЂРѕСЃРѕРј, РІС‹Р±РёСЂР°СЋС‰РёРј Р»СѓС‡С€РёРµ Р·РЅР°С‡РµРЅРёСЏ РёР· h_files_defines
	tag, err := db.execCtx(ctx, `
		UPDATE ds_return_codes rc
		SET message = dv.define_value
		FROM (
			SELECT DISTINCT ON (d.define_name)
				d.define_name,
				d.define_value
			FROM h_files_defines d
			JOIN files f ON f.id = d.file_id
			WHERE d.define_name LIKE 'LOC_RETCODE_%'
			  AND (f.path LIKE '%localize_rus.h' OR f.path LIKE '%localize_eng.h')
			ORDER BY d.define_name,
				CASE WHEN f.path LIKE '%localize_rus.h' THEN 0 ELSE 1 END
		) dv
		WHERE rc.message = dv.define_name
	`)
	if err != nil {
		return 0, fmt.Errorf("resolve retcode constants: %w", err)
	}
	rowsAffected, err := tag.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get rows affected: %w", err)
	}
	return int(rowsAffected), nil
}
