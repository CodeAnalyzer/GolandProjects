package store

import (
	"context"
	"fmt"
)

// ResolveRetCodeConstants обновляет message в ds_return_codes,
// заменяя имена констант вида LOC_RETCODE_<N> на их значения из h_files_defines.
// Выполняется одним SQL-запросом UPDATE ... FROM.
func (db *DB) ResolveRetCodeConstants(ctx context.Context) (int, error) {
	// Один запрос: UPDATE с подзапросом, выбирающим лучшие значения из h_files_defines
	tag, err := db.ExecContext(ctx, `
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
