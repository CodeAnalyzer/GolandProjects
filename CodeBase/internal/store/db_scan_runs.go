package store

import "context"

// CreateScanRun создаёт запись о запуске сканирования
func (db *DB) CreateScanRun(ctx context.Context, rootPath string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO scan_runs (root_path, status) 
		VALUES ($1, 'running') 
		RETURNING id
	`, rootPath).Scan(&id)
	return id, err
}

// HasCompletedInit проверяет, была ли уже завершена первичная инициализация индекса.
func (db *DB) HasCompletedInit(ctx context.Context) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM scan_runs
			WHERE status IN ('completed', 'completed_with_errors')
		)
	`).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// MaxScanRunID возвращает идентификатор последнего завершённого scan_run
// (PK-максимум, суб-мс) — статусы completed/completed_with_errors, идиома
// HasCompletedInit. Используется как «эпоха индексации» — компонент ключа
// кеша displaced-множества desc-поиска: любое завершённое изменение данных
// кодовой базы создаёт новый scan_run (change optimize-query-search-
// bottlenecks, B8/D5). Фильтр статуса обязателен: прогон создаёт scan_run
// со статусом running в начале работы, и без фильтра semantic-вызов во время
// update привязал бы частичный снимок к эпохе ещё не завершённого прогона —
// после его завершения MAX(id) не изменился бы и кеш раздавал бы устаревшее
// множество до следующего прогона.
func (db *DB) MaxScanRunID(ctx context.Context) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(id), 0) FROM scan_runs
		WHERE status IN ('completed', 'completed_with_errors')`).Scan(&id)
	return id, err
}

// UpdateScanRun обновляет статус сканирования и метрики прогона
// (encoding_refined/xml_skipped — сводные счётчики кодировки/skip).
func (db *DB) UpdateScanRun(ctx context.Context, id int64, filesScanned, filesIndexed, errorsCount, encodingRefined, xmlSkipped int, status string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE scan_runs
		SET finished_at = NOW(),
		    status = $6,
		    files_scanned = $1,
		    files_indexed = $2,
		    errors_count = $3,
		    encoding_refined = $4,
		    xml_skipped = $5
		WHERE id = $7
	`, filesScanned, filesIndexed, errorsCount, encodingRefined, xmlSkipped, status, id)
	return err
}
