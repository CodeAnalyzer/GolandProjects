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

// UpdateScanRun обновляет статус сканирования
func (db *DB) UpdateScanRun(ctx context.Context, id int64, filesScanned, filesIndexed, errorsCount int, status string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE scan_runs 
		SET finished_at = NOW(),
		    status = $4,
		    files_scanned = $1,
		    files_indexed = $2,
		    errors_count = $3
		WHERE id = $5
	`, filesScanned, filesIndexed, errorsCount, status, id)
	return err
}
