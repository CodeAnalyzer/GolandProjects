package store

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// GetLatestFilesByRootPath возвращает последнее известное состояние файлов для указанного root path.
func (db *DB) GetLatestFilesByRootPath(rootPath string) (map[string]*model.File, error) {
	normalizedRoot := strings.ReplaceAll(strings.TrimSpace(rootPath), `\`, "/")
	normalizedRoot = strings.TrimSuffix(normalizedRoot, "/")
	rows, err := db.Query(`
		SELECT DISTINCT ON (path)
			id, scan_run_id, ds_product_id, path, rel_path, extension, size_bytes,
			hash_sha256, modified_at, encoding, language, created_at, updated_at
		FROM files
		WHERE path = $1 OR path LIKE $2
		ORDER BY path, id DESC
	`, normalizedRoot, normalizedRoot+"/%")
	if err != nil {
		return nil, fmt.Errorf("failed to query indexed files: %w", err)
	}
	defer rows.Close()

	files := make(map[string]*model.File)
	for rows.Next() {
		var f model.File
		var dsProductID sql.NullInt64
		if err := rows.Scan(
			&f.ID,
			&f.ScanRunID,
			&dsProductID,
			&f.Path,
			&f.RelPath,
			&f.Extension,
			&f.SizeBytes,
			&f.HashSHA256,
			&f.ModifiedAt,
			&f.Encoding,
			&f.Language,
			&f.CreatedAt,
			&f.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan indexed file row: %w", err)
		}
		if dsProductID.Valid {
			f.DsProductID = dsProductID.Int64
		}
		files[f.Path] = &f
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate indexed files: %w", err)
	}

	return files, nil
}

// DeleteFilesByPath удаляет все записи файла по path вместе с зависимыми сущностями.
func (db *DB) DeleteFilesByPath(path string) error {
	_, err := db.Exec(`DELETE FROM files WHERE path = $1`, path)
	if err != nil {
		return fmt.Errorf("failed to delete file by path: %w", err)
	}
	return nil
}

// DeleteFilesByPathExcept удаляет все записи файла по path, кроме указанной.
func (db *DB) DeleteFilesByPathExcept(path string, keepID int64) error {
	_, err := db.Exec(`DELETE FROM files WHERE path = $1 AND id <> $2`, path, keepID)
	if err != nil {
		return fmt.Errorf("failed to delete outdated file rows: %w", err)
	}
	return nil
}

// FindLatestFileIDByPaths возвращает последний file id, найденный по одному из path/rel_path кандидатов.
func (db *DB) FindLatestFileIDByPaths(paths []string) (int64, error) {
	if len(paths) == 0 {
		return 0, sql.ErrNoRows
	}

	seen := make(map[string]struct{}, len(paths))
	uniquePaths := make([]string, 0, len(paths))
	for _, path := range paths {
		normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(path), `\`, "/"))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		uniquePaths = append(uniquePaths, normalized)
	}

	if len(uniquePaths) == 0 {
		return 0, sql.ErrNoRows
	}

	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM files
		WHERE LOWER(path) = ANY($1) OR LOWER(rel_path) = ANY($1)
		ORDER BY id DESC
		LIMIT 1
	`, pq.Array(uniquePaths)).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}
