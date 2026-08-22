package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// GetLatestFilesByRootPath возвращает последнее известное состояние файлов для указанного root path.
func (db *DB) GetLatestFilesByRootPath(ctx context.Context, rootPath string) (map[string]*model.File, error) {
	normalizedRoot := strings.ReplaceAll(strings.TrimSpace(rootPath), `\`, "/")
	normalizedRoot = strings.TrimSuffix(normalizedRoot, "/")
	rows, err := db.QueryContext(ctx, `
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
func (db *DB) DeleteFilesByPath(ctx context.Context, path string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM files WHERE path = $1`, path)
	if err != nil {
		return fmt.Errorf("failed to delete file by path: %w", err)
	}
	return nil
}

// DeleteFilesByPathExcept удаляет все записи файла по path, кроме указанной.
func (db *DB) DeleteFilesByPathExcept(ctx context.Context, path string, keepID int64) error {
	_, err := db.ExecContext(ctx, `DELETE FROM files WHERE path = $1 AND id <> $2`, path, keepID)
	if err != nil {
		return fmt.Errorf("failed to delete outdated file rows: %w", err)
	}
	return nil
}

// DeleteFilesByPaths удаляет все записи файлов по списку path одним батчем.
func (db *DB) DeleteFilesByPaths(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	const chunkSize = 500
	for i := 0; i < len(paths); i += chunkSize {
		end := i + chunkSize
		if end > len(paths) {
			end = len(paths)
		}
		chunk := paths[i:end]
		if _, err := db.ExecContext(ctx, `DELETE FROM files WHERE path = ANY($1)`, pq.Array(chunk)); err != nil {
			return fmt.Errorf("failed to delete files by paths: %w", err)
		}
	}
	return nil
}

// DeleteFilesByPathsExcept удаляет все записи файлов по списку path, кроме указанных ID.
// keepIDs — map[path]id, которые нужно сохранить.
func (db *DB) DeleteFilesByPathsExcept(ctx context.Context, paths []string, keepIDs map[string]int64) error {
	if len(paths) == 0 {
		return nil
	}
	const chunkSize = 500
	for i := 0; i < len(paths); i += chunkSize {
		end := i + chunkSize
		if end > len(paths) {
			end = len(paths)
		}
		withKeep, withoutKeep := splitPathsByKeepIDs(paths[i:end], keepIDs)
		if len(withoutKeep) > 0 {
			if _, err := db.ExecContext(ctx, `DELETE FROM files WHERE path = ANY($1)`, pq.Array(withoutKeep)); err != nil {
				return fmt.Errorf("failed to delete files by paths: %w", err)
			}
		}
		if len(withKeep) > 0 {
			keepPathArr := make([]string, len(withKeep))
			keepIDArr := make([]int64, len(withKeep))
			for j, p := range withKeep {
				keepPathArr[j] = p
				keepIDArr[j] = keepIDs[p]
			}
			if _, err := db.ExecContext(ctx, `
				DELETE FROM files
				WHERE path = ANY($1) AND NOT (id = ANY($2))
			`, pq.Array(keepPathArr), pq.Array(keepIDArr)); err != nil {
				return fmt.Errorf("failed to delete outdated file rows: %w", err)
			}
		}
	}
	return nil
}

func splitPathsByKeepIDs(paths []string, keepIDs map[string]int64) (withKeep, withoutKeep []string) {
	withKeep = make([]string, 0, len(paths))
	withoutKeep = make([]string, 0, len(paths))
	for _, p := range paths {
		if _, ok := keepIDs[p]; ok {
			withKeep = append(withKeep, p)
			continue
		}
		withoutKeep = append(withoutKeep, p)
	}
	return withKeep, withoutKeep
}

func chunkStrings(values []string, chunkSize int) [][]string {
	if chunkSize <= 0 || len(values) == 0 {
		return nil
	}
	chunks := make([][]string, 0, (len(values)+chunkSize-1)/chunkSize)
	for i := 0; i < len(values); i += chunkSize {
		end := i + chunkSize
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[i:end])
	}
	return chunks
}

// FindLatestFileIDByPaths возвращает последний file id, найденный по одному из path/rel_path кандидатов.
func (db *DB) FindLatestFileIDByPaths(ctx context.Context, paths []string) (int64, error) {
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
	err := db.QueryRowContext(ctx, `
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
