package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
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

// UpsertDSProductBranch — upsert релизной линии по каталогу-источнику:
// путь уникален, один каталог = одна строка (смена ветки в каталоге
// обновляет ту же строку). Линия (продукт, ключ) в другом каталоге
// освобождается только если устарела: без файлов И существовала до cutoff
// (каталог перенесён). Живая строка с файлами или свежая строка текущего
// скана приводит вставку к отказу на unique — два каталога одной линии
// запрещены (требование «Хранение релизных линий»).
func (db *DB) UpsertDSProductBranch(ctx context.Context, productID int64, branchName, branchKey, path, commitSHA string, cutoff time.Time) (int64, error) {
	if _, err := db.ExecContext(ctx, `
		DELETE FROM ds_products_branch b
		WHERE b.ds_product_id = $1 AND b.branch_key = $2 AND b.path <> $3
		  AND b.indexed_at < $4
		  AND NOT EXISTS (SELECT 1 FROM files f WHERE f.ds_product_branch_id = b.id)
	`, productID, branchKey, path, cutoff); err != nil {
		return 0, fmt.Errorf("failed to free stale ds product branch: %w", err)
	}

	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, commit_sha, indexed_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (path) DO UPDATE SET
			ds_product_id = EXCLUDED.ds_product_id,
			branch_name   = EXCLUDED.branch_name,
			branch_key    = EXCLUDED.branch_key,
			commit_sha    = EXCLUDED.commit_sha,
			is_default    = FALSE,
			indexed_at    = NOW(),
			updated_at    = NOW()
		RETURNING id
	`, productID, branchName, branchKey, path, commitSHA).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to upsert ds product branch: %w", err)
	}
	return id, nil
}

// SetDefaultBranch помечает линию продукта с именем ветки branchName как
// default (is_default = TRUE), снимая флаг с прочих линий продукта
// (смена default_branch в конфигурации перекладывает флаг). Отсутствие
// линии с таким именем — не ошибка: default-ветка может быть не
// проиндексирована.
func (db *DB) SetDefaultBranch(ctx context.Context, productID int64, branchName string) error {
	if _, err := db.ExecContext(ctx, `
		UPDATE ds_products_branch
		SET is_default = (branch_name = $2), updated_at = NOW()
		WHERE ds_product_id = $1
	`, productID, branchName); err != nil {
		return fmt.Errorf("failed to set default ds product branch: %w", err)
	}
	return nil
}

// DeleteStaleDSProductBranches удаляет строки линий без файлов, существовавшие
// до cutoff: каталог источника исчез (файлы удалены сканом) или переключен на
// другую линию. Свежие строки текущего скана (indexed_at >= cutoff) не
// трогаются — их файлы ещё индексируются.
func (db *DB) DeleteStaleDSProductBranches(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := db.ExecContext(ctx, `
		DELETE FROM ds_products_branch b
		WHERE b.indexed_at < $1
		  AND NOT EXISTS (SELECT 1 FROM files f WHERE f.ds_product_branch_id = b.id)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to delete stale ds product branches: %w", err)
	}
	rows, err := tag.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to count deleted stale ds product branches: %w", err)
	}
	return rows, nil
}

// FileSourceBinding — привязка каталога-источника к продукту и релизной
// линии (префиксная карта discovery).
type FileSourceBinding struct {
	Path      string
	ProductID int64
	BranchID  int64
}

// BackfillFilesFromSources проставляет продукт/линию существующим строкам
// files по префиксу каталога источника (граница — разделитель пути, '/' и
// '\'): чинит строки, пропущенные pre-filter'ом по mtime+size, и старые
// псевдопродукты эвристики по имени каталога — без переиндексации
// содержимого. Источники — непосредственные дети root_path, префиксы не
// пересекаются. Возвращает число изменённых строк.
func (db *DB) BackfillFilesFromSources(ctx context.Context, sources []FileSourceBinding) (int64, error) {
	if len(sources) == 0 {
		return 0, nil
	}

	var sb strings.Builder
	sb.WriteString(`WITH src(prefix, product_id, branch_id) AS (VALUES `)
	args := make([]interface{}, 0, len(sources)*3)
	for i, s := range sources {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 3
		fmt.Fprintf(&sb, "($%d::text, $%d::bigint, $%d::bigint)", base+1, base+2, base+3)
		args = append(args, s.Path, s.ProductID, s.BranchID)
	}
	sb.WriteString(`)
		UPDATE files f SET ds_product_id = s.product_id, ds_product_branch_id = s.branch_id
		FROM src s
		WHERE left(f.path, length(s.prefix)) = s.prefix
		  AND (length(f.path) = length(s.prefix)
		       OR substring(f.path from length(s.prefix) + 1 for 1) IN ('/', '\'))
		  AND (f.ds_product_id IS DISTINCT FROM s.product_id
		       OR f.ds_product_branch_id IS DISTINCT FROM s.branch_id)`)

	tag, err := db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("failed to backfill files from sources: %w", err)
	}
	rows, err := tag.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to count backfilled files: %w", err)
	}
	return rows, nil
}
