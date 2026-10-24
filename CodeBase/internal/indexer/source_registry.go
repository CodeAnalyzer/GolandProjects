package indexer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codebase/internal/fswalk"
	"github.com/codebase/internal/store"
)

// sourceEntry — каталог-источник: привязка (префикс → продукт/линия) плюс
// имя продукта для мест, где нужно строковое имя (spec-парсинг).
type sourceEntry struct {
	store.FileSourceBinding
	ProductName string
}

// sourceRegistry — префиксная карта git-источников, разделяемая воркерами
// индексации: файл берёт продукт/линию источника с самым длинным подходящим
// префиксом пути.
type sourceRegistry struct {
	mu       sync.RWMutex
	bindings []sourceEntry
}

func newSourceRegistry() *sourceRegistry {
	return &sourceRegistry{}
}

func (r *sourceRegistry) set(bindings []sourceEntry) {
	r.mu.Lock()
	r.bindings = bindings
	r.mu.Unlock()
}

// match возвращает привязку источника с самым длинним префиксом path
// (граница префикса — разделитель пути, '/' и '\'), либо nil — файл вне
// git-источников, продукт определяется прежней эвристикой.
func (r *sourceRegistry) match(path string) *sourceEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var best *sourceEntry
	bestLen := -1
	for i := range r.bindings {
		prefix := r.bindings[i].Path
		if path == prefix {
			// Точное совпадение на практике не встречается (индексируются
			// файлы), но семантика «источник покрывает свой каталог» корректна.
			if len(prefix) > bestLen {
				best = &r.bindings[i]
				bestLen = len(prefix)
			}
			continue
		}
		if len(path) <= len(prefix) {
			continue
		}
		if !hasPathPrefix(path, prefix) {
			continue
		}
		if c := path[len(prefix)]; c != '/' && c != '\\' {
			continue
		}
		if len(prefix) > bestLen {
			best = &r.bindings[i]
			bestLen = len(prefix)
		}
	}
	return best
}

// hasPathPrefix — bytes.HasPrefix без import strings в горячем пути.
func hasPathPrefix(path, prefix string) bool {
	return len(path) >= len(prefix) && path[:len(prefix)] == prefix
}

// resolveFileProduct определяет продукт файла: git-источник (discovery)
// имеет приоритет над эвристикой по имени каталога — имена вроде
// fa-contracts-11017 больше не порождают псевдопродуктов.
func (idx *Indexer) resolveFileProduct(ctx context.Context, file fswalk.FileInfo) (productName string, dsProductID int64, err error) {
	if src := idx.shared.sourceRegistry.match(file.Path); src != nil {
		return src.ProductName, src.ProductID, nil
	}
	if p := extractCanonicalDSProductName(file.Path, file.RelPath); p != "" {
		pid, resolveErr := idx.resolveDSProductID(ctx, p)
		if resolveErr != nil {
			return "", 0, resolveErr
		}
		return p, pid, nil
	}
	return "", 0, nil
}

// discoverGitSources — discovery git-источников среди непосредственных
// детей root_path перед обходом файлов: git-детекция каждого каталога с
// .git, upsert продуктов/линий, пометка default-ветки, публикация
// префиксной карты в реестр и backfill существующих строк files.
// Возвращает cutoff (до upsert) для последующей очистки устаревших линий.
// Не-git каталоги и каталоги без remote остаются на прежней эвристике
// продукта по пути; недоступный git CLI — warning и пропуск каталога.
func (idx *Indexer) discoverGitSources(ctx context.Context, rootPath string, detector *gitDetector) time.Time {
	cutoff := time.Now()

	entries, err := os.ReadDir(rootPath)
	if err != nil {
		idx.logError(rootPath, "Source discovery: %v", err)
		return cutoff
	}

	defaultBranch := "develop"
	if idx.config != nil && idx.config.Indexer.DefaultBranch != "" {
		defaultBranch = idx.config.Indexer.DefaultBranch
	}

	bindings := make([]sourceEntry, 0, len(entries))
	markedProducts := make(map[int64]struct{})
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(rootPath, entry.Name())
		if _, err := os.Stat(filepath.Join(dir, ".git")); err != nil {
			continue
		}

		src, err := detector.Detect(ctx, dir)
		if err != nil {
			idx.logError(dir, "Source discovery skipped (git unavailable): %v", err)
			continue
		}
		if src == nil {
			// нет remote / unborn HEAD — прежняя эвристика по пути
			continue
		}

		productID, err := idx.resolveDSProductID(ctx, src.Product)
		if err != nil {
			idx.logError(dir, "Source discovery product upsert: %v", err)
			continue
		}
		branchID, err := idx.db.UpsertDSProductBranch(ctx, productID, src.BranchName, src.BranchKey, dir, src.CommitSHA, cutoff)
		if err != nil {
			idx.logError(dir, "Source discovery branch upsert: %v", err)
			continue
		}
		if _, done := markedProducts[productID]; !done {
			if err := idx.db.SetDefaultBranch(ctx, productID, defaultBranch); err != nil {
				idx.logError(dir, "Source discovery default mark: %v", err)
			}
			markedProducts[productID] = struct{}{}
		}
		bindings = append(bindings, sourceEntry{
			// Нормализация как в walker (filepath.ToSlash): пути файлов и
			// префиксы источников в одной нотации.
			FileSourceBinding: store.FileSourceBinding{
				Path:      filepath.ToSlash(dir),
				ProductID: productID,
				BranchID:  branchID,
			},
			ProductName: src.Product,
		})
	}

	idx.shared.sourceRegistry.set(bindings)

	if len(bindings) > 0 {
		backfill := make([]store.FileSourceBinding, len(bindings))
		for i := range bindings {
			backfill[i] = bindings[i].FileSourceBinding
		}
		if n, err := idx.db.BackfillFilesFromSources(ctx, backfill); err != nil {
			idx.logError(rootPath, "Source discovery backfill: %v", err)
		} else if n > 0 {
			fmt.Printf("Source discovery: %d git sources, backfilled %d existing file rows\n", len(bindings), n)
		}
	}
	return cutoff
}

// cleanupStaleBranches удаляет строки линий без файлов, существовавшие до
// discovery (каталог источника исчез или переключен): вызывается в конце
// скана, когда удаление исчезнувших файлов уже выполнено.
func (idx *Indexer) cleanupStaleBranches(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}
	n, err := idx.db.DeleteStaleDSProductBranches(ctx, cutoff)
	if err != nil {
		idx.logError("<source-discovery>", "stale branch cleanup: %v", err)
		return
	}
	if n > 0 {
		fmt.Printf("Source discovery: removed %d stale branch rows without files\n", n)
	}
}
