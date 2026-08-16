package indexer

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/codebase/internal/fswalk"
	"github.com/codebase/internal/model"
)

func (idx *Indexer) walkerPatterns() ([]string, []string) {
	supported := fswalk.GetSupportedExtensions()
	supportedSet := make(map[string]struct{}, len(supported))
	for _, ext := range supported {
		supportedSet[strings.ToLower(strings.TrimSpace(ext))] = struct{}{}
	}

	includePatterns := supported
	excludePatterns := []string(nil)
	if idx.config != nil {
		excludePatterns = idx.config.Indexer.ExcludePatterns
		if len(idx.config.Indexer.IncludePatterns) > 0 {
			filtered := make([]string, 0, len(idx.config.Indexer.IncludePatterns))
			seen := make(map[string]struct{}, len(idx.config.Indexer.IncludePatterns))
			for _, pattern := range idx.config.Indexer.IncludePatterns {
				normalized := strings.ToLower(strings.TrimSpace(pattern))
				if normalized == "" {
					continue
				}
				ext := normalized

				ext = strings.TrimPrefix(ext, "*")

				if !strings.HasPrefix(ext, ".") {
					continue
				}
				if _, ok := supportedSet[ext]; !ok {
					continue
				}
				if _, ok := seen[normalized]; ok {
					continue
				}
				seen[normalized] = struct{}{}
				filtered = append(filtered, normalized)
			}
			if len(filtered) > 0 {
				sort.Strings(filtered)
				includePatterns = filtered
			}
		}
	}

	return includePatterns, excludePatterns
}

func (idx *Indexer) InitCtx(ctx context.Context, rootPath string, parallel int) (*model.ScanStats, error) {
	startedAt := time.Now()
	scanRunID, err := idx.db.CreateScanRun(ctx, rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create scan run: %w", err)
	}

	collector := &statsCollector{}
	stopProgress := startProgressReporter("init", collector.Snapshot)
	defer stopProgress()

	includePatterns, excludePatterns := idx.walkerPatterns()
	walker := fswalk.NewWalker(rootPath, includePatterns, excludePatterns)
	filesCh, errsCh := walker.WalkParallel(parallel)

	// Воркеры читают файлы напрямую из filesCh и делают saveFile + processFile
	// параллельно. Это устраняет bottleneck последовательных INSERT-ов в один поток.
	var workersWG sync.WaitGroup
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		idx.processFilesWorkerPoolInit(ctx, parallel, filesCh, scanRunID, collector)
	}()

	for err := range errsCh {
		idx.logError(rootPath, "Walker error: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
	}

	workersWG.Wait()
	walkSaveDone := time.Now()
	idx.runPostProcessingParallel(ctx, collector, parallel)
	postProcessDone := time.Now()
	stats := collector.Snapshot()
	stats.WalkSaveMs = walkSaveDone.Sub(startedAt).Milliseconds()
	stats.ProcessMs = 0 // Init: saveFile+processFile в одном цикле, per-phase через SaveMs/ParseMs
	stats.PostProcessMs = postProcessDone.Sub(walkSaveDone).Milliseconds()
	status := "completed"
	if stats.Errors > 0 {
		status = "completed_with_errors"
	}
	if err := idx.db.UpdateScanRun(ctx, scanRunID, stats.FilesScanned, stats.FilesIndexed, stats.Errors, status); err != nil {
		return nil, fmt.Errorf("failed to finalize scan run: %w", err)
	}
	return &stats, nil
}

func (idx *Indexer) Init(rootPath string, parallel int) (*model.ScanStats, error) {
	return idx.InitCtx(context.Background(), rootPath, parallel)
}

func (idx *Indexer) UpdateCtx(ctx context.Context, rootPath string, onlyModified bool, parallel int) (*model.ScanStats, error) {
	startedAt := time.Now()
	scanRunID, err := idx.db.CreateScanRun(ctx, rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create scan run: %w", err)
	}

	existing, err := idx.db.GetLatestFilesByRootPath(ctx, rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load previous file state: %w", err)
	}

	collector := &statsCollector{}
	stopProgress := startProgressReporter("update", collector.Snapshot)
	defer stopProgress()

	includePatterns, excludePatterns := idx.walkerPatterns()
	walker := fswalk.NewWalker(rootPath, includePatterns, excludePatterns)

	// Pre-filter: пропускаем чтение файлов, у которых mtime+size не изменились.
	fingerprints := make(map[string]fswalk.FileFingerprint, len(existing))
	for path, f := range existing {
		fingerprints[path] = fswalk.FileFingerprint{Size: f.SizeBytes, ModTime: f.ModifiedAt}
	}
	walker.SetPreFilter(fingerprints)

	filesCh, errsCh := walker.WalkParallel(parallel)
	jobs := make(chan indexedFileJob, 128)
	seen := make(map[string]struct{})
	var workersWG sync.WaitGroup
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		idx.processFilesWorkerPool(ctx, parallel, jobs, collector)
	}()

	// Collect paths and new file IDs for modified files.
	modifiedPaths := make([]string, 0, 128)
	newFileIDs := make(map[string]int64, 128)

	var feederWG sync.WaitGroup
	feederWG.Add(1)
	go func() {
		defer feederWG.Done()
		for file := range filesCh {
			normalizedPath := filepath.ToSlash(strings.TrimSpace(file.Path))
			seen[normalizedPath] = struct{}{}
			collector.Add(func(stats *model.ScanStats) {
				stats.FilesScanned++
			})
			prev := existing[normalizedPath]
			// Pre-filtered файл (Hash пустой — не читался, mtime+size совпадают)
			if file.Hash == "" && prev != nil {
				collector.Add(func(stats *model.ScanStats) { stats.PreFilteredFiles++ })
				continue
			}
			if onlyModified && prev != nil && prev.HashSHA256 == file.Hash {
				continue
			}
			saveStart := time.Now()
			fileID, err := idx.saveFileCtx(ctx, file, scanRunID)
			saveElapsed := time.Since(saveStart).Milliseconds()
			collector.Add(func(stats *model.ScanStats) { stats.SaveMs += saveElapsed })
			if err != nil {
				idx.logError(file.Path, "Error saving file row: %v", err)
				collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
				continue
			}
			if prev != nil {
				modifiedPaths = append(modifiedPaths, normalizedPath)
				newFileIDs[normalizedPath] = fileID
				collector.Add(func(stats *model.ScanStats) { stats.FilesUpdated++ })
			} else {
				collector.Add(func(stats *model.ScanStats) { stats.FilesAdded++ })
			}
			jobs <- indexedFileJob{file: file, fileID: fileID}
		}
		close(jobs)
	}()

	for err := range errsCh {
		idx.logError(rootPath, "Walker error: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
	}

	feederWG.Wait()
	walkSaveDone := time.Now()

	// Batch delete old rows for modified files, keeping the new file IDs.
	if len(modifiedPaths) > 0 {
		if err := idx.db.DeleteFilesByPathsExcept(ctx, modifiedPaths, newFileIDs); err != nil {
			idx.logError("<cleanup>", "Error batch deleting outdated file rows: %v", err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		}
	}

	workersWG.Wait()
	processDone := time.Now()
	idx.runPostProcessingParallel(ctx, collector, parallel)
	postProcessDone := time.Now()

	// Batch delete removed files (not seen in walker).
	removedPaths := make([]string, 0, 128)
	for path := range existing {
		if _, ok := seen[path]; ok {
			continue
		}
		removedPaths = append(removedPaths, path)
	}
	if err := idx.db.DeleteFilesByPaths(ctx, removedPaths); err != nil {
		idx.logError("<cleanup>", "Error batch deleting removed files: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
	}
	collector.Add(func(stats *model.ScanStats) { stats.FilesDeleted += len(removedPaths) })
	cleanupDone := time.Now()

	stats := collector.Snapshot()
	stats.WalkSaveMs = walkSaveDone.Sub(startedAt).Milliseconds()
	stats.ProcessMs = processDone.Sub(walkSaveDone).Milliseconds()
	stats.PostProcessMs = postProcessDone.Sub(processDone).Milliseconds()
	stats.CleanupMs = cleanupDone.Sub(postProcessDone).Milliseconds()
	status := "completed"
	if stats.Errors > 0 {
		status = "completed_with_errors"
	}
	if err := idx.db.UpdateScanRun(ctx, scanRunID, stats.FilesScanned, stats.FilesIndexed, stats.Errors, status); err != nil {
		return nil, fmt.Errorf("failed to finalize scan run: %w", err)
	}
	return &stats, nil
}

func (idx *Indexer) Update(rootPath string, onlyModified bool, parallel int) (*model.ScanStats, error) {
	return idx.UpdateCtx(context.Background(), rootPath, onlyModified, parallel)
}

// runPostProcessingParallel запускает все независимые пост-обработки параллельно.
// Каждая пост-обработка работает с разными типами relations и не конфликтует с другими.
// DELETE выполняется до запуска горутин, чтобы не конкурировать с параллельными COPY INTO relations.
func (idx *Indexer) runPostProcessingParallel(ctx context.Context, collector *statsCollector, parallel int) {
	if err := idx.db.DeleteSubscribesToEventRelations(ctx); err != nil {
		idx.logError("<post-processing>", "Error deleting subscribes_to_event relations: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
	}

	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		idx.postProcessPASPending(collector)
	}()
	go func() {
		defer wg.Done()
		idx.postProcessSQLProcedureCallRelations(collector, parallel)
	}()
	go func() {
		defer wg.Done()
		idx.postProcessCallbackEventRelations(collector)
	}()
	go func() {
		defer wg.Done()
		idx.postProcessRetCodeConstants(collector)
	}()
	go func() {
		defer wg.Done()
		idx.postProcessAllFragmentRelations(collector)
	}()
	wg.Wait()
}
