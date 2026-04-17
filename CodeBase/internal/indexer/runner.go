package indexer

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

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

func (idx *Indexer) Init(rootPath string, parallel int) (*model.ScanStats, error) {
	scanRunID, err := idx.db.CreateScanRun(rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create scan run: %w", err)
	}

	collector := &statsCollector{}
	stopProgress := startProgressReporter("init", collector.Snapshot)
	defer stopProgress()

	includePatterns, excludePatterns := idx.walkerPatterns()
	walker := fswalk.NewWalker(rootPath, includePatterns, excludePatterns)
	filesCh, errsCh := walker.Walk()
	jobs := make(chan indexedFileJob, 128)
	var workersWG sync.WaitGroup
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		idx.processFilesWorkerPool(parallel, jobs, collector)
	}()

	go func() {
		for file := range filesCh {
			collector.Add(func(stats *model.ScanStats) {
				stats.FilesScanned++
			})
			fileID, err := idx.saveFile(file, scanRunID)
			if err != nil {
				idx.logError(file.Path, "Error saving file row: %v", err)
				collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
				continue
			}
			jobs <- indexedFileJob{file: file, fileID: fileID}
		}
		close(jobs)
	}()

	for err := range errsCh {
		idx.logError(rootPath, "Walker error: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
	}

	workersWG.Wait()
	idx.postProcessPASPending(collector)
	stats := collector.Snapshot()
	status := "completed"
	if stats.Errors > 0 {
		status = "completed_with_errors"
	}
	if err := idx.db.UpdateScanRun(scanRunID, stats.FilesScanned, stats.FilesIndexed, stats.Errors, status); err != nil {
		return nil, fmt.Errorf("failed to finalize scan run: %w", err)
	}
	return &stats, nil
}

func (idx *Indexer) Update(rootPath string, onlyModified bool, parallel int) (*model.ScanStats, error) {
	scanRunID, err := idx.db.CreateScanRun(rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create scan run: %w", err)
	}

	existing, err := idx.db.GetLatestFilesByRootPath(rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load previous file state: %w", err)
	}

	collector := &statsCollector{}
	stopProgress := startProgressReporter("update", collector.Snapshot)
	defer stopProgress()

	includePatterns, excludePatterns := idx.walkerPatterns()
	walker := fswalk.NewWalker(rootPath, includePatterns, excludePatterns)
	filesCh, errsCh := walker.Walk()
	jobs := make(chan indexedFileJob, 128)
	seen := make(map[string]struct{})
	var workersWG sync.WaitGroup
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		idx.processFilesWorkerPool(parallel, jobs, collector)
	}()

	go func() {
		for file := range filesCh {
			normalizedPath := filepath.ToSlash(strings.TrimSpace(file.Path))
			seen[normalizedPath] = struct{}{}
			collector.Add(func(stats *model.ScanStats) {
				stats.FilesScanned++
			})
			prev := existing[normalizedPath]
			if onlyModified && prev != nil && prev.HashSHA256 == file.Hash {
				continue
			}
			if prev != nil {
				if err := idx.db.DeleteFilesByPathExcept(normalizedPath, 0); err != nil {
					idx.logError(file.Path, "Error deleting outdated file rows: %v", err)
					collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
					continue
				}
				collector.Add(func(stats *model.ScanStats) { stats.FilesUpdated++ })
			} else {
				collector.Add(func(stats *model.ScanStats) { stats.FilesAdded++ })
			}
			fileID, err := idx.saveFile(file, scanRunID)
			if err != nil {
				idx.logError(file.Path, "Error saving file row: %v", err)
				collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
				continue
			}
			jobs <- indexedFileJob{file: file, fileID: fileID}
		}
		close(jobs)
	}()

	for err := range errsCh {
		idx.logError(rootPath, "Walker error: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
	}

	workersWG.Wait()
	idx.postProcessPASPending(collector)

	for path := range existing {
		if _, ok := seen[path]; ok {
			continue
		}
		if err := idx.db.DeleteFilesByPath(path); err != nil {
			idx.logError(path, "Error deleting removed file: %v", err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}
		collector.Add(func(stats *model.ScanStats) { stats.FilesDeleted++ })
	}

	stats := collector.Snapshot()
	status := "completed"
	if stats.Errors > 0 {
		status = "completed_with_errors"
	}
	if err := idx.db.UpdateScanRun(scanRunID, stats.FilesScanned, stats.FilesIndexed, stats.Errors, status); err != nil {
		return nil, fmt.Errorf("failed to finalize scan run: %w", err)
	}
	return &stats, nil
}
