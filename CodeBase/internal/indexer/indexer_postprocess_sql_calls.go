package indexer

import (
	"fmt"
	"strings"
	"sync"

	"github.com/codebase/internal/model"
)

func (idx *Indexer) postProcessSQLProcedureCallRelations(collector *statsCollector, parallel int) {
	pending := idx.snapshotPendingSQLCalls()
	if len(pending) == 0 {
		return
	}

	calleeNames := collectUniqueSQLCallCalleeNames(pending)
	targetIDs, err := idx.db.FindLatestSQLProcedureIDsByNames(calleeNames)
	if err != nil {
		idx.logError("<post-processing>", "Error resolving SQL procedure call targets: %v", err)
		collector.Add(func(stats *model.ScanStats) {
			stats.Errors++
		})
		return
	}

	relations := buildSQLProcedureCallRelationsParallel(pending, targetIDs, parallel, func(count int) {
		collector.Add(func(stats *model.ScanStats) {
			stats.PostProcessed += count
		})
	})
	localStats := &model.ScanStats{}
	if err := idx.saveRelations(relations, "<post-processing>", localStats); err != nil {
		collector.Add(func(stats *model.ScanStats) {
			mergeScanStats(stats, localStats)
		})
		return
	}
	collector.Add(func(stats *model.ScanStats) {
		mergeScanStats(stats, localStats)
	})
}

func collectUniqueSQLCallCalleeNames(pending []*PendingSQLCallFile) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, item := range pending {
		if item == nil {
			continue
		}
		for _, call := range item.Calls {
			if call == nil {
				continue
			}
			name := strings.TrimSpace(call.CalleeName)
			key := strings.ToLower(name)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			result = append(result, name)
		}
	}
	return result
}

func buildSQLProcedureCallRelationsParallel(pending []*PendingSQLCallFile, targetIDs map[string]int64, parallel int, reportProgress func(int)) []*model.Relation {
	workerCount := normalizeParallel(parallel)
	if workerCount > len(pending) {
		workerCount = len(pending)
	}
	if workerCount <= 1 {
		return buildSQLProcedureCallRelationsWithTargetIDs(pending, targetIDs, reportProgress)
	}

	chunkSize := (len(pending) + workerCount - 1) / workerCount
	results := make([][]*model.Relation, workerCount)
	var wg sync.WaitGroup
	for workerIndex := 0; workerIndex < workerCount; workerIndex++ {
		start := workerIndex * chunkSize
		if start >= len(pending) {
			break
		}
		end := start + chunkSize
		if end > len(pending) {
			end = len(pending)
		}
		wg.Add(1)
		go func(index int, chunk []*PendingSQLCallFile) {
			defer wg.Done()
			results[index] = buildSQLProcedureCallRelationsWithTargetIDs(chunk, targetIDs, reportProgress)
		}(workerIndex, pending[start:end])
	}
	wg.Wait()

	total := 0
	for _, relations := range results {
		total += len(relations)
	}
	merged := make([]*model.Relation, 0, total)
	seen := make(map[string]struct{}, total)
	for _, relations := range results {
		for _, relation := range relations {
			if relation == nil {
				continue
			}
			key := relationDedupKey(relation)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, relation)
		}
	}
	return merged
}

func relationDedupKey(relation *model.Relation) string {
	return fmt.Sprintf("%s|%d|%s|%d|%s|%d", relation.SourceType, relation.SourceID, relation.TargetType, relation.TargetID, relation.RelationType, relation.LineNumber)
}
