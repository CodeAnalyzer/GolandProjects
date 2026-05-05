package indexer

import "github.com/codebase/internal/model"

func (idx *Indexer) postProcessSQLProcedureCallRelations(collector *statsCollector) {
	pending := idx.snapshotPendingSQLCalls()
	if len(pending) == 0 {
		return
	}

	relations, err := buildSQLProcedureCallRelationsWithResolvers(pending, idx.db.FindLatestSQLProcedureIDByName)
	if err != nil {
		idx.logError("<post-processing>", "Error building SQL procedure call relations: %v", err)
		collector.Add(func(stats *model.ScanStats) {
			stats.Errors++
		})
		return
	}

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
