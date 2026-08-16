package indexer

import (
	"context"

	"github.com/codebase/internal/model"
)

// postProcessRetCodeConstants резолвит LOC_RETCODE_* константы в ds_return_codes
// через h_files_defines, заменяя имена констант на реальные тексты сообщений.
func (idx *Indexer) postProcessRetCodeConstants(collector *statsCollector) {
	updated, err := idx.db.ResolveRetCodeConstants(context.Background())
	if err != nil {
		idx.logError("<post-processing>", "Error resolving retcode constants: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	_ = updated
}
