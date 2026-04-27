package indexer

import (
	dbsql "database/sql"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
)

func (idx *Indexer) postProcessCallbackEventRelations(collector *statsCollector) {
	if err := idx.db.DeleteSubscribesToEventRelations(); err != nil {
		idx.logError("<post-processing>", "Error deleting subscribes_to_event relations: %v", err)
		collector.Add(func(stats *model.ScanStats) {
			stats.Errors++
		})
		return
	}

	callbacks, err := idx.db.FindAPIContractsByKind("callback_event")
	if err != nil {
		idx.logError("<post-processing>", "Error loading callback_event contracts: %v", err)
		collector.Add(func(stats *model.ScanStats) {
			stats.Errors++
		})
		return
	}

	relations := make([]*model.Relation, 0, len(callbacks))
	seen := make(map[string]struct{})
	for _, callback := range callbacks {
		if callback == nil || callback.ID == 0 {
			continue
		}
		usedObjectName := strings.TrimSpace(callback.UsedObjectName)
		if usedObjectName == "" {
			continue
		}

		targetID, resolveErr := idx.db.FindLatestAPIContractIDByNameKindAndOwnerModule(usedObjectName, "event", strings.TrimSpace(callback.UsedModuleSysName))
		if resolveErr != nil {
			if resolveErr == dbsql.ErrNoRows {
				continue
			}
			idx.logError("<post-processing>", "Error resolving event contract for callback %s (id=%d): %v", callback.ContractName, callback.ID, resolveErr)
			collector.Add(func(stats *model.ScanStats) {
				stats.Errors++
			})
			continue
		}
		if targetID == 0 {
			continue
		}

		key := fmt.Sprintf("api_contract|%d|api_contract|%d|subscribes_to_event", callback.ID, targetID)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "api_contract",
			SourceID:     callback.ID,
			TargetType:   "api_contract",
			TargetID:     targetID,
			RelationType: "subscribes_to_event",
			Confidence:   "xml",
			LineNumber:   1,
		})
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
