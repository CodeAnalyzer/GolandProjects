package indexer

import (
	"context"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
)

func (idx *Indexer) postProcessCallbackEventRelations(collector *statsCollector) {
	callbacks, err := idx.db.FindAPIContractsByKind(context.Background(), "callback_event")
	if err != nil {
		idx.logError("<post-processing>", "Error loading callback_event contracts: %v", err)
		collector.Add(func(stats *model.ScanStats) {
			stats.Errors++
		})
		return
	}

	// Collect unique event names from callbacks for batch-resolve.
	eventNames := make([]string, 0, len(callbacks))
	for _, cb := range callbacks {
		if cb == nil {
			continue
		}
		name := strings.TrimSpace(cb.UsedObjectName)
		if name != "" {
			eventNames = append(eventNames, name)
		}
	}

	lookup, err := idx.db.FindLatestEventContractIDsByNames(context.Background(), eventNames)
	if err != nil {
		idx.logError("<post-processing>", "Error batch-loading event contracts: %v", err)
		collector.Add(func(stats *model.ScanStats) {
			stats.Errors++
		})
		return
	}

	relations := buildCallbackEventRelations(callbacks, lookup)

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

func buildCallbackEventRelations(callbacks []*model.APIContract, lookup *store.EventContractLookup) []*model.Relation {
	if lookup == nil {
		return nil
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

		nameKey := strings.ToLower(usedObjectName)
		moduleKey := strings.ToLower(strings.TrimSpace(callback.UsedModuleSysName))

		var targetID int64
		if moduleKey != "" {
			targetID = lookup.ByNameAndModule[nameKey+"|"+moduleKey]
		}
		if targetID == 0 {
			targetID = lookup.ByName[nameKey]
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
	return relations
}
