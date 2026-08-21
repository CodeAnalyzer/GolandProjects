package indexer

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
)

// postProcessFragmentRelations выполняет глобальный резолв всех накопленных
// pending fragment refs: один FindLatestSQLTableIDsByNames + один
// FindLatestSQLProcedureIDsByNames на все уникальные имена всех файлов.
func (idx *Indexer) postProcessFragmentRelations(ctx context.Context, collector *statsCollector) {
	refs := idx.snapshotPendingFragmentRefs()
	if len(refs) == 0 {
		return
	}

	tableNameSet := make(map[string]struct{})
	procNameSet := make(map[string]struct{})
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		for _, tableName := range ref.TablesReferenced {
			tableNameSet[strings.ToLower(strings.TrimSpace(tableName))] = struct{}{}
		}
		for _, procName := range ref.ProcCalls {
			procNameSet[strings.ToLower(strings.TrimSpace(procName))] = struct{}{}
		}
	}

	tableNames := make([]string, 0, len(tableNameSet))
	for name := range tableNameSet {
		if name != "" {
			tableNames = append(tableNames, name)
		}
	}
	procNames := make([]string, 0, len(procNameSet))
	for name := range procNameSet {
		if name != "" {
			procNames = append(procNames, name)
		}
	}

	tableIDMap, err := idx.db.FindLatestSQLTableIDsByNames(ctx, tableNames)
	if err != nil {
		idx.logError("<post-processing>", "Error resolving fragment table refs: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	procIDMap, err := idx.db.FindLatestSQLProcedureIDsByNames(ctx, procNames)
	if err != nil {
		idx.logError("<post-processing>", "Error resolving fragment proc refs: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	relations := buildFragmentRefRelations(refs, tableIDMap, procIDMap)
	localStats := &model.ScanStats{}
	if err := idx.saveRelations(ctx, relations, "<post-processing>", localStats); err != nil {
		collector.Add(func(stats *model.ScanStats) {
			mergeScanStats(stats, localStats)
		})
		return
	}
	collector.Add(func(stats *model.ScanStats) {
		mergeScanStats(stats, localStats)
	})
}

func buildFragmentRefRelations(refs []*PendingFragmentRef, tableIDMap map[string]int64, procIDMap map[string]int64) []*model.Relation {
	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})

	for _, ref := range refs {
		if ref == nil {
			continue
		}
		for _, tableName := range ref.TablesReferenced {
			targetID := tableIDMap[strings.ToLower(strings.TrimSpace(tableName))]
			if targetID == 0 {
				continue
			}
			key := fmt.Sprintf("query_fragment|%d|sql_table|%d|references_table|%d", ref.FragmentID, targetID, ref.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "query_fragment",
				SourceID:     ref.FragmentID,
				TargetType:   "sql_table",
				TargetID:     targetID,
				RelationType: "references_table",
				Confidence:   "regex",
				LineNumber:   ref.LineNumber,
			})
		}
		for _, procName := range ref.ProcCalls {
			targetID := procIDMap[strings.ToLower(strings.TrimSpace(procName))]
			if targetID == 0 {
				continue
			}
			key := fmt.Sprintf("query_fragment|%d|sql_procedure|%d|calls_procedure|%d", ref.FragmentID, targetID, ref.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "query_fragment",
				SourceID:     ref.FragmentID,
				TargetType:   "sql_procedure",
				TargetID:     targetID,
				RelationType: "calls_procedure",
				Confidence:   "regex",
				LineNumber:   ref.LineNumber,
			})
		}
	}

	return relations
}

// postProcessJSCallRelations выполняет глобальный резолв JS procedure call refs.
func (idx *Indexer) postProcessJSCallRelations(ctx context.Context, collector *statsCollector) {
	refs := idx.snapshotPendingJSCallRefs()
	if len(refs) == 0 {
		return
	}

	procNameSet := make(map[string]struct{})
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		procNameSet[strings.ToLower(strings.TrimSpace(ref.ProcName))] = struct{}{}
	}
	procNames := make([]string, 0, len(procNameSet))
	for name := range procNameSet {
		if name != "" {
			procNames = append(procNames, name)
		}
	}

	procIDMap, err := idx.db.FindLatestSQLProcedureIDsByNames(ctx, procNames)
	if err != nil {
		idx.logError("<post-processing>", "Error resolving JS call targets: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	relations := buildJSCallRefRelations(refs, procIDMap)
	localStats := &model.ScanStats{}
	if err := idx.saveRelations(ctx, relations, "<post-processing>", localStats); err != nil {
		collector.Add(func(stats *model.ScanStats) {
			mergeScanStats(stats, localStats)
		})
		return
	}
	collector.Add(func(stats *model.ScanStats) {
		mergeScanStats(stats, localStats)
	})
}

func buildJSCallRefRelations(refs []*PendingJSCallRef, procIDMap map[string]int64) []*model.Relation {
	relations := make([]*model.Relation, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))

	for _, ref := range refs {
		if ref == nil {
			continue
		}
		targetID := procIDMap[strings.ToLower(strings.TrimSpace(ref.ProcName))]
		if targetID == 0 {
			continue
		}
		key := fmt.Sprintf("js_function|%d|sql_procedure|%d|calls_procedure|%d", ref.SourceID, targetID, ref.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "js_function",
			SourceID:     ref.SourceID,
			TargetType:   "sql_procedure",
			TargetID:     targetID,
			RelationType: "calls_procedure",
			Confidence:   "regex",
			LineNumber:   ref.LineNumber,
		})
	}

	return relations
}

// postProcessT01SubscriberRelations выполняет глобальный резолв T01 subscriber refs.
func (idx *Indexer) postProcessT01SubscriberRelations(ctx context.Context, collector *statsCollector) {
	refs := idx.snapshotPendingT01SubscriberRefs()
	if len(refs) == 0 {
		return
	}

	calleeNameSet := make(map[string]struct{})
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		calleeNameSet[strings.ToLower(strings.TrimSpace(ref.CalleeName))] = struct{}{}
	}
	calleeNames := make([]string, 0, len(calleeNameSet))
	for name := range calleeNameSet {
		if name != "" {
			calleeNames = append(calleeNames, name)
		}
	}

	calleeIDMap, err := idx.db.FindLatestSQLProcedureIDsByNames(ctx, calleeNames)
	if err != nil {
		idx.logError("<post-processing>", "Error resolving T01 subscriber targets: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	relations := buildT01SubscriberRefRelations(refs, calleeIDMap)
	localStats := &model.ScanStats{}
	if err := idx.saveRelations(ctx, relations, "<post-processing>", localStats); err != nil {
		collector.Add(func(stats *model.ScanStats) {
			mergeScanStats(stats, localStats)
		})
		return
	}
	collector.Add(func(stats *model.ScanStats) {
		mergeScanStats(stats, localStats)
	})
}

func buildT01SubscriberRefRelations(refs []*PendingT01SubscriberRef, calleeIDMap map[string]int64) []*model.Relation {
	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})

	for _, ref := range refs {
		if ref == nil {
			continue
		}
		targetID := calleeIDMap[strings.ToLower(strings.TrimSpace(ref.CalleeName))]
		if targetID == 0 {
			continue
		}
		key := fmt.Sprintf("sql_procedure|%d|sql_procedure|%d|dispatches_to_subscriber|%d", ref.SourceID, targetID, ref.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "sql_procedure",
			SourceID:     ref.SourceID,
			TargetType:   "sql_procedure",
			TargetID:     targetID,
			RelationType: "dispatches_to_subscriber",
			Confidence:   "regex",
			LineNumber:   ref.LineNumber,
		})
	}

	return relations
}

// postProcessAPIMacroRelations выполняет глобальный резолв API macro refs.
func (idx *Indexer) postProcessAPIMacroRelations(ctx context.Context, collector *statsCollector) {
	refs := idx.snapshotPendingAPIMacroRefs()
	if len(refs) == 0 {
		return
	}

	contractPairs := make([]store.APIContractNameKind, 0)
	procNameSet := make(map[string]struct{})
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		targetName := strings.TrimSpace(ref.TargetName)
		if targetName == "" {
			continue
		}
		switch ref.MacroType {
		case "create_proc":
			contractPairs = append(contractPairs, store.APIContractNameKind{Name: targetName, Kind: "service"})
			contractPairs = append(contractPairs, store.APIContractNameKind{Name: targetName, Kind: "callback_event"})
		case "init_event":
			contractPairs = append(contractPairs, store.APIContractNameKind{Name: targetName, Kind: "event"})
		case "exec_contract":
			contractPairs = append(contractPairs, store.APIContractNameKind{Name: targetName, Kind: "used_service"})
			contractPairs = append(contractPairs, store.APIContractNameKind{Name: targetName, Kind: "service"})
		case "dispatches_to":
			procNameSet[strings.ToLower(targetName)] = struct{}{}
		}
	}

	contractIDMap, err := idx.db.FindLatestAPIContractIDsByNamesAndKinds(ctx, contractPairs)
	if err != nil {
		contractIDMap = map[string]int64{}
	}
	procNames := make([]string, 0, len(procNameSet))
	for name := range procNameSet {
		if name != "" {
			procNames = append(procNames, name)
		}
	}
	procIDMap, err := idx.db.FindLatestSQLProcedureIDsByNames(ctx, procNames)
	if err != nil {
		procIDMap = map[string]int64{}
	}

	relations := buildAPIMacroRefRelations(refs, contractIDMap, procIDMap)
	localStats := &model.ScanStats{}
	if err := idx.saveRelations(ctx, relations, "<post-processing>", localStats); err != nil {
		collector.Add(func(stats *model.ScanStats) {
			mergeScanStats(stats, localStats)
		})
		return
	}
	collector.Add(func(stats *model.ScanStats) {
		mergeScanStats(stats, localStats)
	})
}

func buildAPIMacroRefRelations(refs []*PendingAPIMacroRef, contractIDMap map[string]int64, procIDMap map[string]int64) []*model.Relation {
	relations := make([]*model.Relation, 0, len(refs))

	for _, ref := range refs {
		if ref == nil {
			continue
		}
		nameKey := strings.ToLower(strings.TrimSpace(ref.TargetName))
		switch ref.MacroType {
		case "create_proc":
			if targetID := contractIDMap[nameKey+"|service"]; targetID != 0 {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: ref.SourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "implements_contract", Confidence: "regex", LineNumber: ref.LineNumber})
			} else if targetID := contractIDMap[nameKey+"|callback_event"]; targetID != 0 {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: ref.SourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "implements_contract", Confidence: "regex", LineNumber: ref.LineNumber})
			}
		case "init_event":
			if targetID := contractIDMap[nameKey+"|event"]; targetID != 0 {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: ref.SourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "publishes_event", Confidence: "regex", LineNumber: ref.LineNumber})
			}
		case "exec_contract":
			if targetID := contractIDMap[nameKey+"|used_service"]; targetID != 0 {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: ref.SourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "executes_contract", Confidence: "regex", LineNumber: ref.LineNumber})
			} else if targetID := contractIDMap[nameKey+"|service"]; targetID != 0 {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: ref.SourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "executes_contract", Confidence: "regex", LineNumber: ref.LineNumber})
			}
		case "dispatches_to":
			targetID := procIDMap[nameKey]
			if targetID != 0 {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: ref.SourceID, TargetType: "sql_procedure", TargetID: targetID, RelationType: "dispatches_to", Confidence: "regex", LineNumber: ref.LineNumber})
			}
		}
	}

	return relations
}

// postProcessAllFragmentRelations запускает все 4 глобальных резолва
// параллельно (fragment refs, JS call refs, T01 subscriber refs, API macro refs).
func (idx *Indexer) postProcessAllFragmentRelations(ctx context.Context, collector *statsCollector) {
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		idx.postProcessFragmentRelations(ctx, collector)
	}()
	go func() {
		defer wg.Done()
		idx.postProcessJSCallRelations(ctx, collector)
	}()
	go func() {
		defer wg.Done()
		idx.postProcessT01SubscriberRelations(ctx, collector)
	}()
	go func() {
		defer wg.Done()
		idx.postProcessAPIMacroRelations(ctx, collector)
	}()
	wg.Wait()
}
