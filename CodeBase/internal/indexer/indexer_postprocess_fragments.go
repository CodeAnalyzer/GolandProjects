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
// pending fragment refs в scope ветки источника: фрагменты группируются по
// (product, branch) файла, на каждую группу — один FindLatestSQLTableIDsByNames
// + один FindLatestSQLProcedureIDsByNames (D4 среза branch-aware-lookup).
func (idx *Indexer) postProcessFragmentRelations(ctx context.Context, collector *statsCollector) {
	refs := idx.snapshotPendingFragmentRefs()
	if len(refs) == 0 {
		return
	}

	fragmentIDs := collectEntityIDs(refs, func(r *PendingFragmentRef) int64 { return r.FragmentID })
	scopes, err := idx.db.EntityFileScopes(ctx, "query_fragments", fragmentIDs)
	if err != nil {
		idx.logError("<post-processing>", "Error loading fragment file scopes: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	groups := groupByScope(refs, scopes, func(r *PendingFragmentRef) int64 { return r.FragmentID })

	var relations []*model.Relation
	for scope, group := range groups {
		tableIDMap, procIDMap, err := idx.resolveFragmentRefNames(ctx, group, scope)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving fragment refs (scope %+v): %v", scope, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}
		relations = append(relations, buildFragmentRefRelations(group, tableIDMap, procIDMap)...)
	}

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

// resolveFragmentRefNames собирает уникальные имена таблиц и процедур пачки
// фрагментов и резолвит их batch-запросами в scope группы.
func (idx *Indexer) resolveFragmentRefNames(ctx context.Context, refs []*PendingFragmentRef, scope store.LookupScope) (map[string]int64, map[string]int64, error) {
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

	tableIDMap, err := idx.db.FindLatestSQLTableIDsByNames(ctx, tableNames, scope)
	if err != nil {
		return nil, nil, err
	}
	procIDMap, err := idx.db.FindLatestSQLProcedureIDsByNames(ctx, procNames, scope)
	if err != nil {
		return nil, nil, err
	}
	return tableIDMap, procIDMap, nil
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

// postProcessJSCallRelations выполняет глобальный резолв JS procedure call refs
// в scope ветки файла JS-функции.
func (idx *Indexer) postProcessJSCallRelations(ctx context.Context, collector *statsCollector) {
	refs := idx.snapshotPendingJSCallRefs()
	if len(refs) == 0 {
		return
	}

	sourceIDs := collectEntityIDs(refs, func(r *PendingJSCallRef) int64 { return r.SourceID })
	scopes, err := idx.db.EntityFileScopes(ctx, "js_functions", sourceIDs)
	if err != nil {
		idx.logError("<post-processing>", "Error loading JS function file scopes: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	groups := groupByScope(refs, scopes, func(r *PendingJSCallRef) int64 { return r.SourceID })

	var relations []*model.Relation
	for scope, group := range groups {
		procIDMap, err := resolveProcedureNames(ctx, idx.db, group, func(r *PendingJSCallRef) string { return r.ProcName }, scope)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving JS call targets (scope %+v): %v", scope, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}
		relations = append(relations, buildJSCallRefRelations(group, procIDMap)...)
	}

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

// resolveProcedureNames перенесён в indexer_postprocess_scope.go (generic).

// postProcessT01SubscriberRelations выполняет глобальный резолв T01 subscriber
// refs в scope ветки файла процедуры-источника.
func (idx *Indexer) postProcessT01SubscriberRelations(ctx context.Context, collector *statsCollector) {
	refs := idx.snapshotPendingT01SubscriberRefs()
	if len(refs) == 0 {
		return
	}

	sourceIDs := collectEntityIDs(refs, func(r *PendingT01SubscriberRef) int64 { return r.SourceID })
	scopes, err := idx.db.EntityFileScopes(ctx, "sql_procedures", sourceIDs)
	if err != nil {
		idx.logError("<post-processing>", "Error loading T01 source file scopes: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	groups := groupByScope(refs, scopes, func(r *PendingT01SubscriberRef) int64 { return r.SourceID })

	var relations []*model.Relation
	for scope, group := range groups {
		calleeIDMap, err := resolveProcedureNames(ctx, idx.db, group, func(r *PendingT01SubscriberRef) string { return r.CalleeName }, scope)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving T01 subscriber targets (scope %+v): %v", scope, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}
		relations = append(relations, buildT01SubscriberRefRelations(group, calleeIDMap)...)
	}

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

// postProcessAPIMacroRelations выполняет глобальный резолв API macro refs
// в scope ветки файла процедуры-источника.
func (idx *Indexer) postProcessAPIMacroRelations(ctx context.Context, collector *statsCollector) {
	refs := idx.snapshotPendingAPIMacroRefs()
	if len(refs) == 0 {
		return
	}

	sourceIDs := collectEntityIDs(refs, func(r *PendingAPIMacroRef) int64 { return r.SourceID })
	scopes, err := idx.db.EntityFileScopes(ctx, "sql_procedures", sourceIDs)
	if err != nil {
		idx.logError("<post-processing>", "Error loading API macro source file scopes: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	groups := groupByScope(refs, scopes, func(r *PendingAPIMacroRef) int64 { return r.SourceID })

	var relations []*model.Relation
	for scope, group := range groups {
		contractIDMap, procIDMap, err := idx.resolveAPIMacroNames(ctx, group, scope)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving API macro targets (scope %+v): %v", scope, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}
		relations = append(relations, buildAPIMacroRefRelations(group, contractIDMap, procIDMap)...)
	}

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

// resolveAPIMacroNames резолвит контрактные пары и имена процедур пачки
// API-макросов batch-запросами в scope группы.
func (idx *Indexer) resolveAPIMacroNames(ctx context.Context, refs []*PendingAPIMacroRef, scope store.LookupScope) (map[string]int64, map[string]int64, error) {
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

	contractIDMap, err := idx.db.FindLatestAPIContractIDsByNamesAndKinds(ctx, contractPairs, scope)
	if err != nil {
		return nil, nil, err
	}
	procNames := make([]string, 0, len(procNameSet))
	for name := range procNameSet {
		if name != "" {
			procNames = append(procNames, name)
		}
	}
	procIDMap, err := idx.db.FindLatestSQLProcedureIDsByNames(ctx, procNames, scope)
	if err != nil {
		return nil, nil, err
	}
	return contractIDMap, procIDMap, nil
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
