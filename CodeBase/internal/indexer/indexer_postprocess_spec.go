package indexer

import (
	"context"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
)

// postProcessSpecCodeMentions резолвит spec_code_mentions в relations
// (references_code) и связывает их с реальными сущностями кода.
// Также строит depends_on_capability и change_modifies отношения.
func (idx *Indexer) postProcessSpecCodeMentions(ctx context.Context, collector *statsCollector) {
	if err := idx.db.ResolveSpecMentionSourceIDs(ctx); err != nil {
		idx.logError("<post-processing>", "Error resolving spec mention source IDs: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	mentions, err := idx.db.LoadAllSpecCodeMentions(ctx)
	if err != nil {
		idx.logError("<post-processing>", "Error loading spec_code_mentions: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	if len(mentions) == 0 {
		return
	}

	// 1. Собираем уникальные имена для batch-lookup
	procNames := collectMentionNamesByKind(mentions, "procedure")
	tableNames := collectMentionNamesByKind(mentions, "table")
	formNames := collectMentionNamesByKind(mentions, "form")
	smfNames := collectMentionNamesByKind(mentions, "smf")
	methodNames := collectMentionNamesByKind(mentions, "method")
	apiNames := collectMentionNamesByKind(mentions, "api")

	// 2. Batch-resolve имён в ID сущностей
	lookup := &specMentionLookup{}
	if len(procNames) > 0 {
		ids, err := idx.db.FindLatestSQLProcedureIDsByNames(ctx, procNames)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving procedure names for spec mentions: %v", err)
		}
		lookup.Procedures = ids
	}
	if len(tableNames) > 0 {
		ids, err := idx.db.FindLatestSQLTableIDsByNames(ctx, tableNames)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving table names for spec mentions: %v", err)
		}
		lookup.Tables = ids
	}
	if len(formNames) > 0 {
		ids, err := idx.db.FindDFMFormIDsByNames(ctx, formNames)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving form names for spec mentions: %v", err)
		}
		lookup.Forms = ids
	}
	if len(smfNames) > 0 {
		ids, err := idx.db.FindSMFInstrumentIDsByNames(ctx, smfNames)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving SMF names for spec mentions: %v", err)
		}
		lookup.SMF = ids
	}
	if len(methodNames) > 0 {
		ids, err := idx.db.FindPASMethodIDsByNames(ctx, methodNames)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving method names for spec mentions: %v", err)
		}
		lookup.Methods = ids
	}
	if len(apiNames) > 0 {
		ids, err := idx.db.FindAPIContractIDsByNames(ctx, apiNames)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving API names for spec mentions: %v", err)
		}
		lookup.APIs = ids
	}

	// 3. Строим relations
	relations := buildSpecMentionRelations(mentions, lookup)

	// 4. Удаляем старые references_code relations от spec-сущностей
	if err := idx.db.DeleteSpecReferenceRelations(ctx); err != nil {
		idx.logError("<post-processing>", "Error deleting old spec references_code relations: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	localStats := &model.ScanStats{}
	if err := idx.saveRelations(ctx, relations, "<spec-post-processing>", localStats); err != nil {
		collector.Add(func(stats *model.ScanStats) { mergeScanStats(stats, localStats) })
		return
	}
	collector.Add(func(stats *model.ScanStats) { mergeScanStats(stats, localStats) })
}

// specMentionLookup — batch-resolved ID для каждого kind.
type specMentionLookup struct {
	Procedures map[string]int64 // lower(name) → id
	Tables     map[string]int64
	Forms      map[string]int64
	SMF        map[string]int64
	Methods    map[string]int64
	APIs       map[string]int64
}

// collectMentionNamesByKind собирает уникальные имена упоминаний заданного kind.
func collectMentionNamesByKind(mentions []*model.SpecCodeMention, kind string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0)
	for _, m := range mentions {
		if m == nil || m.MentionKind != kind {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(m.MentionName))
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, m.MentionName)
	}
	return result
}

// buildSpecMentionRelations строит relations (references_code) из mentions.
func buildSpecMentionRelations(mentions []*model.SpecCodeMention, lookup *specMentionLookup) []*model.Relation {
	var relations []*model.Relation
	seen := map[string]struct{}{}

	for _, m := range mentions {
		if m == nil {
			continue
		}
		var targetType string
		var targetID int64

		nameKey := strings.ToLower(m.MentionName)
		switch m.MentionKind {
		case "procedure":
			targetType = "sql_procedure"
			targetID = lookup.Procedures[nameKey]
		case "table":
			targetType = "sql_table"
			targetID = lookup.Tables[nameKey]
		case "form":
			targetType = "dfm_form"
			targetID = lookup.Forms[nameKey]
		case "smf":
			targetType = "smf_instrument"
			targetID = lookup.SMF[nameKey]
		case "method":
			targetType = "pas_method"
			targetID = lookup.Methods[nameKey]
		case "api":
			targetType = "api_contract"
			targetID = lookup.APIs[nameKey]
		}
		if targetID == 0 {
			continue // нерезолвнутое упоминание остаётся в staging
		}

		dedupKey := fmt.Sprintf("%s|%d|%s|%d|references_code|%d",
			m.SourceType, m.SourceID, targetType, targetID, m.LineNumber)
		if _, exists := seen[dedupKey]; exists {
			continue
		}
		seen[dedupKey] = struct{}{}

		relations = append(relations, &model.Relation{
			SourceType:   m.SourceType,
			SourceID:     m.SourceID,
			TargetType:   targetType,
			TargetID:     targetID,
			RelationType: "references_code",
			Confidence:   "spec_mention",
			LineNumber:   m.LineNumber,
		})
	}
	return relations
}
