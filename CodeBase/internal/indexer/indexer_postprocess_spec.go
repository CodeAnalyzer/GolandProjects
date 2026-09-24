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
	reportNames := collectMentionNamesByKind(mentions, "report")
	eventNames := collectMentionNamesByKind(mentions, "event")
	apiTableNames := collectMentionNamesByKind(mentions, "api_table")
	unknownNames := collectMentionNamesByKind(mentions, "unknown")

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
		// Фолбэк .pas-упоминаний: имена-промахи (юниты форм, не методы) ищем в dfm_forms
		missedMethods := make([]string, 0)
		for _, name := range methodNames {
			if lookup.Methods[strings.ToLower(name)] == 0 {
				missedMethods = append(missedMethods, name)
			}
		}
		if len(missedMethods) > 0 {
			formIDs, err := idx.db.FindDFMFormIDsByNames(ctx, missedMethods)
			if err != nil {
				idx.logError("<post-processing>", "Error resolving method fallback form names for spec mentions: %v", err)
			} else {
				lookup.MethodForms = formIDs
			}
		}
	}
	if len(apiNames) > 0 || len(eventNames) > 0 {
		apiEventNames := append(append([]string{}, apiNames...), eventNames...)
		ids, err := idx.db.FindAPIContractIDsByNames(ctx, apiEventNames)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving API names for spec mentions: %v", err)
		}
		lookup.APIs = ids
	}
	if len(reportNames) > 0 {
		ids, err := idx.db.FindReportFormIDsByNames(ctx, reportNames)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving report names for spec mentions: %v", err)
		}
		lookup.Reports = ids
	}
	if len(apiTableNames) > 0 {
		ids, err := idx.db.FindAPIContractIDsByTableNames(ctx, apiTableNames)
		if err != nil {
			idx.logError("<post-processing>", "Error resolving API table names for spec mentions: %v", err)
		}
		lookup.APITables = ids
	}
	if len(unknownNames) > 0 {
		// Second-chance: unknown-имена, совпавшие с процедурами (строчные rpt_*_proc)
		ids, err := idx.db.FindLatestSQLProcedureIDsByNames(ctx, unknownNames)
		if err != nil {
			idx.logError("<post-processing>", "Error second-chance resolving unknown names for spec mentions: %v", err)
		}
		lookup.UnknownProcs = ids
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
	// Reports — report_forms по lower(report_name) (kind report).
	Reports map[string]int64
	// APITables — контрактные таблицы: lower(table_name) → id контрактов-владельцев.
	APITables map[string][]int64
	// MethodForms — фолбэк .pas-упоминаний: lower(name) → dfm_forms.id.
	MethodForms map[string]int64
	// UnknownProcs — second-chance: unknown-имена → sql_procedures.id.
	UnknownProcs map[string]int64
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

	appendRelation := func(m *model.SpecCodeMention, targetType string, targetID int64) {
		if m == nil || targetID == 0 {
			return // нерезолвнутое упоминание остаётся в staging
		}
		dedupKey := fmt.Sprintf("%s|%d|%s|%d|references_code|%d",
			m.SourceType, m.SourceID, targetType, targetID, m.LineNumber)
		if _, exists := seen[dedupKey]; exists {
			return
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

	for _, m := range mentions {
		if m == nil {
			continue
		}
		nameKey := strings.ToLower(m.MentionName)
		switch m.MentionKind {
		case "procedure":
			appendRelation(m, "sql_procedure", lookup.Procedures[nameKey])
		case "table":
			appendRelation(m, "sql_table", lookup.Tables[nameKey])
		case "form":
			appendRelation(m, "dfm_form", lookup.Forms[nameKey])
		case "smf":
			appendRelation(m, "smf_instrument", lookup.SMF[nameKey])
		case "method":
			if id := lookup.Methods[nameKey]; id != 0 {
				appendRelation(m, "pas_method", id)
				continue
			}
			// Фолбэк: упоминание формы через .pas-путь (имя юнита, не метода)
			appendRelation(m, "dfm_form", lookup.MethodForms[nameKey])
		case "api", "event":
			appendRelation(m, "api_contract", lookup.APIs[nameKey])
		case "report":
			appendRelation(m, "report_form", lookup.Reports[nameKey])
		case "api_table":
			// Контрактная таблица → все DISTINCT контракты-владельцы
			for _, contractID := range lookup.APITables[nameKey] {
				appendRelation(m, "api_contract", contractID)
			}
		case "unknown":
			// Second-chance: имя совпало с процедурой (строчные rpt_*_proc и т.п.)
			appendRelation(m, "sql_procedure", lookup.UnknownProcs[nameKey])
		}
	}
	return relations
}
