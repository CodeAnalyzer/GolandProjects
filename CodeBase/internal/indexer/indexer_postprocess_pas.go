package indexer

import (
	"context"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
)

// PendingMethod метод, ожидающий привязки к классу
type PendingClass struct {
	ClassID   int64
	ClassName string
	FilePath  string
}

// PendingMethod метод, ожидающий привязки к классу
type PendingMethod struct {
	MethodID   int64
	ClassName  string
	MethodName string
	FilePath   string
}

// PendingField поле, ожидающее привязки к классу
type PendingField struct {
	FieldID   int64
	ClassName string
	FieldName string
	FilePath  string
}

func (idx *Indexer) addPendingMethod(methodID int64, className string, methodName string, filePath string) {
	idx.shared.pendingMu.Lock()
	defer idx.shared.pendingMu.Unlock()
	idx.shared.pendingMethods = append(idx.shared.pendingMethods, &PendingMethod{
		MethodID:   methodID,
		ClassName:  className,
		MethodName: methodName,
		FilePath:   filePath,
	})
}

func (idx *Indexer) addPendingClass(classID int64, className string, filePath string) {
	idx.shared.pendingMu.Lock()
	defer idx.shared.pendingMu.Unlock()
	idx.shared.pendingClasses = append(idx.shared.pendingClasses, &PendingClass{
		ClassID:   classID,
		ClassName: className,
		FilePath:  filePath,
	})
}

func (idx *Indexer) addPendingField(fieldID int64, className string, fieldName string, filePath string) {
	idx.shared.pendingMu.Lock()
	defer idx.shared.pendingMu.Unlock()
	idx.shared.pendingFields = append(idx.shared.pendingFields, &PendingField{
		FieldID:   fieldID,
		ClassName: className,
		FieldName: fieldName,
		FilePath:  filePath,
	})
}

func (idx *Indexer) postProcessPASPending(ctx context.Context, collector *statsCollector) {
	idx.shared.pendingMu.Lock()
	pendingClasses := append([]*PendingClass(nil), idx.shared.pendingClasses...)
	pendingMethods := append([]*PendingMethod(nil), idx.shared.pendingMethods...)
	pendingFields := append([]*PendingField(nil), idx.shared.pendingFields...)
	idx.shared.pendingClasses = idx.shared.pendingClasses[:0]
	idx.shared.pendingMethods = idx.shared.pendingMethods[:0]
	idx.shared.pendingFields = idx.shared.pendingFields[:0]
	idx.shared.pendingMu.Unlock()

	// Scope ветки источника: классы/методы/поля группируются по (product,
	// branch) своего файла; имена классов резолвятся в scope группы.
	classScopes := idx.loadEntityScopes(ctx, collector, "pas_classes", collectEntityIDs(pendingClasses, func(p *PendingClass) int64 { return p.ClassID }))
	methodScopes := idx.loadEntityScopes(ctx, collector, "pas_methods", collectEntityIDs(pendingMethods, func(p *PendingMethod) int64 { return p.MethodID }))
	fieldScopes := idx.loadEntityScopes(ctx, collector, "pas_fields", collectEntityIDs(pendingFields, func(p *PendingField) int64 { return p.FieldID }))
	if classScopes == nil || methodScopes == nil || fieldScopes == nil {
		return
	}

	// Batch-resolve PAS class IDs + DFM form IDs per scope.
	classIDByScope, dfmFormIDByScope := idx.resolvePASClassTargets(ctx, collector,
		pendingClasses, classScopes, pendingMethods, methodScopes, pendingFields, fieldScopes)

	// Process pending classes: link to DFM forms (batch).
	classDFMPairs := make([]store.PASUpdatePair, 0, len(pendingClasses))
	for _, pending := range pendingClasses {
		scope := classScopes[pending.ClassID]
		dfmFormID := dfmFormIDByScope[scope][strings.ToLower(strings.TrimSpace(pending.ClassName))]
		if dfmFormID == 0 {
			continue
		}
		classDFMPairs = append(classDFMPairs, store.PASUpdatePair{ID: pending.ClassID, ValueID: dfmFormID})
	}
	if len(classDFMPairs) > 0 {
		if err := idx.db.BatchUpdatePASClassDFMForm(ctx, classDFMPairs); err != nil {
			idx.logError("<post-processing>", "Error batch updating DFM form links for PAS classes: %v", err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		}
	}

	// Process pending methods: link to classes (batch).
	methodClassPairs := make([]store.PASUpdatePair, 0, len(pendingMethods))
	for _, pending := range pendingMethods {
		scope := methodScopes[pending.MethodID]
		classID := classIDByScope[scope][strings.ToLower(strings.TrimSpace(pending.ClassName))]
		if classID == 0 {
			idx.logError(pending.FilePath, "Warning: class %s not found for PAS method %s during post-processing", pending.ClassName, pending.MethodName)
			continue
		}
		methodClassPairs = append(methodClassPairs, store.PASUpdatePair{ID: pending.MethodID, ValueID: classID})
	}
	if len(methodClassPairs) > 0 {
		if err := idx.db.BatchUpdatePASMethodClass(ctx, methodClassPairs); err != nil {
			idx.logError("<post-processing>", "Error batch updating class links for PAS methods: %v", err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		}
	}

	// Process pending fields: link to classes (batch).
	fieldClassPairs := make([]store.PASUpdatePair, 0, len(pendingFields))
	for _, pending := range pendingFields {
		scope := fieldScopes[pending.FieldID]
		classID := classIDByScope[scope][strings.ToLower(strings.TrimSpace(pending.ClassName))]
		if classID == 0 {
			idx.logError(pending.FilePath, "Warning: class %s not found for PAS field %s during post-processing", pending.ClassName, pending.FieldName)
			continue
		}
		fieldClassPairs = append(fieldClassPairs, store.PASUpdatePair{ID: pending.FieldID, ValueID: classID})
	}
	if len(fieldClassPairs) > 0 {
		if err := idx.db.BatchUpdatePASFieldClass(ctx, fieldClassPairs); err != nil {
			idx.logError("<post-processing>", "Error batch updating class links for PAS fields: %v", err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		}
	}

	// DFM component linking for PAS fields.
	fieldCandidates, err := idx.db.FindPASFieldDFMLinkCandidates(ctx)
	if err != nil {
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		idx.logError("<post-processing>", "Error loading PAS field DFM link candidates: %v", err)
		return
	}

	// Group candidates by FormID for batch-resolve.
	candidatesByForm := make(map[int64][]store.PASFieldDFMLinkCandidate)
	for _, c := range fieldCandidates {
		candidatesByForm[c.DFMFormID] = append(candidatesByForm[c.DFMFormID], c)
	}

	for formID, candidates := range candidatesByForm {
		fieldNames := make([]string, 0, len(candidates))
		for _, c := range candidates {
			fieldNames = append(fieldNames, c.FieldName)
		}
		componentIDMap, err := idx.db.FindLatestDFMComponentIDsByFormAndNames(ctx, formID, fieldNames)
		if err != nil {
			idx.logError("<post-processing>", "Error batch-resolving DFM components for form %d: %v", formID, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}
		componentPairs := make([]store.PASUpdatePair, 0, len(candidates))
		for _, candidate := range candidates {
			compKey := strings.ToLower(strings.TrimSpace(candidate.FieldName))
			dfmComponentID := componentIDMap[compKey]
			if dfmComponentID == 0 {
				continue
			}
			componentPairs = append(componentPairs, store.PASUpdatePair{ID: candidate.FieldID, ValueID: dfmComponentID})
		}
		if len(componentPairs) > 0 {
			if err := idx.db.BatchUpdatePASFieldDFMComponent(ctx, componentPairs); err != nil {
				idx.logError("<post-processing>", "Error batch updating DFM component links for form %d: %v", formID, err)
				collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			}
		}
	}
}

// loadEntityScopes загружает scope файлов сущностей; при ошибке логирует и
// возвращает nil.
func (idx *Indexer) loadEntityScopes(ctx context.Context, collector *statsCollector, table string, ids []int64) map[int64]store.LookupScope {
	scopes, err := idx.db.EntityFileScopes(ctx, table, ids)
	if err != nil {
		idx.logError("<post-processing>", "Error loading %s file scopes: %v", table, err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return nil
	}
	return scopes
}

// resolvePASClassTargets резолвит id PAS-классов и DFM-форм по именам классов
// отдельно для каждой scope-группы pending-элементов. Возвращает карты
// scope → (lower(name) → id); nil при ошибке загрузки scopes.
func (idx *Indexer) resolvePASClassTargets(
	ctx context.Context, collector *statsCollector,
	pendingClasses []*PendingClass, classScopes map[int64]store.LookupScope,
	pendingMethods []*PendingMethod, methodScopes map[int64]store.LookupScope,
	pendingFields []*PendingField, fieldScopes map[int64]store.LookupScope,
) (map[store.LookupScope]map[string]int64, map[store.LookupScope]map[string]int64) {
	// Собираем уникальные имена классов по каждой scope-группе.
	namesByScope := make(map[store.LookupScope]map[string]struct{})
	addNames := func(scope store.LookupScope, className string) {
		key := strings.ToLower(strings.TrimSpace(className))
		if key == "" {
			return
		}
		if namesByScope[scope] == nil {
			namesByScope[scope] = make(map[string]struct{})
		}
		namesByScope[scope][key] = struct{}{}
	}
	for _, p := range pendingClasses {
		addNames(classScopes[p.ClassID], p.ClassName)
	}
	for _, p := range pendingMethods {
		addNames(methodScopes[p.MethodID], p.ClassName)
	}
	for _, p := range pendingFields {
		addNames(fieldScopes[p.FieldID], p.ClassName)
	}

	classIDByScope := make(map[store.LookupScope]map[string]int64, len(namesByScope))
	dfmFormIDByScope := make(map[store.LookupScope]map[string]int64, len(namesByScope))
	for scope, nameSet := range namesByScope {
		names := make([]string, 0, len(nameSet))
		for name := range nameSet {
			names = append(names, name)
		}
		classIDMap, err := idx.db.FindLatestPASClassIDsByNames(ctx, names, scope)
		if err != nil {
			idx.logError("<post-processing>", "Error batch-resolving PAS class IDs (scope %+v): %v", scope, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}
		dfmFormIDMap, err := idx.db.FindLatestDFMFormIDsByClassNames(ctx, names, scope)
		if err != nil {
			idx.logError("<post-processing>", "Error batch-resolving DFM form IDs (scope %+v): %v", scope, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}
		classIDByScope[scope] = classIDMap
		dfmFormIDByScope[scope] = dfmFormIDMap
	}
	return classIDByScope, dfmFormIDByScope
}
