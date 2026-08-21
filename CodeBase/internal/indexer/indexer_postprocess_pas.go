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

	// Collect unique class names from all pending items for batch-resolve.
	classNameSet := make(map[string]struct{})
	for _, p := range pendingClasses {
		classNameSet[strings.ToLower(strings.TrimSpace(p.ClassName))] = struct{}{}
	}
	for _, p := range pendingMethods {
		classNameSet[strings.ToLower(strings.TrimSpace(p.ClassName))] = struct{}{}
	}
	for _, p := range pendingFields {
		classNameSet[strings.ToLower(strings.TrimSpace(p.ClassName))] = struct{}{}
	}
	classNames := make([]string, 0, len(classNameSet))
	for name := range classNameSet {
		if name != "" {
			classNames = append(classNames, name)
		}
	}

	// Batch-resolve PAS class IDs.
	classIDMap, err := idx.db.FindLatestPASClassIDsByNames(ctx, classNames)
	if err != nil {
		idx.logError("<post-processing>", "Error batch-resolving PAS class IDs: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	// Batch-resolve DFM form IDs by class names.
	dfmFormIDMap, err := idx.db.FindLatestDFMFormIDsByClassNames(ctx, classNames)
	if err != nil {
		idx.logError("<post-processing>", "Error batch-resolving DFM form IDs: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	// Process pending classes: link to DFM forms (batch).
	classDFMPairs := make([]store.PASUpdatePair, 0, len(pendingClasses))
	for _, pending := range pendingClasses {
		classKey := strings.ToLower(strings.TrimSpace(pending.ClassName))
		dfmFormID := dfmFormIDMap[classKey]
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
		classKey := strings.ToLower(strings.TrimSpace(pending.ClassName))
		classID := classIDMap[classKey]
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
		classKey := strings.ToLower(strings.TrimSpace(pending.ClassName))
		classID := classIDMap[classKey]
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
