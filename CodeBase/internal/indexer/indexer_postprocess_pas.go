package indexer

import (
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
	idx.pendingMu.Lock()
	defer idx.pendingMu.Unlock()
	idx.pendingMethods = append(idx.pendingMethods, &PendingMethod{
		MethodID:   methodID,
		ClassName:  className,
		MethodName: methodName,
		FilePath:   filePath,
	})
}

func (idx *Indexer) addPendingClass(classID int64, className string, filePath string) {
	idx.pendingMu.Lock()
	defer idx.pendingMu.Unlock()
	idx.pendingClasses = append(idx.pendingClasses, &PendingClass{
		ClassID:   classID,
		ClassName: className,
		FilePath:  filePath,
	})
}

func (idx *Indexer) addPendingField(fieldID int64, className string, fieldName string, filePath string) {
	idx.pendingMu.Lock()
	defer idx.pendingMu.Unlock()
	idx.pendingFields = append(idx.pendingFields, &PendingField{
		FieldID:   fieldID,
		ClassName: className,
		FieldName: fieldName,
		FilePath:  filePath,
	})
}

func (idx *Indexer) postProcessPASPending(collector *statsCollector) {
	idx.pendingMu.Lock()
	pendingClasses := append([]*PendingClass(nil), idx.pendingClasses...)
	pendingMethods := append([]*PendingMethod(nil), idx.pendingMethods...)
	pendingFields := append([]*PendingField(nil), idx.pendingFields...)
	idx.pendingClasses = idx.pendingClasses[:0]
	idx.pendingMethods = idx.pendingMethods[:0]
	idx.pendingFields = idx.pendingFields[:0]
	idx.pendingMu.Unlock()

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
	classIDMap, err := idx.db.FindLatestPASClassIDsByNames(classNames)
	if err != nil {
		idx.logError("<post-processing>", "Error batch-resolving PAS class IDs: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	// Batch-resolve DFM form IDs by class names.
	dfmFormIDMap, err := idx.db.FindLatestDFMFormIDsByClassNames(classNames)
	if err != nil {
		idx.logError("<post-processing>", "Error batch-resolving DFM form IDs: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	// Process pending classes: link to DFM forms.
	for _, pending := range pendingClasses {
		classKey := strings.ToLower(strings.TrimSpace(pending.ClassName))
		dfmFormID := dfmFormIDMap[classKey]
		if dfmFormID == 0 {
			continue
		}
		if err := idx.db.UpdatePASClassDFMForm(pending.ClassID, dfmFormID); err != nil {
			idx.logError(pending.FilePath, "Error updating DFM form link for PAS class %s: %v", pending.ClassName, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		}
	}

	// Process pending methods: link to classes.
	for _, pending := range pendingMethods {
		classKey := strings.ToLower(strings.TrimSpace(pending.ClassName))
		classID := classIDMap[classKey]
		if classID == 0 {
			idx.logError(pending.FilePath, "Warning: class %s not found for PAS method %s during post-processing", pending.ClassName, pending.MethodName)
			continue
		}
		if err := idx.db.UpdatePASMethodClass(pending.MethodID, classID); err != nil {
			idx.logError(pending.FilePath, "Error updating class for PAS method %s: %v", pending.MethodName, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		}
	}

	// Process pending fields: link to classes.
	for _, pending := range pendingFields {
		classKey := strings.ToLower(strings.TrimSpace(pending.ClassName))
		classID := classIDMap[classKey]
		if classID == 0 {
			idx.logError(pending.FilePath, "Warning: class %s not found for PAS field %s during post-processing", pending.ClassName, pending.FieldName)
			continue
		}
		if err := idx.db.UpdatePASFieldClass(pending.FieldID, classID); err != nil {
			idx.logError(pending.FilePath, "Error updating class for PAS field %s: %v", pending.FieldName, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		}
	}

	// DFM component linking for PAS fields.
	fieldCandidates, err := idx.db.FindPASFieldDFMLinkCandidates()
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
		componentIDMap, err := idx.db.FindLatestDFMComponentIDsByFormAndNames(formID, fieldNames)
		if err != nil {
			idx.logError("<post-processing>", "Error batch-resolving DFM components for form %d: %v", formID, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}
		for _, candidate := range candidates {
			compKey := strings.ToLower(strings.TrimSpace(candidate.FieldName))
			dfmComponentID := componentIDMap[compKey]
			if dfmComponentID == 0 {
				continue
			}
			if err := idx.db.UpdatePASFieldDFMComponent(candidate.FieldID, dfmComponentID); err != nil {
				idx.logError("<post-processing>", "Error updating DFM component link for PAS field %s: %v", candidate.FieldName, err)
				collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			}
		}
	}
}
