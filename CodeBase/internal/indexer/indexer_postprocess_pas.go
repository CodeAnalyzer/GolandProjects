package indexer

import (
	dbsql "database/sql"

	"github.com/codebase/internal/model"
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

	for _, pending := range pendingClasses {
		dfmFormID, err := idx.db.FindLatestDFMFormIDByClassName(pending.ClassName)
		if err != nil {
			if err != dbsql.ErrNoRows {
				idx.logError(pending.FilePath, "Error post-processing PAS class %s for DFM form link: %v", pending.ClassName, err)
				collector.Add(func(stats *model.ScanStats) {
					stats.Errors++
				})
			}
			continue
		}

		if err := idx.db.UpdatePASClassDFMForm(pending.ClassID, dfmFormID); err != nil {
			idx.logError(pending.FilePath, "Error updating DFM form link for PAS class %s: %v", pending.ClassName, err)
			collector.Add(func(stats *model.ScanStats) {
				stats.Errors++
			})
		}
	}

	for _, pending := range pendingMethods {
		classID, err := idx.db.FindLatestPASClassIDByName(pending.ClassName)
		if err != nil {
			if err != dbsql.ErrNoRows {
				idx.logError(pending.FilePath, "Error post-processing PAS method %s for class %s: %v", pending.MethodName, pending.ClassName, err)
				collector.Add(func(stats *model.ScanStats) {
					stats.Errors++
				})
				continue
			}
			idx.logError(pending.FilePath, "Warning: class %s not found for PAS method %s during post-processing", pending.ClassName, pending.MethodName)
			continue
		}

		if err := idx.db.UpdatePASMethodClass(pending.MethodID, classID); err != nil {
			idx.logError(pending.FilePath, "Error updating class for PAS method %s: %v", pending.MethodName, err)
			collector.Add(func(stats *model.ScanStats) {
				stats.Errors++
			})
		}
	}

	for _, pending := range pendingFields {
		classID, err := idx.db.FindLatestPASClassIDByName(pending.ClassName)
		if err != nil {
			if err != dbsql.ErrNoRows {
				idx.logError(pending.FilePath, "Error post-processing PAS field %s for class %s: %v", pending.FieldName, pending.ClassName, err)
				collector.Add(func(stats *model.ScanStats) {
					stats.Errors++
				})
				continue
			}
			idx.logError(pending.FilePath, "Warning: class %s not found for PAS field %s during post-processing", pending.ClassName, pending.FieldName)
			continue
		}

		if err := idx.db.UpdatePASFieldClass(pending.FieldID, classID); err != nil {
			idx.logError(pending.FilePath, "Error updating class for PAS field %s: %v", pending.FieldName, err)
			collector.Add(func(stats *model.ScanStats) {
				stats.Errors++
			})
		}
	}

	fieldCandidates, err := idx.db.FindPASFieldDFMLinkCandidates()
	if err != nil {
		collector.Add(func(stats *model.ScanStats) {
			stats.Errors++
		})
		idx.logError("<post-processing>", "Error loading PAS field DFM link candidates: %v", err)
		return
	}

	for _, candidate := range fieldCandidates {
		dfmComponentID, err := idx.db.FindLatestDFMComponentIDByFormAndName(candidate.DFMFormID, candidate.FieldName)
		if err != nil {
			if err != dbsql.ErrNoRows {
				idx.logError("<post-processing>", "Error resolving DFM component for PAS field %s: %v", candidate.FieldName, err)
				collector.Add(func(stats *model.ScanStats) {
					stats.Errors++
				})
			}
			continue
		}

		if err := idx.db.UpdatePASFieldDFMComponent(candidate.FieldID, dfmComponentID); err != nil {
			idx.logError("<post-processing>", "Error updating DFM component link for PAS field %s: %v", candidate.FieldName, err)
			collector.Add(func(stats *model.ScanStats) {
				stats.Errors++
			})
		}
	}
}
