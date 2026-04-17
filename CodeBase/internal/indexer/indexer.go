package indexer

import (
	"crypto/sha256"
	dbsql "database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/fswalk"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/parser/apimacro"
	"github.com/codebase/internal/parser/dfm"
	"github.com/codebase/internal/parser/dsxml"
	"github.com/codebase/internal/parser/h"
	jsparser "github.com/codebase/internal/parser/js"
	pasparser "github.com/codebase/internal/parser/pas"
	rptparser "github.com/codebase/internal/parser/rpt"
	smfparser "github.com/codebase/internal/parser/smf"
	sqlparser "github.com/codebase/internal/parser/sql"
	tprparser "github.com/codebase/internal/parser/tpr"
	"github.com/codebase/internal/store"
)

// Indexer индексатор файлов
type Indexer struct {
	db          *store.DB
	config      *config.Config
	errorLogger *log.Logger
	pendingMu   sync.Mutex
	// Для пост-обработки методов и полей с отсутствующими классами
	pendingClasses []*PendingClass
	pendingMethods []*PendingMethod
	pendingFields  []*PendingField
}

type statsCollector struct {
	mu    sync.RWMutex
	stats model.ScanStats
}

type indexedFileJob struct {
	file   fswalk.FileInfo
	fileID int64
}

func (c *statsCollector) Add(update func(*model.ScanStats)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update(&c.stats)
}

func (c *statsCollector) Snapshot() model.ScanStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func mergeScanStats(dst *model.ScanStats, src *model.ScanStats) {
	dst.FilesScanned += src.FilesScanned
	dst.FilesIndexed += src.FilesIndexed
	dst.FilesUpdated += src.FilesUpdated
	dst.FilesAdded += src.FilesAdded
	dst.FilesDeleted += src.FilesDeleted
	dst.SQLFiles += src.SQLFiles
	dst.PASFiles += src.PASFiles
	dst.JSFiles += src.JSFiles
	dst.HFiles += src.HFiles
	dst.DFMFiles += src.DFMFiles
	dst.SMFFiles += src.SMFFiles
	dst.TPRFiles += src.TPRFiles
	dst.RPTFiles += src.RPTFiles
	dst.XMLFiles += src.XMLFiles
	dst.APIContracts += src.APIContracts
	dst.APIParams += src.APIParams
	dst.APITables += src.APITables
	dst.APITableFields += src.APITableFields
	dst.Procedures += src.Procedures
	dst.Tables += src.Tables
	dst.Columns += src.Columns
	dst.Units += src.Units
	dst.Classes += src.Classes
	dst.Methods += src.Methods
	dst.PASFields += src.PASFields
	dst.JSFunctions += src.JSFunctions
	dst.SMFInstruments += src.SMFInstruments
	dst.Forms += src.Forms
	dst.ReportFields += src.ReportFields
	dst.ReportParams += src.ReportParams
	dst.VBFunctions += src.VBFunctions
	dst.QueryFragments += src.QueryFragments
	dst.Relations += src.Relations
	dst.Errors += src.Errors
}

func normalizeParallel(parallel int) int {
	if parallel <= 0 {
		return 1
	}
	return parallel
}

func (idx *Indexer) processFilesWorkerPool(parallel int, jobs <-chan indexedFileJob, collector *statsCollector) {
	workerCount := normalizeParallel(parallel)
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				localStats := &model.ScanStats{}
				if err := idx.processFile(job.file, job.fileID, localStats); err != nil {
					idx.logError(job.file.Path, "Error processing file: %v", err)
					localStats.Errors++
				}
				collector.Add(func(stats *model.ScanStats) {
					mergeScanStats(stats, localStats)
				})
			}
		}()
	}

	wg.Wait()
}

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

// New создаёт новый индексатор
func New(db *store.DB, cfg *config.Config) *Indexer {
	// Создаем файл для логирования ошибок
	errorLogName := fmt.Sprintf("indexer_errors_%s.log", time.Now().Format("20060102_150405"))
	errorFile, err := os.OpenFile(errorLogName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Printf("Failed to create error log file: %v", err)
		// Если не удалось создать файл, используем стандартный лог
		return &Indexer{
			db:             db,
			config:         cfg,
			errorLogger:    log.New(os.Stderr, "ERROR: ", log.LstdFlags),
			pendingClasses: make([]*PendingClass, 0),
			pendingMethods: make([]*PendingMethod, 0),
			pendingFields:  make([]*PendingField, 0),
		}
	}

	return &Indexer{
		db:             db,
		config:         cfg,
		errorLogger:    log.New(errorFile, "", log.LstdFlags),
		pendingClasses: make([]*PendingClass, 0),
		pendingMethods: make([]*PendingMethod, 0),
		pendingFields:  make([]*PendingField, 0),
	}
}

// logError логирует ошибку с указанием файла и увеличивает счетчик ошибок
func (idx *Indexer) logError(filePath string, format string, args ...interface{}) {
	// Добавляем путь файла к сообщению об ошибке
	message := fmt.Sprintf("File: %s - %s", filePath, fmt.Sprintf(format, args...))
	idx.errorLogger.Print(message)
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

var execProcNameRe = regexp.MustCompile(`(?i)\bexec\s+([A-Za-z_][A-Za-z0-9_]*)`)

func mapTableRelationType(context string) string {
	switch strings.ToLower(strings.TrimSpace(context)) {
	case "select":
		return "selects_from"
	case "insert":
		return "inserts_into"
	case "update":
		return "updates"
	case "delete":
		return "deletes_from"
	default:
		return "references_table"
	}
}

func computeQueryHash(query string) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(query)))
	return hex.EncodeToString(h[:])
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, value)
	}
	return result
}

func extractProcedureCallsFromQuery(queryText string) []string {
	matches := execProcNameRe.FindAllStringSubmatch(queryText, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		result = append(result, match[1])
	}
	return uniqueStrings(result)
}

func mapQueryFragmentParentRelationType(parentType string, fragment *model.QueryFragment) string {
	switch parentType {
	case "pas_method":
		if fragment != nil && strings.EqualFold(strings.TrimSpace(fragment.Context), "method") {
			return "builds_query"
		}
		return "executes_query"
	case "sql_procedure":
		return "executes_query"
	case "js_function", "smf_instrument", "dfm_form":
		return "executes_query"
	case "report_form", "vb_function":
		return "executes_query"
	default:
		return ""
	}
}

func (idx *Indexer) enrichQueryFragmentsWithSQL(fileID int64, fragments []*model.QueryFragment) ([]*model.SQLTable, []*model.SQLColumn, error) {
	parser := sqlparser.NewParser()
	tablesBatch := make([]*model.SQLTable, 0)
	columnsBatch := make([]*model.SQLColumn, 0)

	for _, fragment := range fragments {
		if fragment == nil || strings.TrimSpace(fragment.QueryText) == "" {
			continue
		}
		parsed, err := parser.ParseContent(fragment.QueryText)
		if err != nil {
			return nil, nil, err
		}
		for _, table := range parsed.Tables {
			if table == nil {
				continue
			}
			table.FileID = fileID
			if table.LineNumber <= 0 {
				table.LineNumber = fragment.LineNumber
			} else {
				table.LineNumber += fragment.LineNumber - 1
			}
			tablesBatch = append(tablesBatch, table)
			fragment.TablesReferenced = append(fragment.TablesReferenced, table.TableName)
		}
		for _, column := range parsed.Columns {
			if column == nil {
				continue
			}
			column.FileID = fileID
			if column.LineNumber <= 0 {
				column.LineNumber = fragment.LineNumber
			} else {
				column.LineNumber += fragment.LineNumber - 1
			}
			columnsBatch = append(columnsBatch, column)
		}
		fragment.TablesReferenced = uniqueStrings(fragment.TablesReferenced)
	}

	return tablesBatch, columnsBatch, nil
}

func (idx *Indexer) saveRelations(relations []*model.Relation, path string, stats *model.ScanStats) error {
	if len(relations) == 0 {
		return nil
	}
	if err := idx.db.BatchInsertRelations(relations, idx.config.Indexer.BatchSize); err != nil {
		idx.logError(path, "Error batch inserting relations: %v", err)
		stats.Errors += len(relations)
		return err
	}
	stats.Relations += len(relations)
	return nil
}

func (idx *Indexer) buildSQLProcedureRelations(fileID int64, procedures []*model.SQLProcedure, tables []*model.SQLTable, calls []*model.SQLProcedureCall) ([]*model.Relation, error) {
	procedureIDs, err := idx.db.FindSQLProcedureIDsByFile(fileID)
	if err != nil {
		return nil, err
	}
	tableIDs, err := idx.db.FindSQLTableIDsByFileAndLine(fileID)
	if err != nil {
		return nil, err
	}

	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})

	for _, call := range calls {
		if call == nil {
			continue
		}
		sourceID := procedureIDs[strings.ToLower(strings.TrimSpace(call.CallerName))]
		if sourceID == 0 {
			continue
		}
		targetID, err := idx.db.FindLatestSQLProcedureIDByName(call.CalleeName)
		if err != nil {
			if err == dbsql.ErrNoRows {
				continue
			}
			return nil, err
		}
		key := fmt.Sprintf("sql_procedure|%d|sql_procedure|%d|calls_procedure|%d", sourceID, targetID, call.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "sql_procedure",
			SourceID:     sourceID,
			TargetType:   "sql_procedure",
			TargetID:     targetID,
			RelationType: "calls_procedure",
			Confidence:   "regex",
			LineNumber:   call.LineNumber,
		})
	}

	for _, proc := range procedures {
		if proc == nil {
			continue
		}
		sourceID := procedureIDs[strings.ToLower(strings.TrimSpace(proc.ProcName))]
		if sourceID == 0 {
			continue
		}
		for _, table := range tables {
			if table == nil {
				continue
			}
			if table.LineNumber < proc.LineStart || (proc.LineEnd > 0 && table.LineNumber > proc.LineEnd) {
				continue
			}
			targetID := tableIDs[store.BuildSQLTableLookupKey(table.TableName, table.Context, table.LineNumber)]
			if targetID == 0 {
				continue
			}
			relationType := mapTableRelationType(table.Context)
			key := fmt.Sprintf("sql_procedure|%d|sql_table|%d|%s|%d", sourceID, targetID, relationType, table.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "sql_procedure",
				SourceID:     sourceID,
				TargetType:   "sql_table",
				TargetID:     targetID,
				RelationType: relationType,
				Confidence:   "regex",
				LineNumber:   table.LineNumber,
			})
		}
	}

	return relations, nil
}

func (idx *Indexer) buildReportStructureRelations(reportFormID int64, fields []*model.ReportField, params []*model.ReportParam) ([]*model.Relation, error) {
	relations := make([]*model.Relation, 0, len(fields)+len(params))
	seen := make(map[string]struct{})

	fieldIDs, err := idx.db.FindReportFieldIDsByForm(reportFormID)
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		if field == nil {
			continue
		}
		targetID := fieldIDs[store.BuildReportFieldLookupKey(field.FieldName, field.LineNumber)]
		if targetID == 0 {
			continue
		}
		key := fmt.Sprintf("report_form|%d|report_field|%d|has_field|%d", reportFormID, targetID, field.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "report_form",
			SourceID:     reportFormID,
			TargetType:   "report_field",
			TargetID:     targetID,
			RelationType: "has_field",
			Confidence:   "regex",
			LineNumber:   field.LineNumber,
		})
	}

	paramIDs, err := idx.db.FindReportParamIDsByForm(reportFormID)
	if err != nil {
		return nil, err
	}
	for _, param := range params {
		if param == nil {
			continue
		}
		targetID := paramIDs[store.BuildReportParamLookupKey(param.ParamName, param.LineNumber)]
		if targetID == 0 {
			continue
		}
		key := fmt.Sprintf("report_form|%d|report_param|%d|has_param|%d", reportFormID, targetID, param.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "report_form",
			SourceID:     reportFormID,
			TargetType:   "report_param",
			TargetID:     targetID,
			RelationType: "has_param",
			Confidence:   "regex",
			LineNumber:   param.LineNumber,
		})
	}

	return relations, nil
}

func extractReportParamRefs(text string, params []*model.ReportParam) []string {
	if strings.TrimSpace(text) == "" || len(params) == 0 {
		return nil
	}
	textLower := strings.ToLower(text)
	result := make([]string, 0)
	seen := make(map[string]struct{})
	for _, param := range params {
		if param == nil {
			continue
		}
		name := strings.TrimSpace(param.ParamName)
		if name == "" {
			continue
		}
		nameLower := strings.ToLower(name)
		patterns := []string{
			"%" + nameLower,
			":" + nameLower,
			"@" + nameLower,
			"[" + nameLower + "]",
			nameLower,
		}
		for _, p := range patterns {
			if strings.Contains(textLower, p) {
				if _, exists := seen[nameLower]; !exists {
					seen[nameLower] = struct{}{}
					result = append(result, name)
				}
				break
			}
		}
	}
	return result
}

func (idx *Indexer) buildReportParamUsageRelations(fileID int64, reportFormID int64, fragments []*model.QueryFragment, params []*model.ReportParam) ([]*model.Relation, error) {
	fragmentIDs, err := idx.db.FindQueryFragmentIDsByFileAndHash(fileID)
	if err != nil {
		return nil, err
	}
	paramIDs, err := idx.db.FindReportParamIDsByForm(reportFormID)
	if err != nil {
		return nil, err
	}

	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})
	for _, fragment := range fragments {
		if fragment == nil {
			continue
		}
		fragmentID := fragmentIDs[store.BuildQueryFragmentLookupKey(fragment.QueryHash, fragment.Context, fragment.LineNumber)]
		if fragmentID == 0 {
			continue
		}
		for _, paramName := range extractReportParamRefs(fragment.QueryText, params) {
			param := findReportParamByName(params, paramName)
			if param == nil {
				continue
			}
			targetID := paramIDs[store.BuildReportParamLookupKey(param.ParamName, param.LineNumber)]
			if targetID == 0 {
				continue
			}
			key := fmt.Sprintf("query_fragment|%d|report_param|%d|uses_param|%d", fragmentID, targetID, fragment.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "query_fragment",
				SourceID:     fragmentID,
				TargetType:   "report_param",
				TargetID:     targetID,
				RelationType: "uses_param",
				Confidence:   "regex",
				LineNumber:   fragment.LineNumber,
			})
		}
	}
	return relations, nil
}

func findReportParamByName(params []*model.ReportParam, name string) *model.ReportParam {
	for _, param := range params {
		if param != nil && strings.EqualFold(strings.TrimSpace(param.ParamName), strings.TrimSpace(name)) {
			return param
		}
	}
	return nil
}

func (idx *Indexer) buildVBFunctionQueryRelations(fileID int64, reportFormID int64, functions []*model.VBFunction, fragments []*model.QueryFragment) ([]*model.Relation, error) {
	fragmentIDs, err := idx.db.FindQueryFragmentIDsByFileAndHash(fileID)
	if err != nil {
		return nil, err
	}
	functionIDs, err := idx.db.FindVBFunctionIDsByForm(reportFormID)
	if err != nil {
		return nil, err
	}

	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})
	for _, fn := range functions {
		if fn == nil {
			continue
		}
		fnID := functionIDs[store.BuildVBFunctionLookupKey(fn.FunctionName, fn.LineStart)]
		if fnID == 0 {
			continue
		}
		bodyLower := strings.ToLower(fn.BodyText)
		for _, fragment := range fragments {
			if fragment == nil {
				continue
			}
			fragmentID := fragmentIDs[store.BuildQueryFragmentLookupKey(fragment.QueryHash, fragment.Context, fragment.LineNumber)]
			if fragmentID == 0 {
				continue
			}
			componentName := strings.ToLower(strings.TrimSpace(fragment.ComponentName))
			if componentName == "" || !strings.Contains(bodyLower, componentName) {
				continue
			}
			key := fmt.Sprintf("vb_function|%d|query_fragment|%d|executes_query|%d", fnID, fragmentID, fragment.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "vb_function",
				SourceID:     fnID,
				TargetType:   "query_fragment",
				TargetID:     fragmentID,
				RelationType: "executes_query",
				Confidence:   "regex",
				LineNumber:   fragment.LineNumber,
			})
		}
	}
	return relations, nil
}

func (idx *Indexer) buildQueryFragmentRelations(fileID int64, fragments []*model.QueryFragment) ([]*model.Relation, error) {
	fragmentIDs, err := idx.db.FindQueryFragmentIDsByFileAndHash(fileID)
	if err != nil {
		return nil, err
	}
	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})

	for _, fragment := range fragments {
		if fragment == nil {
			continue
		}
		fragmentID := fragmentIDs[store.BuildQueryFragmentLookupKey(fragment.QueryHash, fragment.Context, fragment.LineNumber)]
		if fragmentID == 0 {
			continue
		}
		parentRelationType := mapQueryFragmentParentRelationType(fragment.ParentType, fragment)
		if fragment.ParentID > 0 && parentRelationType != "" {
			key := fmt.Sprintf("%s|%d|query_fragment|%d|%s|%d", fragment.ParentType, fragment.ParentID, fragmentID, parentRelationType, fragment.LineNumber)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				relations = append(relations, &model.Relation{
					SourceType:   fragment.ParentType,
					SourceID:     fragment.ParentID,
					TargetType:   "query_fragment",
					TargetID:     fragmentID,
					RelationType: parentRelationType,
					Confidence:   "regex",
					LineNumber:   fragment.LineNumber,
				})
			}
		}
		for _, tableName := range uniqueStrings(fragment.TablesReferenced) {
			targetID, err := idx.db.FindLatestSQLTableIDByName(tableName)
			if err != nil {
				if err == dbsql.ErrNoRows {
					continue
				}
				return nil, err
			}
			key := fmt.Sprintf("query_fragment|%d|sql_table|%d|references_table|%d", fragmentID, targetID, fragment.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "query_fragment",
				SourceID:     fragmentID,
				TargetType:   "sql_table",
				TargetID:     targetID,
				RelationType: "references_table",
				Confidence:   "regex",
				LineNumber:   fragment.LineNumber,
			})
		}
		for _, procName := range extractProcedureCallsFromQuery(fragment.QueryText) {
			targetID, err := idx.db.FindLatestSQLProcedureIDByName(procName)
			if err != nil {
				if err == dbsql.ErrNoRows {
					continue
				}
				return nil, err
			}
			key := fmt.Sprintf("query_fragment|%d|sql_procedure|%d|calls_procedure|%d", fragmentID, targetID, fragment.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "query_fragment",
				SourceID:     fragmentID,
				TargetType:   "sql_procedure",
				TargetID:     targetID,
				RelationType: "calls_procedure",
				Confidence:   "regex",
				LineNumber:   fragment.LineNumber,
			})
		}
	}

	return relations, nil
}

// parseSQLFile парсит SQL-файл с использованием batch-вставки
func (idx *Indexer) parseSQLFile(path string, fileID int64, stats *model.ScanStats) error {
	parser := sqlparser.NewParser()
	result, err := parser.ParseFile(path)
	if err != nil {
		return fmt.Errorf("failed to parse SQL file: %w", err)
	}

	// Готовим batch-срезы для всех сущностей
	proceduresBatch := make([]*model.SQLProcedure, 0, len(result.Procedures))
	tablesBatch := make([]*model.SQLTable, 0, len(result.Tables))
	columnsBatch := make([]*model.SQLColumn, 0, len(result.Columns))
	columnDefinitionsBatch := make([]*model.SQLColumnDefinition, 0, len(result.ColumnDefinitions))
	indexDefinitionsBatch := make([]*model.SQLIndexDefinition, 0, len(result.IndexDefinitions))
	indexDefinitionFieldsBatch := make([]*model.SQLIndexDefinitionField, 0, len(result.IndexFields))
	fragmentsBatch := make([]*model.QueryFragment, 0, len(result.Fragments))
	symbolsBatch := make([]*model.Symbol, 0, len(result.Procedures)+len(result.Tables))

	// Собираем процедуры в batch
	for _, proc := range result.Procedures {
		proc.FileID = fileID
		proceduresBatch = append(proceduresBatch, proc)

		// Готовим символ для процедуры
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: proc.ProcName,
			SymbolType: "procedure",
			EntityType: "sql",
			LineNumber: proc.LineStart,
			Signature:  proc.ProcName,
		})
	}

	// Собираем таблицы в batch
	for _, table := range result.Tables {
		table.FileID = fileID
		tablesBatch = append(tablesBatch, table)

		// Готовим символ для таблицы
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: table.TableName,
			SymbolType: "table",
			EntityType: "sql",
			LineNumber: table.LineNumber,
			SQLContext: table.Context,
			Signature:  table.TableName,
		})
	}

	// Собираем колонки в batch
	for _, column := range result.Columns {
		column.FileID = fileID
		columnsBatch = append(columnsBatch, column)
	}

	for _, columnDefinition := range result.ColumnDefinitions {
		columnDefinition.FileID = fileID
		columnDefinitionsBatch = append(columnDefinitionsBatch, columnDefinition)
	}

	for _, indexDefinition := range result.IndexDefinitions {
		indexDefinition.FileID = fileID
		indexDefinitionsBatch = append(indexDefinitionsBatch, indexDefinition)
	}

	for _, indexField := range result.IndexFields {
		indexDefinitionFieldsBatch = append(indexDefinitionFieldsBatch, indexField)
	}

	for _, fragment := range result.Fragments {
		if fragment == nil {
			continue
		}
		fragment.FileID = fileID
		fragment.QueryHash = computeQueryHash(fragment.QueryText)
		fragment.ParentType = "sql_file"
		fragment.ParentID = 0
		tablesReferenced := make([]string, 0)
		for _, table := range result.Tables {
			if table == nil {
				continue
			}
			if table.LineNumber < fragment.LineNumber {
				continue
			}
			if fragment.LineEnd > 0 && table.LineNumber > fragment.LineEnd {
				continue
			}
			tablesReferenced = append(tablesReferenced, table.TableName)
		}
		fragment.TablesReferenced = uniqueStrings(tablesReferenced)
		fragmentsBatch = append(fragmentsBatch, fragment)
	}

	// Выполняем batch-вставку процедур
	if len(proceduresBatch) > 0 {
		if err := idx.db.BatchInsertSQLProcedures(proceduresBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting procedures: %v", err)
			stats.Errors += len(proceduresBatch)
			return err
		}
		stats.Procedures += len(proceduresBatch)
	}

	// Выполняем batch-вставку таблиц
	if len(tablesBatch) > 0 {
		if err := idx.db.BatchInsertSQLTables(tablesBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting tables: %v", err)
			stats.Errors += len(tablesBatch)
			return err
		}
		stats.Tables += len(tablesBatch)
	}

	// Выполняем batch-вставку колонок
	if len(columnsBatch) > 0 {
		if err := idx.db.BatchInsertSQLColumns(columnsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting columns: %v", err)
			stats.Errors += len(columnsBatch)
			return err
		}
		stats.Columns += len(columnsBatch)
	}

	if len(columnDefinitionsBatch) > 0 {
		if err := idx.db.BatchInsertSQLColumnDefinitions(columnDefinitionsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting column definitions: %v", err)
			stats.Errors += len(columnDefinitionsBatch)
			return err
		}
	}

	if len(indexDefinitionsBatch) > 0 {
		if err := idx.db.BatchInsertSQLIndexDefinitions(indexDefinitionsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting SQL index definitions: %v", err)
			stats.Errors += len(indexDefinitionsBatch)
			return err
		}
	}

	if len(indexDefinitionFieldsBatch) > 0 {
		indexIDs, err := idx.db.FindSQLIndexDefinitionIDsByFile(fileID)
		if err != nil {
			return fmt.Errorf("failed to resolve SQL index definition ids: %w", err)
		}
		fieldsToPersist := make([]*model.SQLIndexDefinitionField, 0, len(indexDefinitionFieldsBatch))
		for _, field := range indexDefinitionFieldsBatch {
			if field == nil {
				continue
			}
			key := store.BuildSQLIndexDefinitionLookupKey(field.ParentTableName, field.ParentIndexName, field.LineNumber)
			field.TableIndexID = indexIDs[key]
			if field.TableIndexID == 0 {
				continue
			}
			fieldsToPersist = append(fieldsToPersist, field)
		}
		if len(fieldsToPersist) > 0 {
			if err := idx.db.BatchInsertSQLIndexDefinitionFields(fieldsToPersist, idx.config.Indexer.BatchSize); err != nil {
				idx.logError(path, "Error batch inserting SQL index definition fields: %v", err)
				stats.Errors += len(fieldsToPersist)
				return err
			}
		}
	}

	procedureIDs, err := idx.db.FindSQLProcedureIDsByFile(fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve SQL procedure ids for symbols: %w", err)
	}
	tableIDs, err := idx.db.FindSQLTableIDsByFileAndLine(fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve SQL table ids for symbols: %w", err)
	}
	for _, symbol := range symbolsBatch {
		switch symbol.SymbolType {
		case "procedure":
			symbol.EntityID = procedureIDs[strings.ToLower(strings.TrimSpace(symbol.SymbolName))]
		case "table":
			key := store.BuildSQLTableLookupKey(symbol.SymbolName, symbol.SQLContext, symbol.LineNumber)
			symbol.EntityID = tableIDs[key]
		}
	}
	for _, fragment := range fragmentsBatch {
		if fragment == nil {
			continue
		}
		for _, proc := range proceduresBatch {
			if proc == nil {
				continue
			}
			if fragment.LineNumber < proc.LineStart || (proc.LineEnd > 0 && fragment.LineNumber > proc.LineEnd) {
				continue
			}
			fragment.ParentType = "sql_procedure"
			fragment.ParentID = procedureIDs[strings.ToLower(strings.TrimSpace(proc.ProcName))]
			fragment.ComponentName = proc.ProcName
			fragment.ComponentType = "sql_procedure"
			break
		}
		if fragment.ParentID == 0 && strings.TrimSpace(fragment.ComponentName) == "" {
			fragment.ComponentName = "sql_script"
		}
		if fragment.ParentID == 0 && strings.TrimSpace(fragment.ComponentType) == "" {
			fragment.ComponentType = "sql_script"
		}
	}

	// Выполняем batch-вставку символов
	if len(symbolsBatch) > 0 {
		if err := idx.db.BatchInsertSymbols(symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting symbols: %v", err)
			stats.Errors += len(symbolsBatch)
			return err
		}
	}
	if len(fragmentsBatch) > 0 {
		if err := idx.db.BatchInsertQueryFragments(fragmentsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting SQL query fragments: %v", err)
			stats.Errors += len(fragmentsBatch)
			return err
		}
		stats.QueryFragments += len(fragmentsBatch)
	}

	relations, err := idx.buildSQLProcedureRelations(fileID, proceduresBatch, tablesBatch, result.Calls)
	if err != nil {
		return fmt.Errorf("failed to build SQL relations: %w", err)
	}
	macroRelations, err := idx.indexAPIMacros(path, fileID, "SQL", stats)
	if err != nil {
		return fmt.Errorf("failed to index SQL API macros: %w", err)
	}
	queryRelations, err := idx.buildQueryFragmentRelations(fileID, fragmentsBatch)
	if err != nil {
		return fmt.Errorf("failed to build SQL query relations: %w", err)
	}
	relations = append(relations, macroRelations...)
	relations = append(relations, queryRelations...)
	if err := idx.saveRelations(relations, path, stats); err != nil {
		return err
	}

	// Обрабатываем include директивы (остаются как есть)
	for _, inc := range result.Includes {
		if err := idx.saveIncludeDirective(fileID, path, inc.IncludePath, inc.LineNumber); err != nil {
			idx.logError(path, "Error saving include %s: %v", inc.IncludePath, err)
			stats.Errors++
		}
	}

	return nil
}

func (idx *Indexer) parsePASFile(path string, fileID int64, stats *model.ScanStats) error {
	parser := pasparser.NewParser()
	result, err := parser.ParseFile(path)
	if err != nil {
		return fmt.Errorf("failed to parse PAS file: %w", err)
	}

	if len(result.Units) == 0 {
		return nil
	}

	unit := result.Units[0]
	unit.FileID = fileID
	if err := idx.db.BatchInsertPASUnits([]*model.PASUnit{unit}, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS unit: %w", err)
	}
	unitIDs, err := idx.db.FindPASUnitIDsByFile(fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve PAS unit ids: %w", err)
	}
	unitID := unitIDs[store.BuildPASUnitLookupKey(unit.UnitName, unit.LineStart)]
	if unitID == 0 {
		return fmt.Errorf("failed to resolve persisted PAS unit id for %s", unit.UnitName)
	}
	stats.Units++

	classIDs := make(map[string]int64)
	methodIDs := make(map[string]int64)
	classesBatch := make([]*model.PASClass, 0, len(result.Classes))
	for _, class := range result.Classes {
		class.UnitID = unitID
		classesBatch = append(classesBatch, class)
	}
	if err := idx.db.BatchInsertPASClasses(classesBatch, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS classes: %w", err)
	}
	stats.Classes += len(classesBatch)

	classIDByLookup, err := idx.db.FindPASClassIDsByUnit(unitID)
	if err != nil {
		return fmt.Errorf("failed to resolve PAS class ids: %w", err)
	}
	for _, class := range classesBatch {
		classID := classIDByLookup[store.BuildPASClassLookupKey(class.ClassName, class.LineStart)]
		if classID == 0 {
			continue
		}
		classIDs[strings.ToLower(strings.TrimSpace(class.ClassName))] = classID
		idx.addPendingClass(classID, class.ClassName, path)
	}

	methodsBatch := make([]*model.PASMethod, 0, len(result.Methods))
	for _, method := range result.Methods {
		method.UnitID = unitID
		if method.ClassName != "" {
			method.ClassID = classIDs[strings.ToLower(strings.TrimSpace(method.ClassName))]
		}
		methodsBatch = append(methodsBatch, method)
	}
	if err := idx.db.BatchInsertPASMethods(methodsBatch, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS methods: %w", err)
	}
	stats.Methods += len(methodsBatch)

	methodIDByLookup, err := idx.db.FindPASMethodIDsByUnit(unitID)
	if err != nil {
		return fmt.Errorf("failed to resolve PAS method ids: %w", err)
	}
	for _, method := range methodsBatch {
		methodID := methodIDByLookup[store.BuildPASMethodLookupKey(method.ClassName, method.MethodName, method.LineNumber)]
		if methodID == 0 {
			continue
		}
		methodKey := strings.ToLower(strings.TrimSpace(method.ClassName)) + "|" + strings.ToLower(strings.TrimSpace(method.MethodName))
		methodIDs[methodKey] = methodID
		if method.ClassID == 0 && strings.TrimSpace(method.ClassName) != "" {
			idx.addPendingMethod(methodID, method.ClassName, method.MethodName, path)
		}
	}

	fieldsBatch := make([]*model.PASField, 0, len(result.Fields))
	for _, field := range result.Fields {
		if field.ClassName != "" {
			field.ClassID = classIDs[strings.ToLower(strings.TrimSpace(field.ClassName))]
		}
		fieldsBatch = append(fieldsBatch, field)
	}
	if err := idx.db.BatchInsertPASFields(fieldsBatch, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS fields: %w", err)
	}
	stats.PASFields += len(fieldsBatch)

	fieldIDByLookup, err := idx.db.FindPASFieldIDsByUnit(unitID)
	if err != nil {
		return fmt.Errorf("failed to resolve PAS field ids: %w", err)
	}
	for _, field := range fieldsBatch {
		fieldID := fieldIDByLookup[store.BuildPASFieldLookupKey(field.ClassName, field.FieldName, field.LineNumber)]
		if fieldID == 0 {
			continue
		}
		if field.ClassID == 0 && strings.TrimSpace(field.ClassName) != "" {
			idx.addPendingField(fieldID, field.ClassName, field.FieldName, path)
		}
	}

	for _, table := range result.Tables {
		table.FileID = fileID
	}
	if err := idx.db.BatchInsertSQLTables(result.Tables, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS SQL tables: %w", err)
	}
	stats.Tables += len(result.Tables)

	tablesByLine := make(map[int][]string)
	for _, table := range result.Tables {
		if table == nil {
			continue
		}
		if table.LineNumber <= 0 || strings.TrimSpace(table.TableName) == "" {
			continue
		}
		tablesByLine[table.LineNumber] = append(tablesByLine[table.LineNumber], table.TableName)
	}

	unitName := ""
	for _, unit := range result.Units {
		if unit == nil {
			continue
		}
		unitName = strings.TrimSpace(unit.UnitName)
		if unitName != "" {
			break
		}
	}

	fragmentsBatch := make([]*model.QueryFragment, 0, len(result.SQLFragments))
	for _, fragment := range result.SQLFragments {
		parentType := "pas_unit"
		parentID := unitID
		tablesReferenced := uniqueStrings(tablesByLine[fragment.LineNumber])
		componentName := ""
		componentType := "pas_unit"
		className := strings.TrimSpace(fragment.ClassName)
		methodName := strings.TrimSpace(fragment.MethodName)
		if methodName != "" {
			componentType = "pas_method"
			if className != "" {
				componentName = className + "." + methodName
			} else {
				componentName = methodName
			}
		} else if className != "" {
			componentType = "pas_class"
			componentName = className
		} else {
			componentName = unitName
		}
		if fragment.MethodName != "" {
			methodKey := strings.ToLower(strings.TrimSpace(fragment.ClassName)) + "|" + strings.ToLower(strings.TrimSpace(fragment.MethodName))
			if methodID := methodIDs[methodKey]; methodID > 0 {
				parentType = "pas_method"
				parentID = methodID
			}
		}
		fragmentsBatch = append(fragmentsBatch, &model.QueryFragment{
			FileID:           fileID,
			ParentType:       parentType,
			ParentID:         parentID,
			ComponentName:    componentName,
			ComponentType:    componentType,
			QueryText:        fragment.QueryText,
			QueryHash:        computeQueryHash(fragment.QueryText),
			TablesReferenced: tablesReferenced,
			Context:          fragment.Context,
			LineNumber:       fragment.LineNumber,
		})
	}
	if err := idx.db.BatchInsertQueryFragments(fragmentsBatch, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS query fragments: %w", err)
	}
	stats.QueryFragments += len(fragmentsBatch)

	relations, err := idx.buildQueryFragmentRelations(fileID, fragmentsBatch)
	if err != nil {
		return fmt.Errorf("failed to build PAS query relations: %w", err)
	}
	if err := idx.saveRelations(relations, path, stats); err != nil {
		return err
	}

	return nil
}

func (idx *Indexer) parseHFile(path string, fileID int64, stats *model.ScanStats) error {
	parser := h.NewParser()
	result, err := parser.ParseFile(path)
	if err != nil {
		return fmt.Errorf("failed to parse H file: %w", err)
	}

	definesBatch := make([]*model.HDefine, 0, len(result.Defines))
	symbolsBatch := make([]*model.Symbol, 0, len(result.Defines))

	for _, define := range result.Defines {
		define.FileID = fileID
		definesBatch = append(definesBatch, define)
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: define.DefineName,
			SymbolType: "define",
			EntityType: "h",
			LineNumber: define.LineNumber,
			Signature:  define.DefineValue,
		})
	}

	if err := idx.db.BatchInsertHDefines(definesBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	defineIDs, err := idx.db.FindHDefineIDsByFile(fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve H define ids for symbols: %w", err)
	}
	for _, symbol := range symbolsBatch {
		symbol.EntityID = defineIDs[store.BuildHDefineLookupKey(symbol.SymbolName, symbol.LineNumber)]
	}
	if err := idx.db.BatchInsertSymbols(symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}

	for _, inc := range result.Includes {
		if err := idx.saveIncludeDirective(fileID, path, inc.IncludePath, inc.LineNumber); err != nil {
			idx.logError(path, "Error saving include %s: %v", inc.IncludePath, err)
			stats.Errors++
		}
	}

	return nil
}

func (idx *Indexer) parseDFMFile(path string, fileID int64, stats *model.ScanStats) error {
	parser := dfm.NewParser()
	result, err := parser.ParseFile(path)
	if err != nil {
		return fmt.Errorf("failed to parse DFM file: %w", err)
	}

	formsBatch := make([]*model.DFMForm, 0, len(result.Forms))
	componentsBatch := make([]*model.DFMComponent, 0, len(result.Components))
	fragmentsBatch := make([]*model.QueryFragment, 0, len(result.Queries))
	tablesBatch := make([]*model.SQLTable, 0, len(result.Tables))
	symbolsBatch := make([]*model.Symbol, 0, len(result.Forms))

	for _, form := range result.Forms {
		form.FileID = fileID
		formsBatch = append(formsBatch, form)
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: form.FormName,
			SymbolType: "form",
			EntityType: "dfm",
			LineNumber: form.LineStart,
			Signature:  form.FormClass,
		})
	}

	for _, query := range result.Queries {
		tablesReferenced := make([]string, 0)
		for _, table := range result.Tables {
			if table.LineNumber == query.LineNumber {
				tablesReferenced = append(tablesReferenced, table.TableName)
			}
		}
		fragmentsBatch = append(fragmentsBatch, &model.QueryFragment{
			FileID:           fileID,
			ParentType:       "dfm_form",
			ParentID:         0,
			ComponentName:    query.ComponentName,
			ComponentType:    query.ComponentType,
			QueryText:        query.QueryText,
			QueryHash:        computeQueryHash(query.QueryText),
			TablesReferenced: tablesReferenced,
			Context:          "dfm_query",
			LineNumber:       query.LineNumber,
		})
	}

	for _, table := range result.Tables {
		table.FileID = fileID
		tablesBatch = append(tablesBatch, table)
	}

	if err := idx.db.BatchInsertDFMForms(formsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	formIDs, err := idx.db.FindDFMFormIDsByFile(fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve DFM form ids for symbols: %w", err)
	}
	for _, symbol := range symbolsBatch {
		symbol.EntityID = formIDs[store.BuildDFMFormLookupKey(symbol.SymbolName, symbol.LineNumber)]
	}
	formIDByName := make(map[string]int64)
	for _, form := range formsBatch {
		if form == nil {
			continue
		}
		formID := formIDs[store.BuildDFMFormLookupKey(form.FormName, form.LineStart)]
		if formID > 0 {
			formIDByName[strings.ToLower(strings.TrimSpace(form.FormName))] = formID
		}
	}
	for _, component := range result.Components {
		if component == nil {
			continue
		}
		component.FileID = fileID
		component.FormID = formIDByName[strings.ToLower(strings.TrimSpace(component.FormName))]
		if component.FormID == 0 {
			continue
		}
		componentsBatch = append(componentsBatch, component)
	}
	for _, fragment := range fragmentsBatch {
		if fragment == nil || fragment.ParentType != "dfm_form" || fragment.ParentID > 0 {
			continue
		}
		formID, err := idx.db.FindDFMFormIDByFileAndLine(fileID, fragment.LineNumber)
		if err != nil {
			if err == dbsql.ErrNoRows {
				continue
			}
			return fmt.Errorf("failed to resolve DFM form for query fragment: %w", err)
		}
		fragment.ParentID = formID
	}
	if err := idx.db.BatchInsertQueryFragments(fragmentsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertDFMComponents(componentsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertSQLTables(tablesBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertSymbols(symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	relations, err := idx.buildQueryFragmentRelations(fileID, fragmentsBatch)
	if err != nil {
		return fmt.Errorf("failed to build DFM query relations: %w", err)
	}
	if err := idx.saveRelations(relations, path, stats); err != nil {
		return err
	}

	stats.Forms += len(formsBatch)
	stats.QueryFragments += len(fragmentsBatch)
	stats.Tables += len(tablesBatch)
	return nil
}

func (idx *Indexer) parseJSFile(path string, fileID int64, stats *model.ScanStats) error {
	parser := jsparser.NewParser()
	result, err := parser.ParseFile(path)
	if err != nil {
		return fmt.Errorf("failed to parse JS file: %w", err)
	}

	functionsBatch := make([]*model.JSFunction, 0, len(result.Functions))
	fragmentsBatch := make([]*model.QueryFragment, 0, len(result.QueryCalls))
	symbolsBatch := make([]*model.Symbol, 0, len(result.Functions))

	for _, fn := range result.Functions {
		fn.FileID = fileID
		functionsBatch = append(functionsBatch, fn)
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: fn.FunctionName,
			SymbolType: "function",
			EntityType: "js",
			LineNumber: fn.LineStart,
			Signature:  fn.Signature,
		})
	}

	for _, query := range result.QueryCalls {
		parentType := "js_file"
		parentID := int64(0)
		functionID, err := idx.db.FindJSFunctionIDByFileAndLine(fileID, query.LineNumber)
		if err == nil {
			parentType = "js_function"
			parentID = functionID
		} else if err != dbsql.ErrNoRows {
			return fmt.Errorf("failed to resolve JS function for query fragment: %w", err)
		}
		fragmentsBatch = append(fragmentsBatch, &model.QueryFragment{
			FileID:        fileID,
			ParentType:    parentType,
			ParentID:      parentID,
			ComponentName: query.ObjectName,
			ComponentType: "js_object",
			QueryText:     query.QueryText,
			QueryHash:     computeQueryHash(query.QueryText),
			Context:       "js_query",
			LineNumber:    query.LineNumber,
		})
	}

	if err := idx.db.BatchInsertJSFunctions(functionsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	functionIDs, err := idx.db.FindJSFunctionIDsByFile(fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve JS function ids for symbols: %w", err)
	}
	for _, symbol := range symbolsBatch {
		symbol.EntityID = functionIDs[store.BuildJSFunctionLookupKey(symbol.SymbolName, symbol.LineNumber)]
	}
	if err := idx.db.BatchInsertQueryFragments(fragmentsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertSymbols(symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	relations, err := idx.buildQueryFragmentRelations(fileID, fragmentsBatch)
	if err != nil {
		return fmt.Errorf("failed to build JS query relations: %w", err)
	}
	if err := idx.saveRelations(relations, path, stats); err != nil {
		return err
	}

	stats.JSFunctions += len(functionsBatch)
	stats.QueryFragments += len(fragmentsBatch)
	return nil
}

func (idx *Indexer) parseTPRFile(path string, fileID int64, stats *model.ScanStats) error {
	parser := tprparser.NewParser()
	result, err := parser.ParseFile(path)
	if err != nil {
		return fmt.Errorf("failed to parse TPR file: %w", err)
	}

	result.Form.FileID = fileID
	if err := idx.db.BatchInsertReportForms([]*model.ReportForm{result.Form}, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	reportFormID, err := idx.db.FindReportFormIDByFileAndLine(fileID, result.Form.LineStart)
	if err != nil {
		return fmt.Errorf("failed to resolve report form id: %w", err)
	}

	for _, field := range result.Fields {
		field.ReportFormID = reportFormID
	}
	for _, param := range result.Params {
		param.ReportFormID = reportFormID
	}
	symbolsBatch := make([]*model.Symbol, 0, 1+len(result.Params))
	symbolsBatch = append(symbolsBatch, &model.Symbol{
		FileID:     fileID,
		SymbolName: result.Form.ReportName,
		SymbolType: "report_form",
		EntityType: "report",
		LineNumber: result.Form.LineStart,
		Signature:  strings.ToUpper(result.Form.ReportType),
	})
	for _, param := range result.Params {
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: param.ParamName,
			SymbolType: "report_param",
			EntityType: "report",
			LineNumber: param.LineNumber,
			Signature:  param.DataType,
		})
	}
	for _, fragment := range result.Fragments {
		fragment.FileID = fileID
		fragment.ParentType = "report_form"
		fragment.ParentID = reportFormID
		fragment.QueryHash = computeQueryHash(fragment.QueryText)
	}

	tablesBatch, columnsBatch, err := idx.enrichQueryFragmentsWithSQL(fileID, result.Fragments)
	if err != nil {
		return fmt.Errorf("failed to parse TPR embedded SQL: %w", err)
	}

	if err := idx.db.BatchInsertReportFields(result.Fields, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertReportParams(result.Params, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	paramIDs, err := idx.db.FindReportParamIDsByForm(reportFormID)
	if err != nil {
		return fmt.Errorf("failed to resolve TPR report param ids for symbols: %w", err)
	}
	for _, symbol := range symbolsBatch {
		switch symbol.SymbolType {
		case "report_form":
			symbol.EntityID = reportFormID
		case "report_param":
			symbol.EntityID = paramIDs[store.BuildReportParamLookupKey(symbol.SymbolName, symbol.LineNumber)]
		}
	}
	if err := idx.db.BatchInsertSymbols(symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertQueryFragments(result.Fragments, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertSQLTables(tablesBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertSQLColumns(columnsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	for _, inc := range result.Includes {
		if err := idx.saveIncludeDirective(fileID, path, inc.IncludePath, inc.LineNumber); err != nil {
			idx.logError(path, "Error saving include %s: %v", inc.IncludePath, err)
			stats.Errors++
		}
	}
	relations, err := idx.buildQueryFragmentRelations(fileID, result.Fragments)
	if err != nil {
		return fmt.Errorf("failed to build TPR query relations: %w", err)
	}
	if err := idx.saveRelations(relations, path, stats); err != nil {
		return err
	}
	structureRelations, err := idx.buildReportStructureRelations(reportFormID, result.Fields, result.Params)
	if err != nil {
		return fmt.Errorf("failed to build TPR report structure relations: %w", err)
	}
	if err := idx.saveRelations(structureRelations, path, stats); err != nil {
		return err
	}
	paramRelations, err := idx.buildReportParamUsageRelations(fileID, reportFormID, result.Fragments, result.Params)
	if err != nil {
		return fmt.Errorf("failed to build TPR param usage relations: %w", err)
	}
	if err := idx.saveRelations(paramRelations, path, stats); err != nil {
		return err
	}

	stats.Forms++
	stats.ReportFields += len(result.Fields)
	stats.ReportParams += len(result.Params)
	stats.QueryFragments += len(result.Fragments)
	stats.Tables += len(tablesBatch)
	stats.Columns += len(columnsBatch)
	return nil
}

func (idx *Indexer) parseRPTFile(path string, fileID int64, stats *model.ScanStats) error {
	parser := rptparser.NewParser()
	result, err := parser.ParseFile(path)
	if err != nil {
		return fmt.Errorf("failed to parse RPT file: %w", err)
	}

	result.Form.FileID = fileID
	if err := idx.db.BatchInsertReportForms([]*model.ReportForm{result.Form}, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	reportFormID, err := idx.db.FindReportFormIDByFileAndLine(fileID, result.Form.LineStart)
	if err != nil {
		return fmt.Errorf("failed to resolve report form id: %w", err)
	}

	for _, param := range result.Params {
		param.ReportFormID = reportFormID
	}
	symbolsBatch := make([]*model.Symbol, 0, 1+len(result.Params)+len(result.Functions))
	symbolsBatch = append(symbolsBatch, &model.Symbol{
		FileID:     fileID,
		SymbolName: result.Form.ReportName,
		SymbolType: "report_form",
		EntityType: "report",
		LineNumber: result.Form.LineStart,
		Signature:  strings.ToUpper(result.Form.ReportType),
	})
	for _, param := range result.Params {
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: param.ParamName,
			SymbolType: "report_param",
			EntityType: "report",
			LineNumber: param.LineNumber,
			Signature:  param.DataType,
		})
	}
	for _, fn := range result.Functions {
		fn.ReportFormID = reportFormID
		fn.BodyHash = computeQueryHash(fn.BodyText)
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: fn.FunctionName,
			SymbolType: "vb_function",
			EntityType: "report",
			LineNumber: fn.LineStart,
			Signature:  fn.Signature,
		})
	}
	for _, fragment := range result.Fragments {
		fragment.FileID = fileID
		fragment.ParentType = "report_form"
		fragment.ParentID = reportFormID
		fragment.QueryHash = computeQueryHash(fragment.QueryText)
	}

	tablesBatch, columnsBatch, err := idx.enrichQueryFragmentsWithSQL(fileID, result.Fragments)
	if err != nil {
		return fmt.Errorf("failed to parse RPT embedded SQL: %w", err)
	}

	if err := idx.db.BatchInsertReportParams(result.Params, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertVBFunctions(result.Functions, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	paramIDs, err := idx.db.FindReportParamIDsByForm(reportFormID)
	if err != nil {
		return fmt.Errorf("failed to resolve RPT report param ids for symbols: %w", err)
	}
	vbFunctionIDs, err := idx.db.FindVBFunctionIDsByForm(reportFormID)
	if err != nil {
		return fmt.Errorf("failed to resolve RPT vb function ids for symbols: %w", err)
	}
	for _, symbol := range symbolsBatch {
		switch symbol.SymbolType {
		case "report_form":
			symbol.EntityID = reportFormID
		case "report_param":
			symbol.EntityID = paramIDs[store.BuildReportParamLookupKey(symbol.SymbolName, symbol.LineNumber)]
		case "vb_function":
			symbol.EntityID = vbFunctionIDs[store.BuildVBFunctionLookupKey(symbol.SymbolName, symbol.LineNumber)]
		}
	}
	if err := idx.db.BatchInsertSymbols(symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertQueryFragments(result.Fragments, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertSQLTables(tablesBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertSQLColumns(columnsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	relations, err := idx.buildQueryFragmentRelations(fileID, result.Fragments)
	if err != nil {
		return fmt.Errorf("failed to build RPT query relations: %w", err)
	}
	if err := idx.saveRelations(relations, path, stats); err != nil {
		return err
	}
	structureRelations, err := idx.buildReportStructureRelations(reportFormID, nil, result.Params)
	if err != nil {
		return fmt.Errorf("failed to build RPT report structure relations: %w", err)
	}
	if err := idx.saveRelations(structureRelations, path, stats); err != nil {
		return err
	}
	paramRelations, err := idx.buildReportParamUsageRelations(fileID, reportFormID, result.Fragments, result.Params)
	if err != nil {
		return fmt.Errorf("failed to build RPT param usage relations: %w", err)
	}
	if err := idx.saveRelations(paramRelations, path, stats); err != nil {
		return err
	}
	vbRelations, err := idx.buildVBFunctionQueryRelations(fileID, reportFormID, result.Functions, result.Fragments)
	if err != nil {
		return fmt.Errorf("failed to build RPT vb-function query relations: %w", err)
	}
	if err := idx.saveRelations(vbRelations, path, stats); err != nil {
		return err
	}

	stats.Forms++
	stats.ReportParams += len(result.Params)
	stats.VBFunctions += len(result.Functions)
	stats.QueryFragments += len(result.Fragments)
	stats.Tables += len(tablesBatch)
	stats.Columns += len(columnsBatch)
	return nil
}

func (idx *Indexer) parseSMFFile(path string, fileID int64, stats *model.ScanStats) error {
	parser := smfparser.NewParser()
	result, err := parser.ParseFile(path)
	if err != nil {
		return fmt.Errorf("failed to parse SMF file: %w", err)
	}

	if result.Instrument != nil {
		result.Instrument.FileID = fileID
		if err := idx.db.BatchInsertSMFInstruments([]*model.SMFInstrument{result.Instrument}, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
		stats.SMFInstruments++
	}

	if strings.TrimSpace(result.PrequerySQL) != "" {
		instrumentID := int64(0)
		if result.Instrument != nil {
			instrumentID, err = idx.db.FindLatestSMFInstrumentIDByFile(fileID)
			if err != nil {
				if err != dbsql.ErrNoRows {
					return fmt.Errorf("failed to resolve SMF instrument for query fragment: %w", err)
				}
				instrumentID = 0
			}
		}
		fragment := &model.QueryFragment{
			FileID:        fileID,
			ParentType:    "smf_instrument",
			ParentID:      instrumentID,
			ComponentName: "prequery",
			ComponentType: "smf",
			QueryText:     result.PrequerySQL,
			QueryHash:     computeQueryHash(result.PrequerySQL),
			Context:       "smf_prequery",
			LineNumber:    1,
		}
		if err := idx.db.BatchInsertQueryFragments([]*model.QueryFragment{fragment}, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
		stats.QueryFragments++
		relations, err := idx.buildQueryFragmentRelations(fileID, []*model.QueryFragment{fragment})
		if err != nil {
			return fmt.Errorf("failed to build SMF query relations: %w", err)
		}
		if err := idx.saveRelations(relations, path, stats); err != nil {
			return err
		}
	}

	for _, inc := range result.Includes {
		if err := idx.saveIncludeDirective(fileID, path, inc, 1); err != nil {
			idx.logError(path, "Error saving include %s: %v", inc, err)
			stats.Errors++
		}
	}

	if result.JSResult != nil {
		functionsBatch := make([]*model.JSFunction, 0, len(result.JSResult.Functions))
		symbolsBatch := make([]*model.Symbol, 0, len(result.JSResult.Functions))
		for _, fn := range result.JSResult.Functions {
			fn.FileID = fileID
			functionsBatch = append(functionsBatch, fn)
			symbolsBatch = append(symbolsBatch, &model.Symbol{
				FileID:     fileID,
				SymbolName: fn.FunctionName,
				SymbolType: "function",
				EntityType: "js",
				LineNumber: fn.LineStart,
				Signature:  fn.Signature,
			})
		}
		if err := idx.db.BatchInsertJSFunctions(functionsBatch, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
		functionIDs, err := idx.db.FindJSFunctionIDsByFile(fileID)
		if err != nil {
			return fmt.Errorf("failed to resolve SMF JS function ids for symbols: %w", err)
		}
		for _, symbol := range symbolsBatch {
			symbol.EntityID = functionIDs[store.BuildJSFunctionLookupKey(symbol.SymbolName, symbol.LineNumber)]
		}
		if err := idx.db.BatchInsertSymbols(symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
		stats.JSFunctions += len(functionsBatch)
	}

	return nil
}

func (idx *Indexer) parseXMLFile(path string, fileID int64, stats *model.ScanStats) error {
	parser := dsxml.NewParser()
	result, err := parser.ParseFile(path)
	if err != nil {
		return fmt.Errorf("failed to parse XML file: %w", err)
	}
	for _, item := range result.BusinessObjects {
		item.FileID = fileID
	}
	for _, item := range result.Contracts {
		item.FileID = fileID
	}
	for _, item := range result.BusinessObjectParams {
		item.FileID = fileID
	}
	for _, item := range result.BusinessObjectTables {
		item.FileID = fileID
	}
	if err := idx.db.BatchInsertAPIBusinessObjects(result.BusinessObjects, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	businessObjectIDs, err := idx.db.FindAPIBusinessObjectIDsByFile(fileID)
	if err != nil {
		return err
	}
	for _, item := range result.Contracts {
		if item.BusinessObject != "" {
			item.BusinessObjectID = businessObjectIDs[strings.ToLower(strings.TrimSpace(item.BusinessObject))]
		}
	}
	if err := idx.db.BatchInsertAPIContracts(result.Contracts, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	contractIDs, err := idx.db.FindAPIContractIDsByFile(fileID)
	if err != nil {
		return err
	}
	for _, item := range result.Params {
		if len(result.Contracts) > 0 {
			key := store.BuildAPIContractLookupKey(result.Contracts[0].ContractName, result.Contracts[0].ContractKind)
			item.ContractID = contractIDs[key]
		}
	}
	for _, item := range result.Tables {
		if len(result.Contracts) > 0 {
			key := store.BuildAPIContractLookupKey(result.Contracts[0].ContractName, result.Contracts[0].ContractKind)
			item.ContractID = contractIDs[key]
		}
	}
	for _, item := range result.ReturnValues {
		if len(result.Contracts) > 0 {
			key := store.BuildAPIContractLookupKey(result.Contracts[0].ContractName, result.Contracts[0].ContractKind)
			item.ContractID = contractIDs[key]
		}
	}
	for _, item := range result.Contexts {
		if len(result.Contracts) > 0 {
			key := store.BuildAPIContractLookupKey(result.Contracts[0].ContractName, result.Contracts[0].ContractKind)
			item.ContractID = contractIDs[key]
		}
	}
	if err := idx.db.BatchInsertAPIContractParams(result.Params, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertAPIContractTables(result.Tables, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	contractTableIDs, err := idx.db.FindAPIContractTableIDsByFile(fileID)
	if err != nil {
		return err
	}
	for _, item := range result.TableFields {
		key := store.BuildAPIContractTableLookupKey(item.ParentDirection, item.ParentTableName)
		item.ContractTableID = contractTableIDs[key]
	}
	if err := idx.db.BatchInsertAPIContractTableFields(result.TableFields, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertAPIContractReturnValues(result.ReturnValues, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertAPIContractContexts(result.Contexts, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertAPIBusinessObjectParams(result.BusinessObjectParams, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertAPIBusinessObjectTables(result.BusinessObjectTables, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	businessTableIDs, err := idx.db.FindAPIBusinessObjectTableIDsByFile(fileID)
	if err != nil {
		return err
	}
	for _, item := range result.BusinessTableFields {
		key := store.BuildAPIBusinessObjectTableLookupKey(item.BusinessObject, item.ParentTableName)
		item.BusinessTableID = businessTableIDs[key]
	}
	for _, item := range result.BusinessTableIndexes {
		key := store.BuildAPIBusinessObjectTableLookupKey(item.BusinessObject, item.ParentTableName)
		item.BusinessTableID = businessTableIDs[key]
	}
	if err := idx.db.BatchInsertAPIBusinessObjectTableFields(result.BusinessTableFields, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	if err := idx.db.BatchInsertAPIBusinessObjectTableIndexes(result.BusinessTableIndexes, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	businessTableIndexIDs, err := idx.db.FindAPIBusinessObjectTableIndexIDsByFile(fileID)
	if err != nil {
		return err
	}
	for _, item := range result.BusinessIndexFields {
		key := store.BuildAPIBusinessObjectTableIndexLookupKey(item.BusinessObject, item.ParentTableName, item.ParentIndexName)
		item.TableIndexID = businessTableIndexIDs[key]
	}
	if err := idx.db.BatchInsertAPIBusinessObjectTableIndexFields(result.BusinessIndexFields, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	for _, symbol := range result.Symbols {
		symbol.FileID = fileID
		if symbol.EntityType == "xml" && len(result.Contracts) > 0 {
			if id, ok := contractIDs[store.BuildAPIContractLookupKey(result.Contracts[0].ContractName, result.Contracts[0].ContractKind)]; ok {
				symbol.EntityID = id
			}
		}
	}
	if err := idx.db.BatchInsertSymbols(result.Symbols, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	stats.APIContracts += len(result.Contracts)
	stats.APIParams += len(result.Params) + len(result.BusinessObjectParams)
	stats.APITables += len(result.Tables) + len(result.BusinessObjectTables)
	stats.APITableFields += len(result.TableFields) + len(result.BusinessTableFields)
	stats.APITableIndexes += len(result.BusinessTableIndexes)
	return nil
}

func (idx *Indexer) parseT01File(path string, fileID int64, stats *model.ScanStats) error {
	_, err := idx.indexAPIMacros(path, fileID, "T01", stats)
	return err
}

func (idx *Indexer) indexAPIMacros(path string, fileID int64, language string, stats *model.ScanStats) ([]*model.Relation, error) {
	parser := apimacro.NewParser()
	result, err := parser.ParseFile(path, language)
	if err != nil {
		return nil, err
	}
	if len(result.Invocations) == 0 {
		return nil, nil
	}
	procedureIDs, err := idx.db.FindSQLProcedureIDsByFile(fileID)
	if err != nil {
		procedureIDs = map[string]int64{}
	}
	for _, invocation := range result.Invocations {
		invocation.FileID = fileID
	}
	if err := idx.db.BatchInsertAPIMacroInvocations(result.Invocations, idx.config.Indexer.BatchSize); err != nil {
		return nil, err
	}
	relations := make([]*model.Relation, 0, len(result.Invocations))
	for _, invocation := range result.Invocations {
		sourceID := procedureIDs[strings.ToLower(strings.TrimSpace(invocation.ProcedureName))]
		if sourceID == 0 {
			continue
		}
		switch invocation.MacroType {
		case "create_proc":
			if targetID, err := idx.db.FindLatestAPIContractIDByNameAndKind(invocation.TargetName, "service"); err == nil {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: sourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "implements_contract", Confidence: "regex", LineNumber: invocation.LineNumber})
			} else if targetID, err := idx.db.FindLatestAPIContractIDByNameAndKind(invocation.TargetName, "callback_event"); err == nil {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: sourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "implements_contract", Confidence: "regex", LineNumber: invocation.LineNumber})
			}
		case "init_event":
			if targetID, err := idx.db.FindLatestAPIContractIDByNameAndKind(invocation.TargetName, "event"); err == nil {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: sourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "publishes_event", Confidence: "regex", LineNumber: invocation.LineNumber})
			}
		case "exec_contract":
			if targetID, err := idx.db.FindLatestAPIContractIDByNameAndKind(invocation.TargetName, "used_service"); err == nil {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: sourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "executes_contract", Confidence: "regex", LineNumber: invocation.LineNumber})
			} else if targetID, err := idx.db.FindLatestAPIContractIDByNameAndKind(invocation.TargetName, "service"); err == nil {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: sourceID, TargetType: "api_contract", TargetID: targetID, RelationType: "executes_contract", Confidence: "regex", LineNumber: invocation.LineNumber})
			}
		case "dispatches_to":
			targetID, err := idx.db.FindLatestSQLProcedureIDByName(invocation.TargetName)
			if err == nil && targetID != 0 {
				relations = append(relations, &model.Relation{SourceType: "sql_procedure", SourceID: sourceID, TargetType: "sql_procedure", TargetID: targetID, RelationType: "dispatches_to", Confidence: "regex", LineNumber: invocation.LineNumber})
			}
		}
	}
	stats.Relations += len(relations)
	return relations, nil
}

func (idx *Indexer) processFile(file fswalk.FileInfo, fileID int64, stats *model.ScanStats) error {
	switch strings.ToUpper(file.Language) {
	case "SQL":
		stats.SQLFiles++
		if err := idx.parseSQLFile(file.Path, fileID, stats); err != nil {
			return err
		}
		stats.FilesIndexed++
	case "PAS":
		stats.PASFiles++
		if err := idx.parsePASFile(file.Path, fileID, stats); err != nil {
			return err
		}
		stats.FilesIndexed++
	case "JS":
		stats.JSFiles++
		if err := idx.parseJSFile(file.Path, fileID, stats); err != nil {
			return err
		}
		stats.FilesIndexed++
	case "H":
		stats.HFiles++
		if err := idx.parseHFile(file.Path, fileID, stats); err != nil {
			return err
		}
		stats.FilesIndexed++
	case "DFM":
		stats.DFMFiles++
		if err := idx.parseDFMFile(file.Path, fileID, stats); err != nil {
			return err
		}
		stats.FilesIndexed++
	case "SMF":
		stats.SMFFiles++
		if err := idx.parseSMFFile(file.Path, fileID, stats); err != nil {
			return err
		}
		stats.FilesIndexed++
	case "TPR":
		stats.TPRFiles++
		if err := idx.parseTPRFile(file.Path, fileID, stats); err != nil {
			return err
		}
		stats.FilesIndexed++
	case "RPT":
		stats.RPTFiles++
		if err := idx.parseRPTFile(file.Path, fileID, stats); err != nil {
			return err
		}
		stats.FilesIndexed++
	case "XML":
		if strings.Contains(filepath.ToSlash(file.Path), "/DSArchitectData/") {
			stats.XMLFiles++
			if err := idx.parseXMLFile(file.Path, fileID, stats); err != nil {
				return err
			}
			stats.FilesIndexed++
		}
	case "T01":
		if err := idx.parseT01File(file.Path, fileID, stats); err != nil {
			return err
		}
		stats.FilesIndexed++
	}

	return nil
}

func (idx *Indexer) saveFile(file fswalk.FileInfo, scanRunID int64) (int64, error) {
	var id int64
	err := idx.db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`,
		scanRunID,
		file.Path,
		file.RelPath,
		file.Extension,
		file.Size,
		file.Hash,
		file.ModifiedAt,
		file.Encoding,
		file.Language,
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (idx *Indexer) saveIncludeDirective(fileID int64, path string, includePath string, lineNum int) error {
	var resolvedFileID interface{}
	if id, err := idx.db.FindLatestFileIDByPaths(idx.buildIncludePathCandidates(path, includePath)); err == nil {
		resolvedFileID = id
	} else if err != dbsql.ErrNoRows {
		return err
	} else if id, err := idx.db.FindLatestHFileIDByNameLike(includePath); err == nil {
		resolvedFileID = id
	} else if err != dbsql.ErrNoRows {
		return err
	}

	_, err := idx.db.Exec(`
		INSERT INTO include_directives (file_id, include_path, resolved_file_id, line_number)
		VALUES ($1, $2, $3, $4)
	`, fileID, includePath, resolvedFileID, lineNum)
	return err
}

func (idx *Indexer) buildIncludePathCandidates(sourcePath string, includePath string) []string {
	normalizedInclude := strings.ReplaceAll(strings.TrimSpace(includePath), `\`, "/")
	if normalizedInclude == "" {
		return nil
	}

	seen := make(map[string]struct{})
	candidates := make([]string, 0, 3)
	addCandidate := func(candidate string) {
		normalized := filepath.ToSlash(filepath.Clean(strings.TrimSpace(candidate)))
		if normalized == "" {
			return
		}
		if _, exists := seen[normalized]; exists {
			return
		}
		seen[normalized] = struct{}{}
		candidates = append(candidates, normalized)
	}

	addCandidate(normalizedInclude)
	baseDir := filepath.Dir(sourcePath)
	addCandidate(filepath.Join(baseDir, normalizedInclude))
	parentDir := filepath.Dir(baseDir)
	addCandidate(filepath.Join(parentDir, "Include", filepath.Base(normalizedInclude)))

	return candidates
}

func startProgressReporter(mode string, snapshot func() model.ScanStats) func() {
	done := make(chan struct{})
	var once sync.Once
	frames := []rune{'|', '/', '-', '\\'}

	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		frameIndex := 0

		for {
			select {
			case <-done:
				fmt.Printf("\r%s %s\n", mode, strings.Repeat(" ", 80))
				return
			case <-ticker.C:
				stats := snapshot()
				frame := frames[frameIndex%len(frames)]
				fmt.Printf(
					"\r%s %c scanned=%d indexed=%d errors=%d",
					mode,
					frame,
					stats.FilesScanned,
					stats.FilesIndexed,
					stats.Errors,
				)
				frameIndex++
			}
		}
	}()

	return func() {
		once.Do(func() {
			close(done)
		})
	}
}
