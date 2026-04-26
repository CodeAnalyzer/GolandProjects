package indexer

import (
	dbsql "database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	rptparser "github.com/codebase/internal/parser/rpt"
	smfparser "github.com/codebase/internal/parser/smf"
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
	dst.APITableIndexes += src.APITableIndexes
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
	return idx.parseSQLLikeFile(path, fileID, stats, false, true)
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
