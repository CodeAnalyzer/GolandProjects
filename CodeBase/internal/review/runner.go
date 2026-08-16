package review

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store"
)

type Runner struct {
	db              *store.DB
	parser          *sqlparser.Parser
	exec            *reviewExecContext
	colTypeCache    map[string]string
	colTypeMu       sync.Mutex
	macroTypeCache  map[string]string
	macroTypeMu     sync.Mutex
	indexCandCache  map[string][]tableIndexCandidate
	indexCandMu     sync.Mutex
	indexFieldsCache map[string][]string
	onProgress      func(completed, total int)
	// prewarm caches for batch-loaded DB data (nil = not prewarmed, fallback to per-call)
	procParamsCache     map[string][]model.SQLParam
	procProductIDCache  map[string]int64
	tableProductIDCache map[string]map[int64]struct{}
}

type reviewExecContext struct {
	filePath    string
	content     []byte
	macroResult macroReplaceResult
	lines       []string
}

type ruleTask struct {
	rule RuleID
	run  func(ctx context.Context) ([]Finding, error)
}

func NewRunner(db *store.DB) *Runner {
	return &Runner{db: db, parser: sqlparser.NewParser(), colTypeCache: make(map[string]string), macroTypeCache: make(map[string]string), indexCandCache: make(map[string][]tableIndexCandidate), indexFieldsCache: make(map[string][]string), procParamsCache: nil, procProductIDCache: nil, tableProductIDCache: nil}
}

func (r *Runner) SetOnProgress(fn func(completed, total int)) {
	r.onProgress = fn
}

func (r *Runner) RunSQLFileCtx(ctx context.Context, path string, opts Options) (*Result, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("sql file path is required")
	}
	if r.db == nil {
		return nil, fmt.Errorf("database is not initialized")
	}

	r.colTypeMu.Lock()
	r.colTypeCache = make(map[string]string)
	r.colTypeMu.Unlock()

	r.macroTypeMu.Lock()
	r.macroTypeCache = make(map[string]string)
	r.macroTypeMu.Unlock()

	r.indexCandMu.Lock()
	r.indexCandCache = make(map[string][]tableIndexCandidate)
	r.indexFieldsCache = make(map[string][]string)
	r.indexCandMu.Unlock()

	// Сбрасываем prewarm-кэши
	r.procParamsCache = nil
	r.procProductIDCache = nil
	r.tableProductIDCache = nil

	normalizedPath := normalizePath(path)
	file, err := r.getIndexedFile(ctx, normalizedPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("file is not indexed: %s", normalizedPath)
		}
		return nil, err
	}
	if file.DsProductID == 0 {
		return nil, fmt.Errorf("ds_product_id is not set for file: %s", normalizedPath)
	}

	// Читаем файл с кодировкой CP866 (как в sql_parser.ParseFile)
	fileContent, err := encoding.ReadFile(normalizedPath, encoding.CP866)
	if err != nil {
		return nil, err
	}

	// Выполняем подстановку макросов ДО парсинга
	macroResult := replaceMacros(fileContent)

	// Парсим expanded-контент (с раскрытыми макросами)
	parsed, err := r.parser.ParseContent(macroResult.Content)
	if err != nil {
		return nil, err
	}

	// Expanded-контент (после replaceMacros) — все правила работают в expanded-пространстве,
	// а ремаппинг Finding.Line в конце конвертирует обратно к оригинальным строкам
	r.exec = &reviewExecContext{
		filePath:    normalizePath(file.Path),
		content:     []byte(macroResult.Content),
		macroResult: macroResult,
		lines:       strings.Split(macroResult.Content, "\n"),
	}
	defer func() {
		r.exec = nil
	}()

	// Предзагружаем кэши типов колонок и индексов одним batch-запросом каждый.
	if err := r.prewarmColTypeCache(ctx, parsed, r.exec.lines); err != nil {
		return nil, err
	}
	if err := r.prewarmIndexCache(ctx, r.exec.lines); err != nil {
		return nil, err
	}

	// Предзагружаем параметры процедур, productID процедур и productID таблиц
	// одним batch-запросом каждый, чтобы избежать N+1 DB-запросов в правилах.
	r.prewarmProcCaches(ctx, parsed)
	r.prewarmTableProductIDs(ctx, parsed)

	ruleSet := enabledRuleSet(opts.Rules)
	tasks := r.buildRuleTasks(ctx, ruleSet, parsed, file)
	maxWorkers := r.maxRuleWorkers(ctx, len(tasks))
	findings, err := runRuleTasks(ctx, tasks, maxWorkers, r.onProgress)
	if err != nil {
		return nil, err
	}

	minSeverity := opts.MinSeverity
	if minSeverity <= 0 {
		minSeverity = SeverityFineCode
	}

	// Ремаппим номера строк в findings из expanded-контента обратно к оригинальному файлу
	for i := range findings {
		findings[i].Line = mapExpandedLineToOriginal(findings[i].Line, macroResult.SourceMap)
	}

	filtered := make([]Finding, 0, len(findings))
	for _, finding := range findings {
		if finding.Severity <= minSeverity {
			filtered = append(filtered, finding)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Line == filtered[j].Line {
			return filtered[i].Rule < filtered[j].Rule
		}
		return filtered[i].Line < filtered[j].Line
	})

	result := &Result{
		AnalyzedFile: normalizedPath,
		Summary:      buildSummary(filtered),
		Findings:     filtered,
	}
	return result, nil
}

// RunSQLFile - deprecated thin wrapper.
func (r *Runner) RunSQLFile(path string, opts Options) (*Result, error) {
	return r.RunSQLFileCtx(context.Background(), path, opts)
}

func (r *Runner) buildRuleTasks(ctx context.Context, ruleSet map[RuleID]bool, parsed *sqlparser.ParseResult, file *indexedFile) []ruleTask {
	_ = ctx
	tasks := make([]ruleTask, 0, len(ruleSet))
	if ruleSet[RuleForeignTablesUsing] {
		tasks = append(tasks, ruleTask{rule: RuleForeignTablesUsing, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkForeignTables(ctx, parsed, file, "t")
		}})
	}
	if ruleSet[RuleForeignPTablesUsing] {
		tasks = append(tasks, ruleTask{rule: RuleForeignPTablesUsing, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkForeignPTables(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleForeignProcedureUsing] {
		tasks = append(tasks, ruleTask{rule: RuleForeignProcedureUsing, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkForeignProcedures(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleExecNotExistsProc] {
		tasks = append(tasks, ruleTask{rule: RuleExecNotExistsProc, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkExecNotExistsProcedures(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleProcDuplicate] {
		tasks = append(tasks, ruleTask{rule: RuleProcDuplicate, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkProcDuplicate(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleProcParamDefValue] {
		tasks = append(tasks, ruleTask{rule: RuleProcParamDefValue, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkProcParamDefValue(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleProcElseCase] {
		tasks = append(tasks, ruleTask{rule: RuleProcElseCase, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkProcElseCase(ctx, file)
		}})
	}
	if ruleSet[RuleUseSelectAll] {
		tasks = append(tasks, ruleTask{rule: RuleUseSelectAll, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUseSelectAll(ctx, file)
		}})
	}
	if ruleSet[RuleTruncTbl] {
		tasks = append(tasks, ruleTask{rule: RuleTruncTbl, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTruncTbl(ctx, file)
		}})
	}
	if ruleSet[RuleAnsiInJoin] {
		tasks = append(tasks, ruleTask{rule: RuleAnsiInJoin, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkAnsiInJoin(ctx, file)
		}})
	}
	if ruleSet[RuleDatatype] {
		tasks = append(tasks, ruleTask{rule: RuleDatatype, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkDatatype(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleInsertRowLock] {
		tasks = append(tasks, ruleTask{rule: RuleInsertRowLock, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkInsertRowLock(ctx, file)
		}})
	}
	if ruleSet[RuleUseEqColumn] {
		tasks = append(tasks, ruleTask{rule: RuleUseEqColumn, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUseEqColumn(ctx, file)
		}})
	}
	if ruleSet[RuleTableFullScan] {
		tasks = append(tasks, ruleTask{rule: RuleTableFullScan, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTableFullScan(ctx, file)
		}})
	}
	if ruleSet[RuleTableHintExists] {
		tasks = append(tasks, ruleTask{rule: RuleTableHintExists, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTableHintExists(ctx, file)
		}})
	}
	if ruleSet[RuleTableHintIsRight] {
		tasks = append(tasks, ruleTask{rule: RuleTableHintIsRight, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTableHintIsRight(ctx, file)
		}})
	}
	if ruleSet[RuleIndexExistsInDB] {
		tasks = append(tasks, ruleTask{rule: RuleIndexExistsInDB, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkIndexExistsInDB(ctx, file)
		}})
	}
	if ruleSet[RuleIndexWrong] {
		tasks = append(tasks, ruleTask{rule: RuleIndexWrong, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkIndexWrong(ctx, file)
		}})
	}
	if ruleSet[RuleUpdateOnlyVar] {
		tasks = append(tasks, ruleTask{rule: RuleUpdateOnlyVar, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUpdateOnlyVar(ctx, file)
		}})
	}
	if ruleSet[RulePTableSpid] {
		tasks = append(tasks, ruleTask{rule: RulePTableSpid, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkPTableSpid(ctx, file)
		}})
	}
	if ruleSet[RuleForceOrder2Tbl] {
		tasks = append(tasks, ruleTask{rule: RuleForceOrder2Tbl, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkForceOrder2Tbl(ctx, file)
		}})
	}
	if ruleSet[RuleSaveTran] {
		tasks = append(tasks, ruleTask{rule: RuleSaveTran, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkSaveTran(ctx, file)
		}})
	}
	if ruleSet[RuleUseDrop] {
		tasks = append(tasks, ruleTask{rule: RuleUseDrop, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUseDrop(ctx, file)
		}})
	}
	if ruleSet[RuleMathOperations] {
		tasks = append(tasks, ruleTask{rule: RuleMathOperations, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkMathOperations(ctx, file)
		}})
	}
	if ruleSet[RuleExistsWithAndInIf] {
		tasks = append(tasks, ruleTask{rule: RuleExistsWithAndInIf, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkExistsWithAndInIf(ctx, file)
		}})
	}
	if ruleSet[RuleNullComparison] {
		tasks = append(tasks, ruleTask{rule: RuleNullComparison, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkNullComparison(ctx, file)
		}})
	}
	if ruleSet[RuleShouldBeCP866] {
		tasks = append(tasks, ruleTask{rule: RuleShouldBeCP866, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkShouldBeCP866(ctx, file)
		}})
	}
	if ruleSet[RuleTooManyJoins] {
		tasks = append(tasks, ruleTask{rule: RuleTooManyJoins, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTooManyJoins(ctx, file)
		}})
	}
	if ruleSet[RuleMaxProcParam] {
		tasks = append(tasks, ruleTask{rule: RuleMaxProcParam, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkMaxProcParam(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleModifyOutProc] {
		tasks = append(tasks, ruleTask{rule: RuleModifyOutProc, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkModifyOutProc(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleEmptyReturn] {
		tasks = append(tasks, ruleTask{rule: RuleEmptyReturn, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkEmptyReturn(ctx, file)
		}})
	}
	if ruleSet[RuleRawTransactionControl] {
		tasks = append(tasks, ruleTask{rule: RuleRawTransactionControl, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkRawTransactionControl(ctx, file)
		}})
	}
	if ruleSet[RuleDeferredUpdate] {
		tasks = append(tasks, ruleTask{rule: RuleDeferredUpdate, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkDeferredUpdate(ctx, file)
		}})
	}
	if ruleSet[RuleInSubQuery] {
		tasks = append(tasks, ruleTask{rule: RuleInSubQuery, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkInSubQuery(ctx, file)
		}})
	}
	if ruleSet[RuleVarcharSize] {
		tasks = append(tasks, ruleTask{rule: RuleVarcharSize, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkVarcharSize(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleColumnInsert] {
		tasks = append(tasks, ruleTask{rule: RuleColumnInsert, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkColumnInsert(ctx, file)
		}})
	}
	if ruleSet[RulePostgreLabelGotoLevel] {
		tasks = append(tasks, ruleTask{rule: RulePostgreLabelGotoLevel, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkPostgreLabelGotoLevel(ctx, file)
		}})
	}
	if ruleSet[RuleDateIntoString] {
		tasks = append(tasks, ruleTask{rule: RuleDateIntoString, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkDateIntoString(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleEmptyStringDate] {
		tasks = append(tasks, ruleTask{rule: RuleEmptyStringDate, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkEmptyStringDate(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleVarUseAfterCursor] {
		tasks = append(tasks, ruleTask{rule: RuleVarUseAfterCursor, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkVarUseAfterCursor(ctx, file)
		}})
	}
	if ruleSet[RuleExcessProcParams] {
		tasks = append(tasks, ruleTask{rule: RuleExcessProcParams, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkExcessProcParams(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleDuplicateOutputVariable] {
		tasks = append(tasks, ruleTask{rule: RuleDuplicateOutputVariable, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkDuplicateOutputVariable(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleUseOnlyDeclaredCursors] {
		tasks = append(tasks, ruleTask{rule: RuleUseOnlyDeclaredCursors, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUseOnlyDeclaredCursors(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleCursorFetchArguments] {
		tasks = append(tasks, ruleTask{rule: RuleCursorFetchArguments, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkCursorFetchArguments(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleUsageVarInSameSelect] {
		tasks = append(tasks, ruleTask{rule: RuleUsageVarInSameSelect, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUsageVarInSameSelect(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleVarAssignInUpdate] {
		tasks = append(tasks, ruleTask{rule: RuleVarAssignInUpdate, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkVarAssignInUpdate(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleStatementsWithJoinsRequireAliases] {
		tasks = append(tasks, ruleTask{rule: RuleStatementsWithJoinsRequireAliases, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkStatementsWithJoinsRequireAliases(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleUseFuncInIndCol] {
		tasks = append(tasks, ruleTask{rule: RuleUseFuncInIndCol, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUseFuncInIndCol(ctx, file)
		}})
	}
	if ruleSet[RuleIsNullSameTypes] {
		tasks = append(tasks, ruleTask{rule: RuleIsNullSameTypes, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkIsNullSameTypes(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleDiffTypesComparison] {
		tasks = append(tasks, ruleTask{rule: RuleDiffTypesComparison, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkDiffTypesComparison(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleFloatToStringConvert] {
		tasks = append(tasks, ruleTask{rule: RuleFloatToStringConvert, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkFloatToStringConvert(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleSelectAfterSetRowcount] {
		tasks = append(tasks, ruleTask{rule: RuleSelectAfterSetRowcount, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkSelectAfterSetRowcount(ctx, parsed, file)
		}})
	}
	if ruleSet[RuleAliasWhenUsingUnion] {
		tasks = append(tasks, ruleTask{rule: RuleAliasWhenUsingUnion, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkAliasWhenUsingUnion(ctx, parsed, file)
		}})
	}
	return tasks
}

func (r *Runner) maxRuleWorkers(ctx context.Context, enabledRules int) int {
	_ = ctx
	if enabledRules <= 0 {
		return 1
	}
	limit := runtime.NumCPU()
	if limit <= 0 {
		limit = 1
	}
	if r.db != nil {
		dbLimit := r.db.Stats().MaxOpenConnections
		if dbLimit > 0 && dbLimit < limit {
			limit = dbLimit
		}
	}
	if enabledRules < limit {
		limit = enabledRules
	}
	if limit < 1 {
		return 1
	}
	return limit
}

func runRuleTasks(parentCtx context.Context, tasks []ruleTask, maxWorkers int, onProgress func(int, int)) ([]Finding, error) {
	if len(tasks) == 0 {
		return nil, nil
	}
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	type taskResult struct {
		rule     RuleID
		findings []Finding
		duration time.Duration
		err      error
	}

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	tasksCh := make(chan ruleTask)
	resultsCh := make(chan taskResult, len(tasks))

	workerCount := maxWorkers
	if workerCount > len(tasks) {
		workerCount = len(tasks)
	}

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-tasksCh:
					if !ok {
						return
					}
					start := time.Now()
					items, err := task.run(ctx)
					resultsCh <- taskResult{rule: task.rule, findings: items, duration: time.Since(start), err: err}
					if err != nil {
						cancel()
						return
					}
				}
			}
		}()
	}

	go func() {
		defer close(tasksCh)
		for _, task := range tasks {
			select {
			case <-ctx.Done():
				return
			case tasksCh <- task:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	total := len(tasks)
	completed := 0
	findings := make([]Finding, 0)
	var firstErr error
	for result := range resultsCh {
		completed++
		if onProgress != nil {
			onProgress(completed, total)
		}
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		findings = append(findings, result.findings...)
	}

	if firstErr != nil {
		return nil, firstErr
	}

	return findings, nil
}

func (r *Runner) fileContent(ctx context.Context, path string) ([]byte, error) {
	_ = ctx
	if r.exec != nil && normalizePath(path) == r.exec.filePath {
		return r.exec.content, nil
	}
	return nil, fmt.Errorf("file %q is not the active review target", path)
}

// reUpdateTable, reInsertTable, reFromJoinTable — лёгкие regex для извлечения имён
// таблиц из SQL-фрагментов при предзагрузке кэша типов колонок.
var (
	reUpdateTable  = regexp.MustCompile(`(?i)^\s*update\s+(#?[a-z_][a-z0-9_#]*)`)
	reInsertTable  = regexp.MustCompile(`(?i)^\s*insert\s+(?:into\s+)?(#?[a-z_][a-z0-9_#]*)`)
	reFromJoinTable = regexp.MustCompile(`(?i)\b(?:from|join)\s+(#?[a-z_][a-z0-9_#]*)`)
)

// prewarmColTypeCache собирает все имена таблиц из фрагментов файла и загружает
// типы всех их колонок одним batch-запросом к БД, заполняя colTypeCache.
// Это позволяет избежать тысяч отдельных DB-запросов при параллельном запуске правил.
func (r *Runner) prewarmColTypeCache(ctx context.Context, parsed *sqlparser.ParseResult, fileLines []string) error {
	tableSet := make(map[string]struct{})

	for _, frag := range parsed.Fragments {
		if frag == nil {
			continue
		}
		qt := frag.QueryText
		if len(qt) == 0 {
			continue
		}
		// Собираем UPDATE/INSERT целевые таблицы
		if m := reUpdateTable.FindStringSubmatch(qt); len(m) == 2 {
			tableSet[strings.ToLower(strings.TrimSpace(m[1]))] = struct{}{}
		} else if m := reInsertTable.FindStringSubmatch(qt); len(m) == 2 {
			tableSet[strings.ToLower(strings.TrimSpace(m[1]))] = struct{}{}
		}
		// Собираем таблицы из FROM/JOIN (для diffTypesComparison и indexWrong)
		for _, m := range reFromJoinTable.FindAllStringSubmatch(qt, -1) {
			if len(m) == 2 {
				tableSet[strings.ToLower(strings.TrimSpace(m[1]))] = struct{}{}
			}
		}
	}

	// INSERT ... VALUES разбросаны по строкам файла
	for _, line := range fileLines {
		if !strings.Contains(strings.ToLower(line), "insert") {
			continue
		}
		if m := reInsertTable.FindStringSubmatch(line); len(m) == 2 {
			tableSet[strings.ToLower(strings.TrimSpace(m[1]))] = struct{}{}
		}
	}

	if len(tableSet) == 0 {
		return nil
	}

	names := make([]string, 0, len(tableSet))
	for t := range tableSet {
		names = append(names, t)
	}

	batch, err := r.db.BatchFindColumnDefinitionTypes(ctx, names)
	if err != nil {
		return err
	}

	r.colTypeMu.Lock()
	for k, v := range batch {
		r.colTypeCache[k] = v
	}
	r.colTypeMu.Unlock()
	return nil
}

func (r *Runner) cachedFindColumnDefinitionType(ctx context.Context, tableName, columnName string) (string, error) {
	key := strings.ToLower(strings.TrimSpace(tableName)) + "|" + strings.ToLower(strings.TrimSpace(columnName))
	r.colTypeMu.Lock()
	if v, ok := r.colTypeCache[key]; ok {
		r.colTypeMu.Unlock()
		return v, nil
	}
	r.colTypeMu.Unlock()
	typeName, err := r.db.FindLatestSQLColumnDefinitionType(ctx, tableName, columnName)
	if err != nil {
		if err == sql.ErrNoRows {
			// Fallback: ищем тип в API-контрактах и business objects
			apiType, apiErr := r.db.FindAPIColumnDefinitionType(ctx, tableName, columnName)
			if apiErr == nil && apiType != "" {
				r.colTypeMu.Lock()
				r.colTypeCache[key] = apiType
				r.colTypeMu.Unlock()
				return apiType, nil
			}
			r.colTypeMu.Lock()
			r.colTypeCache[key] = ""
			r.colTypeMu.Unlock()
			return "", nil
		}
		return "", err
	}
	r.colTypeMu.Lock()
	r.colTypeCache[key] = typeName
	r.colTypeMu.Unlock()
	return typeName, nil
}

func (r *Runner) fileProcessedContent(ctx context.Context, path string) (macroReplaceResult, error) {
	_ = ctx
	if r.exec != nil && normalizePath(path) == r.exec.filePath {
		return r.exec.macroResult, nil
	}
	return macroReplaceResult{}, fmt.Errorf("file %q is not the active review target", path)
}

// mapExpandedLineToOriginal преобразует номер строки из expanded-контента
// (после replaceMacros) в номер строки оригинального файла используя SourceMap.
func mapExpandedLineToOriginal(expandedLine int, sourceMap []int) int {
	if expandedLine <= 0 || len(sourceMap) == 0 {
		return expandedLine
	}
	idx := expandedLine - 1 // SourceMap 0-indexed
	if idx < len(sourceMap) {
		origLine := sourceMap[idx]
		if origLine > 0 {
			return origLine
		}
	}
	// Если вышли за пределы — смещаем от последней известной
	if len(sourceMap) > 0 {
		lastOrig := sourceMap[len(sourceMap)-1]
		if lastOrig > 0 {
			return lastOrig + (expandedLine - len(sourceMap))
		}
	}
	return expandedLine
}
