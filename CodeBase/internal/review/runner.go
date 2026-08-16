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
	for ruleID, meta := range ruleCatalog {
		if !ruleSet[ruleID] {
			continue
		}
		rule := ruleID
		build := meta.Build
		tasks = append(tasks, ruleTask{
			rule: rule,
			run: func(ctx context.Context) ([]Finding, error) {
				return build(r, ctx, parsed, file)
			},
		})
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
