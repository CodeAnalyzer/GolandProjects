package review

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"

	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store"
)

type Runner struct {
	db     *store.DB
	parser *sqlparser.Parser
	exec   *reviewExecContext
}

type reviewExecContext struct {
	filePath string
	content  []byte
	lines    []string
}

type ruleTask struct {
	rule RuleID
	run  func(ctx context.Context) ([]Finding, error)
}

func NewRunner(db *store.DB) *Runner {
	return &Runner{db: db, parser: sqlparser.NewParser()}
}

func (r *Runner) RunSQLFile(path string, opts Options) (*Result, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("sql file path is required")
	}
	if r.db == nil {
		return nil, fmt.Errorf("database is not initialized")
	}

	normalizedPath := normalizePath(path)
	file, err := r.getIndexedFile(normalizedPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("file is not indexed: %s", normalizedPath)
		}
		return nil, err
	}
	if file.DsProductID == 0 {
		return nil, fmt.Errorf("ds_product_id is not set for file: %s", normalizedPath)
	}

	parsed, err := r.parser.ParseFile(normalizedPath)
	if err != nil {
		return nil, err
	}

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	r.exec = &reviewExecContext{
		filePath: normalizePath(file.Path),
		content:  content,
		lines:    strings.Split(string(content), "\n"),
	}
	defer func() {
		r.exec = nil
	}()

	ruleSet := enabledRuleSet(opts.Rules)
	tasks := r.buildRuleTasks(ruleSet, parsed, file)
	maxWorkers := r.maxRuleWorkers(len(tasks))
	findings, err := runRuleTasks(tasks, maxWorkers)
	if err != nil {
		return nil, err
	}

	minSeverity := opts.MinSeverity
	if minSeverity <= 0 {
		minSeverity = SeverityFineCode
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

func (r *Runner) buildRuleTasks(ruleSet map[RuleID]bool, parsed *sqlparser.ParseResult, file *indexedFile) []ruleTask {
	tasks := make([]ruleTask, 0, len(ruleSet))
	if ruleSet[RuleForeignTablesUsing] {
		tasks = append(tasks, ruleTask{rule: RuleForeignTablesUsing, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkForeignTables(parsed, file, "t")
		}})
	}
	if ruleSet[RuleForeignPTablesUsing] {
		tasks = append(tasks, ruleTask{rule: RuleForeignPTablesUsing, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkForeignPTables(parsed, file)
		}})
	}
	if ruleSet[RuleForeignProcedureUsing] {
		tasks = append(tasks, ruleTask{rule: RuleForeignProcedureUsing, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkForeignProcedures(parsed, file)
		}})
	}
	if ruleSet[RuleExecNotExistsProc] {
		tasks = append(tasks, ruleTask{rule: RuleExecNotExistsProc, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkExecNotExistsProcedures(parsed, file)
		}})
	}
	if ruleSet[RuleProcDuplicate] {
		tasks = append(tasks, ruleTask{rule: RuleProcDuplicate, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkProcDuplicate(parsed, file)
		}})
	}
	if ruleSet[RuleProcParamDefValue] {
		tasks = append(tasks, ruleTask{rule: RuleProcParamDefValue, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkProcParamDefValue(parsed, file)
		}})
	}
	if ruleSet[RuleProcElseCase] {
		tasks = append(tasks, ruleTask{rule: RuleProcElseCase, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkProcElseCase(file)
		}})
	}
	if ruleSet[RuleUseSelectAll] {
		tasks = append(tasks, ruleTask{rule: RuleUseSelectAll, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUseSelectAll(file)
		}})
	}
	if ruleSet[RuleTruncTbl] {
		tasks = append(tasks, ruleTask{rule: RuleTruncTbl, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTruncTbl(file)
		}})
	}
	if ruleSet[RuleAnsiInJoin] {
		tasks = append(tasks, ruleTask{rule: RuleAnsiInJoin, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkAnsiInJoin(file)
		}})
	}
	if ruleSet[RuleDatatype] {
		tasks = append(tasks, ruleTask{rule: RuleDatatype, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkDatatype(parsed, file)
		}})
	}
	if ruleSet[RuleInsertRowLock] {
		tasks = append(tasks, ruleTask{rule: RuleInsertRowLock, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkInsertRowLock(file)
		}})
	}
	if ruleSet[RuleUseEqColumn] {
		tasks = append(tasks, ruleTask{rule: RuleUseEqColumn, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUseEqColumn(file)
		}})
	}
	if ruleSet[RuleTableFullScan] {
		tasks = append(tasks, ruleTask{rule: RuleTableFullScan, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTableFullScan(file)
		}})
	}
	if ruleSet[RuleTableHintExists] {
		tasks = append(tasks, ruleTask{rule: RuleTableHintExists, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTableHintExists(file)
		}})
	}
	if ruleSet[RuleTableHintIsRight] {
		tasks = append(tasks, ruleTask{rule: RuleTableHintIsRight, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTableHintIsRight(file)
		}})
	}
	if ruleSet[RuleIndexExistsInDB] {
		tasks = append(tasks, ruleTask{rule: RuleIndexExistsInDB, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkIndexExistsInDB(file)
		}})
	}
	if ruleSet[RuleIndexWrong] {
		tasks = append(tasks, ruleTask{rule: RuleIndexWrong, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkIndexWrong(file)
		}})
	}
	if ruleSet[RuleUpdateOnlyVar] {
		tasks = append(tasks, ruleTask{rule: RuleUpdateOnlyVar, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUpdateOnlyVar(file)
		}})
	}
	if ruleSet[RulePTableSpid] {
		tasks = append(tasks, ruleTask{rule: RulePTableSpid, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkPTableSpid(file)
		}})
	}
	if ruleSet[RuleForceOrder2Tbl] {
		tasks = append(tasks, ruleTask{rule: RuleForceOrder2Tbl, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkForceOrder2Tbl(file)
		}})
	}
	if ruleSet[RuleSaveTran] {
		tasks = append(tasks, ruleTask{rule: RuleSaveTran, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkSaveTran(file)
		}})
	}
	if ruleSet[RuleUseDrop] {
		tasks = append(tasks, ruleTask{rule: RuleUseDrop, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUseDrop(file)
		}})
	}
	if ruleSet[RuleMathOperations] {
		tasks = append(tasks, ruleTask{rule: RuleMathOperations, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkMathOperations(file)
		}})
	}
	if ruleSet[RuleExistsWithAndInIf] {
		tasks = append(tasks, ruleTask{rule: RuleExistsWithAndInIf, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkExistsWithAndInIf(file)
		}})
	}
	if ruleSet[RuleNullComparison] {
		tasks = append(tasks, ruleTask{rule: RuleNullComparison, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkNullComparison(file)
		}})
	}
	if ruleSet[RuleShouldBeCP866] {
		tasks = append(tasks, ruleTask{rule: RuleShouldBeCP866, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkShouldBeCP866(file)
		}})
	}
	if ruleSet[RuleTooManyJoins] {
		tasks = append(tasks, ruleTask{rule: RuleTooManyJoins, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkTooManyJoins(file)
		}})
	}
	if ruleSet[RuleMaxProcParam] {
		tasks = append(tasks, ruleTask{rule: RuleMaxProcParam, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkMaxProcParam(parsed, file)
		}})
	}
	if ruleSet[RuleModifyOutProc] {
		tasks = append(tasks, ruleTask{rule: RuleModifyOutProc, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkModifyOutProc(parsed, file)
		}})
	}
	if ruleSet[RuleEmptyReturn] {
		tasks = append(tasks, ruleTask{rule: RuleEmptyReturn, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkEmptyReturn(file)
		}})
	}
	if ruleSet[RuleRawTransactionControl] {
		tasks = append(tasks, ruleTask{rule: RuleRawTransactionControl, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkRawTransactionControl(file)
		}})
	}
	if ruleSet[RuleDeferredUpdate] {
		tasks = append(tasks, ruleTask{rule: RuleDeferredUpdate, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkDeferredUpdate(file)
		}})
	}
	if ruleSet[RuleInSubQuery] {
		tasks = append(tasks, ruleTask{rule: RuleInSubQuery, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkInSubQuery(file)
		}})
	}
	if ruleSet[RuleVarcharSize] {
		tasks = append(tasks, ruleTask{rule: RuleVarcharSize, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkVarcharSize(parsed, file)
		}})
	}
	if ruleSet[RuleColumnInsert] {
		tasks = append(tasks, ruleTask{rule: RuleColumnInsert, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkColumnInsert(file)
		}})
	}
	if ruleSet[RulePostgreLabelGotoLevel] {
		tasks = append(tasks, ruleTask{rule: RulePostgreLabelGotoLevel, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkPostgreLabelGotoLevel(file)
		}})
	}
	if ruleSet[RuleDateIntoString] {
		tasks = append(tasks, ruleTask{rule: RuleDateIntoString, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkDateIntoString(parsed, file)
		}})
	}
	if ruleSet[RuleEmptyStringDate] {
		tasks = append(tasks, ruleTask{rule: RuleEmptyStringDate, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkEmptyStringDate(parsed, file)
		}})
	}
	if ruleSet[RuleVarUseAfterCursor] {
		tasks = append(tasks, ruleTask{rule: RuleVarUseAfterCursor, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkVarUseAfterCursor(file)
		}})
	}
	if ruleSet[RuleExcessProcParams] {
		tasks = append(tasks, ruleTask{rule: RuleExcessProcParams, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkExcessProcParams(parsed, file)
		}})
	}
	if ruleSet[RuleDuplicateOutputVariable] {
		tasks = append(tasks, ruleTask{rule: RuleDuplicateOutputVariable, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkDuplicateOutputVariable(parsed, file)
		}})
	}
	if ruleSet[RuleUseOnlyDeclaredCursors] {
		tasks = append(tasks, ruleTask{rule: RuleUseOnlyDeclaredCursors, run: func(ctx context.Context) ([]Finding, error) {
			return r.checkUseOnlyDeclaredCursors(parsed, file)
		}})
	}
	return tasks
}

func (r *Runner) maxRuleWorkers(enabledRules int) int {
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

func runRuleTasks(tasks []ruleTask, maxWorkers int) ([]Finding, error) {
	if len(tasks) == 0 {
		return nil, nil
	}
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	type taskResult struct {
		findings []Finding
		err      error
	}

	ctx, cancel := context.WithCancel(context.Background())
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
					items, err := task.run(ctx)
					resultsCh <- taskResult{findings: items, err: err}
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

	findings := make([]Finding, 0)
	var firstErr error
	for result := range resultsCh {
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

func (r *Runner) fileContent(path string) ([]byte, error) {
	if r.exec != nil {
		if normalizePath(path) == r.exec.filePath {
			return r.exec.content, nil
		}
	}
	return os.ReadFile(path)
}

