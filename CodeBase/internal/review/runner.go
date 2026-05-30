package review

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store"
)

type Runner struct {
	db     *store.DB
	parser *sqlparser.Parser
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

	ruleSet := enabledRuleSet(opts.Rules)
	findings := make([]Finding, 0)

	if ruleSet[RuleForeignTablesUsing] {
		items, err := r.checkForeignTables(parsed, file, "t")
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleForeignPTablesUsing] {
		items, err := r.checkForeignPTables(parsed, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleForeignProcedureUsing] {
		items, err := r.checkForeignProcedures(parsed, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleExecNotExistsProc] {
		items, err := r.checkExecNotExistsProcedures(parsed, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleProcDuplicate] {
		items, err := r.checkProcDuplicate(parsed, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleProcParamDefValue] {
		items, err := r.checkProcParamDefValue(parsed, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleProcElseCase] {
		items, err := r.checkProcElseCase(file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleUseSelectAll] {
		items, err := r.checkUseSelectAll(file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleTruncTbl] {
		items, err := r.checkTruncTbl(file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleAnsiInJoin] {
		items, err := r.checkAnsiInJoin(file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleDatatype] {
		items := r.checkDatatype(parsed, file)
		findings = append(findings, items...)
	}
	if ruleSet[RuleInsertRowLock] {
		items, err := r.checkInsertRowLock(file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
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






