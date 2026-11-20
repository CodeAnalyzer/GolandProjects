package review

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store"
)

// ruleCase — один кейс проверки правила: SQL-текст и ожидаемый результат.
type ruleCase struct {
	name       string
	content    string
	wantCount  int
	wantLine   int
	wantObject string
}

// validateRuleCases проверяет, что набор покрывает и положительный, и
// отрицательный кейсы.
func validateRuleCases(rule RuleID, cases []ruleCase) error {
	if len(cases) == 0 {
		return fmt.Errorf("%s: empty case set", rule)
	}
	hasPositive := false
	hasNegative := false
	for _, tc := range cases {
		if tc.wantCount > 0 {
			hasPositive = true
		}
		if tc.wantCount == 0 {
			hasNegative = true
		}
	}
	if !hasPositive {
		return fmt.Errorf("%s: case set has no positive case", rule)
	}
	if !hasNegative {
		return fmt.Errorf("%s: case set has no negative case", rule)
	}
	return nil
}

func newRuleTestFile(t *testing.T, content string) *indexedFile {
	t.Helper()
	f, err := os.CreateTemp("", "review_rule_*.sql")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		t.Fatalf("write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	return &indexedFile{Path: f.Name(), DsProductID: 1}
}

func newRuleTestExec(path, content string) *reviewExecContext {
	return &reviewExecContext{
		filePath:    normalizePath(path),
		content:     []byte(content),
		rawContent:  []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}
}

func assertRuleFindings(t *testing.T, rule RuleID, tc ruleCase, findings []Finding) {
	t.Helper()
	if len(findings) != tc.wantCount {
		t.Fatalf("%s: findings count = %d, want %d: %#v", tc.name, len(findings), tc.wantCount, findings)
	}
	if tc.wantCount == 0 {
		return
	}
	for _, f := range findings {
		if f.Rule != rule {
			t.Fatalf("%s: finding rule = %q, want %q", tc.name, f.Rule, rule)
		}
		if meta, ok := ruleCatalog[rule]; ok && f.Severity != meta.Severity {
			t.Fatalf("%s: finding severity = %d, want %d", tc.name, f.Severity, meta.Severity)
		}
	}
	if tc.wantLine > 0 && findings[0].Line != tc.wantLine {
		t.Fatalf("%s: finding line = %d, want %d", tc.name, findings[0].Line, tc.wantLine)
	}
	if tc.wantObject != "" && !strings.EqualFold(findings[0].Object, tc.wantObject) {
		t.Fatalf("%s: finding object = %q, want %q", tc.name, findings[0].Object, tc.wantObject)
	}
}

// runFileRuleCases прогоняет кейсы правила вида check(ctx, file).
func runFileRuleCases(t *testing.T, runner *Runner, rule RuleID, check func(context.Context, *indexedFile) ([]Finding, error), cases []ruleCase) {
	t.Helper()
	if err := validateRuleCases(rule, cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner.exec = newRuleTestExec(file.Path, tc.content)
			findings, err := check(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, rule, tc, findings)
		})
	}
}

// runParsedRuleCases прогоняет кейсы правила вида check(ctx, parsed, file).
func runParsedRuleCases(t *testing.T, runner *Runner, rule RuleID, check func(context.Context, *sqlparser.ParseResult, *indexedFile) ([]Finding, error), cases []ruleCase) {
	t.Helper()
	if err := validateRuleCases(rule, cases); err != nil {
		t.Fatal(err)
	}
	parser := sqlparser.NewParser()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse content: %v", err)
			}
			file := newRuleTestFile(t, tc.content)
			runner.exec = newRuleTestExec(file.Path, tc.content)
			findings, err := check(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, rule, tc, findings)
		})
	}
}

func TestRuleCaseHelper_ValidateCoverage(t *testing.T) {
	rule := RuleTruncTbl
	cases := []struct {
		name    string
		set     []ruleCase
		wantErr bool
	}{
		{name: "empty", set: nil, wantErr: true},
		{name: "positive only", set: []ruleCase{{name: "p", content: "x", wantCount: 1}}, wantErr: true},
		{name: "negative only", set: []ruleCase{{name: "n", content: "x", wantCount: 0}}, wantErr: true},
		{name: "both", set: []ruleCase{{name: "p", content: "x", wantCount: 1}, {name: "n", content: "x", wantCount: 0}}, wantErr: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRuleCases(rule, tc.set)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestFindAPITableNames_NilDB_NoPanic(t *testing.T) {
	r := &Runner{}
	got, err := r.findAPITableNames(context.Background(), []string{"pTables"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result, got %#v", got)
	}
}

func TestLookupProcedureCreateFiles_NilDB_NoPanic(t *testing.T) {
	r := &Runner{}
	got, err := r.lookupProcedureCreateFiles(context.Background(), "SomeProc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no files, got %#v", got)
	}
}

func TestCheckProcElseCase_Coverage(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleProcElseCase, runner.checkProcElseCase, []ruleCase{
		{name: "case without else", content: "select case when a = 1 then 1 end from t", wantCount: 1, wantLine: 1},
		{name: "case with else", content: "select case when a = 1 then 1 else 0 end from t", wantCount: 0},
		{name: "case in line comment", content: "-- select case when a = 1 then 1 end", wantCount: 0},
		{name: "case with else multiline", content: "select\n  case\n    when a = 1 then 1\n    else 0\n  end\nfrom t", wantCount: 0},
		{name: "nested case inside else", content: "select case when a = 1 then case when b = 1 then 1 end else 0 end from t", wantCount: 0},
	})
}

func TestCheckTruncTbl_Coverage(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleTruncTbl, runner.checkTruncTbl, []ruleCase{
		{name: "truncate in procedure", content: "create proc P as\nbegin\n  truncate table pFoo\nend", wantCount: 1, wantLine: 3, wantObject: "pFoo"},
		{name: "procedure without truncate", content: "create proc P as\nbegin\n  select 1\nend", wantCount: 0},
		{name: "line comment", content: "-- truncate table pFoo", wantCount: 0},
	})
}

func TestCheckUseSelectAll_Coverage(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleUseSelectAll, runner.checkUseSelectAll, []ruleCase{
		{name: "select star", content: "select * from t", wantCount: 1, wantLine: 1},
		{name: "explicit columns", content: "select id, name from t", wantCount: 0},
		{name: "comment", content: "-- select * from t", wantCount: 0},
	})
}

func TestCheckIndexExistsInDB_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "missing index", content: "select * from pT t M_NOLOCK_INDEX(XPK_missing) where t.SPID = 1", wantCount: 1, wantLine: 1},
		{name: "existing index", content: "select * from pT t M_NOLOCK_INDEX(XPKpT) where t.SPID = 1", wantCount: 0},
		{name: "delete ptable existing index", content: "M_DELETE_PTABLE_INDEX(pT, XPKpT)", wantCount: 0},
	}
	if err := validateRuleCases(RuleIndexExistsInDB, cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.indexCandCache = map[string][]tableIndexCandidate{
				"pt": {{Name: "XPKpT", Fields: []string{"spid"}}},
			}
			findings, err := runner.checkIndexExistsInDB(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleIndexExistsInDB, tc, findings)
		})
	}
}

func TestCheckDeferredUpdate_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "index column updated", content: "update pFoo set KeyCol = @v from pFoo f M_UPDLOCK_INDEX(XPKpFoo) where f.SPID = 1", wantCount: 1, wantLine: 1},
		{name: "non index column updated", content: "update pFoo set OtherCol = @v from pFoo f M_UPDLOCK_INDEX(XPKpFoo) where f.SPID = 1", wantCount: 0},
		{name: "update without index hint", content: "update pFoo set KeyCol = @v", wantCount: 0},
		{name: "temp table", content: "update #tmp set KeyCol = @v from #tmp f M_UPDLOCK_INDEX(XPKtmp) where f.SPID = 1", wantCount: 0},
	}
	if err := validateRuleCases(RuleDeferredUpdate, cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.indexFieldsCache = map[string][]string{"xpkpfoo": {"keycol", "spid"}}
			findings, err := runner.checkDeferredUpdate(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleDeferredUpdate, tc, findings)
		})
	}
}

func TestCheckDuplicateOutputVariable_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "same var for two out params", content: "exec pDup @a out, @a out", wantCount: 1},
		{name: "different vars", content: "exec pDup @a out, @b out", wantCount: 0},
		{name: "single out param", content: "exec pDup @a out, @b", wantCount: 0},
	}
	if err := validateRuleCases(RuleDuplicateOutputVariable, cases); err != nil {
		t.Fatal(err)
	}
	parser := sqlparser.NewParser()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.procParamsCache = map[string][]model.SQLParam{
				"pdup": {
					{Name: "p1", Direction: "out"},
					{Name: "p2", Direction: "out"},
				},
			}
			findings, err := runner.checkDuplicateOutputVariable(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleDuplicateOutputVariable, tc, findings)
		})
	}
}

func TestCheckFloatToStringConvert_Coverage(t *testing.T) {
	runner := &Runner{}
	runParsedRuleCases(t, runner, RuleFloatToStringConvert, runner.checkFloatToStringConvert, []ruleCase{
		{name: "convert float to varchar", content: "declare @f float\nselect convert(varchar(10), @f) from t", wantCount: 1},
		{name: "convert int to varchar", content: "declare @i int\nselect convert(varchar(10), @i) from t", wantCount: 0},
		{name: "cast float to varchar", content: "declare @f float\nselect cast(@f as varchar(10)) from t", wantCount: 1},
		{name: "convert float to numeric", content: "declare @f float\nselect convert(numeric(18,2), @f) from t", wantCount: 0},
	})
}

func TestCheckSelectAfterSetRowcount_Coverage(t *testing.T) {
	cases := []struct {
		name      string
		fragments []*model.QueryFragment
		wantCount int
	}{
		{
			name: "select after rowcount",
			fragments: []*model.QueryFragment{
				{QueryText: "set rowcount 10", LineNumber: 1},
				{QueryText: "select @a = col from t", LineNumber: 2},
			},
			wantCount: 1,
		},
		{
			name: "select without rowcount",
			fragments: []*model.QueryFragment{
				{QueryText: "select @a = col from t", LineNumber: 1},
			},
			wantCount: 0,
		},
		{
			name: "rowcount reset",
			fragments: []*model.QueryFragment{
				{QueryText: "set rowcount 10", LineNumber: 1},
				{QueryText: "set rowcount 0", LineNumber: 2},
				{QueryText: "select @a = col from t", LineNumber: 3},
			},
			wantCount: 0,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			content := "select 1"
			file := newRuleTestFile(t, content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, content)
			parsed := &sqlparser.ParseResult{Fragments: tc.fragments}
			findings, err := runner.checkSelectAfterSetRowcount(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleSelectAfterSetRowcount, ruleCase{name: tc.name, wantCount: tc.wantCount}, findings)
		})
	}
}

func TestCheckIndexWrong_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "suboptimal index", content: "select * from pT t M_NOLOCK_INDEX(XIE1pT) where t.SPID = 1", wantCount: 1, wantLine: 1},
		{name: "optimal index", content: "select * from pT t M_NOLOCK_INDEX(XPKpT) where t.SPID = 1", wantCount: 0},
		{name: "no condition columns", content: "select * from pT t M_NOLOCK_INDEX(XIE1pT)", wantCount: 0},
	}
	if err := validateRuleCases(RuleIndexWrong, cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.indexCandCache = map[string][]tableIndexCandidate{
				"pt": {
					{Name: "XPKpT", Fields: []string{"spid"}},
					{Name: "XIE1pT", Fields: []string{"other"}},
				},
			}
			findings, err := runner.checkIndexWrong(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleIndexWrong, tc, findings)
		})
	}
}

func TestCheckIndexWrong_VariableInConditionIsNotColumn(t *testing.T) {
	const prefix = "delete pBPConsCoolingOff\n" +
		"  from pBPConsCoolingOff M_ROWLOCK_INDEX(XPKpBPConsCoolingOff)\n" +
		" where SPID = @@spid\n"

	cases := []ruleCase{
		{
			name:      "self compared variable is not a column predicate",
			content:   prefix + "   and @BankProductID = @BankProductID\nM_KEEPPLAN\n",
			wantCount: 0,
		},
		{
			name:       "real column predicate suggests XIE1",
			content:    prefix + "   and BankProductID = @BankProductID\nM_KEEPPLAN\n",
			wantCount:  1,
			wantLine:   1,
			wantObject: "pbpconscoolingoff.xpkpbpconscoolingoff",
		},
	}
	if err := validateRuleCases(RuleIndexWrong, cases); err != nil {
		t.Fatal(err)
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.indexCandCache = map[string][]tableIndexCandidate{
				"pbpconscoolingoff": {
					{Name: "XPKpBPConsCoolingOff", Fields: []string{"SPID", "AutoID"}, IsUnique: true},
					{Name: "XIE1pBPConsCoolingOff", Fields: []string{"SPID", "BankProductID", "AmountMin"}},
				},
			}
			findings, err := runner.checkIndexWrong(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleIndexWrong, tc, findings)
		})
	}
}

func TestCheckForeignPTablesUsing_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "foreign p table", content: "select * from pForeign", wantCount: 1, wantObject: "pForeign"},
		{name: "own p table", content: "select * from pOwn", wantCount: 0},
		{name: "t table", content: "select * from tForeign", wantCount: 0},
	}
	if err := validateRuleCases(RuleForeignPTablesUsing, cases); err != nil {
		t.Fatal(err)
	}
	parser := sqlparser.NewParser()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.tableProductIDCache = map[string]map[int64]struct{}{
				"pforeign": {2: {}},
				"pown":     {1: {}},
				"tforeign": {2: {}},
			}
			findings, err := runner.checkForeignPTables(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleForeignPTablesUsing, tc, findings)
		})
	}
}

func TestCheckDatatype_BranchesCoverage(t *testing.T) {
	parser := sqlparser.NewParser()
	type branchCase struct {
		name       string
		content    string
		colTypes   map[string]string
		columnDefs []*model.SQLColumnDefinition
		fragments  []*model.QueryFragment
		procParams map[string]model.SQLParam
		wantCount  int
	}
	cases := []branchCase{
		{
			name:    "column definitions conflict",
			content: "select 1",
			columnDefs: []*model.SQLColumnDefinition{
				{TableName: "pA", ColumnName: "Col", DataType: "int", LineNumber: 1},
				{TableName: "pA", ColumnName: "Col", DataType: "varchar(10)", LineNumber: 2},
			},
			wantCount: 1,
		},
		{
			name:    "column definitions same type",
			content: "select 1",
			columnDefs: []*model.SQLColumnDefinition{
				{TableName: "pA", ColumnName: "Col", DataType: "int", LineNumber: 1},
				{TableName: "pA", ColumnName: "Col", DataType: "int", LineNumber: 2},
			},
			wantCount: 0,
		},
		{
			name:      "select assign numeric to int",
			content:   "declare @target int, @src numeric(18,2)\nselect @target = @src from t",
			fragments: []*model.QueryFragment{{QueryText: "select @target = @src from t", LineNumber: 2}},
			wantCount: 1,
		},
		{
			name:      "select assign int to int",
			content:   "declare @target int, @src int\nselect @target = @src from t",
			fragments: []*model.QueryFragment{{QueryText: "select @target = @src from t", LineNumber: 2}},
			wantCount: 0,
		},
		{
			name:      "update set numeric to int",
			content:   "update tSet set Col = src.Val from tSet s inner join tSrc src on src.ID = s.ID",
			colTypes:  map[string]string{"tset|col": "int", "tsrc|val": "numeric(18,2)", "src|val": "numeric(18,2)"},
			wantCount: 1,
		},
		{
			name:      "update set int to int",
			content:   "update tSet set Col = src.Val from tSet s inner join tSrc src on src.ID = s.ID",
			colTypes:  map[string]string{"tset|col": "int", "tsrc|val": "int", "src|val": "int"},
			wantCount: 0,
		},
		{
			name:      "insert select numeric to int",
			content:   "insert into tIns (Col) select src.Val from tSrc src",
			colTypes:  map[string]string{"tins|col": "int", "tsrc|val": "numeric(18,2)", "src|val": "numeric(18,2)"},
			wantCount: 1,
		},
		{
			name:      "insert select int to int",
			content:   "insert into tIns (Col) select src.Val from tSrc src",
			colTypes:  map[string]string{"tins|col": "int", "tsrc|val": "int", "src|val": "int"},
			wantCount: 0,
		},
		{
			name:      "fetch into numeric to int",
			content:   "declare @target int\ndeclare c cursor for select Val from tSrc\nfetch next from c into @target",
			colTypes:  map[string]string{"tsrc|val": "numeric(18,2)"},
			wantCount: 1,
		},
		{
			name:      "fetch into int to int",
			content:   "declare @target int\ndeclare c cursor for select Val from tSrc\nfetch next from c into @target",
			colTypes:  map[string]string{"tsrc|val": "int"},
			wantCount: 0,
		},
		{
			name:       "exec param numeric to int",
			content:    "declare @target numeric(18,2)\nexec pIns @target",
			procParams: map[string]model.SQLParam{"p1": {Name: "p1", Type: "int", Direction: "in"}},
			wantCount:  1,
		},
		{
			name:       "exec param int to int",
			content:    "declare @target int\nexec pIns @target",
			procParams: map[string]model.SQLParam{"p1": {Name: "p1", Type: "int", Direction: "in"}},
			wantCount:  0,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if tc.columnDefs != nil {
				parsed.ColumnDefinitions = tc.columnDefs
			}
			if tc.fragments != nil {
				parsed.Fragments = tc.fragments
			}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.colTypeCache = tc.colTypes
			if tc.procParams != nil {
				runner.procParamsCache = map[string][]model.SQLParam{"pins": {{Name: "p1", Type: "int", Direction: "in"}}}
			}
			findings, err := runner.checkDatatype(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleDatatype, ruleCase{name: tc.name, wantCount: tc.wantCount}, findings)
		})
	}
}

func TestCheckExecNotExistsProcedures_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "missing procedure", content: "exec pMissing", wantCount: 1, wantObject: "pMissing"},
		{name: "existing procedure", content: "exec pExists", wantCount: 0},
	}
	if err := validateRuleCases(RuleExecNotExistsProc, cases); err != nil {
		t.Fatal(err)
	}
	parser := sqlparser.NewParser()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.procProductIDCache = map[string]store.ProcedureScopeInfo{
				"pexists": {ProductID: 1, ExistsInArea: true},
			}
			findings, err := runner.checkExecNotExistsProcedures(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleExecNotExistsProc, tc, findings)
		})
	}
}

func TestCheckProcDuplicate_Coverage(t *testing.T) {
	content := "create proc pDup as\nbegin\n  select 1\nend\ngo\ncreate proc pDup as\nbegin\n  select 2\nend"
	negative := "create proc pSingle as\nbegin\n  select 1\nend"
	cases := []ruleCase{
		{name: "within file duplicate", content: content, wantCount: 1, wantLine: 6, wantObject: "pDup"},
		{name: "single procedure", content: negative, wantCount: 0},
	}
	if err := validateRuleCases(RuleProcDuplicate, cases); err != nil {
		t.Fatal(err)
	}
	parser := sqlparser.NewParser()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			findings, err := runner.checkProcDuplicate(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleProcDuplicate, tc, findings)
		})
	}
}

func TestCheckMemoryUniqueIndexUpdate_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "unique index column updated", content: "update pMem set KeyCol = 1", wantCount: 1, wantObject: "pmem"},
		{name: "non index column updated", content: "update pMem set OtherCol = 1", wantCount: 0},
		{name: "not in memory", content: "update pOther set KeyCol = 1", wantCount: 0},
	}
	if err := validateRuleCases(RuleMemoryUniqueIndexUpdate, cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.memoryTableCache = map[string]struct{}{"pmem": {}}
			runner.memoryTablesLoaded = true
			runner.indexCandCache = map[string][]tableIndexCandidate{
				"pmem": {{Name: "XPKpMem", Fields: []string{"keycol"}, IsUnique: true}},
			}
			findings, err := runner.checkMemoryUniqueIndexUpdate(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleMemoryUniqueIndexUpdate, tc, findings)
		})
	}
}

func TestCheckForeignTablesUsing_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "foreign t table", content: "select * from tForeign", wantCount: 1, wantObject: "tForeign"},
		{name: "own t table", content: "select * from tOwn", wantCount: 0},
		{name: "p table ignored", content: "select * from pForeign", wantCount: 0},
	}
	if err := validateRuleCases(RuleForeignTablesUsing, cases); err != nil {
		t.Fatal(err)
	}
	parser := sqlparser.NewParser()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.tableProductIDCache = map[string]map[int64]struct{}{
				"tforeign": {2: {}},
				"town":     {1: {}},
			}
			findings, err := runner.checkForeignTables(context.Background(), parsed, file, "t")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleForeignTablesUsing, tc, findings)
		})
	}
}

func TestCheckAliasWhenUsingUnion_Coverage(t *testing.T) {
	runner := &Runner{}
	runParsedRuleCases(t, runner, RuleAliasWhenUsingUnion, runner.checkAliasWhenUsingUnion, []ruleCase{
		{name: "order by alias missing in first select", content: "select ColA from t1 union all select ColB from t2 order by ColC", wantCount: 1, wantObject: "ColC"},
		{name: "order by first select column", content: "select ColA from t1 union all select ColB from t2 order by ColA", wantCount: 0},
	})
}

func TestCheckColumnInsert_Coverage(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleColumnInsert, runner.checkColumnInsert, []ruleCase{
		{name: "insert without columns", content: "insert into pFoo values (1)", wantCount: 1, wantObject: "pFoo"},
		{name: "insert with columns", content: "insert into pFoo (Col) values (1)", wantCount: 0},
		{name: "comment", content: "-- insert into pFoo values (1)", wantCount: 0},
	})
}

func TestCheckExistsWithAndInIf_Coverage(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleExistsWithAndInIf, runner.checkExistsWithAndInIf, []ruleCase{
		{name: "exists with and", content: "if exists(select 1 from t where t.ID = 1) and @flag = 1\nbegin\nselect 1\nend", wantCount: 1},
		{name: "exists without and", content: "if exists(select 1 from t where t.ID = 1)\nbegin\nselect 1\nend", wantCount: 0},
		{name: "and inside subquery", content: "if exists(select 1 from t where a = 1 and b = 2)\nbegin\nselect 1\nend", wantCount: 0},
	})
}

func TestCheckInSubQuery_Coverage(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleInSubQuery, runner.checkInSubQuery, []ruleCase{
		{name: "in subquery", content: "select * from t where ID in (select ID from s)", wantCount: 1},
		{name: "in constant list", content: "select * from t where ID in (1, 2, 3)", wantCount: 0},
		{name: "in variable", content: "select * from t where ID in (@id)", wantCount: 0},
		{name: "exists", content: "select * from t where exists (select 1 from s where s.ID = t.ID)", wantCount: 0},
	})
}

func TestCheckIsNullSameTypes_Coverage(t *testing.T) {
	cases := []struct {
		name      string
		content   string
		fragment  string
		wantCount int
	}{
		{
			name:      "different types",
			content:   "declare @a int, @b varchar(10)\nselect isnull(@a, @b) from t",
			fragment:  "select isnull(@a, @b) from t",
			wantCount: 1,
		},
		{
			name:      "same types",
			content:   "declare @a int, @b int\nselect isnull(@a, @b) from t",
			fragment:  "select isnull(@a, @b) from t",
			wantCount: 0,
		},
		{
			name:      "second argument literal",
			content:   "declare @a int\nselect isnull(@a, 0) from t",
			fragment:  "select isnull(@a, 0) from t",
			wantCount: 0,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := sqlparser.NewParser().ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			parsed.Fragments = []*model.QueryFragment{{QueryText: tc.fragment, LineNumber: 2}}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			findings, err := runner.checkIsNullSameTypes(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleIsNullSameTypes, ruleCase{name: tc.name, wantCount: tc.wantCount}, findings)
		})
	}
}

func TestCheckMathOperations_Coverage(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleMathOperations, runner.checkMathOperations, []ruleCase{
		{name: "arithmetic in comparison", content: "select * from t where\n  @limit > t.A + 1", wantCount: 1},
		{name: "plain comparison", content: "select * from t where\n  @limit > t.A", wantCount: 0},
		{name: "converted comparison", content: "select * from t where\n  @limit > convert(int, t.A)", wantCount: 0},
	})
}

func TestCheckProcParamDefValue_Coverage(t *testing.T) {
	cases := []struct {
		name         string
		defaultValue string
		body         string
		wantCount    int
	}{
		{
			name:         "param default null without assignment",
			defaultValue: "null",
			body:         "create proc P (@x int = null) as\nbegin\n  select @x\nend",
			wantCount:    1,
		},
		{
			name:         "param has default",
			defaultValue: "0",
			body:         "create proc P (@x int = 0) as\nbegin\n  select @x\nend",
			wantCount:    0,
		},
		{
			name:         "assigned before use",
			defaultValue: "null",
			body:         "create proc P (@x int = null)\nas\nbegin\n  select @x = 1\n  select @x\nend",
			wantCount:    0,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed := &sqlparser.ParseResult{Procedures: []*model.SQLProcedure{{
				ProcName:  "P",
				LineStart: 1,
				LineEnd:   len(strings.Split(tc.body, "\n")),
				Params:    []model.SQLParam{{Name: "@x", Type: "int", DefaultValue: tc.defaultValue}},
			}}}
			file := newRuleTestFile(t, tc.body)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.body)
			findings, err := runner.checkProcParamDefValue(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleProcParamDefValue, ruleCase{name: tc.name, wantCount: tc.wantCount, wantObject: "@x"}, findings)
		})
	}
}

func TestCheckSaveTran_Coverage(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleSaveTran, runner.checkSaveTran, []ruleCase{
		{name: "save tran", content: "save tran sp1", wantCount: 1},
		{name: "save transaction", content: "save transaction sp1", wantCount: 1},
		{name: "no save tran", content: "begin tran\ncommit", wantCount: 0},
		{name: "comment", content: "-- save tran sp1", wantCount: 0},
	})
}

func TestCheckUseDrop_Coverage(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleUseDrop, runner.checkUseDrop, []ruleCase{
		{name: "drop inside procedure", content: "create proc P as\nbegin\n  drop table #tmp\nend", wantCount: 1},
		{name: "drop outside procedure", content: "drop table #tmp", wantCount: 0},
		{name: "comment", content: "-- drop table #tmp", wantCount: 0},
	})
}

func TestCheckUseFuncInIndCol_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "function over indexed column", content: "select * from pT t M_NOLOCK_INDEX(XPKpT) where abs(t.SPID) = 1", wantCount: 1},
		{name: "indexed column without function", content: "select * from pT t M_NOLOCK_INDEX(XPKpT) where t.SPID = 1", wantCount: 0},
		{name: "function over non indexed column", content: "select * from pT t M_NOLOCK_INDEX(XPKpT) where abs(t.Other) = 1", wantCount: 0},
	}
	if err := validateRuleCases(RuleUseFuncInIndCol, cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.indexFieldsCache = map[string][]string{"xpkpt": {"spid"}}
			findings, err := runner.checkUseFuncInIndCol(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleUseFuncInIndCol, tc, findings)
		})
	}
}

func TestCheckVarcharSize_Coverage(t *testing.T) {
	cases := []struct {
		name      string
		paramType string
		wantCount int
	}{
		{name: "varchar without size", paramType: "varchar", wantCount: 1},
		{name: "varchar with size", paramType: "varchar(10)", wantCount: 0},
		{name: "nvarchar max", paramType: "nvarchar(max)", wantCount: 0},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			content := "create proc P (@a " + tc.paramType + ") as\nbegin\n  select 1\nend"
			parsed := &sqlparser.ParseResult{Procedures: []*model.SQLProcedure{{
				ProcName:  "P",
				LineStart: 1,
				LineEnd:   4,
				Params:    []model.SQLParam{{Name: "@a", Type: tc.paramType}},
			}}}
			file := newRuleTestFile(t, content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, content)
			findings, err := runner.checkVarcharSize(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleVarcharSize, ruleCase{name: tc.name, wantCount: tc.wantCount, wantObject: "P"}, findings)
		})
	}
}

func TestCheckExcessProcParams_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "extra named param", content: "exec pEx @a = 1, @b = 2, @c = 3", wantCount: 1, wantObject: "pEx"},
		{name: "duplicate named param", content: "exec pEx @a = 1, @a = 2", wantCount: 1},
		{name: "extra positional param", content: "exec pEx 1, 2, 3", wantCount: 1},
		{name: "valid params", content: "exec pEx @a = 1, @b = 2", wantCount: 0},
	}
	if err := validateRuleCases(RuleExcessProcParams, cases); err != nil {
		t.Fatal(err)
	}
	parser := sqlparser.NewParser()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.procParamsCache = map[string][]model.SQLParam{
				"pex": {
					{Name: "a", Type: "int", Direction: "in"},
					{Name: "b", Type: "int", Direction: "in"},
				},
			}
			findings, err := runner.checkExcessProcParams(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleExcessProcParams, tc, findings)
		})
	}
}

func TestCheckDiffTypesComparison_NegativeCoverage(t *testing.T) {
	cases := []struct {
		name     string
		content  string
		fragment string
	}{
		{
			name:     "compatible variable types",
			content:  "declare @a int, @b int\nselect * from t where @a = @b",
			fragment: "select * from t where @a = @b",
		},
		{
			name:     "comparison with literal",
			content:  "declare @a int\nselect * from t where @a = 1",
			fragment: "select * from t where @a = 1",
		},
		{
			name:     "comparison with string literal",
			content:  "declare @a varchar(10)\nselect * from t where @a = 'x'",
			fragment: "select * from t where @a = 'x'",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := sqlparser.NewParser().ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			parsed.Fragments = []*model.QueryFragment{{QueryText: tc.fragment, LineNumber: 2}}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			findings, err := runner.checkDiffTypesComparison(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != 0 {
				t.Fatalf("%s: expected no findings, got %#v", tc.name, findings)
			}
		})
	}
}

func TestCheckForeignProcedures_Coverage(t *testing.T) {
	cases := []ruleCase{
		{name: "foreign procedure", content: "exec pForeignProc", wantCount: 1, wantObject: "pForeignProc"},
		{name: "own procedure", content: "exec pOwnProc", wantCount: 0},
	}
	if err := validateRuleCases(RuleForeignProcedureUsing, cases); err != nil {
		t.Fatal(err)
	}
	parser := sqlparser.NewParser()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseContent(tc.content)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.procProductIDCache = map[string]store.ProcedureScopeInfo{
				"pforeignproc": {ProductID: 2, ExistsInArea: true},
				"pownproc":     {ProductID: 1, ExistsInArea: true},
			}
			findings, err := runner.checkForeignProcedures(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleForeignProcedureUsing, tc, findings)
		})
	}
}
