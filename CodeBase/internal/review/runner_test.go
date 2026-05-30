package review

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
)

func TestIsSharedTTable(t *testing.T) {
	cases := []struct {
		name      string
		tableName string
		want      bool
	}{
		{name: "shared tContract", tableName: "tContract", want: true},
		{name: "shared uppercase", tableName: "TDEAL", want: true},
		{name: "shared with spaces", tableName: "  tManyNumber  ", want: true},
		{name: "non shared", tableName: "tActionPlan", want: false},
		{name: "p table", tableName: "pAPI_TaskPlan_List", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSharedTTable(tc.tableName); got != tc.want {
				t.Fatalf("isSharedTTable(%q) = %v, want %v", tc.tableName, got, tc.want)
			}
		})
	}
}

func TestDedupeProcedureCalls_FiltersKeywordsAndDeduplicates(t *testing.T) {
	calls := []*model.SQLProcedureCall{
		{CalleeName: "DateCorrectByClnFrw", LineNumber: 97},
		{CalleeName: "on", LineNumber: 216},
		{CalleeName: "DateCorrectByClnFrw", LineNumber: 97},
		{CalleeName: "Signature_Carry", LineNumber: 202},
		nil,
	}

	got := dedupeProcedureCalls(calls)
	want := []procedureRef{
		{Name: "DateCorrectByClnFrw", Line: 97},
		{Name: "Signature_Carry", Line: 202},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dedupeProcedureCalls(...) = %#v, want %#v", got, want)
	}
}

func TestDedupeTableRefs_FilterByPrefixAndDeduplicate(t *testing.T) {
	tables := []*model.SQLTable{
		{TableName: "tActionPlan", LineNumber: 10},
		{TableName: "TActionPlan", LineNumber: 10},
		{TableName: "tActionPeriod", LineNumber: 11},
		{TableName: "pAPI_TaskPlan_List", LineNumber: 20},
		{TableName: "tActionPlan", LineNumber: 12},
		nil,
	}

	got := dedupeTableRefs(tables, "t")
	want := []tableRef{
		{Name: "tActionPlan", Line: 10},
		{Name: "tActionPeriod", Line: 11},
		{Name: "tActionPlan", Line: 12},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dedupeTableRefs(...) = %#v, want %#v", got, want)
	}
}

func TestEnabledRuleSet_CustomSubset(t *testing.T) {
	rules := []RuleID{RuleForeignTablesUsing, RuleDatatype}
	set := enabledRuleSet(rules)

	if !set[RuleForeignTablesUsing] {
		t.Fatalf("RuleForeignTablesUsing should be enabled")
	}
	if !set[RuleDatatype] {
		t.Fatalf("RuleDatatype should be enabled")
	}
	if set[RuleForeignPTablesUsing] {
		t.Fatalf("RuleForeignPTablesUsing should be disabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
	if set[RuleExecNotExistsProc] {
		t.Fatalf("RuleExecNotExistsProc should be disabled")
	}
}

func TestEnabledRuleSet_ExecNotExistsProc(t *testing.T) {
	rules := []RuleID{RuleExecNotExistsProc}
	set := enabledRuleSet(rules)

	if !set[RuleExecNotExistsProc] {
		t.Fatalf("RuleExecNotExistsProc should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ProcDuplicate(t *testing.T) {
	rules := []RuleID{RuleProcDuplicate}
	set := enabledRuleSet(rules)

	if !set[RuleProcDuplicate] {
		t.Fatalf("RuleProcDuplicate should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ProcParamDefValue(t *testing.T) {
	rules := []RuleID{RuleProcParamDefValue}
	set := enabledRuleSet(rules)

	if !set[RuleProcParamDefValue] {
		t.Fatalf("RuleProcParamDefValue should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ProcElseCase(t *testing.T) {
	rules := []RuleID{RuleProcElseCase}
	set := enabledRuleSet(rules)

	if !set[RuleProcElseCase] {
		t.Fatalf("RuleProcElseCase should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_UseSelectAll(t *testing.T) {
	rules := []RuleID{RuleUseSelectAll}
	set := enabledRuleSet(rules)

	if !set[RuleUseSelectAll] {
		t.Fatalf("RuleUseSelectAll should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_TruncTbl(t *testing.T) {
	rules := []RuleID{RuleTruncTbl}
	set := enabledRuleSet(rules)

	if !set[RuleTruncTbl] {
		t.Fatalf("RuleTruncTbl should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_AnsiInJoin(t *testing.T) {
	rules := []RuleID{RuleAnsiInJoin}
	set := enabledRuleSet(rules)

	if !set[RuleAnsiInJoin] {
		t.Fatalf("RuleAnsiInJoin should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestParseInsertSelectStatement_WithCaseExpression(t *testing.T) {
	query := `insert pCons_AutoFullPrepDate (
		SPID,
		Date
	)
	select @@SPID,
		case when cc.Flag2 > 0 then cc.CreditDateFrom else cr.DateFrom end
	from tContractCredit cc
	join tCtrCtrRelation cr on cr.ContractID = cc.ContractCreditID`

	stmt, ok := parseInsertSelectStatement(query)
	if !ok {
		t.Fatalf("parseInsertSelectStatement should parse INSERT...SELECT")
	}
	if stmt.TargetTable != "pCons_AutoFullPrepDate" {
		t.Fatalf("target table = %q, want pCons_AutoFullPrepDate", stmt.TargetTable)
	}
	if len(stmt.TargetColumns) != 2 {
		t.Fatalf("target columns count = %d, want 2", len(stmt.TargetColumns))
	}
	if len(stmt.SelectExpressions) != 2 {
		t.Fatalf("select expressions count = %d, want 2", len(stmt.SelectExpressions))
	}
}

func TestSplitTopLevelCSV_RespectsCaseCommas(t *testing.T) {
	value := "@@SPID, case when a = 1 then b, c else d end, cr.DateFrom"
	items := splitTopLevelCSV(value)
	if len(items) != 3 {
		t.Fatalf("splitTopLevelCSV items = %d, want 3: %#v", len(items), items)
	}
}

func TestIsPotentialPrecisionLoss_DateTimeToOperDay(t *testing.T) {
	if !isPotentialPrecisionLoss("DSDATETIME", "DSOPERDAY") {
		t.Fatalf("expected precision loss for DSDATETIME -> DSOPERDAY")
	}
	if isPotentialPrecisionLoss("DSOPERDAY", "DSDATETIME") {
		t.Fatalf("did not expect precision loss for DSOPERDAY -> DSDATETIME")
	}
	if isPotentialPrecisionLoss("DSOPERDAY", "DSOPERDAY") {
		t.Fatalf("did not expect precision loss for equal types")
	}
}

func TestParseUpdateSetStatement_WithFromAndCase(t *testing.T) {
	query := `update pCons_AutoFullPrepDate d
set d.Date = case when cc.Flag2 > 0 then cc.CreditDateFrom else cr.DateFrom end,
    d.FlagCrd2 = cc.Flag2
from tContractCredit cc
join tCtrCtrRelation cr on cr.ContractID = cc.ContractCreditID
where d.SPID = @@SPID`

	stmt, ok := parseUpdateSetStatement(query)
	if !ok {
		t.Fatalf("parseUpdateSetStatement should parse UPDATE ... SET")
	}
	if stmt.TargetTable != "pCons_AutoFullPrepDate" {
		t.Fatalf("target table = %q, want pCons_AutoFullPrepDate", stmt.TargetTable)
	}
	if stmt.TargetAlias != "d" {
		t.Fatalf("target alias = %q, want d", stmt.TargetAlias)
	}
	if len(stmt.Assignments) != 2 {
		t.Fatalf("assignments count = %d, want 2", len(stmt.Assignments))
	}
	if stmt.FromClause == "" {
		t.Fatalf("from clause should not be empty")
	}
}

func TestFindTopLevelKeywordPosition_SkipsCaseBody(t *testing.T) {
	text := strings.ToLower("col = case when a = 1 then b from c else d end, x = y from t")
	pos := findTopLevelKeywordPosition(text, "from")
	if pos <= 0 {
		t.Fatalf("expected to find top-level from keyword")
	}
	if got := text[pos : pos+4]; got != "from" {
		t.Fatalf("keyword slice = %q, want 'from'", got)
	}
}

func TestNormalizeAssignmentTargetColumn(t *testing.T) {
	stmt := updateSetStatement{TargetTable: "pCons_AutoFullPrepDate", TargetAlias: "d"}
	if got := normalizeAssignmentTargetColumn("d.Date", stmt); got != "Date" {
		t.Fatalf("normalizeAssignmentTargetColumn(d.Date) = %q, want Date", got)
	}
	if got := normalizeAssignmentTargetColumn("pCons_AutoFullPrepDate.Date", stmt); got != "Date" {
		t.Fatalf("normalizeAssignmentTargetColumn(table.Date) = %q, want Date", got)
	}
	if got := normalizeAssignmentTargetColumn("x.Date", stmt); got != "" {
		t.Fatalf("normalizeAssignmentTargetColumn(x.Date) = %q, want empty", got)
	}
}

func TestEnabledRuleSet_InsertRowLock(t *testing.T) {
	rules := []RuleID{RuleInsertRowLock}
	set := enabledRuleSet(rules)

	if !set[RuleInsertRowLock] {
		t.Fatalf("RuleInsertRowLock should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestAnsiInJoin_Multiline(t *testing.T) {
	cases := []struct {
		name         string
		content      string
		expectFinding bool
		findingLine   int
	}{
		{
			name:         "single line old style",
			content:      "SELECT * FROM t1, t2 WHERE t1.id = t2.id",
			expectFinding: true,
			findingLine:   1,
		},
		{
			name: "multiline old style",
			content: `SELECT *
FROM t1, t2
WHERE t1.id = t2.id`,
			expectFinding: true,
			findingLine:   2,
		},
		{
			name: "multiline with comma on next line",
			content: `SELECT *
FROM t1
, t2`,
			expectFinding: true,
			findingLine:   3,
		},
		{
			name: "ansi inner join multiline",
			content: `SELECT *
FROM t1
INNER JOIN t2 ON t1.id = t2.id`,
			expectFinding: false,
		},
		{
			name: "ansi left join multiline",
			content: `SELECT *
FROM t1
LEFT JOIN t2 ON t1.id = t2.id`,
			expectFinding: false,
		},
		{
			name: "single table multiline",
			content: `SELECT *
FROM t1
WHERE id = 1`,
			expectFinding: false,
		},
		{
			name: "subquery with comma inside",
			content: `SELECT * FROM (SELECT 1, 2) AS t`,
			expectFinding: false,
		},
		{
			name: "commented old style",
			content: `-- SELECT * FROM t1, t2
SELECT * FROM t1`,
			expectFinding: false,
		},
		{
			name: "multiple commas old style",
			content: `SELECT *
FROM t1, t2, t3
WHERE 1=1`,
			expectFinding: true,
			findingLine:   2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Создаем временный файл
			tmpFile, err := os.CreateTemp("", "test_ansi_*.sql")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())

			if _, err := tmpFile.WriteString(tc.content); err != nil {
				t.Fatalf("failed to write to temp file: %v", err)
			}
			tmpFile.Close()

			file := &indexedFile{Path: tmpFile.Name(), DsProductID: 1}
			runner := &Runner{}

			findings, err := runner.checkAnsiInJoin(file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.expectFinding {
				if len(findings) == 0 {
					t.Fatalf("expected finding, got none")
				}
				if findings[0].Line != tc.findingLine {
					t.Fatalf("finding line = %d, want %d", findings[0].Line, tc.findingLine)
				}
			} else {
				if len(findings) > 0 {
					t.Fatalf("expected no finding, got %v", findings)
				}
			}
		})
	}
}

func TestInsertRowLock(t *testing.T) {
	cases := []struct {
		name         string
		content      string
		expectFinding bool
		findingLine   int
		tableName     string
	}{
		{
			name:         "insert without rowlock - with INTO",
			content:      "INSERT INTO tTable VALUES (1, 2)",
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
		{
			name:         "insert without rowlock - without INTO",
			content:      "INSERT tTable VALUES (1, 2)",
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
		{
			name:         "insert with M_WITH_ROWLOCK",
			content:      "INSERT INTO tTable M_WITH_ROWLOCK VALUES (1, 2)",
			expectFinding: false,
		},
		{
			name:         "insert with WITH (ROWLOCK)",
			content:      "INSERT INTO tTable WITH (ROWLOCK) VALUES (1, 2)",
			expectFinding: false,
		},
		{
			name:         "insert in subquery - should be ignored",
			content:      "SELECT * FROM (INSERT INTO tTable VALUES (1)) AS x",
			expectFinding: false,
		},
		{
			name:         "insert with select - no rowlock",
			content:      "INSERT INTO tTable SELECT * FROM tSource",
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
		{
			name:         "insert with select and M_WITH_ROWLOCK",
			content:      "INSERT INTO tTable M_WITH_ROWLOCK SELECT * FROM tSource",
			expectFinding: false,
		},
		{
			name:         "commented insert",
			content:      "-- INSERT INTO tTable VALUES (1)",
			expectFinding: false,
		},
		{
			name: "multiline insert with M_WITH_ROWLOCK on next line",
			content: `INSERT INTO tTable
M_WITH_ROWLOCK
VALUES (1, 2)`,
			expectFinding: false,
		},
		{
			name: "multiline insert without rowlock",
			content: `INSERT INTO tTable
VALUES (1, 2)`,
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
		{
			name: "multiline insert with INTO on next line",
			content: `INSERT
INTO tTable
M_WITH_ROWLOCK
VALUES (1, 2)`,
			expectFinding: false,
		},
		{
			name: "multiline insert with semicolon",
			content: `INSERT INTO tTable
VALUES (1, 2);`,
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
		{
			name: "multiline insert with select",
			content: `INSERT INTO tTable
SELECT * FROM tSource`,
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
		{
			name: "multiline insert with M_WITH_ROWLOCK and select",
			content: `INSERT INTO tTable
M_WITH_ROWLOCK
SELECT * FROM tSource`,
			expectFinding: false,
		},
		{
			name: "insert without semicolon before IF",
			content: `insert into tBankProduct (ID) values (1)
if @@error != 0
begin
    select 1
end`,
			expectFinding: true,
			findingLine:   1,
			tableName:     "tBankProduct",
		},
		{
			name: "insert with indented IF (spaces before if)",
			content: `insert into tTable (ID) values (1)
  if @@error != 0
    select 1`,
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "test_rowlock_*.sql")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())

			if _, err := tmpFile.WriteString(tc.content); err != nil {
				t.Fatalf("failed to write to temp file: %v", err)
			}
			tmpFile.Close()

			file := &indexedFile{Path: tmpFile.Name(), DsProductID: 1}
			runner := &Runner{}

			findings, err := runner.checkInsertRowLock(file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.expectFinding {
				if len(findings) == 0 {
					t.Fatalf("expected finding, got none")
				}
				if findings[0].Line != tc.findingLine {
					t.Fatalf("finding line = %d, want %d", findings[0].Line, tc.findingLine)
				}
				if findings[0].Object != tc.tableName {
					t.Fatalf("finding object = %q, want %q", findings[0].Object, tc.tableName)
				}
			} else {
				if len(findings) > 0 {
					t.Fatalf("expected no finding, got %v", findings)
				}
			}
		})
	}
}
