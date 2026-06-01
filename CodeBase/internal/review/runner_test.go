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

func TestTableHintExists_InsertTargetAndSources(t *testing.T) {
	cases := []struct {
		name          string
		content       string
		expectFinding bool
		findingLine   int
		tableName     string
	}{
		{
			name: "from clause must end before next insert statement",
			content: `select ID
from tSource M_NOLOCK_INDEX(XPKtSource)
insert pConsErrMass M_P_WITH_ROWLOCK (SPID, ContractID)
select @@spid, @ObjectID`,
			expectFinding: false,
		},
		{
			name: "insert target ignored but source without hint is checked",
			content: `insert pConsErrMass M_P_WITH_ROWLOCK (SPID, ContractID)
select @@spid, ContractID
from tSource`,
			expectFinding: true,
			findingLine:   1,
			tableName:     "tsource",
		},
		{
			name: "insert target ignored and source with hint passes",
			content: `insert pConsErrMass M_P_WITH_ROWLOCK (SPID, ContractID)
select @@spid, ContractID
from tSource M_NOLOCK_INDEX(XPKtSource)`,
			expectFinding: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "test_tablehint_exists_*.sql")
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

			findings, err := runner.checkTableHintExists(file)
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

func TestEnabledRuleSet_IndexWrong(t *testing.T) {
	rules := []RuleID{RuleIndexWrong}
	set := enabledRuleSet(rules)

	if !set[RuleIndexWrong] {
		t.Fatalf("RuleIndexWrong should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_UpdateOnlyVar(t *testing.T) {
	rules := []RuleID{RuleUpdateOnlyVar}
	set := enabledRuleSet(rules)

	if !set[RuleUpdateOnlyVar] {
		t.Fatalf("RuleUpdateOnlyVar should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_PTableSpid(t *testing.T) {
	rules := []RuleID{RulePTableSpid}
	set := enabledRuleSet(rules)

	if !set[RulePTableSpid] {
		t.Fatalf("RulePTableSpid should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ForceOrder2Tbl(t *testing.T) {
	rules := []RuleID{RuleForceOrder2Tbl}
	set := enabledRuleSet(rules)

	if !set[RuleForceOrder2Tbl] {
		t.Fatalf("RuleForceOrder2Tbl should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_SaveTran(t *testing.T) {
	rules := []RuleID{RuleSaveTran}
	set := enabledRuleSet(rules)

	if !set[RuleSaveTran] {
		t.Fatalf("RuleSaveTran should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_UseDrop(t *testing.T) {
	rules := []RuleID{RuleUseDrop}
	set := enabledRuleSet(rules)

	if !set[RuleUseDrop] {
		t.Fatalf("RuleUseDrop should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_MathOperations(t *testing.T) {
	rules := []RuleID{RuleMathOperations}
	set := enabledRuleSet(rules)

	if !set[RuleMathOperations] {
		t.Fatalf("RuleMathOperations should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ExistsWithAndInIf(t *testing.T) {
	rules := []RuleID{RuleExistsWithAndInIf}
	set := enabledRuleSet(rules)

	if !set[RuleExistsWithAndInIf] {
		t.Fatalf("RuleExistsWithAndInIf should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_IndexExistsInDB(t *testing.T) {
	rules := []RuleID{RuleIndexExistsInDB}
	set := enabledRuleSet(rules)

	if !set[RuleIndexExistsInDB] {
		t.Fatalf("RuleIndexExistsInDB should be enabled")
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

func TestEnabledRuleSet_UseEqColumn(t *testing.T) {
	rules := []RuleID{RuleUseEqColumn}
	set := enabledRuleSet(rules)

	if !set[RuleUseEqColumn] {
		t.Fatalf("RuleUseEqColumn should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_TableFullScan(t *testing.T) {
	rules := []RuleID{RuleTableFullScan}
	set := enabledRuleSet(rules)

	if !set[RuleTableFullScan] {
		t.Fatalf("RuleTableFullScan should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_TableHintExists(t *testing.T) {
	rules := []RuleID{RuleTableHintExists}
	set := enabledRuleSet(rules)

	if !set[RuleTableHintExists] {
		t.Fatalf("RuleTableHintExists should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_TableHintIsRight(t *testing.T) {
	rules := []RuleID{RuleTableHintIsRight}
	set := enabledRuleSet(rules)

	if !set[RuleTableHintIsRight] {
		t.Fatalf("RuleTableHintIsRight should be enabled")
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

func TestAnalyzeConditionForEqColumn_DetectsSelfComparison(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	cases := []struct {
		name      string
		lines     []string
		wantCount int
		wantObj   string
	}{
		{
			name:      "simple column equals itself",
			lines:     []string{"where col1 = col1"},
			wantCount: 1,
			wantObj:   "col1",
		},
		{
			name:      "qualified column equals itself",
			lines:     []string{"where t.col1 = t.col1"},
			wantCount: 1,
			wantObj:   "t.col1",
		},
		{
			name:      "multiline where clause",
			lines:     []string{"where col1 = :param", "  and col2 = col2"},
			wantCount: 1,
			wantObj:   "col2",
		},
		{
			name:      "different columns no finding",
			lines:     []string{"where col1 = col2"},
			wantCount: 0,
		},
		{
			name:      "different tables same column no finding",
			lines:     []string{"where t1.col = t2.col"},
			wantCount: 0,
		},
		{
			name:      "on clause join condition",
			lines:     []string{"on t1.id = t1.id"},
			wantCount: 1,
			wantObj:   "t1.id",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			findings := analyzeConditionForEqColumn(tc.lines, 1, file)
			if len(findings) != tc.wantCount {
				t.Fatalf("findings count = %d, want %d", len(findings), tc.wantCount)
			}
			if tc.wantCount > 0 && findings[0].Object != tc.wantObj {
				t.Fatalf("finding.Object = %q, want %q", findings[0].Object, tc.wantObj)
			}
			if tc.wantCount > 0 && findings[0].Rule != RuleUseEqColumn {
				t.Fatalf("finding.Rule = %q, want %q", findings[0].Rule, RuleUseEqColumn)
			}
		})
	}
}

func TestAnalyzeStatementForFullScan_DetectsFullScan(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	cases := []struct {
		name      string
		lines     []string
		stmtType  string
		wantFound bool
		wantObj   string
	}{
		{
			name:      "select without where",
			lines:     []string{"select * from tContract"},
			stmtType:  "select",
			wantFound: true,
			wantObj:   "tContract",
		},
		{
			name:      "select with where no finding",
			lines:     []string{"select * from tContract where id = 1"},
			stmtType:  "select",
			wantFound: false,
		},
		{
			name:      "delete without where",
			lines:     []string{"delete from tDeal"},
			stmtType:  "delete",
			wantFound: true,
			wantObj:   "tDeal",
		},
		{
			name:      "delete with where no finding",
			lines:     []string{"delete from tDeal where id = :id"},
			stmtType:  "delete",
			wantFound: false,
		},
		{
			name:      "update without where",
			lines:     []string{"update tContract set status = 1"},
			stmtType:  "update",
			wantFound: true,
			wantObj:   "tContract",
		},
		{
			name:      "update with join no finding",
			lines:     []string{"update tContract c join tDeal d on c.id = d.id set c.status = 1"},
			stmtType:  "update",
			wantFound: false,
		},
		{
			name:      "multiline select with where no finding",
			lines:     []string{"select * from tContract", "where id = 1"},
			stmtType:  "select",
			wantFound: false,
		},
		{
			name:      "multiline update without where",
			lines:     []string{"update tContract", "set status = 1"},
			stmtType:  "update",
			wantFound: true,
			wantObj:   "tContract",
		},
		{
			name:      "merge without on condition",
			lines:     []string{"merge into tContract using tTemp on tContract.id = tTemp.id"},
			stmtType:  "merge",
			wantFound: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			finding := analyzeStatementForFullScan(tc.lines, 1, file, tc.stmtType)
			if tc.wantFound {
				if finding == nil {
					t.Fatalf("expected finding, got nil")
				}
				if finding.Rule != RuleTableFullScan {
					t.Fatalf("finding.Rule = %q, want %q", finding.Rule, RuleTableFullScan)
				}
				if finding.Object != tc.wantObj {
					t.Fatalf("finding.Object = %q, want %q", finding.Object, tc.wantObj)
				}
			} else {
				if finding != nil {
					t.Fatalf("expected no finding, got %#v", finding)
				}
			}
		})
	}
}

func TestAnalyzeStatementForHintType_DeleteWrongTargetHint_AfterSelectAssignment(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	lines := []string{
		"select @RetVal = 0",
		"delete pTmpObjectAccount",
		"  from pTmpObjectAccount oa M_NOLOCK_INDEX(XPKpTmpObjectAccount)",
		" where oa.SPID = @@spid",
	}

	findings := analyzeStatementForHintType(lines, 39, file)
	if len(findings) != 1 {
		t.Fatalf("findings count = %d, want 1; findings=%#v", len(findings), findings)
	}

	f := findings[0]
	if f.Rule != RuleTableHintIsRight {
		t.Fatalf("finding.Rule = %q, want %q", f.Rule, RuleTableHintIsRight)
	}
	if f.Object != "ptmpobjectaccount" {
		t.Fatalf("finding.Object = %q, want %q", f.Object, "ptmpobjectaccount")
	}
	if !strings.Contains(strings.ToLower(f.Message), "операции delete") {
		t.Fatalf("finding.Message = %q, want to contain %q", f.Message, "операции delete")
	}
}

func TestParseTableWithAlias_ExtractsHintAndIndexName(t *testing.T) {
	table := parseTableWithAlias("ptmpobjectaccount oa M_NOLOCK_INDEX(XPKpTmpObjectAccount)")

	if table.TableName != "ptmpobjectaccount" {
		t.Fatalf("table.TableName = %q, want %q", table.TableName, "ptmpobjectaccount")
	}
	if table.Alias != "oa" {
		t.Fatalf("table.Alias = %q, want %q", table.Alias, "oa")
	}
	if !strings.EqualFold(table.Hint, "m_nolock_index") {
		t.Fatalf("table.Hint = %q, want case-insensitive %q", table.Hint, "m_nolock_index")
	}
	if !strings.EqualFold(table.IndexName, "xpkptmpobjectaccount") {
		t.Fatalf("table.IndexName = %q, want case-insensitive %q", table.IndexName, "xpkptmpobjectaccount")
	}
}

func TestCalculateIndexPrefixMatch(t *testing.T) {
	cols := map[string]struct{}{"contractid": {}, "branchid": {}}

	if got := calculateIndexPrefixMatch([]string{"ContractID"}, cols); got != 1 {
		t.Fatalf("calculateIndexPrefixMatch single = %d, want 1", got)
	}
	if got := calculateIndexPrefixMatch([]string{"ContractID", "DateFrom"}, cols); got != 1 {
		t.Fatalf("calculateIndexPrefixMatch prefix stop = %d, want 1", got)
	}
	if got := calculateIndexPrefixMatch([]string{"ExternalID"}, cols); got != 0 {
		t.Fatalf("calculateIndexPrefixMatch no match = %d, want 0", got)
	}
}

func TestExtractConditionColumnsForIndexWrong_WithOrIntersection(t *testing.T) {
	tables := []tableFromClause{{TableName: "tContract", Alias: "c"}}
	cols := extractConditionColumnsForIndexWrong("select * from tContract c where c.ContractID = 1 or c.ContractID = 2", tables)

	key := tableConditionKey(tables[0])
	if _, exists := cols[key]["contractid"]; !exists {
		t.Fatalf("expected contractid in OR-intersection columns, got %#v", cols[key])
	}

	cols = extractConditionColumnsForIndexWrong("select * from tContract c where c.ContractID = 1 or c.ExternalID = 'x'", tables)
	if len(cols[key]) != 0 {
		t.Fatalf("expected empty intersection for different OR branches, got %#v", cols[key])
	}
}

func TestSplitTopLevelSetAssignments_WithCaseAndParens(t *testing.T) {
	// Случай с CASE и скобками - запятая внутри не должна разделять
	setPart := "@a = case when x=1 then y else z end, [col] = @a + 1, @b = (select count(*) from t)"
	assignments := splitTopLevelSetAssignments(setPart)

	if len(assignments) != 3 {
		t.Fatalf("expected 3 assignments, got %d: %#v", len(assignments), assignments)
	}

	// Проверяем, что первая часть содержит CASE целиком
	first := strings.ToLower(assignments[0])
	if !strings.Contains(first, "case") || !strings.Contains(first, "end") {
		t.Fatalf("first assignment should contain complete CASE...END, got: %s", assignments[0])
	}
}

func TestAnalyzeStatementForUpdateOnlyVar_OnlyVariables(t *testing.T) {
	lines := []string{"update tContract set @a = 1, @b = col + 2 where id = 3"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	finding := analyzeStatementForUpdateOnlyVar(lines, 1, file)
	if finding == nil {
		t.Fatalf("expected finding for UPDATE with only variables")
	}
	if finding.Rule != RuleUpdateOnlyVar {
		t.Fatalf("expected RuleUpdateOnlyVar, got %s", finding.Rule)
	}
}

func TestAnalyzeStatementForUpdateOnlyVar_WithFieldUpdate(t *testing.T) {
	lines := []string{"update tContract set col1 = 1, @a = col1 where id = 3"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	finding := analyzeStatementForUpdateOnlyVar(lines, 1, file)
	if finding != nil {
		t.Fatalf("expected no finding when field is updated, got: %+v", finding)
	}
}

func TestAnalyzeStatementForUpdateOnlyVar_QualifiedColumn(t *testing.T) {
	lines := []string{"update tContract c set c.col1 = 1, @a = 2 where id = 3"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	finding := analyzeStatementForUpdateOnlyVar(lines, 1, file)
	if finding != nil {
		t.Fatalf("expected no finding when qualified column is updated, got: %+v", finding)
	}
}

func TestExtractSpidConditions_WithAlias(t *testing.T) {
	// p-таблица с алиасом, SPID с префиксом алиаса
	conditions := extractSpidConditions("select * from pTmpObject t where t.SPID = @SPID")

	key := "t"
	if _, exists := conditions[key]; !exists {
		t.Fatalf("expected SPID condition for alias 't', got conditions: %#v", conditions)
	}
}

func TestExtractSpidConditions_NoPrefixSingleTable(t *testing.T) {
	// Единственная p-таблица без алиаса, SPID без префикса
	conditions := extractSpidConditions("select * from pTmpObject where SPID = @SPID")

	key := "ptmpobject"
	if _, exists := conditions[key]; !exists {
		t.Fatalf("expected SPID condition for table 'ptmpobject', got conditions: %#v", conditions)
	}
}

func TestExtractSpidConditions_MissingSpid(t *testing.T) {
	// p-таблица без условия по SPID
	conditions := extractSpidConditions("select * from pTmpObject t where t.OtherCol = 1")

	key := "t"
	if _, exists := conditions[key]; exists {
		t.Fatalf("expected NO SPID condition for alias 't', got conditions: %#v", conditions)
	}
}

func TestAnalyzeStatementForPTableSpid_MissingSpid(t *testing.T) {
	lines := []string{"select * from pTmpObject t where t.OtherCol = 1"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	findings, err := (&Runner{}).analyzeStatementForPTableSpid(lines, 1, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding for missing SPID, got %d", len(findings))
	}
	if findings[0].Rule != RulePTableSpid {
		t.Fatalf("expected RulePTableSpid, got %s", findings[0].Rule)
	}
}

func TestAnalyzeStatementForPTableSpid_WithSpidCondition(t *testing.T) {
	lines := []string{"select * from pTmpObject t where t.SPID = @SPID"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	findings, err := (&Runner{}).analyzeStatementForPTableSpid(lines, 1, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected no findings when SPID condition present, got %d", len(findings))
	}
}

func TestAnalyzeStatementForForceOrder2Tbl_MissingMacro(t *testing.T) {
	lines := []string{"select * from t1, t2 where t1.id = t2.id"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	finding := analyzeStatementForForceOrder2Tbl(lines, 1, file)
	if finding == nil {
		t.Fatalf("expected finding for 2 tables without M_FORCEORDER")
	}
	if finding.Rule != RuleForceOrder2Tbl {
		t.Fatalf("expected RuleForceOrder2Tbl, got %s", finding.Rule)
	}
}

func TestAnalyzeStatementForForceOrder2Tbl_WithMacro(t *testing.T) {
	lines := []string{"select * from t1 M_FORCEORDER, t2 where t1.id = t2.id"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	finding := analyzeStatementForForceOrder2Tbl(lines, 1, file)
	if finding != nil {
		t.Fatalf("expected no finding when M_FORCEORDER present, got: %+v", finding)
	}
}

func TestAnalyzeStatementForForceOrder2Tbl_SingleTable(t *testing.T) {
	lines := []string{"select * from t1 where id = 1"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	finding := analyzeStatementForForceOrder2Tbl(lines, 1, file)
	if finding != nil {
		t.Fatalf("expected no finding for single table, got: %+v", finding)
	}
}

func TestAnalyzeStatementForForceOrder2Tbl_WithNospool(t *testing.T) {
	lines := []string{"select * from t1 M_FORCEORDER_NOSPOOL, t2 where t1.id = t2.id"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	finding := analyzeStatementForForceOrder2Tbl(lines, 1, file)
	if finding != nil {
		t.Fatalf("expected no finding when M_FORCEORDER_NOSPOOL present, got: %+v", finding)
	}
}

func TestHasSaveTran_Detects(t *testing.T) {
	if !hasSaveTran("save tran @savepoint") {
		t.Fatalf("should detect 'save tran'")
	}
	if !hasSaveTran("save transaction @savepoint") {
		t.Fatalf("should detect 'save transaction'")
	}
	if !hasSaveTran("begin tran save tran @s") {
		t.Fatalf("should detect 'save tran' in line")
	}
}

func TestHasSaveTran_NoFalsePositive(t *testing.T) {
	if hasSaveTran("savetransaction @savepoint") {
		t.Fatalf("should not detect 'savetransaction' without space")
	}
	if hasSaveTran("savesomething else") {
		t.Fatalf("should not detect partial match")
	}
}

func TestIsCreateProcStart(t *testing.T) {
	if !isCreateProcStart("create proc myproc") {
		t.Fatalf("should detect 'create proc'")
	}
	if !isCreateProcStart("create procedure myproc") {
		t.Fatalf("should detect 'create procedure'")
	}
	if !isCreateProcStart("  create proc myproc") {
		t.Fatalf("should detect 'create proc' with leading spaces")
	}
	if isCreateProcStart("create table mytable") {
		t.Fatalf("should NOT detect 'create table'")
	}
}

func TestHasDropInLine(t *testing.T) {
	if !hasDropInLine("drop table mytable") {
		t.Fatalf("should detect 'drop' word")
	}
	if !hasDropInLine("drop_create(p_mytable)") {
		t.Fatalf("should detect 'drop_create' macro")
	}
	if hasDropInLine("create table mytable") {
		t.Fatalf("should NOT detect 'create table'")
	}
}

func TestIsProcBodyEnd(t *testing.T) {
	if !isProcBodyEnd("go") {
		t.Fatalf("should detect 'go'")
	}
	if !isProcBodyEnd("go -- comment") {
		t.Fatalf("should detect 'go' with comment")
	}
	if !isProcBodyEnd("create proc newproc") {
		t.Fatalf("should detect new 'create proc' as end of previous")
	}
	if isProcBodyEnd("select * from t") {
		t.Fatalf("should NOT detect 'select' as end")
	}
}

func TestHasBetweenWithMathOp(t *testing.T) {
	if !hasBetweenWithMathOp("between @contractid and 10*@contractid") {
		t.Fatalf("should detect BETWEEN with * after AND")
	}
	if !hasBetweenWithMathOp("where id between 1 and @max+10") {
		t.Fatalf("should detect BETWEEN with + after AND")
	}
	if hasBetweenWithMathOp("between @a and @b") {
		t.Fatalf("should NOT detect BETWEEN without math op")
	}
	if hasBetweenWithMathOp("between 1 and 100") {
		t.Fatalf("should NOT detect BETWEEN with simple numbers")
	}
}

func TestHasComparisonWithMathOp(t *testing.T) {
	if !hasComparisonWithMathOp("@contractid > 10*@contractid") {
		t.Fatalf("should detect comparison with * math op")
	}
	if !hasComparisonWithMathOp("10*@max < @value") {
		t.Fatalf("should detect comparison with * math op on left")
	}
	if !hasComparisonWithMathOp("@a >= @b + 1") {
		t.Fatalf("should detect >= with +")
	}
	if !hasComparisonWithMathOp("@a <= 100/@b") {
		t.Fatalf("should detect <= with /")
	}
	if hasComparisonWithMathOp("@a > @b") {
		t.Fatalf("should NOT detect simple comparison")
	}
	if hasComparisonWithMathOp("a > b") {
		t.Fatalf("should NOT detect comparison without @vars")
	}
}

func TestHasMathOperator(t *testing.T) {
	if !hasMathOperator("10*@var") {
		t.Fatalf("should detect * operator")
	}
	if !hasMathOperator("@var/2") {
		t.Fatalf("should detect / operator")
	}
	if !hasMathOperator("@a+1") {
		t.Fatalf("should detect + operator")
	}
	if !hasMathOperator("100-@b") {
		t.Fatalf("should detect - operator")
	}
	if hasMathOperator("@var") {
		t.Fatalf("should NOT detect @var alone")
	}
	if hasMathOperator("table_name") {
		t.Fatalf("should NOT detect identifier without operator")
	}
	// convert() исключает оператор внутри
	if hasMathOperator("convert(numeric(16,0), 10*@var)") {
		t.Fatalf("should NOT detect operator inside convert()")
	}
	if hasMathOperator("convert(varchar(10), @a+1)") {
		t.Fatalf("should NOT detect operator inside convert()")
	}
	// cast() тоже исключает
	if hasMathOperator("cast(10*@var as numeric(16,0))") {
		t.Fatalf("should NOT detect operator inside cast()")
	}
	// Но оператор снаружи convert() — находим
	if !hasMathOperator("10*convert(int, @a)") {
		t.Fatalf("should detect operator outside convert()")
	}
}

func TestHasIfWithAndAndQuery(t *testing.T) {
	// Положительные случаи
	if !hasIfWithAndAndQuery("if @var = 1 and exists(select 1 from t)") {
		t.Fatalf("should detect IF with AND and EXISTS")
	}
	if !hasIfWithAndAndQuery("if exists(select 1 from t) and @var = 1") {
		t.Fatalf("should detect IF with EXISTS and AND")
	}
	if !hasIfWithAndAndQuery("if @a = 1 and @b in (select col from t)") {
		t.Fatalf("should detect IF with AND and IN (SELECT)")
	}
	if !hasIfWithAndAndQuery("if (select count(*) from t) > 0 and @var = 1") {
		t.Fatalf("should detect IF with scalar subquery and AND")
	}
	// Отрицательные случаи
	if hasIfWithAndAndQuery("if @var = 1 and @b = 2") {
		t.Fatalf("should NOT detect IF with AND but no query")
	}
	if hasIfWithAndAndQuery("if exists(select 1 from t)") {
		t.Fatalf("should NOT detect IF with query but no AND")
	}
	if hasIfWithAndAndQuery("select * from t where id = 1") {
		t.Fatalf("should NOT detect without IF")
	}
}

func TestHasTableQuery(t *testing.T) {
	if !hasTableQuery("exists(select 1 from tcontract)") {
		t.Fatalf("should detect EXISTS")
	}
	if !hasTableQuery("@var in (select id from t)") {
		t.Fatalf("should detect IN (SELECT)")
	}
	if !hasTableQuery("(select count(*) from t) > 0") {
		t.Fatalf("should detect scalar subquery")
	}
	if hasTableQuery("@var = 1 and @b = 2") {
		t.Fatalf("should NOT detect without query")
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
