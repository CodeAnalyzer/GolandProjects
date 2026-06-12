package review

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
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

func TestRunRuleTasks_SuccessAggregatesFindings(t *testing.T) {
	tasks := []ruleTask{
		{rule: RuleUseDrop, run: func(ctx context.Context) ([]Finding, error) {
			return []Finding{{Rule: RuleUseDrop, Line: 10}}, nil
		}},
		{rule: RuleMathOperations, run: func(ctx context.Context) ([]Finding, error) {
			return []Finding{{Rule: RuleMathOperations, Line: 20}}, nil
		}},
	}

	findings, err := runRuleTasks(tasks, 2)
	if err != nil {
		t.Fatalf("runRuleTasks(...) unexpected error: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("findings count = %d, want 2", len(findings))
	}
}

func TestRunRuleTasks_FirstErrorCancels(t *testing.T) {
	wantErr := errors.New("boom")

	tasks := []ruleTask{
		{rule: RuleUseDrop, run: func(ctx context.Context) ([]Finding, error) {
			select {
			case <-ctx.Done():
				return nil, nil
			case <-time.After(2 * time.Second):
				return nil, errors.New("context was not cancelled in time")
			}
		}},
		{rule: RuleMathOperations, run: func(ctx context.Context) ([]Finding, error) {
			return nil, wantErr
		}},
	}

	_, err := runRuleTasks(tasks, 2)
	if !errors.Is(err, wantErr) {
		t.Fatalf("runRuleTasks(...) error = %v, want %v", err, wantErr)
	}
}

func TestHasStatementEnded_EndInsideUpdateCase_DoesNotEnd(t *testing.T) {
	stmtBuffer := []string{
		"update pConsAccListID",
		"   set Rest = case",
		"                when @AccCount = 1 and @Qty > @QtyPrepayment then Rest - @QtyPrepayment",
		"                when @AccCount = 1 and @Qty <= @QtyPrepayment then 0",
		"                when @AccCount > 1 then 0",
		"                else 0",
		"              end",
	}

	if hasStatementEnded("              end", stmtBuffer) {
		t.Fatalf("hasStatementEnded should not end UPDATE on CASE END")
	}
}

func TestHasStatementEnded_StandaloneEnd_EndsStatement(t *testing.T) {
	stmtBuffer := []string{"begin", "select 1"}

	if !hasStatementEnded("end", stmtBuffer) {
		t.Fatalf("hasStatementEnded should end statement on standalone END")
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
		{
			name:      "where 1=1 should not trigger",
			lines:     []string{"where 1=1"},
			wantCount: 0,
		},
		{
			name:      "where 1 = 1 with spaces should not trigger",
			lines:     []string{"where 1 = 1"},
			wantCount: 0,
		},
		{
			name:      "where 0=0 should not trigger",
			lines:     []string{"where 0=0"},
			wantCount: 0,
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
			wantObj:   "tcontract",
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
			wantObj:   "tdeal",
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
			wantObj:   "tcontract",
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
			wantObj:   "tcontract",
		},
		{
			name: "update with case end and where no finding",
			lines: []string{
				"update pConsAccListID",
				"   set Rest = case",
				"                when @AccCount = 1 and @Qty > @QtyPrepayment then Rest - @QtyPrepayment",
				"                when @AccCount = 1 and @Qty <= @QtyPrepayment then 0",
				"                when @AccCount > 1 then 0",
				"                else 0",
				"              end",
				"  from pConsAccListID M_UPDLOCK_INDEX(XPKpConsAccListID)",
				" where SPID      = @@SPID",
				"   and AccountID = @CurrAccountID",
			},
			stmtType:  "update",
			wantFound: false,
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

func TestAnalyzeStatementForHintType_UpdateTargetFromSameTable_AllowsUpdlock(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	lines := []string{
		"update pAPI_Acc_GetListLimit_Out",
		"   set Limit       = @Rest,",
		"       PlanLimit   = @RestPlan",
		"  from pAPI_Acc_GetListLimit_Out p M_UPDLOCK_INDEX(XPKpAPI_Acc_GetListLimit_Out)",
		" where p.SPID = @@SPID",
	}

	findings := analyzeStatementForHintType(lines, 415, file)
	if len(findings) != 0 {
		t.Fatalf("expected no findings for UPDATE target table with M_UPDLOCK_INDEX, got %#v", findings)
	}
}

func TestAnalyzeStatementForHintType_DeleteSameTableWithDifferentAliases(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	lines := []string{
		"delete pConsLIMITPSKRule",
		"  from pConsLIMITPSKRule pConsLIMITPSKRule M_ROWLOCK_INDEX(XPKpConsLIMITPSKRule)",
		" inner join pConsLIMITPSKRule p2 M_NOLOCK_INDEX(XPKpConsLIMITPSKRule)",
		"         on p2.SPID       = pConsLIMITPSKRule.spid",
		"        and p2.ContractID = pConsLIMITPSKRule.ContractID",
		" where pConsLIMITPSKRule.spid          = @@spid",
		"   and pConsLIMITPSKRule.EffRateValue  > p2.EffRateValue",
		" M_FORCEORDER",
	}

	findings := analyzeStatementForHintType(lines, 382, file)
	if len(findings) != 0 {
		t.Fatalf("expected no findings for DELETE with same table using different aliases, got %#v", findings)
	}
}

func TestAnalyzeStatementForHintType_UpdateWrongTargetHint_AfterSelectAssignment(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	lines := []string{
		"select @RetVal = 0",
		"update pTmpObjectAccount",
		"   set ErrorCode = 0",
		"  from pTmpObjectAccount oa M_NOLOCK_INDEX(XPKpTmpObjectAccount)",
		" where oa.SPID = @@spid",
	}

	findings := analyzeStatementForHintType(lines, 57, file)
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
	if !strings.Contains(strings.ToLower(f.Message), "операции update") {
		t.Fatalf("finding.Message = %q, want to contain %q", f.Message, "операции update")
	}
}

func TestAnalyzeStatementForHintType_DeleteAndUpdateWithoutGO(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	lines := []string{
		"delete pTmpObjectAccount",
		"  from pTmpObject                o M_NOLOCK_INDEX(XPKpTmpObject)",
		" inner join pTmpObjectAccount   oa M_NOLOCK_INDEX(XPKpTmpObjectAccount)",
		"         on oa.SPID              = @@spid",
		"        and oa.ObjectID          = o.ObjectID",
		"        and oa.TemplateNumber    = @TemplateNumber",
		"        and oa.ErrorCode         = 25007",
		" inner join pAPI_GetInstrumentID i M_NOLOCK_INDEX(XPKpAPI_GetInstrumentID)",
		"         on i.InstrumentID       = o.InstrumentID",
		"        and i.SPID               = @@spid",
		" where o.SPID = @@spid",
		" M_FORCEORDER",
		"update pTmpObjectAccount",
		"   set ErrorCode = 0",
		"  from pTmpObject                o M_NOLOCK_INDEX(XPKpTmpObject)",
		" inner join pTmpObjectAccount   oa M_NOLOCK_INDEX(XPKpTmpObjectAccount)",
		"         on oa.SPID              = @@spid",
		"        and oa.ObjectID          = o.ObjectID",
		"        and oa.TemplateNumber    = @TemplateNumber",
		"        and oa.ErrorCode         = 25007",
		" inner join pAPI_GetInstrumentID i M_NOLOCK_INDEX(XPKpAPI_GetInstrumentID)",
		"         on i.InstrumentID       = o.InstrumentID",
		"        and i.SPID               = @@spid",
		" where o.SPID = @@spid",
		" M_FORCEORDER",
	}

	findings := analyzeStatementForHintType(lines, 1, file)
	// Ожидаем 2 findings: один для DELETE, один для UPDATE
	if len(findings) != 2 {
		t.Fatalf("findings count = %d, want 2; findings=%#v", len(findings), findings)
	}

	// Проверяем первый finding (DELETE)
	f1 := findings[0]
	if f1.Rule != RuleTableHintIsRight {
		t.Fatalf("finding[0].Rule = %q, want %q", f1.Rule, RuleTableHintIsRight)
	}
	if f1.Object != "ptmpobjectaccount" {
		t.Fatalf("finding[0].Object = %q, want %q", f1.Object, "ptmpobjectaccount")
	}
	if !strings.Contains(strings.ToLower(f1.Message), "операции delete") {
		t.Fatalf("finding[0].Message = %q, want to contain %q", f1.Message, "операции delete")
	}

	// Проверяем второй finding (UPDATE)
	f2 := findings[1]
	if f2.Rule != RuleTableHintIsRight {
		t.Fatalf("finding[1].Rule = %q, want %q", f2.Rule, RuleTableHintIsRight)
	}
	if f2.Object != "ptmpobjectaccount" {
		t.Fatalf("finding[1].Object = %q, want %q", f2.Object, "ptmpobjectaccount")
	}
	if !strings.Contains(strings.ToLower(f2.Message), "операции update") {
		t.Fatalf("finding[1].Message = %q, want to contain %q", f2.Message, "операции update")
	}
}

func TestExtractUpdateTargetTable_WithExtraSpaces(t *testing.T) {
	query := "update   pAPI_Acc_GetListLimit_Out   set Limit = @Rest"
	if got := extractUpdateTargetTable(query); got != "pAPI_Acc_GetListLimit_Out" {
		t.Fatalf("extractUpdateTargetTable() = %q, want %q", got, "pAPI_Acc_GetListLimit_Out")
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

func TestExtractJoinColumnsForIndexWrong(t *testing.T) {
	tables := []tableFromClause{
		{TableName: "tConsAccountLink", Alias: "a"},
		{TableName: "tConsRuleAccSync", Alias: "r"},
	}

	sql := `select *
from tConsAccountLink a
inner join tConsRuleAccSync r
        on r.RuleID = a.RuleID
       and r.PropVal in (1, 2)`

	joinCols := extractJoinColumnsForIndexWrong(sql, tables)

	if _, exists := joinCols["r"]["ruleid"]; !exists {
		t.Fatalf("expected r.ruleid in join cols, got %#v", joinCols["r"])
	}
	if _, exists := joinCols["a"]["ruleid"]; !exists {
		t.Fatalf("expected a.ruleid in join cols, got %#v", joinCols["a"])
	}
	if _, exists := joinCols["r"]["propval"]; exists {
		t.Fatalf("did not expect r.propval in join cols, got %#v", joinCols["r"])
	}
}

func TestExtractJoinColumnsForIndexWrong_WithSubqueryInOn(t *testing.T) {
	tables := []tableFromClause{
		{TableName: "tAccrual", Alias: "p"},
		{TableName: "tContract", Alias: "c"},
	}

	sql := `select *
from tAccrual p
inner join tContract c M_NOLOCK_INDEX(XPKtContract)
        on c.ContractID = p.ObjectID
       and c.DateFrom = isNull((select Max(c1.DateFrom)
                                  from tContract c1 M_NOLOCK_INDEX(XPKtContract)
                                 where c1.ContractID = p.ObjectID), '19000101')`

	joinCols := extractJoinColumnsForIndexWrong(sql, tables)

	if _, exists := joinCols["c"]["contractid"]; !exists {
		t.Fatalf("expected c.contractid in join cols, got %#v", joinCols["c"])
	}
	if _, exists := joinCols["p"]["objectid"]; !exists {
		t.Fatalf("expected p.objectid in join cols, got %#v", joinCols["p"])
	}
}

func TestExtractTablesFromFromClause_WithSubqueryInOn(t *testing.T) {
	sql := `select *
from tAccrual p
inner join tContract c M_NOLOCK_INDEX(XPKtContract)
        on c.ContractID = p.ObjectID
       and c.DateFrom = isNull((select Max(c1.DateFrom)
                                  from tContract c1 M_NOLOCK_INDEX(XPKtContract)
                                 where c1.ContractID = p.ObjectID), '19000101')`

	tables := extractTablesFromFromClause(sql)
	if len(tables) < 2 {
		t.Fatalf("expected at least 2 tables, got %#v", tables)
	}

	foundC := false
	for _, table := range tables {
		if normalizeIdentifier(table.TableName) == "tcontract" && normalizeIdentifier(table.Alias) == "c" {
			foundC = true
			break
		}
	}
	if !foundC {
		t.Fatalf("expected outer tContract alias c in parsed tables, got %#v", tables)
	}
}

func TestExtractTablesFromFromClause_NoPanicOnInvalidUTF8_SelectBranch(t *testing.T) {
	sql := "select * from tContract\xff"

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("extractTablesFromFromClause panicked on invalid utf-8 in select branch: %v", r)
		}
	}()

	tables := extractTablesFromFromClause(sql)
	if len(tables) == 0 {
		t.Fatalf("expected parsed tables, got %#v", tables)
	}
}

func TestExtractTablesFromFromClause_NoPanicOnInvalidUTF8_UpdateBranch(t *testing.T) {
	sql := "update tContract set A=1 from tContract c\xff"

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("extractTablesFromFromClause panicked on invalid utf-8 in update branch: %v", r)
		}
	}()

	tables := extractTablesFromFromClause(sql)
	if len(tables) == 0 {
		t.Fatalf("expected parsed tables, got %#v", tables)
	}
}

func TestShouldKeepChosenIndexForPKJoin(t *testing.T) {
	joinCols := map[string]struct{}{"ruleid": {}}

	if !shouldKeepChosenIndexForPKJoin("XPKtConsRuleAccSync", []string{"RuleID"}, joinCols) {
		t.Fatalf("expected to keep XPK index for PK join")
	}

	if shouldKeepChosenIndexForPKJoin("XIE1tConsRuleAccSync", []string{"RuleID", "PropVal"}, joinCols) {
		t.Fatalf("did not expect keep for non-XPK index")
	}

	if shouldKeepChosenIndexForPKJoin("XPKtConsRuleAccSync", []string{"ObjectID"}, joinCols) {
		t.Fatalf("did not expect keep when XPK does not cover join prefix")
	}

	if !shouldKeepChosenIndexForPKJoin("XPKtContract", nil, joinCols) {
		t.Fatalf("expected keep for XPK when join exists and fields metadata is empty")
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

func TestAnalyzeStatementForFullScan_HashTableNoFilter_NoFinding(t *testing.T) {
	// #-таблица без фильтра — сессионная, finding не нужен
	lines := []string{"select @StateOrder = max(p.StateOrder) from #ProtocolList p M_ISOLAT"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	finding := analyzeStatementForFullScan(lines, 1, file, "select")
	if finding != nil {
		t.Fatalf("expected no finding for #-table without filter, got: %+v", finding)
	}
}

func TestAnalyzeStatementForFullScan_HashTableWithFilter_NoFinding(t *testing.T) {
	// #-таблица с фильтром — тоже не должна давать finding
	lines := []string{"select p.ProtocolID from #ProtocolList p where p.StateOrder = @StateOrder M_ISOLAT"}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	finding := analyzeStatementForFullScan(lines, 1, file, "select")
	if finding != nil {
		t.Fatalf("expected no finding for #-table with filter, got: %+v", finding)
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

func TestAnalyzeStatementForForceOrder2Tbl_UnionWithMacroAfterUnion(t *testing.T) {
	// UNION-запрос с макросом после UNION - не должен выдавать finding
	// Теперь проверяется через checkForceOrder2Tbl, который объединяет UNION-операторы
	lines := []string{
		"select * from t1, t2 where t1.id = t2.id",
		"union",
		"select * from t3, t4 where t3.id = t4.id",
		"M_FORCEORDER",
	}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	// Проверяем через analyzeStatementForForceOrder2Tbl напрямую (для fullText проверки)
	finding := analyzeStatementForForceOrder2Tbl(lines, 1, file)
	if finding != nil {
		t.Fatalf("expected no finding when M_FORCEORDER present after UNION, got: %+v", finding)
	}
}

func TestAnalyzeStatementForForceOrder2Tbl_UnionWithoutMacro(t *testing.T) {
	// UNION-запрос без макроса - должен выдавать finding
	// Теперь проверяется через checkForceOrder2Tbl, который объединяет UNION-операторы
	lines := []string{
		"select * from t1, t2 where t1.id = t2.id",
		"union",
		"select * from t3, t4 where t3.id = t4.id",
	}
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	// Проверяем через analyzeStatementForForceOrder2Tbl напрямую (для fullText проверки)
	finding := analyzeStatementForForceOrder2Tbl(lines, 1, file)
	if finding == nil {
		t.Fatalf("expected finding for UNION with 2 tables without M_FORCEORDER")
	}
	if finding.Rule != RuleForceOrder2Tbl {
		t.Fatalf("expected RuleForceOrder2Tbl, got %s", finding.Rule)
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

func TestIsPotentialPrecisionLoss_NumericDecimal(t *testing.T) {
	if !isPotentialPrecisionLoss("numeric(15,0)", "numeric(10,0)") {
		t.Fatalf("expected precision loss for numeric(15,0) -> numeric(10,0)")
	}
	if !isPotentialPrecisionLoss("decimal(15,4)", "decimal(15,2)") {
		t.Fatalf("expected precision loss for decimal(15,4) -> decimal(15,2)")
	}
	if isPotentialPrecisionLoss("numeric(10,0)", "numeric(15,0)") {
		t.Fatalf("did not expect precision loss for numeric(10,0) -> numeric(15,0)")
	}
}

func TestIsPotentialPrecisionLoss_NumericToDSIntKey(t *testing.T) {
	if !isPotentialPrecisionLoss("numeric(15,0)", "DSINT_KEY") {
		t.Fatalf("expected precision loss for numeric(15,0) -> DSINT_KEY")
	}
	if isPotentialPrecisionLoss("numeric(10,0)", "DSIDENTIFIER") {
		t.Fatalf("did not expect precision loss for numeric(10,0) -> DSIDENTIFIER")
	}
}

func TestParseSelectAssignStatement_Basic(t *testing.T) {
	query := "select @OpenAccBP = t.ID from tConsConfigParamSync t where t.SysName = 'CONSUMER_OPENACC_BANKPARTNER'"
	stmt, ok := parseSelectAssignStatement(query)
	if !ok {
		t.Fatalf("expected parseSelectAssignStatement to parse query")
	}
	if len(stmt.Assignments) != 1 {
		t.Fatalf("assignments len = %d, want 1", len(stmt.Assignments))
	}
	if stmt.Assignments[0].TargetVariable != "@OpenAccBP" {
		t.Fatalf("target = %q, want %q", stmt.Assignments[0].TargetVariable, "@OpenAccBP")
	}
	if stmt.Assignments[0].Expression != "t.ID" {
		t.Fatalf("expression = %q, want %q", stmt.Assignments[0].Expression, "t.ID")
	}
	if !strings.HasPrefix(strings.ToLower(stmt.FromClause), "from") {
		t.Fatalf("fromClause must start with FROM, got %q", stmt.FromClause)
	}
}

func TestParseSelectAssignStatement_MultipleAssignments(t *testing.T) {
	query := "select @a = t.ColA, @b = isnull(t.ColB, 0) from tTable t"
	stmt, ok := parseSelectAssignStatement(query)
	if !ok {
		t.Fatalf("expected parseSelectAssignStatement to parse query")
	}
	if len(stmt.Assignments) != 2 {
		t.Fatalf("assignments len = %d, want 2", len(stmt.Assignments))
	}
}

func TestParseFetchIntoStatement_BasicAndMacro(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		cursorName string
		varCount   int
	}{
		{
			name:       "basic fetch",
			query:      "fetch ActionPeriod_cur into @ActionPeriodID, @UpdateFlag, @DeleteFlag",
			cursorName: "ActionPeriod_cur",
			varCount:   3,
		},
		{
			name:       "macro fetch",
			query:      "__FETCH_NEXT__ ActionPeriod_cur into @ActionPeriodID, @UpdateFlag, @DeleteFlag",
			cursorName: "ActionPeriod_cur",
			varCount:   3,
		},
		{
			name:       "fetch next from",
			query:      "fetch next from ActionPeriod_cur into @ActionPeriodID, @UpdateFlag",
			cursorName: "ActionPeriod_cur",
			varCount:   2,
		},
		{
			name: "open then fetch with tail",
			query: `open ActionPeriod_cur
fetch ActionPeriod_cur into @ActionPeriodID,
                            @UpdateFlag,
                            @DeleteFlag
while __FETCH_STATUS__ = 0`,
			cursorName: "ActionPeriod_cur",
			varCount:   3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, ok := parseFetchIntoStatement(tc.query)
			if !ok {
				t.Fatalf("expected parseFetchIntoStatement to parse query: %s", tc.query)
			}
			if stmt.CursorName != tc.cursorName {
				t.Fatalf("cursorName = %q, want %q", stmt.CursorName, tc.cursorName)
			}
			if len(stmt.Variables) != tc.varCount {
				t.Fatalf("variables len = %d, want %d", len(stmt.Variables), tc.varCount)
			}
		})
	}
}

func TestParseCursorDeclarations_ExplicitAndMacro(t *testing.T) {
	content := `declare ActionPeriod_cur insensitive cursor for
select t.ActionPeriodID,
       t.UpdateFlag,
       t.DeleteFlag
  from pIns_ActionPeriod t
 where t.SPID = @@SPID

__DECLARE_CURSOR__(ActionPeriod_cur2)
select p.ActionID,
       p.InsertFlag
  from pIns_ActionPeriod p

open ActionPeriod_cur`

	decls := parseCursorDeclarations(content)
	if len(decls) != 2 {
		t.Fatalf("cursor declarations count = %d, want 2", len(decls))
	}

	first, ok := decls["actionperiod_cur"]
	if !ok {
		t.Fatalf("expected explicit cursor declaration for actionperiod_cur")
	}
	if len(first.SelectExpressions) != 3 {
		t.Fatalf("explicit cursor select expressions = %d, want 3", len(first.SelectExpressions))
	}
	if !strings.HasPrefix(strings.ToLower(first.FromClause), "from") {
		t.Fatalf("explicit cursor fromClause must start with FROM, got %q", first.FromClause)
	}

	second, ok := decls["actionperiod_cur2"]
	if !ok {
		t.Fatalf("expected macro cursor declaration for actionperiod_cur2")
	}
	if len(second.SelectExpressions) != 2 {
		t.Fatalf("macro cursor select expressions = %d, want 2", len(second.SelectExpressions))
	}
}

func TestCollectVariableTypes_DoesNotCaptureKeywordAcrossNewline(t *testing.T) {
	content := `declare @DeleteFlag DSTINYINT,
        @Counter DSINT_KEY

fetch ActionPeriod_cur into @DeleteFlag

while __FETCH_STATUS__ = 0
begin
    select 1
end`

	types := collectVariableTypes(&sqlparser.ParseResult{}, content)
	if got := strings.ToUpper(types["deleteflag"]); got != "DSTINYINT" {
		t.Fatalf("deleteflag type = %q, want DSTINYINT", got)
	}
}

func TestHasExplicitConversion_DetectsConvertAndCast(t *testing.T) {
	cases := []struct {
		expr      string
		target    string
		wantFound bool
	}{
		// convert() cases - точное совпадение
		{"convert(smalldatetime, col)", "smalldatetime", true},
		{"convert(smalldatetime, cc.CreditDateFrom)", "smalldatetime", true},
		{"convert(varchar(10), col)", "varchar", true},
		{"CONVERT(DATE, col)", "date", true},
		{"convert(int, col)", "smalldatetime", false}, // другой targetType
		{"col", "smalldatetime", false},               // нет convert

		// convert() cases - эквивалентные типы (для datetime)
		{"convert(smalldatetime, col)", "dsoperday", true}, // smalldatetime эквивалентен DSOPERDAY
		{"convert(smalldatetime, col)", "date", true},      // smalldatetime эквивалентен date
		{"convert(date, col)", "dsoperday", true},          // date эквивалентен DSOPERDAY
		{"convert(datetime, col)", "dsdatetime", true},     // datetime эквивалентен DSDATETIME

		// cast() cases - точное совпадение
		{"cast(col as smalldatetime)", "smalldatetime", true},
		{"CAST(cc.CreditDateFrom AS smalldatetime)", "smalldatetime", true},
		{"cast(col as varchar(10))", "varchar", true},
		{"cast(col as date)", "date", true},
		{"cast(col as int)", "smalldatetime", false}, // другой targetType

		// cast() cases - эквивалентные типы (для datetime)
		{"cast(col as smalldatetime)", "dsoperday", true}, // smalldatetime эквивалентен DSOPERDAY
		{"cast(col as smalldatetime)", "date", true},      // smalldatetime эквивалентен date
		{"cast(col as date)", "dsoperday", true},          // date эквивалентен DSOPERDAY

		// case-insensitive
		{"Convert(SmallDateTime, col)", "smalldatetime", true},
		{"CAST(col AS SMALLDATETIME)", "smalldatetime", true},

		// empty cases
		{"", "smalldatetime", false},
		{"convert(smalldatetime, col)", "", false},
	}

	for _, tc := range cases {
		got := hasExplicitConversion(tc.expr, tc.target)
		if got != tc.wantFound {
			t.Fatalf("hasExplicitConversion(%q, %q) = %v, want %v", tc.expr, tc.target, got, tc.wantFound)
		}
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
		name          string
		content       string
		expectFinding bool
		findingLine   int
		tableName     string
	}{
		{
			name:          "single line old style",
			content:       "SELECT * FROM t1, t2 WHERE t1.id = t2.id",
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
			name:          "subquery with comma inside",
			content:       `SELECT * FROM (SELECT 1, 2) AS t`,
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
		{
			name: "INSERT SELECT with INNER JOIN and table hints - should not trigger",
			content: `    insert pContractInfo M_WITH_ROWLOCK
           (
           SPID           ,
           ObjectID       ,
           Date
           )
    select @@Spid,
           a.ContractID,
           Max(a.OnDate)
      from tConsAccountLink          a M_NOLOCK_INDEX(XIE4tConsAccountLink)
     inner join tConsRuleAccSync     r M_NOLOCK_INDEX(XPKtConsRuleAccSync)
             on r.RuleID             = a.RuleID
            and r.PropVal           in (1, 2)
     where a.ID = 1
    group by a.ContractID`,
			expectFinding: false,
		},
		{
			name: "DELETE FROM with table hints - should detect and set object",
			content: `delete pTmpObjectAccount
    from pTmpObjectAccount      oa M_ROWLOCK_INDEX(XPKpTmpObjectAccount),
         pTmpObject              o M_NOLOCK_INDEX(XPKpTmpObject)
   where oa.SPID = @@spid`,
			expectFinding: true,
			findingLine:   2,
			tableName:     "pTmpObjectAccount",
		},
		{
			name: "block comment with from file should not affect object",
			content: `/*
Code had been moved from file Cons_Calc_Find_Account.sql
*/
delete pTmpObjectAccount
  from pTmpObjectAccount oa,
       pTmpObject o
 where oa.ObjectID = o.ObjectID`,
			expectFinding: true,
			findingLine:   5,
			tableName:     "pTmpObjectAccount",
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
				// Проверяем что Object не пустой для старых стилей JOIN
				if findings[0].Object == "" && tc.name != "subquery with comma inside" {
					t.Fatalf("expected Object to be set, got empty")
				}
				if tc.tableName != "" && findings[0].Object != tc.tableName {
					t.Fatalf("finding Object = %q, want %q", findings[0].Object, tc.tableName)
				}
			} else {
				if len(findings) > 0 {
					t.Fatalf("expected no finding, got %v", findings)
				}
			}
		})
	}
}

func TestAnsiInJoin_RepeatedIdenticalFromLines_UsesCurrentOccurrenceLine(t *testing.T) {
	content := `select @ResourceCorrID = r.ResourceID
  from tContract          c    M_NOLOCK_INDEX(XPKtContract),
       tTypeAccLink       ta   M_NOLOCK_INDEX(XIE0tInstrAccLink),
       tAccountLink       al   M_NOLOCK_INDEX(XIE3tAccountLink),
       tLinkedAccType     lat  M_NOLOCK_INDEX(XPKtLinkedAccType),
       tResource          r    M_NOLOCK_INDEX(XPKtResource)
 where 1 = 1

select @CorrAccountID = r.ResourceID
  from tContract          c    M_NOLOCK_INDEX(XPKtContract),
       tTypeAccLink       ta   M_NOLOCK_INDEX(XIE0tInstrAccLink),
       tAccountLink       al   M_NOLOCK_INDEX(XIE3tAccountLink),
       tLinkedAccType     lat  M_NOLOCK_INDEX(XPKtLinkedAccType),
       tResource          r    M_NOLOCK_INDEX(XPKtResource)
 where 1 = 1`

	tmpFile, err := os.CreateTemp("", "test_ansi_repeated_lines_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	file := &indexedFile{Path: tmpFile.Name(), DsProductID: 1}
	runner := &Runner{}

	findings, err := runner.checkAnsiInJoin(file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("findings count = %d, want 2", len(findings))
	}

	if findings[0].Line != 5 {
		t.Fatalf("first finding line = %d, want 5", findings[0].Line)
	}
	if findings[1].Line != 13 {
		t.Fatalf("second finding line = %d, want 13", findings[1].Line)
	}
}

func TestInsertRowLock(t *testing.T) {
	cases := []struct {
		name          string
		content       string
		expectFinding bool
		findingLine   int
		tableName     string
	}{
		{
			name:          "insert without rowlock - with INTO",
			content:       "INSERT INTO tTable VALUES (1, 2)",
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
		{
			name:          "insert without rowlock - without INTO",
			content:       "INSERT tTable VALUES (1, 2)",
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
		{
			name:          "insert with M_WITH_ROWLOCK",
			content:       "INSERT INTO tTable M_WITH_ROWLOCK VALUES (1, 2)",
			expectFinding: false,
		},
		{
			name:          "insert with WITH (ROWLOCK)",
			content:       "INSERT INTO tTable WITH (ROWLOCK) VALUES (1, 2)",
			expectFinding: false,
		},
		{
			name:          "insert in subquery - should be ignored",
			content:       "SELECT * FROM (INSERT INTO tTable VALUES (1)) AS x",
			expectFinding: false,
		},
		{
			name:          "insert with select - no rowlock",
			content:       "INSERT INTO tTable SELECT * FROM tSource",
			expectFinding: true,
			findingLine:   1,
			tableName:     "tTable",
		},
		{
			name:          "insert with select and M_WITH_ROWLOCK",
			content:       "INSERT INTO tTable M_WITH_ROWLOCK SELECT * FROM tSource",
			expectFinding: false,
		},
		{
			name:          "commented insert",
			content:       "-- INSERT INTO tTable VALUES (1)",
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

func TestHasDefaultAssignmentInBody(t *testing.T) {
	runner := &Runner{}

	cases := []struct {
		name          string
		procBody      string
		paramName     string
		expectFound   bool
	}{
		{
			name: "assignment before usage - select isnull",
			procBody: `select @ParentProtocolID = isnull(@ParentProtocolID, 0)
if @ParentProtocolID <> 0
  select 1`,
			paramName:   "@ParentProtocolID",
			expectFound: true,
		},
		{
			name: "assignment before usage - set",
			procBody: `set @ParentProtocolID = 0
if @ParentProtocolID <> 0
  select 1`,
			paramName:   "@ParentProtocolID",
			expectFound: true,
		},
		{
			name: "assignment after usage - should fail",
			procBody: `if @ParentProtocolID <> 0
  select 1
select @ParentProtocolID = isnull(@ParentProtocolID, 0)`,
			paramName:   "@ParentProtocolID",
			expectFound: false,
		},
		{
			name: "no assignment - should fail",
			procBody: `if @ParentProtocolID <> 0
  select 1`,
			paramName:   "@ParentProtocolID",
			expectFound: false,
		},
		{
			name: "assignment but no usage - should pass",
			procBody: `select @ParentProtocolID = isnull(@ParentProtocolID, 0)`,
			paramName:   "@ParentProtocolID",
			expectFound: true,
		},
		{
			name: "param name without @",
			procBody: `select @ParentProtocolID = isnull(@ParentProtocolID, 0)
if @ParentProtocolID <> 0
  select 1`,
			paramName:   "ParentProtocolID",
			expectFound: true,
		},
		{
			name: "assignment in comment - should fail",
			procBody: `-- select @ParentProtocolID = isnull(@ParentProtocolID, 0)
if @ParentProtocolID <> 0
  select 1`,
			paramName:   "@ParentProtocolID",
			expectFound: false,
		},
		{
			name: "real procedure body from ConsChngOMCVTermUndo",
			procBody: `  __BEGIN_PROCEDURE__(ConsChngOMCVTermUndo)
  M_BUSINESSLOG_BEGIN
  M_BUSINESSLOG_BLOCK_BEGIN('ConsChngOMCVTermUndo')

  declare @DealProtocolID DSIDENTIFIER

  select @ParentProtocolID = isnull(@ParentProtocolID, 0)

  if @InstrumentID = 0
    select @InstrumentID = InstrumentID
      from tContract M_NOLOCK_INDEX(XPKtContract)
     where ContractID = @ContractID`,
			paramName:   "@ParentProtocolID",
			expectFound: true,
		},
		{
			name: "multi-line select assignment",
			procBody: `select @RetVal = 0,
       @CalcMode  = isnull(@CalcMode,0),
       @InsurMode = isnull(@InsurMode,0)
if @ContractID is not null
  select 1`,
			paramName:   "@CalcMode",
			expectFound: true,
		},
		{
			name: "multi-line select assignment - second param",
			procBody: `select @RetVal = 0,
       @CalcMode  = isnull(@CalcMode,0),
       @InsurMode = isnull(@InsurMode,0)
if @ContractID is not null
  select 1`,
			paramName:   "@InsurMode",
			expectFound: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := runner.hasDefaultAssignmentInBody(tc.procBody, tc.paramName)
			if result != tc.expectFound {
				t.Fatalf("hasDefaultAssignmentInBody(%q, %q) = %v, want %v", tc.procBody, tc.paramName, result, tc.expectFound)
			}
		})
	}
}

func TestExtractProcedureBody(t *testing.T) {
	runner := &Runner{}

	lines := []string{
		"line 1",
		"line 2",
		"line 3",
		"line 4",
		"line 5",
	}

	cases := []struct {
		name      string
		lineStart int
		lineEnd   int
		expect    string
	}{
		{
			name:      "normal range",
			lineStart: 2,
			lineEnd:   4,
			expect:    "line 2\nline 3\nline 4",
		},
		{
			name:      "single line",
			lineStart: 3,
			lineEnd:   3,
			expect:    "line 3",
		},
		{
			name:      "full range",
			lineStart: 1,
			lineEnd:   5,
			expect:    "line 1\nline 2\nline 3\nline 4\nline 5",
		},
		{
			name:      "invalid range - start > end",
			lineStart: 4,
			lineEnd:   2,
			expect:    "",
		},
		{
			name:      "invalid range - start < 1",
			lineStart: 0,
			lineEnd:   3,
			expect:    "",
		},
		{
			name:      "invalid range - end > len",
			lineStart: 2,
			lineEnd:   10,
			expect:    "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := runner.extractProcedureBody(lines, tc.lineStart, tc.lineEnd)
			if result != tc.expect {
				t.Fatalf("extractProcedureBody(lines, %d, %d) = %q, want %q", tc.lineStart, tc.lineEnd, result, tc.expect)
			}
		})
	}
}

func TestIsDataOrPatchPath(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{`D:\GITHUB\FA\fa-contracts\Consumer\DATA\init.sql`, true},
		{`D:\GITHUB\FA\fa-contracts\Consumer\Patch\fix.sql`, true},
		{`D:\GITHUB\FA\fa-contracts\Consumer\data\init.sql`, true},
		{`D:\GITHUB\FA\fa-contracts\Consumer\patch\fix.sql`, true},
		{`D:\GITHUB\FA\fa-contracts\Consumer\SERVER\Consumer\api_Foo.sql`, false},
		{`D:\GITHUB\FA\fa-contracts\Consumer\Scripts\Consumer\api_Foo.sql`, false},
	}
	for _, tc := range cases {
		got := isDataOrPatchPath(tc.path)
		if got != tc.want {
			t.Errorf("isDataOrPatchPath(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestCheckModifyOutProc(t *testing.T) {
	makeProc := func(lineStart, lineEnd int) *model.SQLProcedure {
		return &model.SQLProcedure{ProcName: "TestProc", LineStart: lineStart, LineEnd: lineEnd}
	}

	writeTemp := func(t *testing.T, content string) string {
		t.Helper()
		f, err := os.CreateTemp("", "modifyoutproc*.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(content); err != nil {
			t.Fatal(err)
		}
		f.Close()
		return f.Name()
	}

	run := func(t *testing.T, content string, procs []*model.SQLProcedure) []Finding {
		t.Helper()
		path := writeTemp(t, content)
		defer os.Remove(path)
		r := &Runner{}
		r.exec = &reviewExecContext{filePath: normalizePath(path), content: []byte(content), lines: strings.Split(content, "\n")}
		parsed := &sqlparser.ParseResult{Procedures: procs}
		findings, err := r.checkModifyOutProc(parsed, &indexedFile{Path: path, DsProductID: 1})
		r.exec = nil
		if err != nil {
			t.Fatal(err)
		}
		return findings
	}

	t.Run("insert outside proc finding", func(t *testing.T) {
		content := "INSERT INTO tFoo (id) VALUES (1)\n"
		findings := run(t, content, nil)
		if len(findings) != 1 {
			t.Errorf("expected 1 finding, got %d", len(findings))
		}
	})

	t.Run("update outside proc finding", func(t *testing.T) {
		content := "UPDATE tFoo SET x = 1 WHERE id = 2\n"
		findings := run(t, content, nil)
		if len(findings) != 1 {
			t.Errorf("expected 1 finding, got %d", len(findings))
		}
	})

	t.Run("delete outside proc finding", func(t *testing.T) {
		content := "DELETE tFoo WHERE id = 1\n"
		findings := run(t, content, nil)
		if len(findings) != 1 {
			t.Errorf("expected 1 finding, got %d", len(findings))
		}
	})

	t.Run("insert inside proc no finding", func(t *testing.T) {
		content := "create proc TestProc\nas\nINSERT INTO tFoo (id) VALUES (1)\ngo\n"
		findings := run(t, content, []*model.SQLProcedure{makeProc(1, 4)})
		if len(findings) != 0 {
			t.Errorf("expected 0 findings, got %d", len(findings))
		}
	})

	t.Run("insert into hash table no finding", func(t *testing.T) {
		content := "INSERT INTO #tmp (id) VALUES (1)\n"
		findings := run(t, content, nil)
		if len(findings) != 0 {
			t.Errorf("expected 0 findings for #tmp, got %d", len(findings))
		}
	})

	t.Run("update statistics no finding", func(t *testing.T) {
		content := "UPDATE STATISTICS tFoo\n"
		findings := run(t, content, nil)
		if len(findings) != 0 {
			t.Errorf("expected 0 findings for UPDATE STATISTICS, got %d", len(findings))
		}
	})

	t.Run("truncate p-table finding", func(t *testing.T) {
		content := "TRUNCATE TABLE pFooData\n"
		findings := run(t, content, nil)
		if len(findings) != 1 {
			t.Errorf("expected 1 finding for TRUNCATE pFooData, got %d", len(findings))
		}
	})

	t.Run("truncate hash table no finding", func(t *testing.T) {
		content := "TRUNCATE TABLE #tmp\n"
		findings := run(t, content, nil)
		if len(findings) != 0 {
			t.Errorf("expected 0 findings for TRUNCATE #tmp, got %d", len(findings))
		}
	})

	t.Run("define macro insert no finding", func(t *testing.T) {
		content := "#define MY_MACRO \\\n  INSERT INTO tFoo (id) VALUES (1)\n"
		findings := run(t, content, nil)
		if len(findings) != 0 {
			t.Errorf("expected 0 findings (INSERT in #define), got %d", len(findings))
		}
	})

	t.Run("comment insert no finding", func(t *testing.T) {
		content := "-- INSERT INTO tFoo (id) VALUES (1)\n"
		findings := run(t, content, nil)
		if len(findings) != 0 {
			t.Errorf("expected 0 findings (INSERT in comment), got %d", len(findings))
		}
	})

	t.Run("data path skip", func(t *testing.T) {
		f, err := os.CreateTemp("", "data_*.sql")
		if err != nil {
			t.Fatal(err)
		}
		content := "INSERT INTO tFoo (id) VALUES (1)\n"
		f.WriteString(content)
		f.Close()
		dataPath := filepath.Join(filepath.Dir(f.Name()), "DATA", "test.sql")
		os.MkdirAll(filepath.Dir(dataPath), 0755)
		os.WriteFile(dataPath, []byte(content), 0644)
		defer os.Remove(f.Name())
		defer os.Remove(dataPath)

		r := &Runner{}
		r.exec = &reviewExecContext{filePath: normalizePath(dataPath), content: []byte(content), lines: strings.Split(content, "\n")}
		parsed := &sqlparser.ParseResult{}
		findings, err := r.checkModifyOutProc(parsed, &indexedFile{Path: dataPath, DsProductID: 1})
		r.exec = nil
		if err != nil {
			t.Fatal(err)
		}
		if len(findings) != 0 {
			t.Errorf("expected 0 findings for DATA path, got %d", len(findings))
		}
	})
}

func TestCheckMaxProcParam(t *testing.T) {
	makeParams := func(n int) []model.SQLParam {
		params := make([]model.SQLParam, n)
		for i := range params {
			params[i] = model.SQLParam{Name: fmt.Sprintf("@p%d", i+1), Type: "int"}
		}
		return params
	}

	cases := []struct {
		name      string
		params    []model.SQLParam
		wantCount int
	}{
		{name: "0 params", params: makeParams(0), wantCount: 0},
		{name: "90 params exactly", params: makeParams(90), wantCount: 0},
		{name: "91 params", params: makeParams(91), wantCount: 1},
		{name: "100 params", params: makeParams(100), wantCount: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &Runner{}
			parsed := &sqlparser.ParseResult{
				Procedures: []*model.SQLProcedure{
					{ProcName: "TestProc", LineStart: 10, Params: tc.params},
				},
			}
			findings, err := r.checkMaxProcParam(parsed, &indexedFile{Path: "test.sql", DsProductID: 1})
			if err != nil {
				t.Fatal(err)
			}
			if len(findings) != tc.wantCount {
				t.Errorf("checkMaxProcParam findings = %d, want %d", len(findings), tc.wantCount)
			}
			if tc.wantCount > 0 && len(findings) > 0 {
				if findings[0].Severity != SeverityPostgreReq {
					t.Errorf("severity = %d, want %d", findings[0].Severity, SeverityPostgreReq)
				}
				if findings[0].Object != "TestProc" {
					t.Errorf("object = %q, want TestProc", findings[0].Object)
				}
				if findings[0].Line != 10 {
					t.Errorf("line = %d, want 10", findings[0].Line)
				}
			}
		})
	}

	t.Run("two procs one exceeds", func(t *testing.T) {
		r := &Runner{}
		parsed := &sqlparser.ParseResult{
			Procedures: []*model.SQLProcedure{
				{ProcName: "ProcOK", LineStart: 1, Params: makeParams(50)},
				{ProcName: "ProcBig", LineStart: 100, Params: makeParams(95)},
			},
		}
		findings, err := r.checkMaxProcParam(parsed, &indexedFile{Path: "test.sql", DsProductID: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(findings) != 1 {
			t.Errorf("expected 1 finding, got %d", len(findings))
		}
		if len(findings) > 0 && findings[0].Object != "ProcBig" {
			t.Errorf("expected ProcBig, got %q", findings[0].Object)
		}
	})
}

func TestCountTopLevelJoins(t *testing.T) {
	cases := []struct {
		name  string
		stmt  string
		want  int
	}{
		{name: "no joins", stmt: "SELECT * FROM t WHERE x = 1", want: 0},
		{name: "one join", stmt: "SELECT * FROM t INNER JOIN s ON t.id = s.id", want: 1},
		{name: "12 joins", stmt: func() string {
			s := "SELECT * FROM t0"
			for i := 1; i <= 12; i++ {
				s += fmt.Sprintf(" JOIN t%d ON t0.id = t%d.id", i, i)
			}
			return s
		}(), want: 12},
		{name: "13 joins", stmt: func() string {
			s := "SELECT * FROM t0"
			for i := 1; i <= 13; i++ {
				s += fmt.Sprintf(" JOIN t%d ON t0.id = t%d.id", i, i)
			}
			return s
		}(), want: 13},
		{name: "join in subquery not counted", stmt: "SELECT * FROM t WHERE id IN (SELECT id FROM a JOIN b ON a.id = b.id)", want: 0},
		{name: "top-level and subquery mixed", stmt: "SELECT * FROM t JOIN s ON t.id = s.id WHERE t.id IN (SELECT id FROM a JOIN b ON a.id = b.id)", want: 1},
		{name: "join in string literal not counted", stmt: "SELECT 'LEFT JOIN foo' FROM t INNER JOIN s ON t.id = s.id", want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := countTopLevelJoins(tc.stmt)
			if got != tc.want {
				t.Errorf("countTopLevelJoins() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCheckTooManyJoins(t *testing.T) {
	buildStmt := func(joinCount int) string {
		s := "SELECT * FROM t0"
		for i := 1; i <= joinCount; i++ {
			s += fmt.Sprintf(" JOIN t%d ON t0.id = t%d.id", i, i)
		}
		return s
	}

	writeTemp := func(t *testing.T, content string) string {
		t.Helper()
		f, err := os.CreateTemp("", "joins*.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(content); err != nil {
			t.Fatal(err)
		}
		f.Close()
		return f.Name()
	}

	t.Run("12 joins no finding", func(t *testing.T) {
		content := buildStmt(12)
		path := writeTemp(t, content)
		defer os.Remove(path)
		r := &Runner{}
		r.exec = &reviewExecContext{filePath: normalizePath(path), content: []byte(content), lines: strings.Split(content, "\n")}
		findings, err := r.checkTooManyJoins(&indexedFile{Path: path, DsProductID: 1})
		r.exec = nil
		if err != nil {
			t.Fatal(err)
		}
		if len(findings) != 0 {
			t.Errorf("expected 0 findings, got %d", len(findings))
		}
	})

	t.Run("13 joins finding", func(t *testing.T) {
		content := buildStmt(13)
		path := writeTemp(t, content)
		defer os.Remove(path)
		r := &Runner{}
		r.exec = &reviewExecContext{filePath: normalizePath(path), content: []byte(content), lines: strings.Split(content, "\n")}
		findings, err := r.checkTooManyJoins(&indexedFile{Path: path, DsProductID: 1})
		r.exec = nil
		if err != nil {
			t.Fatal(err)
		}
		if len(findings) != 1 {
			t.Errorf("expected 1 finding, got %d", len(findings))
		}
		if len(findings) > 0 && findings[0].Severity != SeverityPostgreReq {
			t.Errorf("severity = %d, want %d", findings[0].Severity, SeverityPostgreReq)
		}
	})

	t.Run("join in subquery not counted", func(t *testing.T) {
		content := "SELECT * FROM t WHERE id IN (SELECT id FROM a JOIN b ON a.id = b.id JOIN c ON c.id = b.id JOIN d ON d.id = c.id JOIN e ON e.id = d.id JOIN f ON f.id = e.id JOIN g ON g.id = f.id JOIN h ON h.id = g.id JOIN i ON i.id = h.id JOIN j ON j.id = i.id JOIN k ON k.id = j.id JOIN l ON l.id = k.id JOIN m ON m.id = l.id)"
		path := writeTemp(t, content)
		defer os.Remove(path)
		r := &Runner{}
		r.exec = &reviewExecContext{filePath: normalizePath(path), content: []byte(content), lines: strings.Split(content, "\n")}
		findings, err := r.checkTooManyJoins(&indexedFile{Path: path, DsProductID: 1})
		r.exec = nil
		if err != nil {
			t.Fatal(err)
		}
		if len(findings) != 0 {
			t.Errorf("expected 0 findings (all JOINs in subquery), got %d", len(findings))
		}
	})
}

func TestDetectFileEncoding(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{name: "pure ASCII", data: []byte("SELECT * FROM t WHERE x = 1"), want: "ASCII"},
		{name: "empty", data: []byte{}, want: "ASCII"},
		{name: "UTF-8 with cyrillic", data: []byte("-- комментарий\nSELECT 1"), want: "UTF-8"},
		{name: "UTF-8 BOM", data: append([]byte{0xEF, 0xBB, 0xBF}, []byte("SELECT 1")...), want: "UTF-8"},
		// CP866: А-Я = 0x80–0x9F. Байты 0x80,0x81 — буквы "А","Б" в CP866
		{name: "CP866 cyrillic", data: []byte{0x53, 0x45, 0x4C, 0x80, 0x81, 0x82}, want: "CP866"},
		// CP1251: А-Я = 0xC0–0xDF. Байты 0xC0,0xC1 — буквы "А","Б" в CP1251
		{name: "CP1251 cyrillic", data: []byte{0x53, 0x45, 0x4C, 0xC0, 0xC1, 0xC2, 0xC3}, want: "CP1251"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := detectFileEncoding(tc.data)
			if got != tc.want {
				t.Errorf("detectFileEncoding() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCheckShouldBeCP866(t *testing.T) {
	runner := &Runner{}

	writeTemp := func(t *testing.T, data []byte) string {
		t.Helper()
		f, err := os.CreateTemp("", "cp866*.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
		f.Close()
		return f.Name()
	}

	t.Run("CP866 no finding", func(t *testing.T) {
		// 0x80–0x9F — маркеры CP866
		path := writeTemp(t, []byte{0x53, 0x45, 0x4C, 0x80, 0x81, 0x82})
		defer os.Remove(path)
		findings, err := runner.checkShouldBeCP866(&indexedFile{Path: path, DsProductID: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(findings) != 0 {
			t.Errorf("expected no findings, got %+v", findings)
		}
	})

	t.Run("ASCII no finding", func(t *testing.T) {
		path := writeTemp(t, []byte("SELECT 1"))
		defer os.Remove(path)
		findings, err := runner.checkShouldBeCP866(&indexedFile{Path: path, DsProductID: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(findings) != 0 {
			t.Errorf("expected no findings, got %+v", findings)
		}
	})

	t.Run("UTF-8 finding", func(t *testing.T) {
		path := writeTemp(t, []byte("-- комментарий"))
		defer os.Remove(path)
		findings, err := runner.checkShouldBeCP866(&indexedFile{Path: path, DsProductID: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(findings) != 1 {
			t.Errorf("expected 1 finding, got %d", len(findings))
		}
		if len(findings) > 0 && findings[0].Severity != SeverityPostgreReq {
			t.Errorf("severity = %d, want %d", findings[0].Severity, SeverityPostgreReq)
		}
	})

	t.Run("CP1251 finding", func(t *testing.T) {
		// 0xC0–0xDF доминируют — CP1251
		path := writeTemp(t, []byte{0x53, 0x45, 0x4C, 0xC0, 0xC1, 0xC2, 0xC3, 0xC4})
		defer os.Remove(path)
		findings, err := runner.checkShouldBeCP866(&indexedFile{Path: path, DsProductID: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(findings) != 1 {
			t.Errorf("expected 1 finding, got %d", len(findings))
		}
	})
}

func TestStripLineComments(t *testing.T) {
	cases := []struct {
		name        string
		line        string
		inBlock     bool
		wantOut     string
		wantInBlock bool
	}{
		{name: "no comment", line: "WHERE x = 1", inBlock: false, wantOut: "WHERE x = 1", wantInBlock: false},
		{name: "line comment", line: "WHERE x = 1 -- check null", inBlock: false, wantOut: "WHERE x = 1 ", wantInBlock: false},
		{name: "block comment start", line: "WHERE /* comment", inBlock: false, wantOut: "WHERE ", wantInBlock: true},
		{name: "block comment end", line: "still comment */ AND x = 1", inBlock: true, wantOut: " AND x = 1", wantInBlock: false},
		{name: "full block comment inline", line: "WHERE /* skip this */ x = 1", inBlock: false, wantOut: "WHERE  x = 1", wantInBlock: false},
		{name: "inside block", line: "this is all comment", inBlock: true, wantOut: "", wantInBlock: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotOut, gotBlock := stripLineComments(tc.line, tc.inBlock)
			if gotOut != tc.wantOut {
				t.Errorf("stripLineComments(%q, %v) output = %q, want %q", tc.line, tc.inBlock, gotOut, tc.wantOut)
			}
			if gotBlock != tc.wantInBlock {
				t.Errorf("stripLineComments(%q, %v) inBlock = %v, want %v", tc.line, tc.inBlock, gotBlock, tc.wantInBlock)
			}
		})
	}
}

func TestCheckNullComparison_Detects(t *testing.T) {
	runner := &Runner{}
	cases := []struct {
		name    string
		content string
		wantN   int
	}{
		{name: "eq null", content: "WHERE x = NULL", wantN: 1},
		{name: "neq null", content: "WHERE x <> NULL", wantN: 1},
		{name: "ne null", content: "WHERE x != NULL", wantN: 1},
		{name: "lt null", content: "WHERE x < NULL", wantN: 1},
		{name: "gt null", content: "WHERE x > NULL", wantN: 1},
		{name: "lte null", content: "WHERE x <= NULL", wantN: 1},
		{name: "gte null", content: "WHERE x >= NULL", wantN: 1},
		{name: "null eq left", content: "WHERE NULL = x", wantN: 1},
		{name: "in null only", content: "WHERE x IN (NULL)", wantN: 1},
		{name: "in null mixed", content: "WHERE x IN (1, NULL, 2)", wantN: 1},
		{name: "multiple lines", content: "WHERE a = NULL\nAND b IN (NULL)", wantN: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.CreateTemp("", "nullcmp*.sql")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(f.Name())
			if _, err := f.WriteString(tc.content); err != nil {
				t.Fatal(err)
			}
			f.Close()

			file := &indexedFile{Path: f.Name(), DsProductID: 1}
			runner.exec = &reviewExecContext{
				filePath: normalizePath(f.Name()),
				content:  []byte(tc.content),
				lines:    strings.Split(tc.content, "\n"),
			}
			findings, err := runner.checkNullComparison(file)
			runner.exec = nil
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != tc.wantN {
				t.Errorf("checkNullComparison findings = %d, want %d (findings: %+v)", len(findings), tc.wantN, findings)
			}
		})
	}
}

func TestCheckNullComparison_NoFalsePositive(t *testing.T) {
	runner := &Runner{}
	cases := []struct {
		name    string
		content string
	}{
		{name: "is null", content: "WHERE x IS NULL"},
		{name: "is not null", content: "WHERE x IS NOT NULL"},
		{name: "comment eq null", content: "-- WHERE x = NULL"},
		{name: "block comment eq null", content: "/* WHERE x = NULL */"},
		{name: "string literal null", content: "WHERE x = 'NULL'"},
		{name: "in with values", content: "WHERE x IN (1, 2, 3)"},
		{name: "proc param default null", content: "@Date           DSOPERDAY = null,"},
		{name: "proc param default null no comma", content: "@CalcFlowMaxPrcMode  DSTINYINT    = null"},
		{name: "proc param default null indented", content: "                 @PaymentMode    DSINT_KEY = null,"},
		{name: "declare var default null", content: "@MyVar DSINT_KEY = null"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.CreateTemp("", "nullcmp*.sql")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(f.Name())
			if _, err := f.WriteString(tc.content); err != nil {
				t.Fatal(err)
			}
			f.Close()

			file := &indexedFile{Path: f.Name(), DsProductID: 1}
			runner.exec = &reviewExecContext{
				filePath: normalizePath(f.Name()),
				content:  []byte(tc.content),
				lines:    strings.Split(tc.content, "\n"),
			}
			findings, err := runner.checkNullComparison(file)
			runner.exec = nil
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != 0 {
				t.Errorf("checkNullComparison false positive: got %d findings, want 0 (findings: %+v)", len(findings), findings)
			}
		})
	}
}
