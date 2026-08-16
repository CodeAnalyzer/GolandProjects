package review

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
)

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
			runner.exec = &reviewExecContext{
				filePath:    normalizePath(tmpFile.Name()),
				content:     []byte(tc.content),
				lines:       strings.Split(tc.content, "\n"),
				macroResult: replaceMacros(tc.content),
			}

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

func TestAnalyzeConditionForEqColumn_SelectAlias_NoFinding(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	cases := []struct {
		name  string
		lines []string
	}{
		{
			name: "select alias same name",
			lines: []string{
				"select TownName = TownName",
			},
		},
		{
			name: "select alias same name qualified",
			lines: []string{
				"select a.TownName = a.TownName",
			},
		},
		{
			name: "insert select aliases",
			lines: []string{
				"select SPID        = SPID",
				"      ,ClientID    = LegalID",
				"      ,AddressID   = AddressID",
				"      ,TownName    = TownName",
			},
		},
		{
			name: "subquery select alias",
			lines: []string{
				"where col = (select Alias = Alias from t)",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			findings := analyzeConditionForEqColumn(tc.lines, 1, file)
			if len(findings) != 0 {
				t.Fatalf("expected no finding for SELECT alias, got %v", findings)
			}
		})
	}
}

func TestAnalyzeConditionForEqColumn_LineNumber(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}
	lines := []string{
		"where col1 = :param",
		"  and col2 = col2",
	}
	findings := analyzeConditionForEqColumn(lines, 10, file)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].Object != "col2" {
		t.Fatalf("object = %q, want col2", findings[0].Object)
	}
	if findings[0].Line != 11 {
		t.Fatalf("line = %d, want 11", findings[0].Line)
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

func TestAnalyzeStatementForHintType_InsertSelectDoesNotBreakOnInnerSelect(t *testing.T) {
	file := &indexedFile{Path: "test.sql", DsProductID: 1}

	lines := []string{
		"insert into pAPI_ACCR_AftMassProcNTF M_WITH_ROWLOCK",
		"  (SPID, ObjectID, NTFID)",
		"  select @@spid, LoanID, NTFID",
		"    from pAPI_Loan_Notification M_NOLOCK_INDEX(XPKpAPI_Loan_Notification)",
		"   where SPID = @@spid",
		"  M_KEEPPLAN",
	}

	findings := analyzeStatementForHintType(lines, 3811, file)
	// INSERT не проверяется правилом tableHintIsRight — splitStatementsForHintType
	// пропустит его (не select/update/delete), а внутренний SELECT не должен
	// быть ошибочно выделен как отдельный оператор
	if len(findings) != 0 {
		t.Fatalf("expected no findings for INSERT...SELECT, got %#v", findings)
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

func TestCheckTableFullScan_UpdateWithStringLiteralSemicolon_NoFinding(t *testing.T) {
	// Регрессионный тест: точка с запятой внутри строкового литерала в SET
	// не должна разрывать UPDATE-оператор до FROM/WHERE.
	file := &indexedFile{Path: "test.sql", DsProductID: 1}
	lines := []string{
		"  while @cnt > 0",
		"  begin",
		"",
		"    update pCNENP_LoanExt_CollateralInfo",
		"       set CollateralName = substring(ci.CollateralName + ';' + isnull(le.Name, ''), 1, 1954)",
		"     from pCNENP_LoanExt_CollateralInfo ci M_UPDLOCK_INDEX(XPKpCNENP_LoanExt_CollateralInfo)",
		"      inner join pAPI_CCvr_LinkElement le M_NOLOCK_INDEX(XIE0pAPI_CCvr_LinkElement)",
		"              on le.SPID       = ci.SPID",
		"             and le.ElementID  = ci.ElementID",
		"      where ci.SPID      = @@SPID ",
		"        and ci.ElementID > 0     ",
		"     M_FORCEORDER",
		"",
		"    update pCNENP_LoanExt_CollateralInfo",
		"       set ElementID = isnull((select min(oc.ElementID)",
		"                         from pAPI_CCvr_ObjectCover   oc M_NOLOCK_INDEX(XPKpAPI_CCvr_ObjectCover)",
		"                        where oc.SPID             = ci.SPID ",
		"                          and oc.ContractCoverID  = ci.ContractCoverID",
		"                          and oc.ElementID        > ci.ElementID),0)",
		"      from pCNENP_LoanExt_CollateralInfo ci M_UPDLOCK_INDEX(XPKpCNENP_LoanExt_CollateralInfo)",
		"     where ci.SPID      = @@SPID",
		"       and ci.ElementID > 0 ",
		"    M_FORCEORDER",
	}
	content := strings.Join(lines, "\n")

	runner := &Runner{}
	runner.exec = &reviewExecContext{
		filePath:    normalizePath(file.Path),
		content:     []byte(content),
		macroResult: replaceMacros(content),
		lines:       lines,
	}
	findings, err := runner.checkTableFullScan(file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected no findings, got %#v", findings)
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

func TestHasIfWithAndAndQueryMulti_MultilineIfExistsAndCondition(t *testing.T) {
	// Многострочное IF: exists(select ...) на одной строке, and @var = 0 на другой
	condition := "if exists(select 1 from pLoanBureau_Resource r where r.SPID = @@SPID) and @BCH_INTRATE_CRLINE_ALG = 0 begin"
	if !hasIfWithAndAndQueryMulti(condition) {
		t.Fatalf("should detect multiline IF with EXISTS and AND")
	}
}

func TestHasIfWithAndAndQueryMulti_NoAnd_NoFinding(t *testing.T) {
	condition := "if exists(select 1 from t) begin"
	if hasIfWithAndAndQueryMulti(condition) {
		t.Fatalf("should NOT detect IF with query but no AND")
	}
}

func TestHasTopLevelAnd(t *testing.T) {
	// AND на top-level
	if !hasTopLevelAnd("@a = 1 and @b = 2") {
		t.Fatalf("should detect top-level AND")
	}
	if !hasTopLevelAnd("exists(select 1 from t) and @var = 0") {
		t.Fatalf("should detect top-level AND after EXISTS")
	}
	// AND только внутри скобок — не top-level
	if hasTopLevelAnd("exists(select 1 from t where a = 1 and b = 2)") {
		t.Fatalf("should NOT detect AND inside parentheses")
	}
	// Нет AND вообще
	if hasTopLevelAnd("@a = 1 or @b = 2") {
		t.Fatalf("should NOT detect AND when none present")
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

func TestCollectVariableTypes_ExecOutputDoesNotOverwriteDeclareType(t *testing.T) {
	content := `  declare
    @StrValue                        DSCOMMENT,
    @BCH_DATE_STARTLOADBHONCREDLINE  DSOPERDAY,
    @BCH_INTRATE_CRLINE_ALG          DSIDENTIFIER

  exec FCD_BKI_tConfigParam
         @SysName  = 'BCH_DATE_STARTLOADBHONCREDLINE',
         @StrValue = @StrValue output

  select @BCH_DATE_STARTLOADBHONCREDLINE = convert(smalldatetime, isnull(@StrValue, '19000101'),103)`

	types := collectVariableTypes(&sqlparser.ParseResult{}, content)
	if got := strings.ToUpper(types["strvalue"]); got != "DSCOMMENT" {
		t.Fatalf("strvalue type = %q, want DSCOMMENT (should not be overwritten by 'output' from exec)", got)
	}
	if got := strings.ToUpper(types["bch_intrate_crline_alg"]); got != "DSIDENTIFIER" {
		t.Fatalf("bch_intrate_crline_alg type = %q, want DSIDENTIFIER", got)
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
		{"convert(int, col)", "smalldatetime", false}, // convert к int не относится к smalldatetime
		{"col", "smalldatetime", false},              // нет convert

		// convert() cases - эквивалентные типы (для datetime)
		{"convert(smalldatetime, col)", "dsoperday", true}, // smalldatetime эквивалентен DSOPERDAY
		{"convert(smalldatetime, col)", "date", false},     // smalldatetime -> date: потеря точности
		{"convert(date, col)", "dsoperday", true},          // date эквивалентен DSOPERDAY
		{"convert(datetime, col)", "dsdatetime", true},     // datetime эквивалентен DSDATETIME

		// cast() cases - точное совпадение
		{"cast(col as smalldatetime)", "smalldatetime", true},
		{"CAST(cc.CreditDateFrom AS smalldatetime)", "smalldatetime", true},
		{"cast(col as varchar(10))", "varchar", true},
		{"cast(col as date)", "date", true},
		{"cast(col as int)", "smalldatetime", false}, // cast к int не относится к smalldatetime

		// cast() cases - эквивалентные типы (для datetime)
		{"cast(col as smalldatetime)", "dsoperday", true}, // smalldatetime эквивалентен DSOPERDAY
		{"cast(col as smalldatetime)", "date", false},     // smalldatetime -> date: потеря точности
		{"cast(col as date)", "dsoperday", true},          // date эквивалентен DSOPERDAY

		// case-insensitive
		{"Convert(SmallDateTime, col)", "smalldatetime", true},
		{"CAST(col AS SMALLDATETIME)", "smalldatetime", true},

		// empty cases
		{"", "smalldatetime", false},
		{"convert(smalldatetime, col)", "", false},

		// CASE с вложенным convert к чужому типу — не должно считаться explicit conversion
		{"case when @Tmp = 0 then dateadd(dd, convert(numeric, Val), '19000101') else convert(datetime, Val, 103) end", "dsoperday", false},
		// CASE с convert к целевому типу — должно считаться explicit conversion
		{"case when @Tmp = 0 then convert(dsoperday, Val) else convert(dsoperday, Val2) end", "dsoperday", true},
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
			runner.exec = &reviewExecContext{
				filePath:    normalizePath(tmpFile.Name()),
				content:     []byte(tc.content),
				lines:       strings.Split(tc.content, "\n"),
				macroResult: replaceMacros(tc.content),
			}

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
	runner.exec = &reviewExecContext{
		filePath:    normalizePath(tmpFile.Name()),
		content:     []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}

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
		{
			name:          "insert into temp table (#) - no finding",
			content:       "INSERT INTO #DealAttrValue VALUES (1, 2)",
			expectFinding: false,
		},
		{
			name:          "insert into temp table without INTO (#) - no finding",
			content:       "INSERT #TempTable SELECT * FROM tSource",
			expectFinding: false,
		},
		{
			name: "string literal 'insert' in SELECT list with real INSERT+ROWLOCK",
			content: `insert pTable M_WITH_ROWLOCK
           (ID, Msg)
    select @@spid,
           'insert',
           'BPCondition'

    exec FCD_CON_Message`,
			expectFinding: false,
		},
		{
			name:          "string literal 'insert' in IF condition, no real INSERT",
			content:       "if @Action = 'insert'\n    select 1",
			expectFinding: false,
		},
		{
			name:          "string literal 'insert into tTable' in error message",
			content:       "select 'insert into tTable' as Msg",
			expectFinding: false,
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
			runner.exec = &reviewExecContext{
				filePath:    normalizePath(tmpFile.Name()),
				content:     []byte(tc.content),
				lines:       strings.Split(tc.content, "\n"),
				macroResult: replaceMacros(tc.content),
			}

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

func TestIsInStringLiteral(t *testing.T) {
	cases := []struct {
		name string
		line string
		pos  int
		want bool
	}{
		{"inside single quotes", "select 'insert'", 10, true},
		{"outside quotes", "select insert", 7, false},
		{"no quotes at all", "insert into t", 0, false},
		{"escaped quote inside string", "select 'it''s insert'", 14, true},
		{"after closing quote", "select 'val' insert", 14, false},
		{"pos at opening quote", "select 'insert'", 7, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isInStringLiteral(tc.line, tc.pos)
			if got != tc.want {
				t.Fatalf("isInStringLiteral(%q, %d) = %v, want %v", tc.line, tc.pos, got, tc.want)
			}
		})
	}
}

func TestHasDefaultAssignmentInBody(t *testing.T) {
	runner := &Runner{}

	cases := []struct {
		name        string
		procBody    string
		paramName   string
		expectFound bool
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
			name:        "assignment but no usage - should pass",
			procBody:    `select @ParentProtocolID = isnull(@ParentProtocolID, 0)`,
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
		{
			name: "param prefix collision in declare - AmountPrc vs AmountPrcOvrDbt",
			procBody: `as
  declare @AmountPrcOvrDbt DSMONEY

  select @RetVal       = 0,
         @AmountPrc    = 0
  if @AmountPrc > 0
    select 1`,
			paramName:   "@AmountPrc",
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
		r.exec = &reviewExecContext{filePath: normalizePath(path), content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
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
		if _, err = f.WriteString(content); err != nil {
			t.Fatal(err)
		}
		f.Close()
		dataPath := filepath.Join(filepath.Dir(f.Name()), "DATA", "test.sql")
		if err = os.MkdirAll(filepath.Dir(dataPath), 0755); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(dataPath, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		defer os.Remove(dataPath)

		r := &Runner{}
		r.exec = &reviewExecContext{filePath: normalizePath(dataPath), content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
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
		name string
		stmt string
		want int
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
		r.exec = &reviewExecContext{filePath: normalizePath(path), content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
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
		r.exec = &reviewExecContext{filePath: normalizePath(path), content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
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
		r.exec = &reviewExecContext{filePath: normalizePath(path), content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
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
		{name: "pure ASCII", data: []byte("SELECT * FROM t WHERE x = 1"), want: "CP866"},
		{name: "empty", data: []byte{}, want: "CP866"},
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
				filePath:    normalizePath(f.Name()),
				content:     []byte(tc.content),
				lines:       strings.Split(tc.content, "\n"),
				macroResult: replaceMacros(tc.content),
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
		{name: "proc param default null output", content: "@AmountDelinq      DSMONEY   = null output,"},
		{name: "proc param default null output no comma", content: "@AmountPrc         DSMONEY   = null output"},
		{name: "proc param default null output indented", content: "                 @AmountMain        DSMONEY   = null output,"},
		{name: "select assign null continuation", content: "         @UserAnnual   = NULL"},
		{name: "select assign null continuation comma", content: "         @UserAnnual   = NULL,"},
		{name: "select assign null single line", content: "  select @LimitAmountPeriodTmp = NULL"},
		{name: "select assign null single line trailing spaces", content: "  select @LimitAmountPeriodTmp = NULL    "},
		{name: "select assign null single line comma", content: "  select @UserAnnual = NULL,"},
		{name: "proc param varchar default null", content: "             @DocNumber          varchar(20)   = Null ,"},
		{name: "proc param varchar250 default null", content: "             @AComment           varchar(250)  = Null ,"},
		{name: "proc param numeric default null", content: "             @Amount             numeric(15,2) = Null ,"},
		{name: "proc param decimal default null", content: "             @Rate               decimal(10,4) = Null"},
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
				filePath:    normalizePath(f.Name()),
				content:     []byte(tc.content),
				lines:       strings.Split(tc.content, "\n"),
				macroResult: replaceMacros(tc.content),
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

func TestCheckEmptyReturn(t *testing.T) {
	runner := &Runner{}
	cases := []struct {
		name     string
		content  string
		wantN    int
		wantLine int
	}{
		{name: "standalone return", content: "RETURN", wantN: 1, wantLine: 1},
		{name: "return with semicolon", content: "RETURN;", wantN: 1, wantLine: 1},
		{name: "indented return", content: "    RETURN  ;", wantN: 1, wantLine: 1},
		{name: "return with value", content: "RETURN @code", wantN: 0},
		{name: "return number", content: "RETURN 0", wantN: 0},
		{name: "return expression", content: "RETURN (SELECT 1)", wantN: 0},
		{name: "return in comment", content: "-- RETURN", wantN: 0},
		{name: "return in block comment", content: "/* RETURN */", wantN: 0},
		{name: "return in string", content: "SELECT 'RETURN'", wantN: 0},
		{name: "return after other word", content: "SELECT c.return_date FROM t", wantN: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.CreateTemp("", "emptyret*.sql")
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
				filePath:    normalizePath(f.Name()),
				content:     []byte(tc.content),
				lines:       strings.Split(tc.content, "\n"),
				macroResult: replaceMacros(tc.content),
			}
			findings, err := runner.checkEmptyReturn(file)
			runner.exec = nil
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != tc.wantN {
				t.Errorf("checkEmptyReturn findings = %d, want %d (findings: %+v)", len(findings), tc.wantN, findings)
			}
			if tc.wantN > 0 && len(findings) > 0 && findings[0].Line != tc.wantLine {
				t.Errorf("checkEmptyReturn line = %d, want %d", findings[0].Line, tc.wantLine)
			}
		})
	}
}

func TestCheckRawTransactionControl(t *testing.T) {
	runner := &Runner{}
	cases := []struct {
		name     string
		content  string
		wantN    int
		wantLine int
	}{
		{name: "begin tran", content: "BEGIN TRAN", wantN: 1, wantLine: 1},
		{name: "begin transaction", content: "BEGIN TRANSACTION", wantN: 1, wantLine: 1},
		{name: "commit", content: "COMMIT", wantN: 1, wantLine: 1},
		{name: "rollback", content: "ROLLBACK", wantN: 1, wantLine: 1},
		{name: "save tran", content: "SAVE TRAN", wantN: 1, wantLine: 1},
		{name: "save transaction", content: "SAVE TRANSACTION", wantN: 1, wantLine: 1},
		{name: "end tran", content: "END TRAN", wantN: 1, wantLine: 1},
		{name: "lowercase begin tran", content: "begin tran", wantN: 1, wantLine: 1},
		{name: "begin tran with semicolon", content: "BEGIN TRAN;", wantN: 1, wantLine: 1},
		{name: "indented rollback", content: "    ROLLBACK  ;", wantN: 1, wantLine: 1},
		{name: "BEGIN_TRAN macro", content: "BEGIN_TRAN('name')", wantN: 0},
		{name: "GOEND macro", content: "GOEND(@RetVal)", wantN: 0},
		{name: "COMMIT_TRAN macro", content: "COMMIT_TRAN('name')", wantN: 0},
		{name: "__BEGIN_TRAN__ macro", content: "__BEGIN_TRAN__(T1)", wantN: 0},
		{name: "__ERR_TRAN__ macro", content: "__ERR_TRAN__(T1)", wantN: 0},
		{name: "__COMMIT_TRAN__ macro", content: "__COMMIT_TRAN__", wantN: 0},
		{name: "__END_TRAN__ macro", content: "__END_TRAN__(T1)", wantN: 0},
		{name: "comment begin tran", content: "-- BEGIN TRAN", wantN: 0},
		{name: "block comment rollback", content: "/* ROLLBACK */", wantN: 0},
		{name: "string literal commit", content: "SELECT 'COMMIT'", wantN: 0},
		{name: "word containing begin", content: "SELECT begin_date FROM t", wantN: 0},
		{name: "word containing commit", content: "SELECT commitment FROM t", wantN: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.CreateTemp("", "rawtx*.sql")
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
				filePath:    normalizePath(f.Name()),
				content:     []byte(tc.content),
				lines:       strings.Split(tc.content, "\n"),
				macroResult: replaceMacros(tc.content),
			}
			findings, err := runner.checkRawTransactionControl(file)
			runner.exec = nil
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != tc.wantN {
				t.Errorf("checkRawTransactionControl findings = %d, want %d (findings: %+v)", len(findings), tc.wantN, findings)
			}
			if tc.wantN > 0 && len(findings) > 0 && findings[0].Line != tc.wantLine {
				t.Errorf("checkRawTransactionControl line = %d, want %d", findings[0].Line, tc.wantLine)
			}
		})
	}
}

func TestExtractSetColumns(t *testing.T) {
	cases := []struct {
		name string
		stmt string
		want []string
	}{
		{name: "simple", stmt: "UPDATE t SET a = 1, b = 2 WHERE id = 3", want: []string{"a", "b"}},
		{name: "alias column", stmt: "UPDATE t SET t.a = 1, t.b = 2 FROM t WHERE id = 3", want: []string{"a", "b"}},
		{name: "with isnull", stmt: "UPDATE t SET a = isnull(@a, a), b = @b + 1 WHERE id = 3", want: []string{"a", "b"}},
		{name: "variable only", stmt: "UPDATE t SET @a = 1, @b = 2 WHERE id = 3", want: nil},
		{name: "mixed var and col", stmt: "UPDATE t SET @a = 1, b = 2 WHERE id = 3", want: []string{"b"}},
		{name: "case in set", stmt: "UPDATE t SET a = case when x=1 then y else z end, b = 2 WHERE id = 3", want: []string{"a", "b"}},
		{name: "no from where", stmt: "UPDATE t SET a = 1, b = 2", want: []string{"a", "b"}},
		{name: "semicolon end", stmt: "UPDATE t SET a = 1, b = 2;", want: []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractSetColumns(tc.stmt)
			if len(got) != len(tc.want) {
				t.Fatalf("extractSetColumns got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("extractSetColumns[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestFindInSubqueryPositions(t *testing.T) {
	cases := []struct {
		name string
		stmt string
		want int
	}{
		{name: "in subquery", stmt: "select * from t where id in (select x from s)", want: 1},
		{name: "not in subquery", stmt: "select * from t where id not in (select x from s)", want: 1},
		{name: "constant list", stmt: "select * from t where id in (1, 2, 3)", want: 0},
		{name: "variable", stmt: "select * from t where id in (@ids)", want: 0},
		{name: "exists ok", stmt: "select * from t where exists(select 1 from s where x = t.id)", want: 0},
		{name: "mixed in and subquery", stmt: "select * from t where id in (select x from s) and flag in (1,2)", want: 1},
		{name: "nested in subquery", stmt: "select * from t where id in (select x from s where y in (1,2))", want: 1},
		{name: "update with in subquery", stmt: "update t set a = 1 where id in (select x from s)", want: 1},
		{name: "delete with not in subquery", stmt: "delete from t where id not in (select x from s)", want: 1},
		{name: "string literal containing in", stmt: "select * from t where name = 'in (select * from u)'", want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := findInSubqueryPositions(tc.stmt)
			if len(got) != tc.want {
				t.Fatalf("findInSubqueryPositions findings = %d, want %d (stmt: %s)", len(got), tc.want, tc.stmt)
			}
		})
	}
}

func TestParseVarDeclaration(t *testing.T) {
	cases := []struct {
		item         string
		wantName     string
		wantType     string
		wantFullType string
	}{
		{item: "@x VARCHAR", wantName: "@x", wantType: "VARCHAR", wantFullType: "VARCHAR"},
		{item: "@x AS VARCHAR", wantName: "@x", wantType: "VARCHAR", wantFullType: "VARCHAR"},
		{item: "@name NVARCHAR(100)", wantName: "@name", wantType: "NVARCHAR", wantFullType: "NVARCHAR(100)"},
		{item: "@id INT", wantName: "@id", wantType: "INT", wantFullType: "INT"},
		{item: "@flag AS CHAR(1)", wantName: "@flag", wantType: "CHAR", wantFullType: "CHAR(1)"},
	}
	for _, tc := range cases {
		t.Run(tc.item, func(t *testing.T) {
			name, typ, full := parseVarDeclaration(tc.item)
			if name != tc.wantName || typ != tc.wantType || full != tc.wantFullType {
				t.Fatalf("parseVarDeclaration(%q) = (%q, %q, %q), want (%q, %q, %q)", tc.item, name, typ, full, tc.wantName, tc.wantType, tc.wantFullType)
			}
		})
	}
}

func TestIsVarCharLikeType(t *testing.T) {
	for _, tc := range []struct {
		t    string
		want bool
	}{
		{"varchar", true}, {"VARCHAR", true}, {"nvarchar", true}, {"char", true}, {"nchar", true},
		{"int", false}, {"datetime", false}, {"text", false}, {"ntext", false},
	} {
		if got := isVarCharLikeType(tc.t); got != tc.want {
			t.Fatalf("isVarCharLikeType(%q) = %v, want %v", tc.t, got, tc.want)
		}
	}
}

func TestHasSizeInType(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want bool
	}{
		{"VARCHAR(100)", true}, {"VARCHAR (100)", true}, {"VARCHAR", false}, {"NVARCHAR", false},
		{"CHAR(1)", true}, {"NCHAR", false}, {"VARCHAR(MAX)", true},
	} {
		if got := hasSizeInType(tc.expr); got != tc.want {
			t.Fatalf("hasSizeInType(%q) = %v, want %v", tc.expr, got, tc.want)
		}
	}
}

func TestFindInsertWithoutColumns(t *testing.T) {
	cases := []struct {
		name string
		text string
		want int
	}{
		{name: "insert values no columns", text: "insert into t values (1, 2)", want: 1},
		{name: "insert select no columns", text: "insert into t select * from s", want: 1},
		{name: "insert exec no columns", text: "insert into t exec proc", want: 1},
		{name: "insert default values", text: "insert into t default values", want: 1},
		{name: "insert with columns", text: "insert into t (a, b) values (1, 2)", want: 0},
		{name: "insert select with columns", text: "insert into t (a, b) select c, d from s", want: 0},
		{name: "insert with macro", text: "insert into t M_WITH_ROWLOCK (a, b) values (1, 2)", want: 0},
		{name: "insert macro no columns", text: "insert into t M_WITH_ROWLOCK values (1, 2)", want: 1},
		{name: "insert macro with args no columns", text: "insert into t M_ROWLOCK_INDEX(X) values (1)", want: 1},
		{name: "string literal insert", text: "select 'insert into t values (1)'", want: 0},
		{name: "insert with schema", text: "insert into dbo.t values (1)", want: 1},
		{name: "insert with bracketed name", text: "insert into [dbo].[t] (a) values (1)", want: 0},
		{name: "insert no into with columns", text: "insert t (a) values (1)", want: 0},
		{name: "insert no into no columns", text: "insert t values (1)", want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := findInsertWithoutColumns(tc.text)
			if len(got) != tc.want {
				t.Fatalf("findInsertWithoutColumns findings = %d, want %d (text: %s)", len(got), tc.want, tc.text)
			}
		})
	}
}

func TestCheckPostgreLabelGotoLevel(t *testing.T) {
	runner := &Runner{}
	cases := []struct {
		name     string
		content  string
		wantN    int
		wantMsg  string
		wantLine int
	}{
		{
			name:    "label below goto - ok",
			content: "goto Label1\nLabel1:\nselect 1",
			wantN:   0,
		},
		{
			name:     "label above goto - finding",
			content:  "Label1:\nselect 1\ngoto Label1",
			wantN:    1,
			wantMsg:  "Метка 'Label1' для GOTO расположена выше оператора перехода.",
			wantLine: 3,
		},
		{
			name:    "goto in comment ignored",
			content: "-- goto Label1\nLabel1:\nselect 1",
			wantN:   0,
		},
		{
			name:    "label in string ignored",
			content: "goto Label1\nselect 'Label1:'\nLabel1:\nselect 1",
			wantN:   0,
		},
		{
			name:     "same line label and goto",
			content:  "Label1: goto Label1",
			wantN:    1,
			wantLine: 1,
		},
		{
			name:    "label above in block comment ignored",
			content: "/* Label1: */\ngoto Label1\nLabel1:\nselect 1",
			wantN:   0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.CreateTemp("", "goto_*.sql")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(f.Name())
			if _, err = f.WriteString(tc.content); err != nil {
				t.Fatal(err)
			}
			f.Close()

			runner.exec = &reviewExecContext{
				filePath:    normalizePath(f.Name()),
				content:     []byte(tc.content),
				lines:       strings.Split(tc.content, "\n"),
				macroResult: replaceMacros(tc.content),
			}

			findings, err := runner.checkPostgreLabelGotoLevel(&indexedFile{Path: f.Name(), DsProductID: 1})
			if err != nil {
				t.Fatal(err)
			}
			if len(findings) != tc.wantN {
				t.Fatalf("expected %d findings, got %d", tc.wantN, len(findings))
			}
			if tc.wantN > 0 && len(findings) > 0 {
				if !strings.Contains(findings[0].Message, tc.wantMsg) {
					t.Fatalf("expected message containing %q, got %q", tc.wantMsg, findings[0].Message)
				}
				if tc.wantLine > 0 && findings[0].Line != tc.wantLine {
					t.Fatalf("expected line %d, got %d", tc.wantLine, findings[0].Line)
				}
			}
		})
	}
}

func TestIsDateExpression(t *testing.T) {
	varTypes := map[string]string{
		"datevar": "datetime",
		"strvar":  "varchar(20)",
	}
	cases := []struct {
		expr string
		want bool
	}{
		{"getdate()", true},
		{"sysdatetime()", true},
		{"current_timestamp", true},
		{"@datevar", true},
		{"@strvar", false},
		{"convert(varchar(20), getdate())", true},
		{"substring(@datevar, 1, 10)", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			got := isDateExpression(tc.expr, varTypes)
			if got != tc.want {
				t.Fatalf("isDateExpression(%q) = %v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestCheckDateIntoString(t *testing.T) {
	cases := []struct {
		name       string
		content    string
		procs      []*model.SQLProcedure
		fragments  []*model.QueryFragment
		wantCount  int
		wantLine   int
		wantObject string
	}{
		{
			name: "SELECT date into string var",
			content: `
create proc TestProc
	@DateParam datetime
as
declare @StrVar varchar(20)

select @StrVar = getdate()
`,
			procs: []*model.SQLProcedure{
				{ProcName: "TestProc", LineStart: 2, Params: []model.SQLParam{{Name: "@DateParam", Type: "datetime"}}},
			},
			fragments: []*model.QueryFragment{
				{QueryText: "select @StrVar = getdate()", LineNumber: 7},
			},
			wantCount:  1,
			wantLine:   7,
			wantObject: "@StrVar",
		},
		{
			name: "SET date into string var",
			content: `
create proc TestProc
as
declare @StrVar varchar(20)

set @StrVar = getdate()
`,
			procs: []*model.SQLProcedure{
				{ProcName: "TestProc", LineStart: 2},
			},
			wantCount:  1,
			wantLine:   6,
			wantObject: "@strvar",
		},
		{
			name: "DECLARE string with date init",
			content: `
create proc TestProc
as
declare @StrVar varchar(20) = getdate()
`,
			procs: []*model.SQLProcedure{
				{ProcName: "TestProc", LineStart: 2},
			},
			wantCount:  1,
			wantLine:   4,
			wantObject: "@strvar",
		},
		{
			name: "SELECT date param into string var",
			content: `
create proc TestProc
	@DateParam datetime
as
declare @StrVar varchar(20)

select @StrVar = @DateParam
`,
			procs: []*model.SQLProcedure{
				{ProcName: "TestProc", LineStart: 2, Params: []model.SQLParam{{Name: "@DateParam", Type: "datetime"}}},
			},
			fragments: []*model.QueryFragment{
				{QueryText: "select @StrVar = @DateParam", LineNumber: 7},
			},
			wantCount:  1,
			wantLine:   7,
			wantObject: "@StrVar",
		},
		{
			name: "explicit convert should skip",
			content: `
create proc TestProc
as
declare @StrVar varchar(20)

select @StrVar = convert(varchar(20), getdate())
`,
			procs: []*model.SQLProcedure{
				{ProcName: "TestProc", LineStart: 2},
			},
			fragments: []*model.QueryFragment{
				{QueryText: "select @StrVar = convert(varchar(20), getdate())", LineNumber: 6},
			},
			wantCount: 0,
		},
		{
			name: "int var should skip",
			content: `
create proc TestProc
as
declare @IntVar int

select @IntVar = getdate()
`,
			procs: []*model.SQLProcedure{
				{ProcName: "TestProc", LineStart: 2},
			},
			fragments: []*model.QueryFragment{
				{QueryText: "select @IntVar = getdate()", LineNumber: 6},
			},
			wantCount: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &Runner{}
			path := normalizePath("test.sql")
			r.exec = &reviewExecContext{filePath: path, content: []byte(tc.content), lines: strings.Split(tc.content, "\n"), macroResult: replaceMacros(tc.content)}
			parsed := &sqlparser.ParseResult{
				Procedures: tc.procs,
				Fragments:  tc.fragments,
			}
			findings, err := r.checkDateIntoString(parsed, &indexedFile{Path: path, DsProductID: 1})
			r.exec = nil
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != tc.wantCount {
				t.Fatalf("findings count = %d, want %d", len(findings), tc.wantCount)
			}
			if tc.wantCount > 0 && len(findings) > 0 {
				if findings[0].Line != tc.wantLine {
					t.Fatalf("findings[0].Line = %d, want %d", findings[0].Line, tc.wantLine)
				}
				if findings[0].Object != tc.wantObject {
					t.Fatalf("findings[0].Object = %q, want %q", findings[0].Object, tc.wantObject)
				}
			}
		})
	}
}

func TestCheckVarUseAfterCursor_UseAfterDeallocate_Finding(t *testing.T) {
	content := `create proc TestProc
as
  declare @a int
  declare cur1 cursor for select ID from tContract
  open cur1
  fetch next from cur1 into @a
  deallocate cur1
  select @a
`
	r := &Runner{}
	path := normalizePath("test.sql")
	r.exec = &reviewExecContext{filePath: path, content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
	findings, err := r.checkVarUseAfterCursor(&indexedFile{Path: path, DsProductID: 1})
	r.exec = nil
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings count = %d, want 1", len(findings))
	}
	if findings[0].Rule != RuleVarUseAfterCursor {
		t.Fatalf("rule = %s, want %s", findings[0].Rule, RuleVarUseAfterCursor)
	}
	if findings[0].Object != "@a" {
		t.Fatalf("object = %q, want @a", findings[0].Object)
	}
}

func TestCheckVarUseAfterCursor_UseBeforeDeallocate_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  declare @a int
  declare cur1 cursor for select ID from tContract
  open cur1
  fetch next from cur1 into @a
  select @a
  deallocate cur1
`
	r := &Runner{}
	path := normalizePath("test.sql")
	r.exec = &reviewExecContext{filePath: path, content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
	findings, err := r.checkVarUseAfterCursor(&indexedFile{Path: path, DsProductID: 1})
	r.exec = nil
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings count = %d, want 0", len(findings))
	}
}

func TestCheckVarUseAfterCursor_MacroDeallocateAndFetch_Finding(t *testing.T) {
	content := `create proc TestProc
as
  declare @b varchar(10)
  __DECLARE_CURSOR__(cur2)
    select Name from tContract
  open cur2
  __FETCH_NEXT__ cur2 into @b
  __DEALLOCATE_CURSOR__ cur2
  print @b
`
	r := &Runner{}
	path := normalizePath("test.sql")
	r.exec = &reviewExecContext{filePath: path, content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
	findings, err := r.checkVarUseAfterCursor(&indexedFile{Path: path, DsProductID: 1})
	r.exec = nil
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings count = %d, want 1", len(findings))
	}
	if findings[0].Object != "@b" {
		t.Fatalf("object = %q, want @b", findings[0].Object)
	}
}

func TestCheckVarUseAfterCursor_MultipleCursors_NoCrossFinding(t *testing.T) {
	content := `create proc TestProc
as
  declare @a int
  declare @b int
  declare cur1 cursor for select ID from t1
  declare cur2 cursor for select ID from t2
  open cur1
  open cur2
  fetch next from cur1 into @a
  fetch next from cur2 into @b
  deallocate cur1
  deallocate cur2
  select @b
`
	r := &Runner{}
	path := normalizePath("test.sql")
	r.exec = &reviewExecContext{filePath: path, content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
	findings, err := r.checkVarUseAfterCursor(&indexedFile{Path: path, DsProductID: 1})
	r.exec = nil
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings count = %d, want 1", len(findings))
	}
	if findings[0].Object != "@b" {
		t.Fatalf("object = %q, want @b", findings[0].Object)
	}
}

func TestCheckVarUseAfterCursor_MultilineFetchInto_Finding(t *testing.T) {
	content := `create proc TestProc
as
  declare @a int
  declare @b int
  declare cur1 cursor for select ID from tContract
  open cur1
  fetch next from cur1 into @a,
                            @b
  deallocate cur1
  select @a, @b
`
	r := &Runner{}
	path := normalizePath("test.sql")
	r.exec = &reviewExecContext{filePath: path, content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
	findings, err := r.checkVarUseAfterCursor(&indexedFile{Path: path, DsProductID: 1})
	r.exec = nil
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("findings count = %d, want 2", len(findings))
	}
	objSet := map[string]bool{}
	for _, f := range findings {
		objSet[f.Object] = true
	}
	if !objSet["@a"] || !objSet["@b"] {
		t.Fatalf("expected findings for @a and @b, got %v", objSet)
	}
}

func TestCheckUseOnlyDeclaredCursors_DeclaredAndUsed_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  declare cur1 cursor for select ID from tContract
  open cur1
  fetch next from cur1 into @val
  close cur1
  deallocate cur1
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUseOnlyDeclaredCursors(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

func TestCheckUseOnlyDeclaredCursors_UndeclaredOpen_Finding(t *testing.T) {
	content := `create proc TestProc
as
  open cur1
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUseOnlyDeclaredCursors(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].Rule != RuleUseOnlyDeclaredCursors {
		t.Fatalf("rule = %v, want %v", findings[0].Rule, RuleUseOnlyDeclaredCursors)
	}
	if !strings.Contains(findings[0].Message, "cur1") {
		t.Fatalf("message should contain cursor name 'cur1': %s", findings[0].Message)
	}
}

func TestCheckUseOnlyDeclaredCursors_UndeclaredFetch_Finding(t *testing.T) {
	content := `create proc TestProc
as
  fetch next from cur2 into @val
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUseOnlyDeclaredCursors(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if !strings.Contains(findings[0].Message, "cur2") {
		t.Fatalf("message should contain cursor name 'cur2': %s", findings[0].Message)
	}
}

func TestCheckUseOnlyDeclaredCursors_DeclareMacro_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  __DECLARE_CURSOR__(cur1)
  open cur1
  fetch next from cur1 into @val
  __DEALLOCATE_CURSOR__(cur1)
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUseOnlyDeclaredCursors(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

func TestCheckUseOnlyDeclaredCursors_SystemCursor_NoFinding(t *testing.T) {
	// Системные курсоры (@@) не проверяются
	content := `create proc TestProc
as
  select @@FETCH_STATUS
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUseOnlyDeclaredCursors(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings for system cursor, got %d: %v", len(findings), findings)
	}
}

func TestCheckUseOnlyDeclaredCursors_TempCursor_NoFinding(t *testing.T) {
	// Временные курсоры (#) не проверяются
	content := `create proc TestProc
as
  open #tempCur
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUseOnlyDeclaredCursors(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings for temp cursor, got %d: %v", len(findings), findings)
	}
}

func TestCheckUseOnlyDeclaredCursors_CaseInsensitive(t *testing.T) {
	content := `create proc TestProc
as
  DECLARE CUR1 cursor for select ID from tContract
  OPEN Cur1
  FETCH NEXT FROM cUr1 into @val
  CLOSE CUR1
  DEALLOCATE cur1
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUseOnlyDeclaredCursors(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings (case insensitive), got %d: %v", len(findings), findings)
	}
}

func TestCheckUseOnlyDeclaredCursors_FetchWithoutFrom_TypoDetected(t *testing.T) {
	// Проверяем обнаружение опечатки в имени курсора при использовании FETCH без FROM
	// Объявлено: MyCursor_Cur, Используется: MyCursor_Cu (без 'r')
	content := `create proc TestProc
as
  declare MyCursor_Cur cursor for select ID from tContract
  open MyCursor_Cur
  fetch MyCursor_Cu into @val
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUseOnlyDeclaredCursors(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Должно быть 1 finding - опечатка в курсоре MyCursor_Cu
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding for typo cursor, got %d: %v", len(findings), findings)
	}
	if !strings.Contains(findings[0].Message, "MyCursor_Cu") {
		t.Fatalf("message should contain typo cursor name 'MyCursor_Cu': %s", findings[0].Message)
	}
}

func TestCheckUseOnlyDeclaredCursors_FetchNextMacro_TypoDetected(t *testing.T) {
	// Проверяем обнаружение опечатки при использовании макроса __FETCH_NEXT__
	// Объявлено: RealCursor_Cur, Используется: RealCursor_Cu (опечатка)
	content := `create proc TestProc
as
  declare RealCursor_Cur cursor for select ID from tContract
  open RealCursor_Cur
  __FETCH_NEXT__ RealCursor_Cu into @val
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUseOnlyDeclaredCursors(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Должно быть 1 finding - опечатка в курсоре RealCursor_Cu
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding for typo cursor with __FETCH_NEXT__, got %d: %v", len(findings), findings)
	}
	if !strings.Contains(findings[0].Message, "RealCursor_Cu") {
		t.Fatalf("message should contain typo cursor name 'RealCursor_Cu': %s", findings[0].Message)
	}
}

func TestValidateExecArguments_APIContractParamsFallback(t *testing.T) {
	// Симулируем params, которые вернул бы lookupAPIContractParams из api_contract_params
	params := []model.SQLParam{
		{Name: "RetCode", Type: "DSIDENTIFIER", Direction: "in"},
		{Name: "Message", Type: "varchar(250)", Direction: "out"},
		{Name: "ConstraintMode", Type: "int", Direction: "in"},
	}
	args := []execArgument{
		{IsNamed: true, Name: "retcode", Value: "33166"},
		{IsNamed: true, Name: "message", Value: "@msg", IsOutput: true, VarName: "@msg"},
		{IsNamed: true, Name: "constraintmode", Value: "2"},
	}
	hasFinding, detail := validateExecArguments(args, params)
	if hasFinding {
		t.Fatalf("expected no finding for valid API-contract args, got: %s", detail)
	}
}

func TestValidateExecArguments_APIContractParamWithAtSign(t *testing.T) {
	// lookupAPIContractParams убирает @ из param_name перед созданием SQLParam
	params := []model.SQLParam{
		{Name: "ConstraintMode", Type: "int", Direction: "in"},
	}
	args := []execArgument{
		{IsNamed: true, Name: "constraintmode", Value: "2"},
	}
	hasFinding, detail := validateExecArguments(args, params)
	if hasFinding {
		t.Fatalf("expected no finding when API param name has @ stripped, got: %s", detail)
	}
}

func TestCheckCursorFetchArguments_MatchCount_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  declare cur1 cursor for select ID, Name from tContract
  open cur1
  fetch next from cur1 into @id, @name
  close cur1
  deallocate cur1
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_fetch_args_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()
	r.exec = &reviewExecContext{
		filePath:    normalizePath(tmpFile.Name()),
		content:     []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}

	findings, err := r.checkCursorFetchArguments(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

func TestCheckCursorFetchArguments_MismatchCount_Finding(t *testing.T) {
	content := `create proc TestProc
as
  declare cur1 cursor for select ID, Name, Value from tContract
  open cur1
  fetch next from cur1 into @id, @name
  close cur1
  deallocate cur1
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_fetch_args_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()
	r.exec = &reviewExecContext{
		filePath:    normalizePath(tmpFile.Name()),
		content:     []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}

	findings, err := r.checkCursorFetchArguments(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for mismatched cursor variable count")
	}
	if findings[0].Rule != RuleCursorFetchArguments {
		t.Fatalf("expected RuleCursorFetchArguments, got %s", findings[0].Rule)
	}
	if findings[0].Severity != SeverityPostgreReq {
		t.Fatalf("expected SeverityPostgreReq, got %d", findings[0].Severity)
	}
}

func TestCheckCursorFetchArguments_UndeclaredCursor_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  fetch next from cur_unknown into @id
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_fetch_args_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()
	r.exec = &reviewExecContext{
		filePath:    normalizePath(tmpFile.Name()),
		content:     []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}

	findings, err := r.checkCursorFetchArguments(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings for undeclared cursor (not our rule), got %d", len(findings))
	}
}

func TestCheckCursorFetchArguments_DeclareMacro_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  __DECLARE_CURSOR__(cur1)
  select ID, Name from tContract
  open cur1
  __FETCH_NEXT__ cur1 into @id, @name
  close cur1
  __DEALLOCATE_CURSOR__(cur1)
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_fetch_args_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()
	r.exec = &reviewExecContext{
		filePath:    normalizePath(tmpFile.Name()),
		content:     []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}

	findings, err := r.checkCursorFetchArguments(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings with macro-based cursor, got %d: %v", len(findings), findings)
	}
}

func TestCheckCursorFetchArguments_CommentWithComma_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  __DECLARE_CURSOR__(cur1)
  select c.ContractID,
         c.InstrumentID,
         p.ContractCredPayStatus,
         convert(smalldatetime, cc.CreditDateFrom), --1239761 именно так, а не p.Date
         p.Comment
    from tContract c
   inner join tContractCredit cc on cc.ContractCreditID = c.ContractID
  open cur1
  __FETCH_NEXT__ cur1 into @ContractID,
                           @InstrumentID,
                           @Status,
                           @Date,
                           @Comment
  close cur1
  __DEALLOCATE_CURSOR__(cur1)
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_fetch_args_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()
	r.exec = &reviewExecContext{
		filePath:    normalizePath(tmpFile.Name()),
		content:     []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}

	findings, err := r.checkCursorFetchArguments(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings with comment containing comma, got %d: %v", len(findings), findings)
	}
}

func TestCheckCursorFetchArguments_MoreFetchThanDeclare_Finding(t *testing.T) {
	content := `create proc TestProc
as
  declare cur1 cursor for select ID from tContract
  open cur1
  fetch next from cur1 into @id, @extra
  close cur1
  deallocate cur1
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_cursor_fetch_args_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()
	r.exec = &reviewExecContext{
		filePath:    normalizePath(tmpFile.Name()),
		content:     []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}

	findings, err := r.checkCursorFetchArguments(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding when FETCH has more variables than DECLARE")
	}
}

func TestCheckUsageVarInSameSelect_NoCrossRef_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  select @a = t.ColA, @b = t.ColB from tTable t
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_usage_var_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUsageVarInSameSelect(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

func TestCheckUsageVarInSameSelect_CrossRef_Finding(t *testing.T) {
	content := `create proc TestProc
as
  select @a = 2, @b = @a + 1
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_usage_var_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUsageVarInSameSelect(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for cross-variable reference")
	}
	if findings[0].Rule != RuleUsageVarInSameSelect {
		t.Fatalf("expected RuleUsageVarInSameSelect, got %s", findings[0].Rule)
	}
	if findings[0].Severity != SeverityPostgreReq {
		t.Fatalf("expected SeverityPostgreReq, got %d", findings[0].Severity)
	}
}

func TestCheckUsageVarInSameSelect_FromClause_Finding(t *testing.T) {
	content := `create proc TestProc
as
  select @a = t.Col, @b = @a + 1 from tTable t
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_usage_var_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUsageVarInSameSelect(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for cross-variable reference with FROM clause")
	}
}

func TestCheckUsageVarInSameSelect_ReverseOrder_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  select @b = @a + 1, @a = 2
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_usage_var_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUsageVarInSameSelect(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings (reverse order: @a not yet seen when @b is evaluated), got %d", len(findings))
	}
}

func TestCheckUsageVarInSameSelect_Multiline_Finding(t *testing.T) {
	content := `create proc TestProc
as
  select @a = t.ColA,
         @b = @a + 1
    from tTable t
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_usage_var_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUsageVarInSameSelect(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for multiline cross-variable reference")
	}
	if findings[0].Line <= 0 {
		t.Fatalf("expected positive line number, got %d", findings[0].Line)
	}
}

func TestCheckUsageVarInSameSelect_SingleAssignment_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  select @a = t.ColA from tTable t
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_usage_var_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUsageVarInSameSelect(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings for single assignment, got %d", len(findings))
	}
}

func TestCheckUsageVarInSameSelect_CaseExpression_Finding(t *testing.T) {
	content := `create proc TestProc
as
  select @RetVal = 0,
         case
           when @RetVal = 0 then @NodeID = 1
           else @NodeID = 2
         end
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_usage_var_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkUsageVarInSameSelect(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for @RetVal used in CASE expression")
	}
	if findings[0].Rule != RuleUsageVarInSameSelect {
		t.Fatalf("expected RuleUsageVarInSameSelect, got %s", findings[0].Rule)
	}
}

func TestCheckVarAssignInUpdate_VarAndColumn_Finding(t *testing.T) {
	content := `create proc TestProc
as
  update tContract set @a = 1, col = 2 where id = 3
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_var_assign_update_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkVarAssignInUpdate(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for @a in UPDATE SET")
	}
	if findings[0].Rule != RuleVarAssignInUpdate {
		t.Fatalf("expected RuleVarAssignInUpdate, got %s", findings[0].Rule)
	}
	if findings[0].Severity != SeverityPostgreReq {
		t.Fatalf("expected SeverityPostgreReq, got %d", findings[0].Severity)
	}
}

func TestCheckVarAssignInUpdate_OnlyColumns_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  update tContract set col1 = 1, col2 = 2 where id = 3
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_var_assign_update_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkVarAssignInUpdate(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

func TestCheckVarAssignInUpdate_OnlyVar_Finding(t *testing.T) {
	content := `create proc TestProc
as
  update tContract set @a = 1 where id = 3
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_var_assign_update_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkVarAssignInUpdate(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for @a in UPDATE SET")
	}
}

func TestCheckVarAssignInUpdate_MultipleVars_Findings(t *testing.T) {
	content := `create proc TestProc
as
  update tContract set @a = 1, @b = 2, col = 3 where id = 4
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_var_assign_update_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkVarAssignInUpdate(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("expected 2 findings for @a and @b, got %d", len(findings))
	}
}

func TestCheckVarAssignInUpdate_Multiline_Finding(t *testing.T) {
	content := `create proc TestProc
as
  update tContract
  set col1 = 1,
      @a = 2,
      col2 = 3
  where id = 4
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_var_assign_update_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkVarAssignInUpdate(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for @a in multiline UPDATE SET")
	}
	if findings[0].Line <= 0 {
		t.Fatalf("expected positive line number, got %d", findings[0].Line)
	}
}

func TestCheckStatementsWithJoinsRequireAliases_Qualified_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  select t1.a, t2.b from t1 join t2 on t1.id = t2.id
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_joins_aliases_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

func TestCheckStatementsWithJoinsRequireAliases_Unqualified_Finding(t *testing.T) {
	content := `create proc TestProc
as
  select a, t2.b from t1 join t2 on t1.id = t2.id
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_joins_aliases_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for unqualified column 'a'")
	}
	if findings[0].Rule != RuleStatementsWithJoinsRequireAliases {
		t.Fatalf("expected RuleStatementsWithJoinsRequireAliases, got %s", findings[0].Rule)
	}
	if findings[0].Severity != SeverityPostgreReq {
		t.Fatalf("expected SeverityPostgreReq, got %d", findings[0].Severity)
	}
}

func TestCheckStatementsWithJoinsRequireAliases_SingleTable_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  select a, b from t1
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_joins_aliases_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings for single table, got %d", len(findings))
	}
}

func TestCheckStatementsWithJoinsRequireAliases_Functions_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  select isnull(t1.a, 0), t2.b from t1 join t2 on t1.id = t2.id
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_joins_aliases_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

func TestCheckStatementsWithJoinsRequireAliases_UnqualifiedInWhere_Finding(t *testing.T) {
	content := `create proc TestProc
as
  select t1.a, t2.b from t1 join t2 on t1.id = t2.id where col = 1
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_joins_aliases_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for unqualified column 'col' in WHERE")
	}
}

func TestCheckStatementsWithJoinsRequireAliases_UpdateWithJoin_Finding(t *testing.T) {
	content := `create proc TestProc
as
  update t1 set t1.a = b from t1 join t2 on t1.id = t2.id
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_joins_aliases_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for unqualified column 'b' in UPDATE SET expression")
	}
}

func TestCheckStatementsWithJoinsRequireAliases_UpdateSetColumns_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  update t1 set col1 = @val1, col2 = @val2 from t1 join t2 on t1.id = t2.id
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_joins_aliases_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings (SET columns + @vars), got %d: %v", len(findings), findings)
	}
}

func TestCheckStatementsWithJoinsRequireAliases_UpdateSetWithComment_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  update pLB_Legal_FindList
     set Name1 = convert(varchar(160),isnull(pl.Name,'')) -- Делаем, потому что в банке, по какой то причине, перестало заполняться поле tInstitution.Name1. Разбираться с ГК - долго и не продуктивно. Поле Name1 заполняется на основании альтернативного имени - "АнглНаим"
    from pLB_Legal_FindList      p  M_UPDLOCK_INDEX(XPKpLB_Legal_FindList)
   inner join pAPI_Legal_AlterName pl  M_NOLOCK_INDEX(XIE1pAPI_Legal_AlterName)
           on pl.SPID      = @@spid
          and pl.LegalID   = p.LegalID
          and M_CONVERT_NCHAR(pl.AlterNameType) = 'АнглНаим'
  where p.SPID    = @@spid
    and p.Name1 = ''
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: "test.sql", DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings (SET column + comment with same name), got %d: %v", len(findings), findings)
	}
}

func TestCheckStatementsWithJoinsRequireAliases_UnqualifiedInOn_Finding(t *testing.T) {
	content := `create proc TestProc
as
  select t1.a, t2.b from t1 join t2 on SPID = @@spid and ObjectID = t2.id
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_joins_aliases_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("expected finding for unqualified column in ON clause")
	}
}

func TestCheckStatementsWithJoinsRequireAliases_StuffFunction_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  update t1 set Comment = stuff(t1.Comment, t2.PosTag, t2.LenTag, t2.Value)
  from t1 join t2 on t1.id = t2.id
`
	r := &Runner{}
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test_joins_aliases_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	findings, err := r.checkStatementsWithJoinsRequireAliases(parsed, &indexedFile{Path: tmpFile.Name(), DsProductID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings (stuff is a SQL function), got %d: %v", len(findings), findings)
	}
}

func TestExtractFuncColumnRefs_SimpleFunc(t *testing.T) {
	refs := extractFuncColumnRefs("UPPER(Name) = 'ABC'")
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].funcName != "UPPER" {
		t.Fatalf("expected funcName UPPER, got %s", refs[0].funcName)
	}
	if refs[0].column != "Name" {
		t.Fatalf("expected column Name, got %s", refs[0].column)
	}
}

func TestExtractFuncColumnRefs_AliasedColumn(t *testing.T) {
	refs := extractFuncColumnRefs("ISNULL(t.Status, 0) = 1")
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].funcName != "ISNULL" {
		t.Fatalf("expected funcName ISNULL, got %s", refs[0].funcName)
	}
	if refs[0].column != "Status" {
		t.Fatalf("expected column Status (from t.Status), got %s", refs[0].column)
	}
	if refs[0].alias != "t" {
		t.Fatalf("expected alias t, got %s", refs[0].alias)
	}
}

func TestExtractFuncColumnRefs_NoFunc(t *testing.T) {
	refs := extractFuncColumnRefs("Name = 'ABC' and Status = 1")
	if len(refs) != 0 {
		t.Fatalf("expected 0 refs, got %d: %v", len(refs), refs)
	}
}

func TestExtractFuncColumnRefs_MultipleFuncs(t *testing.T) {
	refs := extractFuncColumnRefs("UPPER(Name) = 'ABC' and ISNULL(Status, 0) = 1")
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %d", len(refs))
	}
}

func TestExtractFuncColumnRefs_AliasedColumnDifferentAlias(t *testing.T) {
	refs := extractFuncColumnRefs("isnull(oa.SPID, 0) >= 0")
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].alias != "oa" {
		t.Fatalf("expected alias oa, got %s", refs[0].alias)
	}
	if refs[0].column != "SPID" {
		t.Fatalf("expected column SPID, got %s", refs[0].column)
	}
	// Verify: isIndexedColumn with table alias "d" should NOT match "SPID" when ref alias is "oa"
	indexSet := map[string]bool{"spid": true}
	// SPID without dot and alias "d" — this matches because column without dot
	// does direct match. The guard is in the calling code (fr.alias != alias → skip).
	// This test documents the behavior: isIndexedColumn itself does direct match
	// for non-dotted columns regardless of alias.
	_ = isIndexedColumn("SPID", "d", indexSet)
	// But with dot: isIndexedColumn("oa.SPID", "d", ...) should NOT match
	if isIndexedColumn("oa.SPID", "d", indexSet) {
		t.Fatalf("oa.SPID should not match when table alias is d")
	}
}

func TestIsIndexedColumn_DirectMatch(t *testing.T) {
	indexSet := map[string]bool{"name": true, "status": true}
	if !isIndexedColumn("Name", "", indexSet) {
		t.Fatalf("Name should be indexed")
	}
	if isIndexedColumn("Other", "", indexSet) {
		t.Fatalf("Other should not be indexed")
	}
}

func TestIsIndexedColumn_AliasedMatch(t *testing.T) {
	indexSet := map[string]bool{"name": true}
	if !isIndexedColumn("t.Name", "t", indexSet) {
		t.Fatalf("t.Name should be indexed (alias.column -> Name)")
	}
	if isIndexedColumn("t.Other", "t", indexSet) {
		t.Fatalf("t.Other should not be indexed")
	}
}

func TestExtractIsnullCalls_Simple(t *testing.T) {
	calls := extractIsnullCalls("ISNULL(col, 0)")
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if strings.TrimSpace(calls[0][0]) != "col" || strings.TrimSpace(calls[0][1]) != "0" {
		t.Fatalf("unexpected args: %v", calls)
	}
}

func TestExtractIsnullCalls_Multiple(t *testing.T) {
	calls := extractIsnullCalls("ISNULL(a, b) and ISNULL(c, d)")
	if len(calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(calls))
	}
}

func TestExtractIsnullCalls_NestedFunc(t *testing.T) {
	calls := extractIsnullCalls("ISNULL(UPPER(col), 'default')")
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if strings.TrimSpace(calls[0][0]) != "UPPER(col)" {
		t.Fatalf("expected UPPER(col) as first arg, got %s", calls[0][0])
	}
}

func TestExtractIsnullCalls_NoCall(t *testing.T) {
	calls := extractIsnullCalls("col = 1 and status = 0")
	if len(calls) != 0 {
		t.Fatalf("expected 0 calls, got %d", len(calls))
	}
}

func TestExtractIsnullCalls_CaseInsensitive(t *testing.T) {
	calls := extractIsnullCalls("isnull(col, 0)")
	if len(calls) != 1 {
		t.Fatalf("expected 1 call for lowercase isnull, got %d", len(calls))
	}
}

func TestExtractComparisons_Simple(t *testing.T) {
	cmps := extractComparisons("col = 1")
	if len(cmps) != 1 {
		t.Fatalf("expected 1 comparison, got %d", len(cmps))
	}
	if strings.TrimSpace(cmps[0].left) != "col" || cmps[0].op != "=" || strings.TrimSpace(cmps[0].right) != "1" {
		t.Fatalf("unexpected: %+v", cmps[0])
	}
}

func TestExtractComparisons_AndOr(t *testing.T) {
	cmps := extractComparisons("a = 1 and b <> 2 or c > 3")
	if len(cmps) != 3 {
		t.Fatalf("expected 3 comparisons, got %d", len(cmps))
	}
}

func TestExtractComparisons_NestedParens(t *testing.T) {
	cmps := extractComparisons("(a = 1 and b = 2) or c = 3")
	if len(cmps) != 3 {
		t.Fatalf("expected 3 comparisons, got %d", len(cmps))
	}
}

func TestFindComparisonOperator_NotEqual(t *testing.T) {
	cmp := findComparisonOperator("a <> b")
	if cmp.op != "<>" {
		t.Fatalf("expected <>, got %s", cmp.op)
	}
}

func TestFindComparisonOperator_LessThanOrEqual(t *testing.T) {
	cmp := findComparisonOperator("a <= b")
	if cmp.op != "<=" {
		t.Fatalf("expected <=, got %s", cmp.op)
	}
}

func TestIsAssignmentFragment_UpdateSet(t *testing.T) {
	if !isAssignmentFragment("update t set col = 1") {
		t.Fatalf("UPDATE SET should be assignment")
	}
}

func TestIsAssignmentFragment_SelectAssign(t *testing.T) {
	if !isAssignmentFragment("select @var = col from t") {
		t.Fatalf("SELECT @var = should be assignment")
	}
}

func TestIsAssignmentFragment_NotAssignment(t *testing.T) {
	if isAssignmentFragment("select col from t where col = 1") {
		t.Fatalf("SELECT with WHERE should not be assignment")
	}
}

func TestExtractCaseWhenConditions_Simple(t *testing.T) {
	conds := extractCaseWhenConditions("case when col = 1 then 'a' else 'b' end")
	if len(conds) != 1 {
		t.Fatalf("expected 1 WHEN condition, got %d", len(conds))
	}
	if strings.TrimSpace(conds[0]) != "col = 1" {
		t.Fatalf("unexpected condition: %s", conds[0])
	}
}

func TestExtractCaseWhenConditions_Multiple(t *testing.T) {
	conds := extractCaseWhenConditions("case when a = 1 then 'a' when b = 2 then 'b' else 'c' end")
	if len(conds) != 2 {
		t.Fatalf("expected 2 WHEN conditions, got %d", len(conds))
	}
}

func TestSplitByAndOr_Simple(t *testing.T) {
	parts := splitByAndOr("a = 1 and b = 2")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
}

func TestSplitByAndOr_OrOnly(t *testing.T) {
	parts := splitByAndOr("a = 1 or b = 2 or c = 3")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
}

func TestExtractCaseWhenConditions_MultilineSelect(t *testing.T) {
	text := "select @RetVal = case\n                     when @NodeID = getdate() then 1\n                     else 0\n                   end"
	conds := extractCaseWhenConditions(text)
	if len(conds) != 1 {
		t.Fatalf("expected 1 WHEN condition, got %d: %v", len(conds), conds)
	}
	cmps := extractComparisons(conds[0])
	if len(cmps) != 1 {
		t.Fatalf("expected 1 comparison in WHEN, got %d: %v", len(cmps), cmps)
	}
	if cmps[0].op != "=" {
		t.Fatalf("expected = operator, got %s", cmps[0].op)
	}
}

func TestExtractCaseThenElseExpressions_Simple(t *testing.T) {
	parts := extractCaseThenElseExpressions("case when x = 1 then 'a' else 'b' end")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d: %v", len(parts), parts)
	}
	if parts[0] != "'a'" || parts[1] != "'b'" {
		t.Fatalf("expected ['a','b'], got %v", parts)
	}
}

func TestExtractCaseThenElseExpressions_MultipleWhen(t *testing.T) {
	parts := extractCaseThenElseExpressions("case when a = 1 then 10 when b = 2 then 20 else 0 end")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d: %v", len(parts), parts)
	}
	if parts[0] != "10" || parts[1] != "20" || parts[2] != "0" {
		t.Fatalf("expected [10,20,0], got %v", parts)
	}
}

func TestExtractCaseThenElseExpressions_Multiline(t *testing.T) {
	text := "case\n  when fo.InterfaceObjectID = 1 then INSP_POLICY_LINK\n  when fo.InterfaceObjectID = 2 then INSP_POLICY_LINK_COM\n  else 0\nend"
	parts := extractCaseThenElseExpressions(text)
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d: %v", len(parts), parts)
	}
	if parts[0] != "INSP_POLICY_LINK" || parts[1] != "INSP_POLICY_LINK_COM" || parts[2] != "0" {
		t.Fatalf("expected [INSP_POLICY_LINK, INSP_POLICY_LINK_COM, 0], got %v", parts)
	}
}

func TestExtractCaseThenElseExpressions_ColumnRefsInThen(t *testing.T) {
	text := "case when cc.Flag2 > 0 then cc.CreditDateFrom else cr.DateFrom end"
	parts := extractCaseThenElseExpressions(text)
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d: %v", len(parts), parts)
	}
	if parts[0] != "cc.CreditDateFrom" || parts[1] != "cr.DateFrom" {
		t.Fatalf("expected [cc.CreditDateFrom, cr.DateFrom], got %v", parts)
	}
}

func TestExtractCaseThenElseExpressions_NoCase(t *testing.T) {
	parts := extractCaseThenElseExpressions("select 1 from t")
	if len(parts) != 0 {
		t.Fatalf("expected 0 parts, got %d: %v", len(parts), parts)
	}
}

func TestExtractParenContent_Simple(t *testing.T) {
	inner, end := extractParenContent("convert(varchar, @x)", 7)
	if inner != "varchar, @x" {
		t.Fatalf("expected 'varchar, @x', got %q", inner)
	}
	if end != 20 {
		t.Fatalf("expected end=20, got %d", end)
	}
}

func TestExtractParenContent_Nested(t *testing.T) {
	inner, end := extractParenContent("convert(varchar, isnull(@x, 0))", 7)
	if inner != "varchar, isnull(@x, 0)" {
		t.Fatalf("expected 'varchar, isnull(@x, 0)', got %q", inner)
	}
	if end != 31 {
		t.Fatalf("expected end=31, got %d", end)
	}
}

func TestExtractParenContent_Empty(t *testing.T) {
	inner, _ := extractParenContent("convert()", 7)
	if inner != "" {
		t.Fatalf("expected empty, got %q", inner)
	}
}

func TestContainsColumnRef_Column(t *testing.T) {
	if !containsColumnRef("field1") {
		t.Fatalf("expected true for column ref")
	}
	if !containsColumnRef("t.field1") {
		t.Fatalf("expected true for qualified column ref")
	}
}

func TestContainsColumnRef_Literal(t *testing.T) {
	if containsColumnRef("123") {
		t.Fatalf("expected false for literal")
	}
	if containsColumnRef("'abc'") {
		t.Fatalf("expected false for string literal")
	}
}

func TestContainsColumnRef_Variable(t *testing.T) {
	if containsColumnRef("@var") {
		t.Fatalf("expected false for variable")
	}
}

func TestHasOrderBy_WithOrderBy(t *testing.T) {
	if !hasOrderBy("select * from t order by id") {
		t.Fatalf("expected true for order by")
	}
}

func TestHasOrderBy_WithoutOrderBy(t *testing.T) {
	if hasOrderBy("select * from t") {
		t.Fatalf("expected false without order by")
	}
}

func TestIsInsertSelectFragment_True(t *testing.T) {
	if !isInsertSelectFragment("insert into t1 (col) select field from t") {
		t.Fatalf("expected true for insert...select")
	}
}

func TestIsInsertSelectFragment_False(t *testing.T) {
	if isInsertSelectFragment("select * from t") {
		t.Fatalf("expected false for plain select")
	}
}

func TestContainsTopLevelUnion_True(t *testing.T) {
	if !containsTopLevelUnion("select a from t union select b from t2") {
		t.Fatalf("expected true for union")
	}
}

func TestContainsTopLevelUnion_False(t *testing.T) {
	if containsTopLevelUnion("select a from t") {
		t.Fatalf("expected false without union")
	}
}

func TestExtractFirstSelectBeforeUnion(t *testing.T) {
	s, ok := extractFirstSelectBeforeUnion("select col1 as a, col2 as b from t union select x, y from t2 order by a")
	if !ok {
		t.Fatalf("expected ok")
	}
	if !strings.Contains(strings.ToLower(s), "select col1 as a") {
		t.Fatalf("expected first select, got %q", s)
	}
	if strings.Contains(strings.ToLower(s), "union") {
		t.Fatalf("should not contain union, got %q", s)
	}
}

func TestExtractSelectColumnNames_WithAlias(t *testing.T) {
	names := extractSelectColumnNames("select col1 as a, col2 as b from t")
	if _, ok := names["a"]; !ok {
		t.Fatalf("expected alias 'a'")
	}
	if _, ok := names["b"]; !ok {
		t.Fatalf("expected alias 'b'")
	}
}

func TestExtractSelectColumnNames_WithoutAlias(t *testing.T) {
	names := extractSelectColumnNames("select col1, col2 from t")
	if _, ok := names["col1"]; !ok {
		t.Fatalf("expected col1")
	}
	if _, ok := names["col2"]; !ok {
		t.Fatalf("expected col2")
	}
}

func TestExtractColumnAliasName_AsAlias(t *testing.T) {
	if name := extractColumnAliasName("col1 as a"); name != "a" {
		t.Fatalf("expected 'a', got %q", name)
	}
}

func TestExtractColumnAliasName_NoAlias(t *testing.T) {
	if name := extractColumnAliasName("col1"); name != "col1" {
		t.Fatalf("expected 'col1', got %q", name)
	}
}

func TestExtractColumnAliasName_QualifiedNoAlias(t *testing.T) {
	if name := extractColumnAliasName("t.col1"); name != "col1" {
		t.Fatalf("expected 'col1', got %q", name)
	}
}

func TestIsLiteralArg_NumericLiteral(t *testing.T) {
	if !isLiteralArg("2") {
		t.Fatalf("2 should be a literal")
	}
	if !isLiteralArg("0") {
		t.Fatalf("0 should be a literal")
	}
	if !isLiteralArg("-1") {
		t.Fatalf("-1 should be a literal")
	}
	if !isLiteralArg("3.14") {
		t.Fatalf("3.14 should be a literal")
	}
}

func TestIsLiteralArg_StringLiteral(t *testing.T) {
	if !isLiteralArg("'19000101'") {
		t.Fatalf("'19000101' should be a literal")
	}
}

func TestIsLiteralArg_Null(t *testing.T) {
	if !isLiteralArg("null") {
		t.Fatalf("null should be a literal")
	}
	if !isLiteralArg("NULL") {
		t.Fatalf("NULL should be a literal")
	}
}

func TestIsLiteralArg_ColumnOrVariable(t *testing.T) {
	if isLiteralArg("@BCH_INTRATE_CRLINE_ALG") {
		t.Fatalf("@BCH_INTRATE_CRLINE_ALG should not be a literal")
	}
	if isLiteralArg("g.GroupID") {
		t.Fatalf("g.GroupID should not be a literal")
	}
	if isLiteralArg("r.AmountOutst") {
		t.Fatalf("r.AmountOutst should not be a literal")
	}
}

func TestContainsSQLStatementKeyword(t *testing.T) {
	if !containsSQLStatementKeyword("inner join tConsInstrumentSync it") {
		t.Fatalf("should detect 'join' keyword")
	}
	if !containsSQLStatementKeyword("on it.InstrumentID") {
		t.Fatalf("should detect 'on' keyword")
	}
	if !containsSQLStatementKeyword("where r.SPID = @@SPID") {
		t.Fatalf("should detect 'where' keyword")
	}
	if containsSQLStatementKeyword("it.InstrumentID") {
		t.Fatalf("it.InstrumentID should not contain SQL keywords")
	}
	if containsSQLStatementKeyword("c.DateFrom") {
		t.Fatalf("c.DateFrom should not contain SQL keywords")
	}
	if containsSQLStatementKeyword("@BCH_INTRATE_CRLINE_ALG") {
		t.Fatalf("@BCH_INTRATE_CRLINE_ALG should not contain SQL keywords")
	}
}

func TestFindComparisonOperator_RejectsSQLKeywordsInOperand(t *testing.T) {
	// Выражение с мусором от парсинга: left содержит "join" и "on"
	expr := `(bc.Flag2 & CONSBPCOND2 = 0
       inner join tConsInstrumentSync  it
               on it.InstrumentID = c.InstrumentID`
	cmp := findComparisonOperator(expr)
	if cmp.op != "" {
		t.Fatalf("should reject comparison with SQL keywords in operand, got op=%q left=%q right=%q", cmp.op, cmp.left, cmp.right)
	}
}

func TestFindComparisonOperator_AcceptsCleanComparison(t *testing.T) {
	cmp := findComparisonOperator("it.InstrumentID = c.InstrumentID")
	if cmp.op != "=" {
		t.Fatalf("expected op '=', got %q", cmp.op)
	}
	if strings.TrimSpace(cmp.left) != "it.InstrumentID" {
		t.Fatalf("expected left 'it.InstrumentID', got %q", cmp.left)
	}
	if strings.TrimSpace(cmp.right) != "c.InstrumentID" {
		t.Fatalf("expected right 'c.InstrumentID', got %q", cmp.right)
	}
}

func TestSqlMacrosMap_ContainsKnownMacros(t *testing.T) {
	if !sqlMacrosMap["m_forceorder"] {
		t.Fatalf("m_forceorder should be in sqlMacrosMap")
	}
	if !sqlMacrosMap["m_keepplan"] {
		t.Fatalf("m_keepplan should be in sqlMacrosMap")
	}
	if !sqlMacrosMap["m_with_rowlock"] {
		t.Fatalf("m_with_rowlock should be in sqlMacrosMap")
	}
	if !sqlMacrosMap["m_delete_ptable"] {
		t.Fatalf("m_delete_ptable should be in sqlMacrosMap")
	}
	if !sqlMacrosMap["m_businesslog_checkpoint"] {
		t.Fatalf("m_businesslog_checkpoint should be in sqlMacrosMap")
	}
	if !sqlMacrosMap["m_log_table"] {
		t.Fatalf("m_log_table should be in sqlMacrosMap")
	}
}

func TestFilterKnownNames_FiltersMacros(t *testing.T) {
	r := &Runner{}
	names := []string{"M_FORCEORDER", "M_KEEPPLAN", "col1", "M_DELETE_PTABLE", "col2"}
	filtered := r.filterKnownNames(names)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 names after filtering, got %d: %v", len(filtered), filtered)
	}
	if filtered[0] != "col1" || filtered[1] != "col2" {
		t.Fatalf("expected [col1 col2], got %v", filtered)
	}
}

func TestExtractOrderByColumns_Simple(t *testing.T) {
	cols := extractOrderByColumns("select a from t union select b from t2 order by a, b desc")
	if len(cols) != 2 {
		t.Fatalf("expected 2 cols, got %d: %v", len(cols), cols)
	}
	if cols[0] != "a" {
		t.Fatalf("expected 'a', got %q", cols[0])
	}
	if cols[1] != "b" {
		t.Fatalf("expected 'b', got %q", cols[1])
	}
}

func TestIsNumericLiteral_True(t *testing.T) {
	if !isNumericLiteral("1") {
		t.Fatalf("expected true for '1'")
	}
}

func TestIsNumericLiteral_False(t *testing.T) {
	if isNumericLiteral("col1") {
		t.Fatalf("expected false for 'col1'")
	}
}

func TestExtractWhereOnlyColumnsForIndexWrong(t *testing.T) {
	tables := []tableFromClause{
		{TableName: "tConsAccountLink", Alias: "a"},
		{TableName: "tConsRuleAccSync", Alias: "r"},
	}

	sql := `select *
from tConsAccountLink a
inner join tConsRuleAccSync r
        on r.RuleID = a.RuleID
       and r.PropVal in (1, 2)
 where a.ResourceID = @AccountID
   and a.OnDate <= @Date`

	whereCols := extractWhereOnlyColumnsForIndexWrong(sql, tables)

	// a.ResourceID и a.OnDate из WHERE
	if _, exists := whereCols["a"]["resourceid"]; !exists {
		t.Fatalf("expected a.resourceid in WHERE-only columns, got %#v", whereCols["a"])
	}
	if _, exists := whereCols["a"]["ondate"]; !exists {
		t.Fatalf("expected a.ondate in WHERE-only columns, got %#v", whereCols["a"])
	}
	// a.RuleID из ON — не должно быть в WHERE-only
	if _, exists := whereCols["a"]["ruleid"]; exists {
		t.Fatalf("a.ruleid should NOT be in WHERE-only columns, got %#v", whereCols["a"])
	}
	// r.RuleID из ON — не должно быть в WHERE-only
	if _, exists := whereCols["r"]["ruleid"]; exists {
		t.Fatalf("r.ruleid should NOT be in WHERE-only columns, got %#v", whereCols["r"])
	}
}

func TestExtractWhereOnlyColumnsForIndexWrong_NoWhere(t *testing.T) {
	tables := []tableFromClause{{TableName: "t1", Alias: "a"}}
	cols := extractWhereOnlyColumnsForIndexWrong("select * from t1 a", tables)
	if len(cols["a"]) != 0 {
		t.Fatalf("expected empty WHERE-only columns when no WHERE clause, got %#v", cols["a"])
	}
}

func TestContainsForceOrderMacro(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"select * from t M_FORCEORDER", true},
		{"select * from t M_FORCEORDER_NOSPOOL", true},
		{"select * from t M_FORCEORDER_FAST", true},
		{"select * from t M_FORCEORDER_WO_LOOPJOIN", true},
		{"select * from t m_forceorder", true},
		{"select * from t", false},
		{"select * from t where x = 1", false},
	}
	for _, tc := range cases {
		got := containsForceOrderMacro(strings.ToLower(tc.text))
		if got != tc.want {
			t.Fatalf("containsForceOrderMacro(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
}

func TestExtractWhereOnlyVsCombined_ForceOrderLeadingTable(t *testing.T) {
	tables := []tableFromClause{
		{TableName: "tConsAccountLink", Alias: "a"},
		{TableName: "tConsRuleAccSync", Alias: "r"},
	}

	sql := `select *
from tConsAccountLink a
inner join tConsRuleAccSync r
        on r.RuleID = a.RuleID
 where a.ResourceID = @AccountID
   and a.OnDate <= @Date`

	combined := extractConditionColumnsForIndexWrong(sql, tables)
	whereOnly := extractWhereOnlyColumnsForIndexWrong(sql, tables)

	// Combined должен включать RuleID из ON
	if _, exists := combined["a"]["ruleid"]; !exists {
		t.Fatalf("combined should include a.ruleid from ON, got %#v", combined["a"])
	}
	// WHERE-only не должен включать RuleID
	if _, exists := whereOnly["a"]["ruleid"]; exists {
		t.Fatalf("WHERE-only should NOT include a.ruleid from ON, got %#v", whereOnly["a"])
	}
	// Оба должны включать ResourceID из WHERE
	if _, exists := whereOnly["a"]["resourceid"]; !exists {
		t.Fatalf("WHERE-only should include a.resourceid, got %#v", whereOnly["a"])
	}
	if _, exists := combined["a"]["resourceid"]; !exists {
		t.Fatalf("combined should include a.resourceid, got %#v", combined["a"])
	}
}

func TestInferTypeFromMacroSignature(t *testing.T) {
	cases := []struct {
		name      string
		signature string
		want      string
	}{
		{
			name:      "convert int from date macro",
			signature: "convert(int, convert(varchar, _date_, 112))",
			want:      "int",
		},
		{
			name:      "cast as varchar",
			signature: "cast(@x as varchar(10))",
			want:      "varchar(10)",
		},
		{
			name:      "cast as datetime",
			signature: "cast(@d as datetime)",
			want:      "datetime",
		},
		{
			name:      "empty signature",
			signature: "",
			want:      "",
		},
		{
			name:      "no convert or cast",
			signature: "@x + 1",
			want:      "",
		},
		{
			name:      "convert with DSINT_KEY type",
			signature: "convert(DSINT_KEY, @val)",
			want:      "dsint_key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := inferTypeFromMacroSignature(tc.signature)
			if got != tc.want {
				t.Fatalf("inferTypeFromMacroSignature(%q) = %q, want %q", tc.signature, got, tc.want)
			}
		})
	}
}

func TestCachedLookupMacroType_CacheHit(t *testing.T) {
	r := &Runner{macroTypeCache: make(map[string]string)}
	r.macroTypeCache["_convert_date_to_int_"] = "int"

	got := r.cachedLookupMacroType("_CONVERT_DATE_TO_INT_")
	if got != "int" {
		t.Fatalf("expected 'int' from cache, got %q", got)
	}
}

func TestCachedLookupMacroType_NegativeCache(t *testing.T) {
	r := &Runner{macroTypeCache: make(map[string]string)}
	r.macroTypeCache["_unknown_macro_"] = ""

	got := r.cachedLookupMacroType("_UNKNOWN_MACRO_")
	if got != "" {
		t.Fatalf("expected empty string from negative cache, got %q", got)
	}
}

func TestResolveArgType_ConvertNumeric(t *testing.T) {
	r := &Runner{macroTypeCache: make(map[string]string)}
	got := r.resolveArgType("convert(numeric(15, 0), @CurrID + MAX_COUNT_ID)", map[string]string{}, map[string]string{})
	if got != "numeric(15, 0)" {
		t.Fatalf("expected 'numeric(15, 0)', got %q", got)
	}
}

func TestResolveArgType_ConvertDatetime(t *testing.T) {
	r := &Runner{macroTypeCache: make(map[string]string)}
	got := r.resolveArgType("convert(datetime, @SomeDate)", map[string]string{}, map[string]string{})
	if got != "datetime" {
		t.Fatalf("expected 'datetime', got %q", got)
	}
}

func TestResolveArgType_CastAsInt(t *testing.T) {
	r := &Runner{macroTypeCache: make(map[string]string)}
	got := r.resolveArgType("cast(@x as int)", map[string]string{}, map[string]string{})
	if got != "int" {
		t.Fatalf("expected 'int', got %q", got)
	}
}

func TestResolveArgType_ConvertNumericEquivalDSIdentifier(t *testing.T) {
	r := &Runner{macroTypeCache: make(map[string]string)}
	t1 := r.resolveArgType("convert(numeric(15, 0), @CurrID + MAX_COUNT_ID)", map[string]string{}, map[string]string{})
	t2 := "DSIDENTIFIER"
	if !areEquivalentTypes(t1, t2) {
		t.Fatalf("convert(numeric(15,0)) type %q should be equivalent to DSIDENTIFIER", t1)
	}
}

func TestResolveArgType_SubstringWithArithmetic_ReturnsVarchar(t *testing.T) {
	r := &Runner{macroTypeCache: make(map[string]string)}
	got := r.resolveArgType("substring(M_CONVERT_NCHAR(otc.Comment), otc.PosTag + 8, otc.LenTag - 9)", map[string]string{}, map[string]string{})
	if got != "varchar" {
		t.Fatalf("expected 'varchar' for substring(), got %q", got)
	}
}

func TestExtractWherePartForIndexWrong_SubqueryInOn_NoFalseWhere(t *testing.T) {
	query := `      insert into pCRINHRESTLIM_Calc M_WITH_ROWLOCK
             (SPID, OperandID)
      select @@spid, p.AutoID
        from pCons_Calc_ObjectOperand    p
       inner join tCreditTurnover       ct
               on ct.ContractCreditID    = p.ObjectID
              and ct.Date                = isnull((select max(ct2.Date)
                                                     from tCreditTurnover      ct2
                                                    where ct2.ContractCreditID   = cd2.ContractCreditID
                                                      and ct2.Direction          = 2), ct.Date)
              and ct.Amount              = p.Value
       inner join tCreditDebt          cd2
               on cd2.ContractCreditID   = ct.ContractCreditID
              and cd2.Date               = isnull((select max(ct2.Date)
                                                     from tCreditTurnover      ct2
                                                    where ct2.ContractCreditID   = cd2.ContractCreditID
                                                      and ct2.Direction          = 2), ct.Date)
      where p.SPID                       = @@spid
        and p.Operand                 like '%CRINHRESTLIM%'
        and p.Value                      > 0`

	wherePart := extractWherePartForIndexWrong(query)
	// WHERE part should start at the outer "where p.SPID", not at the inner subquery "where ct2.ContractCreditID"
	if strings.Contains(wherePart, "ct2.ContractCreditID") {
		t.Fatalf("extractWherePartForIndexWrong should not include subquery WHERE, got: %q", wherePart)
	}
	if !strings.Contains(wherePart, "p.SPID") {
		t.Fatalf("extractWherePartForIndexWrong should include outer WHERE with p.SPID, got: %q", wherePart)
	}

	// ON parts may contain subquery text (inside isnull(...)), but extractComparisons
	// should not extract comparisons from inside parentheses (subquery).
	onParts := extractOnPartsForIndexWrong(query)
	for i, on := range onParts {
		comparisons := extractComparisons(on)
		for _, cmp := range comparisons {
			left := strings.TrimSpace(cmp.left)
			right := strings.TrimSpace(cmp.right)
			if strings.Contains(left, "ct2.") || strings.Contains(right, "ct2.") {
				t.Fatalf("onPart[%d] extractComparisons should not extract subquery comparison: left=%q right=%q", i, left, right)
			}
		}
	}
}

func TestHasPrecisionLoss(t *testing.T) {
	cases := []struct {
		name     string
		source   string
		target   string
		wantKind string
		wantOk   bool
	}{
		{name: "same type", source: "DSOPERDAY", target: "DSOPERDAY", wantKind: "", wantOk: false},
		{name: "datetime to date", source: "datetime", target: "DSOPERDAY", wantKind: "loss", wantOk: true},
		{name: "datetime to smalldatetime", source: "datetime", target: "smalldatetime", wantKind: "loss", wantOk: true},
		{name: "numeric narrowing", source: "numeric(15,2)", target: "numeric(10,0)", wantKind: "loss", wantOk: true},
		{name: "numeric same precision", source: "numeric(15,2)", target: "numeric(15,2)", wantKind: "", wantOk: false},
		{name: "varchar narrowing", source: "varchar(250)", target: "varchar(20)", wantKind: "loss", wantOk: true},
		{name: "varchar same length", source: "varchar(20)", target: "varchar(20)", wantKind: "", wantOk: false},
		{name: "varchar no size", source: "varchar", target: "varchar(20)", wantKind: "", wantOk: false},
		{name: "incompatible varchar to int", source: "varchar", target: "int", wantKind: "incompatible", wantOk: true},
		{name: "incompatible float to varchar", source: "float", target: "varchar", wantKind: "incompatible", wantOk: true},
		{name: "incompatible datetime to int", source: "datetime", target: "int", wantKind: "incompatible", wantOk: true},
		{name: "empty source", source: "", target: "int", wantKind: "", wantOk: false},
		{name: "empty target", source: "int", target: "", wantKind: "", wantOk: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind, ok := hasPrecisionLoss(tc.source, tc.target)
			if kind != tc.wantKind || ok != tc.wantOk {
				t.Errorf("hasPrecisionLoss(%q, %q) = (%q, %v), want (%q, %v)", tc.source, tc.target, kind, ok, tc.wantKind, tc.wantOk)
			}
		})
	}
}

func TestVarcharLength(t *testing.T) {
	cases := []struct {
		input   string
		wantLen int
		wantOk  bool
	}{
		{input: "varchar(20)", wantLen: 20, wantOk: true},
		{input: "varchar(250)", wantLen: 250, wantOk: true},
		{input: "char(10)", wantLen: 10, wantOk: true},
		{input: "nvarchar(100)", wantLen: 100, wantOk: true},
		{input: "varchar", wantLen: 0, wantOk: false},
		{input: "int", wantLen: 0, wantOk: false},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			n, ok := varcharLength(tc.input)
			if n != tc.wantLen || ok != tc.wantOk {
				t.Errorf("varcharLength(%q) = (%d, %v), want (%d, %v)", tc.input, n, ok, tc.wantLen, tc.wantOk)
			}
		})
	}
}

func TestCheckDatatypeExecParams_NoDB_NoPanic(t *testing.T) {
	runner := &Runner{}
	content := "exec my_proc @Date = @OperDay\n"
	f, err := os.CreateTemp("", "dtxec*.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	f.Close()

	file := &indexedFile{Path: f.Name(), DsProductID: 1}
	runner.exec = &reviewExecContext{
		filePath:    normalizePath(f.Name()),
		content:     []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}
	parsed := &sqlparser.ParseResult{}
	findings, err := runner.checkDatatypeExecParams(parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings without DB, got %d", len(findings))
	}
}

func TestResolveArithmeticExprType_DSIDentifierMinusInt(t *testing.T) {
	r := &Runner{macroTypeCache: make(map[string]string)}
	varTypes := map[string]string{
		"maxid": "DSIDENTIFIER",
		"minid": "DSIDENTIFIER",
	}
	got := r.resolveArgType("@MaxID - @MinID + 1", varTypes, map[string]string{})
	if got != "DSIDENTIFIER" {
		t.Fatalf("expected DSIDENTIFIER, got %q", got)
	}
}

func TestResolveArithmeticExprType_DSIntKeyPlusInt(t *testing.T) {
	r := &Runner{macroTypeCache: make(map[string]string)}
	varTypes := map[string]string{
		"delta": "DSINT_KEY",
	}
	got := r.resolveArgType("@Delta + 1", varTypes, map[string]string{})
	if got != "DSINT_KEY" {
		t.Fatalf("expected DSINT_KEY, got %q", got)
	}
}

func TestParseSelectAssignStatement_NoFrom(t *testing.T) {
	stmt, ok := parseSelectAssignStatement("select @Delta = @MaxID - @MinID + 1")
	if !ok {
		t.Fatalf("expected parse success")
	}
	if len(stmt.Assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(stmt.Assignments))
	}
	if stmt.Assignments[0].TargetVariable != "@Delta" {
		t.Fatalf("expected @Delta, got %q", stmt.Assignments[0].TargetVariable)
	}
	if stmt.Assignments[0].Expression != "@MaxID - @MinID + 1" {
		t.Fatalf("expected expression @MaxID - @MinID + 1, got %q", stmt.Assignments[0].Expression)
	}
	if stmt.FromClause != "" {
		t.Fatalf("expected empty FromClause, got %q", stmt.FromClause)
	}
}

func TestParseSelectAssignStatement_WithFrom(t *testing.T) {
	stmt, ok := parseSelectAssignStatement("select @x = a.col from t a")
	if !ok {
		t.Fatalf("expected parse success")
	}
	if len(stmt.Assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(stmt.Assignments))
	}
	if stmt.FromClause == "" {
		t.Fatalf("expected non-empty FromClause")
	}
}

func TestLiteralFitsType(t *testing.T) {
	cases := []struct {
		name     string
		literal  string
		target   string
		want     bool
	}{
		{name: "8 fits tinyint", literal: "8", target: "DSTINYINT", want: true},
		{name: "255 fits tinyint", literal: "255", target: "dstinyint", want: true},
		{name: "256 not fits tinyint", literal: "256", target: "dstinyint", want: false},
		{name: "0 fits tinyint", literal: "0", target: "dstinyint", want: true},
		{name: "negative not fits tinyint", literal: "-1", target: "dstinyint", want: false},
		{name: "2 fits tinyint", literal: "2", target: "DSTINYINT", want: true},
		{name: "100 fits smallint", literal: "100", target: "DSSMALLINT", want: true},
		{name: "40000 not fits smallint", literal: "40000", target: "dssmallint", want: false},
		{name: "100000 fits int", literal: "100000", target: "DSINT_KEY", want: true},
		{name: "999999999999999 fits dsidentifier", literal: "999999999999999", target: "DSIDENTIFIER", want: true},
		{name: "1000000000000000 not fits dsidentifier", literal: "1000000000000000", target: "DSIDENTIFIER", want: false},
		{name: "3.14 fits numeric(10,2)", literal: "3.14", target: "numeric(10,2)", want: true},
		{name: "3.141 not fits numeric(10,2)", literal: "3.141", target: "numeric(10,2)", want: false},
		{name: "8.0 fits tinyint", literal: "8.0", target: "dstinyint", want: true},
		{name: "8.5 not fits tinyint", literal: "8.5", target: "dstinyint", want: false},
		{name: "empty literal", literal: "", target: "int", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := literalFitsType(tc.literal, tc.target)
			if got != tc.want {
				t.Errorf("literalFitsType(%q, %q) = %v, want %v", tc.literal, tc.target, got, tc.want)
			}
		})
	}
}

func TestExtractDeletePtableCalls_BasicMacro(t *testing.T) {
	lines := []string{"  M_DELETE_PTABLE(pMyTable)"}
	calls := extractDeletePtableCalls(lines)
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].TableName != "pMyTable" {
		t.Errorf("TableName = %q, want pMyTable", calls[0].TableName)
	}
	if calls[0].IndexName != "XPKpMyTable" {
		t.Errorf("IndexName = %q, want XPKpMyTable", calls[0].IndexName)
	}
	if calls[0].MacroName != "M_DELETE_PTABLE" {
		t.Errorf("MacroName = %q, want M_DELETE_PTABLE", calls[0].MacroName)
	}
}

func TestExtractDeletePtableCalls_Inmem(t *testing.T) {
	lines := []string{"  M_DELETE_PTABLE_INMEM(pMyTable)"}
	calls := extractDeletePtableCalls(lines)
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].TableName != "pMyTable" {
		t.Errorf("TableName = %q, want pMyTable", calls[0].TableName)
	}
	if calls[0].IndexName != "XPKpMyTable" {
		t.Errorf("IndexName = %q, want XPKpMyTable", calls[0].IndexName)
	}
}

func TestExtractDeletePtableCalls_Parallel(t *testing.T) {
	lines := []string{"  M_DELETE_PTABLE_PARALLEL(pMyTable, @@spid, ID, 1000, 4)"}
	calls := extractDeletePtableCalls(lines)
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].TableName != "pMyTable" {
		t.Errorf("TableName = %q, want pMyTable", calls[0].TableName)
	}
	if calls[0].IndexName != "XPKpMyTable" {
		t.Errorf("IndexName = %q, want XPKpMyTable", calls[0].IndexName)
	}
}

func TestExtractDeletePtableCalls_ExplicitIndex(t *testing.T) {
	lines := []string{"  M_DELETE_PTABLE_INDEX(pMyTable, XPKpMyOtherIndex)"}
	calls := extractDeletePtableCalls(lines)
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].TableName != "pMyTable" {
		t.Errorf("TableName = %q, want pMyTable", calls[0].TableName)
	}
	if calls[0].IndexName != "XPKpMyOtherIndex" {
		t.Errorf("IndexName = %q, want XPKpMyOtherIndex", calls[0].IndexName)
	}
}

func TestExtractDeletePtableCalls_SpidIndex(t *testing.T) {
	lines := []string{"  M_DELETE_PTABLE_SPID_INDEX(pMyTable, XPKpMyTable, @@spid)"}
	calls := extractDeletePtableCalls(lines)
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].TableName != "pMyTable" {
		t.Errorf("TableName = %q, want pMyTable", calls[0].TableName)
	}
	if calls[0].IndexName != "XPKpMyTable" {
		t.Errorf("IndexName = %q, want XPKpMyTable", calls[0].IndexName)
	}
}

func TestExtractDeletePtableCalls_SpidUnique(t *testing.T) {
	lines := []string{"  M_DELETE_PTABLE_SPID_UNIQUE(pMyTable, XPKpMyTable, @@spid, ID)"}
	calls := extractDeletePtableCalls(lines)
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].TableName != "pMyTable" {
		t.Errorf("TableName = %q, want pMyTable", calls[0].TableName)
	}
	if calls[0].IndexName != "XPKpMyTable" {
		t.Errorf("IndexName = %q, want XPKpMyTable", calls[0].IndexName)
	}
}

func TestExtractDeletePtableCalls_MultipleCalls(t *testing.T) {
	lines := []string{
		"  M_DELETE_PTABLE(pTable1)",
		"  M_DELETE_PTABLE_INDEX(pTable2, XPKpTable2)",
	}
	calls := extractDeletePtableCalls(lines)
	if len(calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(calls))
	}
	if calls[0].TableName != "pTable1" || calls[0].IndexName != "XPKpTable1" {
		t.Errorf("call[0] = {%s, %s}, want {pTable1, XPKpTable1}", calls[0].TableName, calls[0].IndexName)
	}
	if calls[1].TableName != "pTable2" || calls[1].IndexName != "XPKpTable2" {
		t.Errorf("call[1] = {%s, %s}, want {pTable2, XPKpTable2}", calls[1].TableName, calls[1].IndexName)
	}
}

func TestExtractDeletePtableCalls_SkipsRun(t *testing.T) {
	lines := []string{"  M_DELETE_PTABLE_RUN(4)"}
	calls := extractDeletePtableCalls(lines)
	if len(calls) != 0 {
		t.Fatalf("expected 0 calls for M_DELETE_PTABLE_RUN, got %d", len(calls))
	}
}

func TestExtractDeletePtableCalls_CaseInsensitive(t *testing.T) {
	lines := []string{"  m_delete_ptable(pMyTable)"}
	calls := extractDeletePtableCalls(lines)
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].TableName != "pMyTable" {
		t.Errorf("TableName = %q, want pMyTable", calls[0].TableName)
	}
	if calls[0].IndexName != "XPKpMyTable" {
		t.Errorf("IndexName = %q, want XPKpMyTable", calls[0].IndexName)
	}
}

func TestSqlMacrosMap_ContainsDeletePtableSpidIndex(t *testing.T) {
	if !sqlMacrosMap["m_delete_ptable_spid_index"] {
		t.Fatalf("m_delete_ptable_spid_index should be in sqlMacrosMap")
	}
	if !sqlMacrosMap["m_delete_ptable_index"] {
		t.Fatalf("m_delete_ptable_index should be in sqlMacrosMap")
	}
}

func TestExtractFuncColumnRefs_SkipsDeletePtableSpidIndex(t *testing.T) {
	refs := extractFuncColumnRefs("M_DELETE_PTABLE_SPID_INDEX(pConsAccrObjDateInterval, XIE1pConsAccrObjDateInterval, -@@spid)")
	for _, ref := range refs {
		if strings.EqualFold(ref.funcName, "m_delete_ptable_spid_index") {
			t.Fatalf("m_delete_ptable_spid_index should be skipped as a macro, but got ref: %+v", ref)
		}
	}
}

func TestExtractWherePartForIndexWrong_StopsAtInsert(t *testing.T) {
	fullText := "update t set x = 1 from t M_UPDLOCK_INDEX(XIE1t) where SPID = @@spid M_FORCEORDER M_LOG_TABLE(t) M_DELETE_PTABLE_SPID_INDEX(t, XIE1t, -@@spid) insert t2 select 1"
	wherePart := extractWherePartForIndexWrong(fullText)
	lower := strings.ToLower(wherePart)
	if strings.Contains(lower, "insert") {
		t.Fatalf("WHERE part should not contain 'insert', got: %s", wherePart)
	}
	// M_DELETE_PTABLE_SPID_INDEX may be in WHERE part (between WHERE and next stmt),
	// but extractFuncColumnRefs should skip it because it's in sqlMacrosMap.
	refs := extractFuncColumnRefs(wherePart)
	for _, ref := range refs {
		if strings.EqualFold(ref.funcName, "m_delete_ptable_spid_index") {
			t.Fatalf("m_delete_ptable_spid_index should be skipped by extractFuncColumnRefs, got ref: %+v", ref)
		}
	}
}

func TestExtractCurrentProcName_FromParsed(t *testing.T) {
	parsed := &sqlparser.ParseResult{
		Procedures: []*model.SQLProcedure{
			{ProcName: "MyProc"},
		},
	}
	name := extractCurrentProcName(parsed, "")
	if name != "MyProc" {
		t.Fatalf("expected MyProc, got %q", name)
	}
}

func TestExtractCurrentProcName_FromAPICreateProc(t *testing.T) {
	parsed := &sqlparser.ParseResult{}
	content := "API_CREATE_PROC(API_COver_CreateAgreement)\n__BEGIN_PROCEDURE__(API_COver_CreateAgreement)"
	name := extractCurrentProcName(parsed, content)
	if name != "API_COver_CreateAgreement" {
		t.Fatalf("expected API_COver_CreateAgreement, got %q", name)
	}
}

func TestExtractCurrentProcName_FromBeginProcedure(t *testing.T) {
	parsed := &sqlparser.ParseResult{}
	content := "  __BEGIN_PROCEDURE__(MyTestProc)\n  select 1"
	name := extractCurrentProcName(parsed, content)
	if name != "MyTestProc" {
		t.Fatalf("expected MyTestProc, got %q", name)
	}
}

func TestExtractCurrentProcName_Empty(t *testing.T) {
	parsed := &sqlparser.ParseResult{}
	name := extractCurrentProcName(parsed, "select 1")
	if name != "" {
		t.Fatalf("expected empty, got %q", name)
	}
}

func TestEnrichVariableTypesFromAPI_NilDB_NoOp(t *testing.T) {
	r := &Runner{}
	vt := map[string]string{"existingvar": "DSINT"}
	content := "API_CREATE_PROC(SomeProc)"
	r.enrichVariableTypesFromAPI(vt, &sqlparser.ParseResult{}, content)
	if len(vt) != 1 {
		t.Fatalf("expected 1 entry (no-op with nil db), got %d", len(vt))
	}
}

func TestEnrichVariableTypesFromAPI_DoesNotOverwriteExisting(t *testing.T) {
	vt := map[string]string{"agreementdate": "DSOPERDAY"}
	// Simulate: declare says DSOPERDAY, API says DSOPERDAY too — no overwrite needed.
	// This test just verifies the guard logic with nil db (no-op).
	r := &Runner{}
	r.enrichVariableTypesFromAPI(vt, &sqlparser.ParseResult{}, "API_CREATE_PROC(Test)")
	if vt["agreementdate"] != "DSOPERDAY" {
		t.Fatalf("existing type should not be changed, got %q", vt["agreementdate"])
	}
}
