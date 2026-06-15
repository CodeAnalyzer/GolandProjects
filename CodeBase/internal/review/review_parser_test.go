package review

import (
	"strings"
	"testing"
)

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

func TestReplaceMacros_SimpleMacro(t *testing.T) {
	content := "#define MY_MACRO select 1\nMY_MACRO\nselect 2"
	result := replaceMacros(content)

	if strings.Contains(result.Content, "#define") {
		t.Fatalf("macro declaration should be removed")
	}
	if !strings.Contains(result.Content, "select 1") {
		t.Fatalf("macro body should be expanded")
	}
	if !strings.Contains(result.Content, "select 2") {
		t.Fatalf("regular line should be preserved")
	}
	if len(result.SourceMap) == 0 {
		t.Fatalf("sourceMap should not be empty")
	}
	// Проверяем, что lineNum маппится корректно
	lineNum := mapProcessedLineNumber(result.SourceMap, 1)
	if lineNum != 2 { // строка "MY_MACRO" была на строке 2 в исходнике
		t.Fatalf("mapProcessedLineNumber for expanded macro should map to original call line, got %d, want 2", lineNum)
	}
}

func TestReplaceMacros_ParametricMacro(t *testing.T) {
	content := "#define ADD(a, b) a + b\nselect ADD(1, 2)"
	result := replaceMacros(content)

	if strings.Contains(result.Content, "#define") {
		t.Fatalf("macro declaration should be removed")
	}
	if !strings.Contains(result.Content, "1 + 2") {
		t.Fatalf("parametric macro should be expanded with arguments substituted, got: %s", result.Content)
	}
	if strings.Contains(result.Content, "ADD") {
		t.Fatalf("macro call should be replaced")
	}
}

func TestReplaceMacros_MultilineMacro(t *testing.T) {
	content := `#define MY_MACRO \
  select 1 \
  from t1
MY_MACRO
select 2`
	result := replaceMacros(content)

	if strings.Contains(result.Content, "#define") {
		t.Fatalf("macro declaration should be removed")
	}
	if !strings.Contains(result.Content, "select 1") {
		t.Fatalf("first line of multiline macro body should be expanded")
	}
	if !strings.Contains(result.Content, "from t1") {
		t.Fatalf("last line of multiline macro body should be expanded")
	}
}

func TestReplaceMacros_SourceMapAfterDeclRemoval(t *testing.T) {
	content := "line1\n#define MY_MACRO select 1\nline3\nMY_MACRO\nline5"
	result := replaceMacros(content)

	lines := strings.Split(result.Content, "\n")
	if len(lines) != 4 { // line1, line3, select 1, line5
		t.Fatalf("expected 4 lines after macro removal and expansion, got %d", len(lines))
	}

	// Проверяем маппинг
	if mapProcessedLineNumber(result.SourceMap, 1) != 1 {
		t.Fatalf("line 1 should map to original line 1")
	}
	if mapProcessedLineNumber(result.SourceMap, 2) != 3 {
		t.Fatalf("line 2 should map to original line 3")
	}
	if mapProcessedLineNumber(result.SourceMap, 3) != 4 {
		t.Fatalf("line 3 (expanded macro) should map to original call line 4")
	}
	if mapProcessedLineNumber(result.SourceMap, 4) != 5 {
		t.Fatalf("line 4 should map to original line 5")
	}
}

func TestReplaceMacros_UnusedMacroNotExpanded(t *testing.T) {
	content := "#define UNUSED_MACRO select 1\nselect 2"
	result := replaceMacros(content)

	if strings.Contains(result.Content, "select 1") {
		t.Fatalf("unused macro should not be expanded")
	}
	if !strings.Contains(result.Content, "select 2") {
		t.Fatalf("regular line should be preserved")
	}
}

func TestReplaceMacros_MultilineExpansionSourceMap(t *testing.T) {
	content := `line1
#define MY_MACRO \
  select 1 \
  from t1
line4
MY_MACRO
line6`
	result := replaceMacros(content)

	lines := strings.Split(result.Content, "\n")
	if len(lines) != 5 { // line1, line4, select 1, from t1, line6
		t.Fatalf("expected 5 lines, got %d: %v", len(lines), lines)
	}

	// Original lines: line1(1), #define(2), select 1 \(3), from t1(4), line4(5), MY_MACRO(6), line6(7)
	// After strip+expand: line1(1), line4(5), select 1(6), from t1(6), line6(7)
	if mapProcessedLineNumber(result.SourceMap, 3) != 6 { // MY_MACRO call was on original line 6
		t.Fatalf("first expanded line should map to macro call line")
	}
	if mapProcessedLineNumber(result.SourceMap, 4) != 6 { // continuation maps to same call line
		t.Fatalf("second expanded line should also map to macro call line")
	}
}

func TestMapProcessedLineNumber_OutOfBounds(t *testing.T) {
	sourceMap := []int{1, 2, 3}

	if mapProcessedLineNumber(sourceMap, 0) != 0 {
		t.Fatalf("line 0 should return 0")
	}
	if mapProcessedLineNumber(sourceMap, -1) != 0 {
		t.Fatalf("negative line should return 0")
	}
	if mapProcessedLineNumber(sourceMap, 5) != 5 { // out of bounds fallback
		t.Fatalf("out of bounds should return processed line number")
	}
}

func TestParseExecArguments(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		calleeName string
		want       []execArgument
	}{
		{
			name:       "Чисто позиционные",
			line:       "exec my_proc 123, 'test', @var",
			calleeName: "my_proc",
			want: []execArgument{
				{Raw: "123", IsNamed: false, Value: "123"},
				{Raw: "'test'", IsNamed: false, Value: "'test'"},
				{Raw: "@var", IsNamed: false, Value: "@var", VarName: "@var"},
			},
		},
		{
			name:       "Именованные",
			line:       "exec my_proc @Id = 10, @Name = 'John'",
			calleeName: "my_proc",
			want: []execArgument{
				{Raw: "@Id = 10", IsNamed: true, Name: "id", Value: "10"},
				{Raw: "@Name = 'John'", IsNamed: true, Name: "name", Value: "'John'"},
			},
		},
		{
			name:       "Именованные и позиционные с output флагом",
			line:       "exec my_proc @Id = @my_id output, @Name = 'John', @Ref = @loc_ref out, 123 out",
			calleeName: "my_proc",
			want: []execArgument{
				{Raw: "@Id = @my_id output", IsNamed: true, Name: "id", Value: "@my_id", IsOutput: true, VarName: "@my_id"},
				{Raw: "@Name = 'John'", IsNamed: true, Name: "name", Value: "'John'", IsOutput: false},
				{Raw: "@Ref = @loc_ref out", IsNamed: true, Name: "ref", Value: "@loc_ref", IsOutput: true, VarName: "@loc_ref"},
				{Raw: "123 out", IsNamed: false, Value: "123", IsOutput: true, VarName: ""},
			},
		},
		{
			name:       "С комментариями внутри",
			line:       "exec my_proc @Id = 10 -- ID контракта\n, @Name = 'John' /* имя */",
			calleeName: "my_proc",
			want: []execArgument{
				{Raw: "@Id = 10", IsNamed: true, Name: "id", Value: "10"},
				{Raw: "@Name = 'John'", IsNamed: true, Name: "name", Value: "'John'"},
			},
		},
		{
			name:       "Пустые скобки и вызовы без параметров",
			line:       "exec my_proc",
			calleeName: "my_proc",
			want:       nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseExecArguments(tt.line, tt.calleeName)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d arguments, want %d: %+v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].IsNamed != tt.want[i].IsNamed ||
					got[i].Name != tt.want[i].Name ||
					got[i].Value != tt.want[i].Value ||
					got[i].IsOutput != tt.want[i].IsOutput ||
					got[i].VarName != tt.want[i].VarName {
					t.Errorf("arg[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestCollectExecCallLines(t *testing.T) {
	lines := []string{
		"  select @RetVal = 0",
		"  exec BankProduct_Delete ",
		"         @BankProductID = @NodeID,",
		"         @BankProductID = @FundID",
		"  return @RetVal",
	}

	got := collectExecCallLines(lines, 2, "BankProduct_Delete")
	want := "  exec BankProduct_Delete \n         @BankProductID = @NodeID,\n         @BankProductID = @FundID"

	if got != want {
		t.Fatalf("collectExecCallLines got:\n%q\nwant:\n%q", got, want)
	}
}


