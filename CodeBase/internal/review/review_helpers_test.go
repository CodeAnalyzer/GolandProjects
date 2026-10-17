package review

import (
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

func TestValidateExecArguments(t *testing.T) {
	params := []model.SQLParam{
		{Name: "Id", Type: "DSINT"},
		{Name: "Name", Type: "DSVARCHAR"},
	}

	tests := []struct {
		name        string
		args        []execArgument
		wantFinding bool
		wantDetail  string
	}{
		{
			name: "Корректные именованные",
			args: []execArgument{
				{IsNamed: true, Name: "id", Value: "1"},
				{IsNamed: true, Name: "name", Value: "'test'"},
			},
			wantFinding: false,
		},
		{
			name: "Корректные позиционные",
			args: []execArgument{
				{IsNamed: false, Value: "1"},
				{IsNamed: false, Value: "'test'"},
			},
			wantFinding: false,
		},
		{
			name: "Лишний именованный параметр",
			args: []execArgument{
				{IsNamed: true, Name: "id", Value: "1"},
				{IsNamed: true, Name: "extra", Value: "123"},
			},
			wantFinding: true,
			wantDetail:  "лишний параметр @extra",
		},
		{
			name: "Дублирование именованного параметра",
			args: []execArgument{
				{IsNamed: true, Name: "id", Value: "1"},
				{IsNamed: true, Name: "id", Value: "2"},
			},
			wantFinding: true,
			wantDetail:  "параметр @id дублируется",
		},
		{
			name: "Лишние позиционные параметры",
			args: []execArgument{
				{IsNamed: false, Value: "1"},
				{IsNamed: false, Value: "2"},
				{IsNamed: false, Value: "3"},
			},
			wantFinding: true,
			wantDetail:  "позиционных параметров",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFinding, gotDetail := validateExecArguments(tt.args, params)
			if gotFinding != tt.wantFinding {
				t.Fatalf("got finding %t, want %t", gotFinding, tt.wantFinding)
			}
			if tt.wantFinding && !strings.Contains(gotDetail, tt.wantDetail) {
				t.Errorf("got detail %q, want it to contain %q", gotDetail, tt.wantDetail)
			}
		})
	}
}

func TestCachedRegexp_SamePatternReturnsSamePointer(t *testing.T) {
	re1 := cachedRegexp(`(?i)\bfoo\b`)
	re2 := cachedRegexp(`(?i)\bfoo\b`)
	if re1 != re2 {
		t.Error("cachedRegexp should return same *regexp.Regexp for identical pattern")
	}
}

func TestCachedRegexp_DifferentPatternReturnsDifferentPointer(t *testing.T) {
	re1 := cachedRegexp(`(?i)\bfoo\b`)
	re2 := cachedRegexp(`(?i)\bbar\b`)
	if re1 == re2 {
		t.Error("cachedRegexp should return different *regexp.Regexp for different patterns")
	}
}

func TestCachedRegexp_FunctionalMatch(t *testing.T) {
	re := cachedRegexp(`(?i)@\w+\b`)
	if !re.MatchString("select @a from t") {
		t.Error("cachedRegexp returned regexp that does not match expected input")
	}
}

func TestContainsVarReference_SystemVarNoMatch(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		varName string
		want    bool
	}{
		{name: "system @@rowcount is not @rowcount", expr: "@@rowcount", varName: "@RowCount", want: false},
		{name: "system @@error is not @error", expr: "@@error", varName: "@Error", want: false},
		{name: "system @@RowCount case-insensitive", expr: "@@RowCount", varName: "@RowCount", want: false},
		{name: "plain reference matches", expr: "@RowCount + 1", varName: "@RowCount", want: true},
		{name: "reference in expression matches", expr: "isnull(@RowCount, 0)", varName: "@RowCount", want: true},
		{name: "case-insensitive reference", expr: "x = @rowcount", varName: "@RowCount", want: true},
		{name: "system and plain in one expression", expr: "@@rowcount + @RowCount", varName: "@RowCount", want: true},
		{name: "no reference", expr: "@@error, 1", varName: "@RowCount", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := containsVarReference(tc.expr, tc.varName); got != tc.want {
				t.Fatalf("containsVarReference(%q, %q) = %v, want %v", tc.expr, tc.varName, got, tc.want)
			}
		})
	}
}

func TestStripStandaloneMacroLines(t *testing.T) {
	cases := []struct {
		name  string
		line  string
		blank bool
	}{
		{name: "businesslog param", line: "M_BUSINESSLOG_PARAM(@RowCount, DSINT_KEY, INT)", blank: true},
		{name: "delete ptable", line: "M_DELETE_PTABLE(pCons_CTL_SubcontoID)", blank: true},
		{name: "log table", line: "M_LOG_TABLE(pCons_CTL_SubcontoID)", blank: true},
		{name: "leading spaces and lowercase", line: "  m_log_table(pX)  ", blank: true},
		{name: "inline nolock index not stripped", line: "from tX M_NOLOCK_INDEX(XPK)", blank: false},
		{name: "forceorder without parens not stripped", line: "M_FORCEORDER", blank: false},
		{name: "select not stripped", line: "select 1", blank: false},
		{name: "macro prefix of longer name not matched", line: "M_DELETE_PTABLE_INDEX(pX, XPK)", blank: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stripStandaloneMacroLines(tc.line)
			wantBlank := ""
			if !tc.blank {
				wantBlank = tc.line
			}
			if got != wantBlank {
				t.Fatalf("stripStandaloneMacroLines(%q) = %q, want %q", tc.line, got, wantBlank)
			}
		})
	}

	t.Run("line numbering preserved", func(t *testing.T) {
		input := "select 1\nM_LOG_TABLE(pX)\nwhere x = 1"
		want := "select 1\n\nwhere x = 1"
		if got := stripStandaloneMacroLines(input); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		if lines := strings.Count(stripStandaloneMacroLines(input), "\n"); lines != 2 {
			t.Fatalf("line count changed: got %d newlines, want 2", lines)
		}
	})
}
