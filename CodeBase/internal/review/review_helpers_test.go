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

