package indexer

import (
	"reflect"
	"testing"

	"github.com/codebase/internal/model"
)

func TestMapTableRelationType(t *testing.T) {
	tests := []struct {
		context string
		want    string
	}{
		{context: "select", want: "selects_from"},
		{context: " INSERT ", want: "inserts_into"},
		{context: "update", want: "updates"},
		{context: "delete", want: "deletes_from"},
		{context: "merge", want: "references_table"},
		{context: "", want: "references_table"},
	}

	for _, tt := range tests {
		if got := mapTableRelationType(tt.context); got != tt.want {
			t.Fatalf("mapTableRelationType(%q) = %q, want %q", tt.context, got, tt.want)
		}
	}
}

func TestComputeQueryHash(t *testing.T) {
	first := computeQueryHash(" select 1 ")
	second := computeQueryHash("select 1")
	third := computeQueryHash("select 2")

	if first != second {
		t.Fatalf("hash must ignore surrounding spaces: %q != %q", first, second)
	}
	if first == third {
		t.Fatalf("different queries must have different hashes")
	}
	if len(first) != 64 {
		t.Fatalf("sha256 hex length = %d, want 64", len(first))
	}
}

func TestUniqueStrings(t *testing.T) {
	got := uniqueStrings([]string{" TableA ", "tablea", "", "TableB", " tableb "})
	want := []string{" TableA ", "TableB"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("uniqueStrings = %#v, want %#v", got, want)
	}
}

func TestExtractProcedureCallsFromQuery(t *testing.T) {
	query := `
exec TestProc
execute dbo.OtherProc
EXEC @rc = [schema].[BracketProc]
exec TestProc
`
	got := extractProcedureCallsFromQuery(query)
	want := []string{"BracketProc", "TestProc"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("procedure calls = %#v, want %#v", got, want)
	}
}

func TestMapQueryFragmentParentRelationType(t *testing.T) {
	if got := mapQueryFragmentParentRelationType("pas_method", &model.QueryFragment{Context: "method"}); got != "builds_query" {
		t.Fatalf("pas method context method = %q, want builds_query", got)
	}
	if got := mapQueryFragmentParentRelationType("pas_method", &model.QueryFragment{Context: "property"}); got != "executes_query" {
		t.Fatalf("pas method context property = %q, want executes_query", got)
	}
	if got := mapQueryFragmentParentRelationType("js_function", nil); got != "executes_query" {
		t.Fatalf("js function = %q, want executes_query", got)
	}
	if got := mapQueryFragmentParentRelationType("unknown", nil); got != "" {
		t.Fatalf("unknown = %q, want empty", got)
	}
}

func TestExtractReportParamRefsAndFindByName(t *testing.T) {
	params := []*model.ReportParam{
		{ParamName: "DateFrom", LineNumber: 1},
		{ParamName: "ClientID", LineNumber: 2},
		{ParamName: "Unused", LineNumber: 3},
		nil,
	}

	refs := extractReportParamRefs("where d >= :datefrom and c = @CLIENTID and again = :DateFrom", params)
	want := []string{"DateFrom", "ClientID"}
	if !reflect.DeepEqual(refs, want) {
		t.Fatalf("refs = %#v, want %#v", refs, want)
	}

	if got := findReportParamByName(params, " clientid "); got == nil || got.ParamName != "ClientID" {
		t.Fatalf("findReportParamByName failed: %+v", got)
	}
	if got := findReportParamByName(params, "missing"); got != nil {
		t.Fatalf("missing param = %+v, want nil", got)
	}
}
