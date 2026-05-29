package indexer

import (
	"path/filepath"
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

func TestCloneInt64Map(t *testing.T) {
	original := map[string]int64{"a": 1, "b": 2, "c": 3}
	cloned := cloneInt64Map(original)

	if !reflect.DeepEqual(cloned, original) {
		t.Fatalf("cloned map differs from original: cloned=%#v original=%#v", cloned, original)
	}
	if &cloned == &original {
		t.Fatalf("cloned map is same reference as original")
	}
	cloned["a"] = 999
	if original["a"] != 1 {
		t.Fatalf("modifying cloned map affected original: original[a]=%d", original["a"])
	}
	if cloned["a"] != 999 {
		t.Fatalf("cloned map modification failed: cloned[a]=%d", cloned["a"])
	}
}

func TestBuildIncludePathCandidates(t *testing.T) {
	idx := &Indexer{}

	candidates := idx.buildIncludePathCandidates("/repo/scripts/Main.sql", "Common.inc")
	if len(candidates) == 0 {
		t.Fatalf("candidates should not be empty")
	}
	foundCommon := false
	for _, c := range candidates {
		if c == "Common.inc" || c == "scripts/Common.inc" || filepath.ToSlash(filepath.Join("scripts", "Common.inc")) == c {
			foundCommon = true
		}
	}
	if !foundCommon {
		t.Fatalf("expected Common.inc candidate in %#v", candidates)
	}

	if candidates := idx.buildIncludePathCandidates("/repo/scripts/Main.sql", ""); candidates != nil {
		t.Fatalf("empty include path should return nil, got %#v", candidates)
	}
}

func TestExtractCanonicalDSProductFromPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "regular fa product", path: "fa-cards/Cards/Server/A.sql", want: "fa-cards"},
		{name: "branched fa product", path: "fa-contracts-ext-develop/API_Credit/A.sql", want: "fa-contracts-ext"},
		{name: "branched fa product with many suffixes", path: "fa-contracts-ext-factr-nomcost/API_Credit/A.sql", want: "fa-contracts-ext"},
		{name: "absolute windows path", path: `D:\\GITHUB\\FA\\fa-contracts-ext-develop\\Consumer\\A.sql`, want: "fa-contracts-ext"},
		{name: "no fa segment", path: "Consumer/Server/A.sql", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractCanonicalDSProductFromPath(tt.path); got != tt.want {
				t.Fatalf("extractCanonicalDSProductFromPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestExtractCanonicalDSProductName(t *testing.T) {
	if got := extractCanonicalDSProductName(`D:\\GITHUB\\FA\\fa-contracts-ext-develop\\Consumer\\A.sql`, "Consumer/A.sql"); got != "fa-contracts-ext" {
		t.Fatalf("extractCanonicalDSProductName fallback to absolute path = %q, want fa-contracts-ext", got)
	}

	if got := extractCanonicalDSProductName(`D:\\GITHUB\\FA\\fa-contracts\\Consumer\\A.sql`, "fa-cards/Cards/A.sql"); got != "fa-cards" {
		t.Fatalf("extractCanonicalDSProductName should prefer relPath = %q, want fa-cards", got)
	}
}
