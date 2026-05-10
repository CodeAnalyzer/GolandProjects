package cmd

import (
	"errors"
	"reflect"
	"testing"

	"github.com/codebase/internal/query"
)

func TestResultCountAndNormalizeNilResults(t *testing.T) {
	if got := resultCount(nil); got != 0 {
		t.Fatalf("resultCount(nil) = %d, want 0", got)
	}
	if got := resultCount([]int{1, 2}); got != 2 {
		t.Fatalf("resultCount(slice) = %d, want 2", got)
	}
	if got := resultCount("value"); got != 1 {
		t.Fatalf("resultCount(single) = %d, want 1", got)
	}

	if got := normalizeNilResults(nil); resultCount(got) != 0 {
		t.Fatalf("normalizeNilResults(nil) = %#v", got)
	}
	var values []query.SymbolResult
	got := normalizeNilResults(values)
	if reflect.ValueOf(got).IsNil() {
		t.Fatalf("normalizeNilResults must convert nil slice to empty slice")
	}
}

func TestFilterEmptyValues(t *testing.T) {
	if got := filterEmptyValues(nil); got != nil {
		t.Fatalf("nil filters = %#v, want nil", got)
	}
	got := filterEmptyValues(map[string]string{"a": "", "b": "value"})
	if len(got) != 1 || got["b"] != "value" {
		t.Fatalf("filtered values = %#v", got)
	}
	if got := filterEmptyValues(map[string]string{"a": ""}); got != nil {
		t.Fatalf("empty filtered map = %#v, want nil", got)
	}
}

func TestDetectQueryOutputMode(t *testing.T) {
	oldJSON, oldNDJSON, oldSummary := outputJSON, outputNDJSON, outputSummary
	t.Cleanup(func() {
		outputJSON, outputNDJSON, outputSummary = oldJSON, oldNDJSON, oldSummary
	})

	outputJSON, outputNDJSON, outputSummary = false, false, false
	if got := detectQueryOutputMode(); got != "text" {
		t.Fatalf("mode = %q, want text", got)
	}
	outputJSON = true
	if got := detectQueryOutputMode(); got != "json" {
		t.Fatalf("mode = %q, want json", got)
	}
	outputNDJSON = true
	if got := detectQueryOutputMode(); got != "ndjson" {
		t.Fatalf("mode = %q, want ndjson", got)
	}
	outputJSON, outputNDJSON, outputSummary = false, false, true
	if got := detectQueryOutputMode(); got != "summary" {
		t.Fatalf("mode = %q, want summary", got)
	}
}

func TestBuildQuerySummary(t *testing.T) {
	items := []query.RelationResult{
		{
			RelationType: "calls_procedure",
			Source:       query.RelationEntityRef{ID: 1, Type: "js_function", Name: "Fn", File: "a.js", FileID: 10, LineNumber: 2},
			Target:       query.RelationEntityRef{ID: 2, Type: "sql_procedure", Name: "Proc", File: "b.sql", FileID: 11, LineNumber: 3},
		},
	}
	summary := buildQuerySummary(items)
	if summary.Count != 1 || summary.Files != 2 || summary.DistinctTargets != 2 {
		t.Fatalf("unexpected summary counts: %+v", summary)
	}
	if summary.RelationTypes["calls_procedure"] != 1 || summary.SourceTypes["js_function"] != 1 || summary.TargetTypes["sql_procedure"] != 1 {
		t.Fatalf("unexpected summary maps: %+v", summary)
	}
}

func TestInspectHelpers(t *testing.T) {
	symbols := []query.SymbolResult{
		{ID: 1, Name: "Other", Type: "procedure", EntityType: "sql"},
		{ID: 2, Name: "Target", Type: "procedure", EntityType: "sql"},
	}
	ordered := prioritizeExactSymbolMatches(symbols, "Target", "procedure")
	if ordered[0].ID != 2 {
		t.Fatalf("exact match must be first: %+v", ordered)
	}
	if got := inspectRelationType(query.SymbolResult{Type: "procedure", EntityType: "sql"}); got != "sql_procedure" {
		t.Fatalf("sql procedure relation type = %q", got)
	}
	if got := inspectRelationType(query.SymbolResult{Type: "form", EntityType: "dfm"}); got != "dfm_form" {
		t.Fatalf("dfm form relation type = %q", got)
	}
	if got := limitInspectSymbols(symbols, 1); len(got) != 1 {
		t.Fatalf("limited symbols count = %d, want 1", len(got))
	}
}

func TestCollectInspectNeighbors(t *testing.T) {
	symbol := query.SymbolResult{ID: 1, EntityType: "sql_procedure"}
	incoming := []query.RelationResult{{Source: query.RelationEntityRef{ID: 2, Type: "js_function", Name: "Fn", File: "a.js", FileID: 10, LineNumber: 5}}}
	outgoing := []query.RelationResult{{Target: query.RelationEntityRef{ID: 3, Type: "sql_table", Name: "Table", File: "b.sql", FileID: 11, LineNumber: 6}}}
	neighbors := collectInspectNeighbors(symbol, incoming, outgoing)
	if len(neighbors) != 2 {
		t.Fatalf("neighbors count = %d, want 2", len(neighbors))
	}
	if neighbors[0].Name != "Fn" || neighbors[1].Name != "Table" {
		t.Fatalf("unexpected neighbors: %+v", neighbors)
	}
}

func TestClassifyQueryError(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{err: errors.New("config not loaded"), want: "config_error"},
		{err: errors.New("required flag(s) name not set"), want: "invalid_arguments"},
		{err: errors.New("failed to connect to database: connection refused"), want: "database_unavailable"},
		{err: errors.New("failed to init schema"), want: "schema_init_failed"},
		{err: errors.New("query failed: bad sql"), want: "query_failed"},
		{err: errors.New("other"), want: "internal_error"},
	}
	for _, tt := range tests {
		if got := classifyQueryError(tt.err); got != tt.want {
			t.Fatalf("classifyQueryError(%q) = %q, want %q", tt.err, got, tt.want)
		}
	}
}
