package query

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuildLookupValue(t *testing.T) {
	if got := buildLookupValue("  SymbolName  ", false); got != "SymbolName" {
		t.Fatalf("exact lookup = %q, want SymbolName", got)
	}
	if got := buildLookupValue("  SymbolName  ", true); got != "%SymbolName%" {
		t.Fatalf("like lookup = %q, want %%SymbolName%%", got)
	}
}

func TestBuildNameLookupCondition(t *testing.T) {
	got := buildNameLookupCondition([]string{" name ", "", "signature"}, false, 2)
	want := "name = $2 OR signature = $2"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}

	got = buildNameLookupCondition([]string{"name", "signature"}, true, 3)
	want = "name ILIKE $3 OR signature ILIKE $3"
	if got != want {
		t.Fatalf("like condition: got=%q want=%q", got, want)
	}

	if got := buildNameLookupCondition(nil, false, 1); got != "" {
		t.Fatalf("empty fields condition = %q, want empty", got)
	}
}

func TestBuildSymbolLookupCondition_FormTypeSearchesNameAndSignature(t *testing.T) {
	got := buildSymbolLookupCondition("form", false, 1)
	want := "s.symbol_name = $1 OR s.signature = $1"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

func TestBuildSymbolLookupCondition_EmptyTypeSearchesNameAndFormSignature(t *testing.T) {
	got := buildSymbolLookupCondition("", false, 1)
	want := "s.symbol_name = $1 OR (s.symbol_type = 'form' AND s.signature = $1)"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

func TestBuildSymbolLookupCondition_OtherTypeKeepsFormSignatureGuarded(t *testing.T) {
	got := buildSymbolLookupCondition("class", false, 1)
	want := "LOWER(s.symbol_name) = LOWER($1)"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

func TestBuildSymbolLookupCondition_ProcedureExactUsesLowerNameOnly(t *testing.T) {
	got := buildSymbolLookupCondition("procedure", false, 3)
	want := "LOWER(s.symbol_name) = LOWER($3)"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

func TestBuildSymbolLookupCondition_LikeUsesILike(t *testing.T) {
	got := buildSymbolLookupCondition("form", true, 2)
	want := "s.symbol_name ILIKE $2 OR s.signature ILIKE $2"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

func TestBuildRelationDetailsQueryByIDs(t *testing.T) {
	queryText, args := buildRelationDetailsQueryByIDs([]int64{10, 20, 30})
	if !strings.Contains(queryText, "WHERE r.id IN ($1,$2,$3)") {
		t.Fatalf("query must contain placeholders, got: %s", queryText)
	}
	if !strings.Contains(queryText, "ORDER BY r.id DESC") {
		t.Fatalf("query must order by relation id desc")
	}
	wantArgs := []interface{}{int64(10), int64(20), int64(30)}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", args, wantArgs)
	}
}

func TestBuildRelationIDsQuery_SourceAndTargetMatches(t *testing.T) {
	sourceMatches := []relationEntityMatch{{Type: "sql_procedure", ID: 10}}
	targetMatches := []relationEntityMatch{{Type: "sql_table", ID: 20}}
	queryText, args := buildRelationIDsQuery(sourceMatches, targetMatches, "", "", "", 5)

	if !strings.Contains(queryText, "AS matched_source(entity_type, entity_id)") {
		t.Fatalf("query must join source matches: %s", queryText)
	}
	if !strings.Contains(queryText, "AS matched_target(entity_type, entity_id)") {
		t.Fatalf("query must join target matches: %s", queryText)
	}
	if !strings.Contains(queryText, "r.source_id = matched_source.entity_id::BIGINT") ||
		!strings.Contains(queryText, "r.target_id = matched_target.entity_id::BIGINT") {
		t.Fatalf("query must match entity ids: %s", queryText)
	}
	if !strings.Contains(queryText, "ORDER BY r.id DESC LIMIT $5") {
		t.Fatalf("query must order and limit: %s", queryText)
	}
	wantArgs := []interface{}{"sql_procedure", int64(10), "sql_table", int64(20), 5}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", args, wantArgs)
	}
}

func TestBuildRelationIDsQuery_TypeOnlyWithoutName(t *testing.T) {
	queryText, args := buildRelationIDsQuery(nil, nil, "sql_procedure", "sql_table", "selects_from", 10)

	if strings.Contains(queryText, "matched_source") || strings.Contains(queryText, "matched_target") {
		t.Fatalf("type-only query must not join matches: %s", queryText)
	}
	if !strings.Contains(queryText, "r.source_type = $1") ||
		!strings.Contains(queryText, "r.target_type = $2") ||
		!strings.Contains(queryText, "r.relation_type = $3") {
		t.Fatalf("query must filter by types and relation type: %s", queryText)
	}
	if !strings.Contains(queryText, "ORDER BY r.id DESC LIMIT $4") {
		t.Fatalf("query must limit: %s", queryText)
	}
	wantArgs := []interface{}{"sql_procedure", "sql_table", "selects_from", 10}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", args, wantArgs)
	}
}

func TestBuildRelationIDsQuery_TypedNameUsesOnlyMatches(t *testing.T) {
	// Типизированное имя приходит уже отфильтрованным первым проходом,
	// поэтому второй проход не добавляет избыточное условие по типу.
	matches := []relationEntityMatch{{Type: "sql_table", ID: 7}}
	queryText, args := buildRelationIDsQuery(nil, matches, "", "sql_table", "", 3)

	if !strings.Contains(queryText, "matched_target") {
		t.Fatalf("query must join target matches: %s", queryText)
	}
	if strings.Contains(queryText, "r.target_type = $") {
		t.Fatalf("typed name must not add redundant type clause: %s", queryText)
	}
	wantArgs := []interface{}{"sql_table", int64(7), 3}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", args, wantArgs)
	}
}

func TestRelationEntityMatchQueryParts_EntityTypeFiltersToOneTable(t *testing.T) {
	parts, ok := relationEntityMatchQueryParts("sql_table", false)
	if !ok || len(parts) != 1 {
		t.Fatalf("parts = %#v, ok=%v; want single part", parts, ok)
	}
	if !strings.Contains(parts[0], "FROM sql_tables") || !strings.Contains(parts[0], "table_name ILIKE $1") {
		t.Fatalf("unexpected part: %s", parts[0])
	}
}

func TestRelationEntityMatchLimit(t *testing.T) {
	tests := []struct {
		limit int
		want  int
	}{
		{limit: 0, want: 100},
		{limit: -1, want: 100},
		{limit: 1, want: 50},
		{limit: 12, want: 50},
		{limit: 20, want: 80},
	}

	for _, tt := range tests {
		if got := relationEntityMatchLimit(tt.limit); got != tt.want {
			t.Fatalf("relationEntityMatchLimit(%d) = %d, want %d", tt.limit, got, tt.want)
		}
	}
}

func TestRelationEntityMatchQueryParts(t *testing.T) {
	parts, ok := relationEntityMatchQueryParts("js_function", true)
	if !ok {
		t.Fatalf("expected js_function query parts")
	}
	if len(parts) != 1 {
		t.Fatalf("parts count = %d, want 1", len(parts))
	}
	if !strings.Contains(parts[0], "SELECT 'js_function' AS entity_type") ||
		!strings.Contains(parts[0], "FROM js_functions") ||
		!strings.Contains(parts[0], "LOWER(function_name) = LOWER($1)") {
		t.Fatalf("unexpected exact part: %s", parts[0])
	}

	parts, ok = relationEntityMatchQueryParts("", false)
	if !ok {
		t.Fatalf("expected all query parts")
	}
	if len(parts) < 10 {
		t.Fatalf("expected query parts for all supported types, got %d", len(parts))
	}
	if !strings.Contains(parts[0], "proc_name ILIKE $1") {
		t.Fatalf("unexpected like part: %s", parts[0])
	}

	if parts, ok := relationEntityMatchQueryParts("unknown", true); ok || parts != nil {
		t.Fatalf("unknown type = %#v, %v; want nil, false", parts, ok)
	}
}

func TestRelationSearchBaseQuery(t *testing.T) {
	queryText := relationSearchBaseQuery()
	required := []string{
		"FROM relations r",
		"LEFT JOIN sql_procedures sp_src",
		"LEFT JOIN js_functions jf_src",
		"LEFT JOIN api_contracts ac_tgt",
		"source_name",
		"target_name",
	}
	for _, fragment := range required {
		if !strings.Contains(queryText, fragment) {
			t.Fatalf("base query does not contain %q", fragment)
		}
	}
}
