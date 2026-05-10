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

func TestBuildRelationNameExistsCondition(t *testing.T) {
	tests := []struct {
		name         string
		side         string
		relationType string
		argPos       int
		want         string
	}{
		{
			name:         "source sql procedure",
			side:         "source",
			relationType: "sql_procedure",
			argPos:       1,
			want:         "EXISTS (SELECT 1 FROM sql_procedures n WHERE n.id = r.source_id AND n.proc_name ILIKE $1)",
		},
		{
			name:         "target js function case insensitive",
			side:         "TARGET",
			relationType: " JS_FUNCTION ",
			argPos:       4,
			want:         "EXISTS (SELECT 1 FROM js_functions n WHERE n.id = r.target_id AND n.function_name ILIKE $4)",
		},
		{
			name:         "api contract",
			side:         "source",
			relationType: "api_contract",
			argPos:       2,
			want:         "EXISTS (SELECT 1 FROM api_contracts n WHERE n.id = r.source_id AND n.contract_name ILIKE $2)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := buildRelationNameExistsCondition(tt.side, tt.relationType, tt.argPos)
			if !ok {
				t.Fatalf("expected supported relation type")
			}
			if got != tt.want {
				t.Fatalf("condition: got=%q want=%q", got, tt.want)
			}
		})
	}

	if got, ok := buildRelationNameExistsCondition("source", "unknown", 1); ok || got != "" {
		t.Fatalf("unsupported type = %q, %v; want empty, false", got, ok)
	}
}

func TestBuildRelationAnyNameExistsCondition(t *testing.T) {
	sourceCondition := buildRelationAnyNameExistsCondition("source", 5)
	if !strings.Contains(sourceCondition, "r.source_type = 'sql_procedure'") ||
		!strings.Contains(sourceCondition, "n.id = r.source_id") ||
		!strings.Contains(sourceCondition, "n.proc_name ILIKE $5") ||
		!strings.Contains(sourceCondition, "r.source_type = 'js_function'") {
		t.Fatalf("unexpected source condition: %s", sourceCondition)
	}

	targetCondition := buildRelationAnyNameExistsCondition("target", 6)
	if !strings.Contains(targetCondition, "r.target_type = 'sql_procedure'") ||
		!strings.Contains(targetCondition, "n.id = r.target_id") ||
		!strings.Contains(targetCondition, "n.proc_name ILIKE $6") ||
		!strings.Contains(targetCondition, "r.target_type = 'smf_instrument'") {
		t.Fatalf("unexpected target condition: %s", targetCondition)
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
