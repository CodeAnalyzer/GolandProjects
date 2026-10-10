package querysvc

import (
	"testing"

	"github.com/codebase/internal/query"
)

func TestInspectRelationType(t *testing.T) {
	cases := []struct {
		symbol query.SymbolResult
		want   string
	}{
		{symbol: query.SymbolResult{Type: "procedure", EntityType: "sql"}, want: "sql_procedure"},
		{symbol: query.SymbolResult{Type: "table", EntityType: "sql"}, want: "sql_table"},
		{symbol: query.SymbolResult{Type: "form", EntityType: "dfm"}, want: "dfm_form"},
		{symbol: query.SymbolResult{Type: "component", EntityType: "dfm"}, want: "dfm_component"},
		{symbol: query.SymbolResult{Type: "method", EntityType: "pas"}, want: "pas_method"},
		{symbol: query.SymbolResult{Type: "function", EntityType: "js"}, want: "js_function"},
		{symbol: query.SymbolResult{Type: "unit", EntityType: "pas"}, want: "pas_unit"},
		{symbol: query.SymbolResult{Type: "class", EntityType: "pas"}, want: "pas_class"},
		{symbol: query.SymbolResult{Type: "service", EntityType: "xml"}, want: "api_contract"},
		{symbol: query.SymbolResult{Type: "event", EntityType: "xml"}, want: "api_contract"},
		{symbol: query.SymbolResult{Type: "callback_event", EntityType: "xml"}, want: "api_contract"},
		{symbol: query.SymbolResult{Type: "used_service", EntityType: "xml"}, want: "api_contract"},
		{symbol: query.SymbolResult{Type: "smf_instrument", EntityType: "smf"}, want: "smf_instrument"},
		{symbol: query.SymbolResult{Type: "vb_function", EntityType: "report"}, want: "vb_function"},
		{symbol: query.SymbolResult{Type: "report_form", EntityType: "report"}, want: "report_form"},
		{symbol: query.SymbolResult{Type: "spec_capability", EntityType: "spec"}, want: "spec_capability"},
		{symbol: query.SymbolResult{Type: "api_business_object", EntityType: "api"}, want: "api_business_object"},
		{symbol: query.SymbolResult{Type: "", EntityType: "sql"}, want: "sql"},
		{symbol: query.SymbolResult{Type: "  ", EntityType: " xml "}, want: "xml"},
	}
	for _, tc := range cases {
		if got := InspectRelationType(tc.symbol); got != tc.want {
			t.Errorf("InspectRelationType(%+v) = %q, want %q", tc.symbol, got, tc.want)
		}
	}
}

func TestNormalizeInspectScoringType(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{raw: "", want: ""},
		{raw: "method", want: "method"},
		{raw: "  Method ", want: "method"},
		{raw: "sql_procedure", want: "procedure"},
		{raw: "pas_method", want: "method"},
		{raw: "js_function", want: "function"},
		{raw: "api_contract", want: "service"},
		{raw: "неизвестный", want: "неизвестный"},
	}
	for _, tc := range cases {
		if got := normalizeInspectScoringType(tc.raw); got != tc.want {
			t.Errorf("normalizeInspectScoringType(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestPrioritizeExactSymbolMatches_AliasNormalizedByCaller(t *testing.T) {
	symbols := []query.SymbolResult{
		{Name: "Other", Type: "procedure", EntityType: "sql"},
		{Name: "MyProc", Type: "procedure", EntityType: "sql"},
	}
	// Вызывающая сторона (RunInspectQuery) нормализует алиас sql_procedure → procedure.
	ordered := PrioritizeExactSymbolMatches(symbols, "MyProc", "procedure")
	if len(ordered) != 2 || ordered[0].Name != "MyProc" {
		t.Fatalf("exact match with normalized type must be first, got %+v", ordered)
	}
}
