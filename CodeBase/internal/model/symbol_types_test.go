package model

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSymbolTypesCanonicalSet(t *testing.T) {
	if len(SymbolTypes) != 29 {
		t.Fatalf("len(SymbolTypes) = %d, want 29", len(SymbolTypes))
	}
	seen := make(map[string]struct{}, len(SymbolTypes))
	for _, typ := range SymbolTypes {
		if _, dup := seen[typ]; dup {
			t.Fatalf("duplicate canonical type %q", typ)
		}
		seen[typ] = struct{}{}
	}
	sorted := append([]string(nil), SymbolTypes...)
	for i := 1; i < len(sorted); i++ {
		if sorted[i-1] > sorted[i] {
			t.Fatalf("SymbolTypes not sorted: %q before %q", sorted[i-1], sorted[i])
		}
	}
}

func TestResolveSymbolTypeAlias(t *testing.T) {
	cases := []struct {
		raw    string
		want   []string
		wantOK bool
	}{
		{raw: "procedure", want: []string{"procedure"}, wantOK: true},
		{raw: "table", want: []string{"table"}, wantOK: true},
		{raw: "method", want: []string{"method"}, wantOK: true},
		{raw: "function", want: []string{"function"}, wantOK: true},
		{raw: "form", want: []string{"form"}, wantOK: true},
		{raw: "component", want: []string{"component"}, wantOK: true},
		{raw: "service", want: []string{"service"}, wantOK: true},
		{raw: "event", want: []string{"event"}, wantOK: true},
		{raw: "callback_event", want: []string{"callback_event"}, wantOK: true},
		{raw: "used_service", want: []string{"used_service"}, wantOK: true},
		{raw: "smf_instrument", want: []string{"smf_instrument"}, wantOK: true},
		{raw: "spec_capability", want: []string{"spec_capability"}, wantOK: true},
		{raw: "api_business_object", want: []string{"api_business_object"}, wantOK: true},
		{raw: "xml", want: []string{"xml"}, wantOK: true},

		{raw: "sql_procedure", want: []string{"procedure"}, wantOK: true},
		{raw: "sql_table", want: []string{"table"}, wantOK: true},
		{raw: "pas_method", want: []string{"method"}, wantOK: true},
		{raw: "js_function", want: []string{"function"}, wantOK: true},
		{raw: "dfm_form", want: []string{"form"}, wantOK: true},
		{raw: "dfm_component", want: []string{"component"}, wantOK: true},
		{raw: "api_contract", want: []string{"service", "event", "callback_event", "used_service"}, wantOK: true},

		{raw: "  SQL_Procedure  ", want: []string{"procedure"}, wantOK: true},
		{raw: " API_Contract ", want: []string{"service", "event", "callback_event", "used_service"}, wantOK: true},
		{raw: "Procedure", want: []string{"procedure"}, wantOK: true},

		{raw: "", want: nil, wantOK: false},
		{raw: "   ", want: nil, wantOK: false},
		{raw: "proc", want: nil, wantOK: false},
		{raw: "sql", want: nil, wantOK: false},
		{raw: "js", want: nil, wantOK: false},
		{raw: "query_fragment", want: nil, wantOK: false},
		{raw: "js_function_typo", want: nil, wantOK: false},
	}
	for _, tc := range cases {
		got, ok := ResolveSymbolTypeAlias(tc.raw)
		if ok != tc.wantOK {
			t.Errorf("ResolveSymbolTypeAlias(%q) ok = %v, want %v", tc.raw, ok, tc.wantOK)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ResolveSymbolTypeAlias(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

func TestResolveSymbolTypeAliasEveryAliasAndCanonical(t *testing.T) {
	for _, alias := range SymbolTypeAliasesList() {
		got, ok := ResolveSymbolTypeAlias(alias)
		if !ok || len(got) == 0 {
			t.Fatalf("alias %q must resolve", alias)
		}
		for _, canonical := range got {
			if _, known := symbolTypeSet[canonical]; !known {
				t.Fatalf("alias %q resolves to unknown canonical type %q", alias, canonical)
			}
		}
	}
	for _, canonical := range SymbolTypes {
		got, ok := ResolveSymbolTypeAlias(canonical)
		if !ok {
			t.Fatalf("canonical type %q must pass through", canonical)
		}
		if fmt.Sprint(got) != fmt.Sprint([]string{canonical}) {
			t.Fatalf("canonical type %q must map to itself, got %v", canonical, got)
		}
	}
}

func TestRelationTypeForSymbol(t *testing.T) {
	cases := []struct {
		symbolType string
		entityType string
		want       string
	}{
		{symbolType: "procedure", entityType: "sql", want: "sql_procedure"},
		{symbolType: "table", entityType: "sql", want: "sql_table"},
		{symbolType: "form", entityType: "dfm", want: "dfm_form"},
		{symbolType: "component", entityType: "dfm", want: "dfm_component"},
		{symbolType: "method", entityType: "pas", want: "pas_method"},
		{symbolType: "function", entityType: "js", want: "js_function"},
		{symbolType: "unit", entityType: "pas", want: "pas_unit"},
		{symbolType: "class", entityType: "pas", want: "pas_class"},
		{symbolType: "service", entityType: "xml", want: "api_contract"},
		{symbolType: "event", entityType: "xml", want: "api_contract"},
		{symbolType: "callback_event", entityType: "xml", want: "api_contract"},
		{symbolType: "used_service", entityType: "xml", want: "api_contract"},
		{symbolType: "smf_instrument", entityType: "smf", want: "smf_instrument"},
		{symbolType: "vb_function", entityType: "report", want: "vb_function"},
		{symbolType: "report_form", entityType: "report", want: "report_form"},
		{symbolType: "spec_capability", entityType: "spec", want: "spec_capability"},
		{symbolType: "api_business_object", entityType: "api", want: "api_business_object"},
		{symbolType: "Procedure", entityType: "sql", want: "sql_procedure"},
		{symbolType: " Method ", entityType: "pas", want: "pas_method"},
		{symbolType: "", entityType: "sql", want: "sql"},
		{symbolType: "  ", entityType: " xml ", want: "xml"},
	}
	for _, tc := range cases {
		if got := RelationTypeForSymbol(tc.symbolType, tc.entityType); got != tc.want {
			t.Errorf("RelationTypeForSymbol(%q, %q) = %q, want %q", tc.symbolType, tc.entityType, got, tc.want)
		}
	}
}
