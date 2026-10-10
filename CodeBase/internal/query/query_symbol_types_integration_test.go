//go:build integration

package query

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/store/testutil"
)

type symbolTypesFixture struct {
	q     *Query
	ids   map[string]int64
	names []string
}

func seedSymbolTypesFixture(t *testing.T) symbolTypesFixture {
	t.Helper()
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	var fileID int64
	err = db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, '/repo/symbols.sql', '/repo/symbols.sql', 'sql', 1, 'hash', NOW(), 'UTF8', 'SQL')
		RETURNING id
	`, scanID).Scan(&fileID)
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}

	symbols := []struct {
		name       string
		symbolType string
		entityType string
	}{
		{"MyProc", "procedure", "sql"},
		{"MyTable", "table", "sql"},
		{"MyMethod", "method", "pas"},
		{"MyJSFunc", "function", "js"},
		{"MyForm", "form", "dfm"},
		{"MyComponent", "component", "dfm"},
		{"MyContractService", "service", "xml"},
		{"MyContractEvent", "event", "xml"},
		{"MyContractCallback", "callback_event", "xml"},
		{"MyContractUsedService", "used_service", "xml"},
	}
	ids := make(map[string]int64, len(symbols))
	for _, s := range symbols {
		var id int64
		err := db.QueryRow(`
			INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id)
			VALUES ($1, $2, $3, $4, 0)
			RETURNING id
		`, fileID, s.name, s.symbolType, s.entityType).Scan(&id)
		if err != nil {
			t.Fatalf("insert symbol %s: %v", s.name, err)
		}
		ids[s.name] = id
	}
	return symbolTypesFixture{q: New(db), ids: ids, names: []string{"MyContractService", "MyContractEvent", "MyContractCallback", "MyContractUsedService"}}
}

func symbolNames(results []SymbolResult) []string {
	names := make([]string, 0, len(results))
	for _, r := range results {
		names = append(names, r.Name)
	}
	return names
}

func TestSearchSymbol_AliasTranslatesToCanonical(t *testing.T) {
	fx := seedSymbolTypesFixture(t)
	ctx := context.Background()

	cases := []struct {
		alias    string
		wantName string
		wantType string
	}{
		{alias: "sql_procedure", wantName: "MyProc", wantType: "procedure"},
		{alias: "sql_table", wantName: "MyTable", wantType: "table"},
		{alias: "pas_method", wantName: "MyMethod", wantType: "method"},
		{alias: "js_function", wantName: "MyJSFunc", wantType: "function"},
		{alias: "dfm_form", wantName: "MyForm", wantType: "form"},
		{alias: "dfm_component", wantName: "MyComponent", wantType: "component"},
	}
	for _, tc := range cases {
		results, err := fx.q.SearchSymbol(ctx, tc.wantName, tc.alias, false, 50)
		if err != nil {
			t.Fatalf("SearchSymbol(%q, %q): %v", tc.wantName, tc.alias, err)
		}
		if len(results) != 1 || results[0].Name != tc.wantName || results[0].Type != tc.wantType {
			t.Fatalf("SearchSymbol(%q, alias %q) = %v, want single %s/%s", tc.wantName, tc.alias, symbolNames(results), tc.wantName, tc.wantType)
		}

		canonical, err := fx.q.SearchSymbol(ctx, tc.wantName, tc.wantType, false, 50)
		if err != nil {
			t.Fatalf("SearchSymbol(%q, %q): %v", tc.wantName, tc.wantType, err)
		}
		if fmt.Sprint(symbolNames(canonical)) != fmt.Sprint(symbolNames(results)) {
			t.Fatalf("alias %q and canonical %q must be equivalent: %v vs %v", tc.alias, tc.wantType, symbolNames(results), symbolNames(canonical))
		}
	}
}

func TestSearchSymbol_AliasApiContractFindsAllKindTypes(t *testing.T) {
	fx := seedSymbolTypesFixture(t)
	ctx := context.Background()

	results, err := fx.q.SearchSymbol(ctx, "MyContract", "api_contract", true, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(api_contract): %v", err)
	}
	got := map[string]bool{}
	for _, r := range results {
		got[r.Type] = true
	}
	for _, kind := range []string{"service", "event", "callback_event", "used_service"} {
		if !got[kind] {
			t.Fatalf("api_contract filter must find kind type %q, got %v", kind, symbolNames(results))
		}
	}
	if len(results) != 4 {
		t.Fatalf("api_contract filter must return exactly 4 kind symbols, got %d: %v", len(results), symbolNames(results))
	}
}

func TestSearchSymbol_AliasJsFunctionEntityIsJs(t *testing.T) {
	fx := seedSymbolTypesFixture(t)
	ctx := context.Background()

	results, err := fx.q.SearchSymbol(ctx, "MyJSFunc", "js_function", false, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(js_function): %v", err)
	}
	if len(results) != 1 || results[0].EntityType != "js" {
		t.Fatalf("js_function alias must find JS function with entity_type=js, got %v", results)
	}
}

func TestSearchSymbol_UnknownTypeFailsWithValidValues(t *testing.T) {
	fx := seedSymbolTypesFixture(t)
	ctx := context.Background()

	_, err := fx.q.SearchSymbol(ctx, "MyProc", "sql", false, 50)
	if err == nil {
		t.Fatal("SearchSymbol with unknown type 'sql' must fail")
	}
	if !errors.Is(err, errs.ErrUnknownSymbolType) {
		t.Fatalf("error must wrap errs.ErrUnknownSymbolType, got %v", err)
	}
	for _, valid := range []string{"procedure", "smf_instrument", "sql_procedure", "api_contract"} {
		if !strings.Contains(err.Error(), valid) {
			t.Fatalf("error message must mention %q: %v", valid, err)
		}
	}

	if _, err := fx.q.SearchSymbol(ctx, "MyProc", "", false, 50); err != nil {
		t.Fatalf("empty type must not filter and not fail: %v", err)
	}
}

func TestSearchSymbol_CaseInsensitiveAlias(t *testing.T) {
	fx := seedSymbolTypesFixture(t)
	ctx := context.Background()

	results, err := fx.q.SearchSymbol(ctx, "MyProc", "  SQL_Procedure ", false, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(SQL_Procedure): %v", err)
	}
	if len(results) != 1 || results[0].Name != "MyProc" {
		t.Fatalf("case/whitespace alias must resolve, got %v", symbolNames(results))
	}
}
