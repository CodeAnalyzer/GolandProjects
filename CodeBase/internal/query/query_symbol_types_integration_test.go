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
	"github.com/lib/pq"
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

// seedReportFormFixture наполняет БД тремя отчётными формами: RPT_ALPHA и
// RPT_BETA используют одну DFM-форму (FormAlpha/TFormAlpha), RPT_GAMMA — свою
// (FormGamma/TFormGamma). Символы report_form ссылаются entity_id на записи
// report_forms, как это делает индексатор (TPR и RPT пути).
func seedReportFormFixture(t *testing.T) *Query {
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
		VALUES ($1, '/repo/reports.rpt', '/repo/reports.rpt', 'rpt', 1, 'hash', NOW(), 'UTF8', 'REPORT')
		RETURNING id
	`, scanID).Scan(&fileID)
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}

	reports := []struct {
		reportName string
		formName   string
		formClass  string
		lineStart  int
	}{
		{"RPT_ALPHA", "FormAlpha", "TFormAlpha", 1},
		{"RPT_BETA", "FormAlpha", "TFormAlpha", 100},
		{"RPT_GAMMA", "FormGamma", "TFormGamma", 200},
	}
	for _, r := range reports {
		var reportFormID int64
		err = db.QueryRow(`
			INSERT INTO report_forms (file_id, report_name, report_type, form_name, form_class, line_start, line_end)
			VALUES ($1, $2, 'RPT', $3, $4, $5, $5)
			RETURNING id
		`, fileID, r.reportName, r.formName, r.formClass, r.lineStart).Scan(&reportFormID)
		if err != nil {
			t.Fatalf("insert report_form %s: %v", r.reportName, err)
		}
		_, err = db.Exec(`
			INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number)
			VALUES ($1, $2, 'report_form', 'report', $3, $4)
		`, fileID, r.reportName, reportFormID, r.lineStart)
		if err != nil {
			t.Fatalf("insert symbol %s: %v", r.reportName, err)
		}
	}
	return New(db)
}

// Поиск без JOIN (условие не ссылается на rf.*) и поиск с рассинхроном
// «условие есть — JOIN нет» дают разные наблюдаемые исходы: первый — пустой
// результат, второй — SQL-ошибку unknown column. Поэтому поведенческих
// проверок ниже достаточно для контроля связности condition↔JOIN.
func TestSearchSymbol_ReportFormFallbackByFormName(t *testing.T) {
	q := seedReportFormFixture(t)
	ctx := context.Background()

	results, err := q.SearchSymbol(ctx, "FormAlpha", "report_form", false, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(FormAlpha, report_form): %v", err)
	}
	got := symbolNames(results)
	if len(got) != 2 || !containsAll(got, "RPT_ALPHA", "RPT_BETA") {
		t.Fatalf("form_name fallback must find RPT_ALPHA and RPT_BETA, got %v", got)
	}
}

func TestSearchSymbol_ReportFormFallbackByFormClass(t *testing.T) {
	q := seedReportFormFixture(t)
	ctx := context.Background()

	results, err := q.SearchSymbol(ctx, "TFormAlpha", "report_form", false, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(TFormAlpha, report_form): %v", err)
	}
	got := symbolNames(results)
	if len(got) != 2 || !containsAll(got, "RPT_ALPHA", "RPT_BETA") {
		t.Fatalf("form_class fallback must find RPT_ALPHA and RPT_BETA, got %v", got)
	}

	// exact-режим сравнивает без учёта регистра (LOWER).
	results, err = q.SearchSymbol(ctx, "tformalpha", "report_form", false, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(tformalpha, report_form): %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("exact lookup must be case-insensitive, got %v", symbolNames(results))
	}
}

func TestSearchSymbol_ReportFormByReportNameStillWorks(t *testing.T) {
	q := seedReportFormFixture(t)
	ctx := context.Background()

	results, err := q.SearchSymbol(ctx, "RPT_ALPHA", "report_form", false, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(RPT_ALPHA, report_form): %v", err)
	}
	if len(results) != 1 || results[0].Name != "RPT_ALPHA" {
		t.Fatalf("report_name lookup must find single RPT_ALPHA, got %v", symbolNames(results))
	}
}

func TestSearchSymbol_ReportFormFallbackNotAppliedWithoutType(t *testing.T) {
	q := seedReportFormFixture(t)
	ctx := context.Background()

	results, err := q.SearchSymbol(ctx, "FormAlpha", "", false, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(FormAlpha, no type): %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("fallback must not apply without explicit type, got %v", symbolNames(results))
	}
}

func TestSearchSymbol_ReportFormLikeMatchesAllThreeFields(t *testing.T) {
	q := seedReportFormFixture(t)
	ctx := context.Background()

	results, err := q.SearchSymbol(ctx, "Form", "report_form", true, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(Form, report_form, like): %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("like over form_name/form_class must find all three reports, got %v", symbolNames(results))
	}
}

// legacyReportFormLookup — прежний OR-запрос трёхполевого поиска report_form
// (до UNION ALL-ветвей, change optimize-query-search-bottlenecks B3): эталон
// для проверки эквивалентности нового плана.
func legacyReportFormLookup(t *testing.T, q *Query, name string, like bool, limit int) []int64 {
	t.Helper()
	lookup := strings.Join(reportFormFieldConditions(like, 1), " OR ")
	query := `
		WITH base AS (
			SELECT s.id, s.file_id, s.entity_id, s.symbol_name, s.symbol_type, s.entity_type,
			       LOWER(s.symbol_name) AS name_key,` + symbolOwnerKeyExpr + ` AS owner_key,
			       COALESCE(f.ds_product_id, 0) AS product_key, f.ds_product_branch_id AS branch_id,
			       f.rel_path, s.line_number, s.signature,
			       COALESCE(pr.product_name, '') AS product, COALESCE(br.branch_key, '') AS branch_used,
			       (NOT f.is_generated) AS not_generated, COALESCE(br.is_default, FALSE) AS is_default
			FROM symbols s
			LEFT JOIN report_forms rf ON s.symbol_type = 'report_form' AND s.entity_id = rf.id
			JOIN files f ON s.file_id = f.id
			LEFT JOIN ds_products pr ON pr.id = f.ds_product_id
			LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
			WHERE (` + lookup + `)
			  AND s.symbol_type = ANY($2)
		), chosen AS (
			SELECT DISTINCT ON (b.name_key, b.symbol_type, b.owner_key, b.product_key)
			       b.name_key, b.symbol_type, b.owner_key, b.product_key, b.branch_id
			FROM base b
			ORDER BY b.name_key, b.symbol_type, b.owner_key, b.product_key,
			         b.not_generated DESC, b.is_default DESC, b.id DESC
		)
		SELECT b.id
		FROM base b
		JOIN chosen ch ON ch.name_key = b.name_key AND ch.symbol_type = b.symbol_type
		 AND ch.owner_key = b.owner_key AND ch.product_key = b.product_key
		 AND ch.branch_id IS NOT DISTINCT FROM b.branch_id
		ORDER BY b.symbol_name, b.id
		LIMIT $3`
	lookupValue := name
	if like {
		lookupValue = "%" + name + "%"
	}
	rows, err := q.db.QueryContext(context.Background(), query, lookupValue, pq.Array([]string{"report_form"}), limit)
	if err != nil {
		t.Fatalf("legacy query %q: %v", name, err)
	}
	defer rows.Close()
	ids := make([]int64, 0, 8)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("legacy scan: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

// TestSearchSymbol_ReportFormUnionAllEquivalence — UNION ALL-ветви дают то же
// упорядоченное множество id, что и прежний OR-запрос, по всем режимам
// (точный по трём полям, регистронезависимость, like).
func TestSearchSymbol_ReportFormUnionAllEquivalence(t *testing.T) {
	q := seedReportFormFixture(t)
	ctx := context.Background()

	cases := []struct {
		name string
		like bool
	}{
		{"RPT_ALPHA", false},
		{"FormAlpha", false},
		{"TFormAlpha", false},
		{"tformalpha", false},
		{"Form", true},
		{"RPT_", true},
		{"NonExistent", false},
	}
	for _, tc := range cases {
		results, err := q.SearchSymbol(ctx, tc.name, "report_form", tc.like, 50)
		if err != nil {
			t.Fatalf("SearchSymbol(%q, like=%v): %v", tc.name, tc.like, err)
		}
		got := make([]int64, 0, len(results))
		for _, r := range results {
			got = append(got, r.ID)
		}
		want := legacyReportFormLookup(t, q, tc.name, tc.like, 50)
		if len(got) != len(want) {
			t.Fatalf("query %q (like=%v): got ids %v, legacy %v", tc.name, tc.like, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("query %q (like=%v): order mismatch at %d: got %v, legacy %v", tc.name, tc.like, i, got, want)
			}
		}
	}
}

// TestSearchSymbol_ReportFormMultiFieldMatchDedup — строка, совпавшая по
// нескольким полям (symbol_name и form_name одновременно), попадает в несколько
// ветвей UNION ALL и возвращается ровно один раз (DISTINCT по id поверх
// веточного дедупа chosen).
func TestSearchSymbol_ReportFormMultiFieldMatchDedup(t *testing.T) {
	q := seedReportFormFixture(t)
	ctx := context.Background()

	db := q.db
	scanID, err := db.CreateScanRun(ctx, "/repo2")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	var fileID int64
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, '/repo2/dual.rpt', '/repo2/dual.rpt', 'rpt', 1, 'hash', NOW(), 'UTF8', 'REPORT')
		RETURNING id
	`, scanID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	// symbol_name = form_name = RPT_DUAL: совпадает и ветвью symbol_name,
	// и ветвью form_name.
	var reportFormID int64
	if err := db.QueryRow(`
		INSERT INTO report_forms (file_id, report_name, report_type, form_name, form_class, line_start, line_end)
		VALUES ($1, 'RPT_DUAL', 'RPT', 'RPT_DUAL', 'TFormDual', 1, 1)
		RETURNING id
	`, fileID).Scan(&reportFormID); err != nil {
		t.Fatalf("insert report_form: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number)
		VALUES ($1, 'RPT_DUAL', 'report_form', 'report', $2, 1)
	`, fileID, reportFormID); err != nil {
		t.Fatalf("insert symbol: %v", err)
	}

	results, err := q.SearchSymbol(ctx, "RPT_DUAL", "report_form", false, 50)
	if err != nil {
		t.Fatalf("SearchSymbol(RPT_DUAL): %v", err)
	}
	if len(results) != 1 || results[0].Name != "RPT_DUAL" {
		t.Fatalf("dual-field match must dedup to single row, got %v", symbolNames(results))
	}
}

func containsAll(items []string, want ...string) bool {
	set := make(map[string]bool, len(items))
	for _, item := range items {
		set[item] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}
