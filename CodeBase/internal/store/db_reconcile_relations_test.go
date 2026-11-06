package store

import (
	"sort"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
)

// TestReconcileEntityBindingsCoverRelationEntityTypes (задача 1.2): маппинг
// purge/delete должен покрывать ровно 17 типов model.RelationEntityTypes,
// без пропусков и лишних типов.
func TestReconcileEntityBindingsCoverRelationEntityTypes(t *testing.T) {
	want := append([]string(nil), model.RelationEntityTypes...)
	sort.Strings(want)

	got := make([]string, 0, len(reconcileEntityBindings))
	for _, binding := range reconcileEntityBindings {
		got = append(got, binding.entityType)
	}
	sort.Strings(got)

	if len(got) != len(want) {
		t.Fatalf("bindings = %d types, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("bindings[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Ровно 3 косвенных типа через report_forms.
	viaParent := 0
	for _, binding := range reconcileEntityBindings {
		if binding.viaReportForms {
			viaParent++
		}
	}
	if viaParent != 3 {
		t.Fatalf("viaReportForms bindings = %d, want 3 (report_field, report_param, vb_function)", viaParent)
	}

	// Remap покрывает ровно три межфайловых типа целей.
	if len(remapTargetBindings) != 3 {
		t.Fatalf("remapTargetBindings = %d, want 3", len(remapTargetBindings))
	}
	wantRemap := map[string]string{
		"sql_procedure": "proc_name",
		"sql_table":     "table_name",
		"api_contract":  "contract_name",
	}
	for _, binding := range remapTargetBindings {
		column, ok := wantRemap[binding.targetType]
		if !ok {
			t.Fatalf("unexpected remap target_type %q", binding.targetType)
		}
		if !strings.Contains(binding.identityMatch, column) {
			t.Fatalf("remap %s identityMatch %q must key on %s", binding.targetType, binding.identityMatch, column)
		}
	}
}

// TestEscapeLikeAll (review F2): rootPath — литеральный префикс, все
// спецсимволы LIKE (%/_/обратный слэш) обязаны экранироваться, иначе
// подчёркивание в корне совпадёт с любым символом (ложный sibling-scope).
func TestEscapeLikeAll(t *testing.T) {
	cases := []struct{ in, want string }{
		{"D:/GITHUB/GolandProjects", `D:/GITHUB/GolandProjects`},
		{"D:/my_project/root", `D:/my\_project/root`},
		{"D:/100%/root", `D:/100\%/root`},
		{`D:\repo`, `D:\\repo`},
		{"mix_%_\\path", `mix\_\%\_\\path`},
	}
	for _, tc := range cases {
		if got := escapeLikeAll(tc.in); got != tc.want {
			t.Fatalf("escapeLikeAll(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestRootLikePattern (change update-startup-visibility): паттерн детей
// корня — нормализация слэшей/хвостового «/», экранирование спецсимволов и
// суффикс-граница «/%». Единый хелпер GetLatestFilesByRootPath и
// FindDuplicateFileGenerations.
func TestRootLikePattern(t *testing.T) {
	cases := []struct{ in, want string }{
		{"D:/GITHUB/FA", `D:/GITHUB/FA/%`},
		{"D:/my_project/root", `D:/my\_project/root/%`},
		{"D:/100%/root", `D:/100\%/root/%`},
		{`D:\repo\`, `D:/repo/%`},                // хвостовой слэш, обратные слэши
		{"  D:/repo/  ", `D:/repo/%`},            // пробелы и хвостовой слэш
		{`D:\repo_x\100%`, `D:/repo\_x/100\%/%`}, // все спецсимволы сразу
	}
	for _, tc := range cases {
		if got := rootLikePattern(tc.in); got != tc.want {
			t.Errorf("rootLikePattern(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
