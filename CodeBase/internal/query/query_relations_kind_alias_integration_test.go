//go:build integration

package query

import (
	"context"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func insertRelationsAPIContract(t *testing.T, db *store.DB, fileID int64, name string, kind string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO api_contracts (file_id, contract_name, contract_kind, line_start, line_end)
		VALUES ($1, $2, $3, 1, 2)
		RETURNING id
	`, fileID, name, kind).Scan(&id)
	if err != nil {
		t.Fatalf("insert api contract %s: %v", name, err)
	}
	return id
}

func insertRelationsDFMForm(t *testing.T, db *store.DB, fileID int64, name string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO dfm_forms (file_id, form_name, form_class, line_start, line_end)
		VALUES ($1, $2, $2, 1, 2)
		RETURNING id
	`, fileID, name).Scan(&id)
	if err != nil {
		t.Fatalf("insert dfm form %s: %v", name, err)
	}
	return id
}

func insertRelationsSpecCapability(t *testing.T, db *store.DB, fileID int64, name string) int64 {
	t.Helper()
	var configID int64
	err := db.QueryRow(`
		INSERT INTO spec_configs (file_id, root_dir, product_name)
		VALUES ($1, '/repo/openspec', 'TestProduct')
		RETURNING id
	`, fileID).Scan(&configID)
	if err != nil {
		t.Fatalf("insert spec config: %v", err)
	}
	var id int64
	err = db.QueryRow(`
		INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name)
		VALUES ($1, $2, $3)
		RETURNING id
	`, fileID, configID, name).Scan(&id)
	if err != nil {
		t.Fatalf("insert spec capability %s: %v", name, err)
	}
	return id
}

// TestSearchRelations_KindAliasEqualsAPIContract проверяет, что kind-типы
// API-контрактов (service/event) нормализуются в api_contract и дают тот же
// результат, что и явный api_contract.
func TestSearchRelations_KindAliasEqualsAPIContract(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	fileID := insertRelationsFile(t, db, scanID, "/repo/api.sql")
	proc := insertRelationsProc(t, db, fileID, "MyProc")
	contract := insertRelationsAPIContract(t, db, fileID, "API_Svc_X", "service")
	edgeID := insertRelationsEdge(t, db, "sql_procedure", proc, "api_contract", contract, "implements_contract")

	q := New(db)

	byAlias, err := q.SearchRelations(ctx, "", "", "service", "API_Svc_X", "implements_contract", 10)
	if err != nil {
		t.Fatalf("SearchRelations(service): %v", err)
	}
	byCanonical, err := q.SearchRelations(ctx, "", "", "api_contract", "API_Svc_X", "implements_contract", 10)
	if err != nil {
		t.Fatalf("SearchRelations(api_contract): %v", err)
	}

	if len(byAlias) != 1 || byAlias[0].ID != edgeID {
		t.Fatalf("service alias ids = %v, want [%d]", relationIDSet(byAlias), edgeID)
	}
	if got, want := relationIDSet(byAlias), relationIDSet(byCanonical); len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("service alias result %v != api_contract result %v", got, want)
	}
	if byAlias[0].Target.Name != "API_Svc_X" {
		t.Fatalf("target name = %q, want API_Svc_X", byAlias[0].Target.Name)
	}
}

// TestSearchRelations_NameFilterNewEntityTypes проверяет, что name-резолв
// покрывает ранее не поддерживаемые типы словаря (dfm_form, spec_capability).
func TestSearchRelations_NameFilterNewEntityTypes(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	fileID := insertRelationsFile(t, db, scanID, "/repo/forms.pas")
	table := insertRelationsTable(t, db, fileID, "tGrid")
	form := insertRelationsDFMForm(t, db, fileID, "FormMain")
	formEdge := insertRelationsEdge(t, db, "dfm_form", form, "sql_table", table, "references_table")

	capA := insertRelationsSpecCapability(t, db, fileID, "cap-a")
	capB := insertRelationsSpecCapability(t, db, fileID, "cap-b")
	capEdge := insertRelationsEdge(t, db, "spec_capability", capA, "spec_capability", capB, "depends_on_capability")

	q := New(db)

	forms, err := q.SearchRelations(ctx, "dfm_form", "FormMain", "", "", "", 10)
	if err != nil {
		t.Fatalf("SearchRelations(dfm_form): %v", err)
	}
	if len(forms) != 1 || forms[0].ID != formEdge {
		t.Fatalf("dfm_form relations = %v, want [%d]", relationIDSet(forms), formEdge)
	}
	if forms[0].Source.Name != "FormMain" {
		t.Fatalf("dfm_form source name = %q, want FormMain", forms[0].Source.Name)
	}

	caps, err := q.SearchRelations(ctx, "spec_capability", "cap-a", "", "", "", 10)
	if err != nil {
		t.Fatalf("SearchRelations(spec_capability): %v", err)
	}
	if len(caps) != 1 || caps[0].ID != capEdge {
		t.Fatalf("spec_capability relations = %v, want [%d]", relationIDSet(caps), capEdge)
	}
	if caps[0].Source.Name != "cap-a" {
		t.Fatalf("spec_capability source name = %q, want cap-a", caps[0].Source.Name)
	}
}
