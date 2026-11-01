//go:build integration

package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// reconcileFixture — минимальные строки files/сущностей/relations для тестов
// этапа согласования графа (change relations-incremental-consistency).
type reconcileFixture struct {
	db      *store.DB
	scanRun int64
}

func openReconcileFixture(t *testing.T) *reconcileFixture {
	t.Helper()
	db := testutil.Open(t)
	var scanRun int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('/reconcile-test', 'completed') RETURNING id`).Scan(&scanRun); err != nil {
		t.Fatalf("insert scan_run: %v", err)
	}
	return &reconcileFixture{db: db, scanRun: scanRun}
}

func (f *reconcileFixture) fileID(t *testing.T, path string) int64 {
	t.Helper()
	var id int64
	err := f.db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at)
		VALUES ($1, $2, $3, 'sql', 1, $4, NOW())
		RETURNING id
	`, f.scanRun, path, path, fmt.Sprintf("hash-%s-%d", path, 0)).Scan(&id)
	if err != nil {
		t.Fatalf("insert file %s: %v", path, err)
	}
	return id
}

func (f *reconcileFixture) procedureID(t *testing.T, fileID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := f.db.QueryRow(`INSERT INTO sql_procedures (file_id, proc_name) VALUES ($1, $2) RETURNING id`, fileID, name).Scan(&id); err != nil {
		t.Fatalf("insert procedure %s: %v", name, err)
	}
	return id
}

func (f *reconcileFixture) tableID(t *testing.T, fileID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := f.db.QueryRow(`INSERT INTO sql_tables (file_id, table_name, context) VALUES ($1, $2, 'create_table') RETURNING id`, fileID, name).Scan(&id); err != nil {
		t.Fatalf("insert table %s: %v", name, err)
	}
	return id
}

func (f *reconcileFixture) contractID(t *testing.T, fileID int64, name, kind string) int64 {
	t.Helper()
	var id int64
	if err := f.db.QueryRow(`INSERT INTO api_contracts (file_id, contract_name, contract_kind) VALUES ($1, $2, $3) RETURNING id`, fileID, name, kind).Scan(&id); err != nil {
		t.Fatalf("insert contract %s/%s: %v", name, kind, err)
	}
	return id
}

func (f *reconcileFixture) reportFormID(t *testing.T, fileID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := f.db.QueryRow(`INSERT INTO report_forms (file_id, report_name, report_type) VALUES ($1, $2, 'tpr') RETURNING id`, fileID, name).Scan(&id); err != nil {
		t.Fatalf("insert report form %s: %v", name, err)
	}
	return id
}

func (f *reconcileFixture) reportFieldID(t *testing.T, formID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := f.db.QueryRow(`INSERT INTO report_fields (report_form_id, field_name) VALUES ($1, $2) RETURNING id`, formID, name).Scan(&id); err != nil {
		t.Fatalf("insert report field %s: %v", name, err)
	}
	return id
}

func (f *reconcileFixture) vbFunctionID(t *testing.T, formID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := f.db.QueryRow(`INSERT INTO vb_functions (report_form_id, function_name, function_type) VALUES ($1, $2, 'function') RETURNING id`, formID, name).Scan(&id); err != nil {
		t.Fatalf("insert vb function %s: %v", name, err)
	}
	return id
}

func (f *reconcileFixture) fragmentID(t *testing.T, fileID int64, name string) int64 {
	t.Helper()
	var id int64
	err := f.db.QueryRow(`
		INSERT INTO query_fragments (file_id, parent_type, parent_id, component_name, component_type, query_text, context, line_number)
		VALUES ($1, 'sql_procedure', 0, $2, 'select', 'select 1', 'body', 1)
		RETURNING id
	`, fileID, name).Scan(&id)
	if err != nil {
		t.Fatalf("insert fragment %s: %v", name, err)
	}
	return id
}

func (f *reconcileFixture) relation(t *testing.T, sourceType string, sourceID int64, targetType string, targetID int64, relationType string, line int) {
	t.Helper()
	if _, err := f.db.Exec(`
		INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type, confidence, line_number)
		VALUES ($1, $2, $3, $4, $5, 'regex', $6)
	`, sourceType, sourceID, targetType, targetID, relationType, line); err != nil {
		t.Fatalf("insert relation: %v", err)
	}
}

func (f *reconcileFixture) relationsExist(t *testing.T, where string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := f.db.QueryRow("SELECT count(*) FROM relations WHERE "+where, args...).Scan(&n); err != nil {
		t.Fatalf("count relations (%s): %v", where, err)
	}
	return n
}

// TestGetOldFileGenerationIDs (задача 2.1): изменённый путь отдаёт все
// поколения, кроме keep-id; удалённый путь — все поколения.
func TestGetOldFileGenerationIDs(t *testing.T) {
	f := openReconcileFixture(t)
	ctx := context.Background()

	gen1 := f.fileID(t, "/src/a.sql")
	gen2 := f.fileID(t, "/src/a.sql")
	removed := f.fileID(t, "/src/gone.sql")

	ids, err := f.db.GetOldFileGenerationIDs(ctx, []string{"/src/a.sql"}, map[string]int64{"/src/a.sql": gen2}, []string{"/src/gone.sql"})
	if err != nil {
		t.Fatalf("GetOldFileGenerationIDs: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("old generations = %v, want [%d %d]", ids, gen1, removed)
	}
	gotGen1, gotRemoved := false, false
	for _, id := range ids {
		if id == gen1 {
			gotGen1 = true
		}
		if id == removed {
			gotRemoved = true
		}
		if id == gen2 {
			t.Fatalf("keep id %d must not be returned as old generation", gen2)
		}
	}
	if !gotGen1 || !gotRemoved {
		t.Fatalf("old generations = %v, want gen1=%d and removed=%d", ids, gen1, removed)
	}

	// Пустой набор путей — без запросов, nil.
	if ids, err := f.db.GetOldFileGenerationIDs(ctx, nil, nil, nil); err != nil || ids != nil {
		t.Fatalf("empty input: ids=%v err=%v, want nil/nil", ids, err)
	}
}

// TestPurgeRelationSourcesByFileIDs (задача 2.2): после purge остаются только
// рёбра источников нового поколения; счётчики по source_type.
func TestPurgeRelationSourcesByFileIDs(t *testing.T) {
	f := openReconcileFixture(t)
	ctx := context.Background()

	oldFile := f.fileID(t, "/src/a.sql")
	newFile := f.fileID(t, "/src/a.sql")
	oldProc := f.procedureID(t, oldFile, "Caller")
	newProc := f.procedureID(t, newFile, "Caller")
	oldFragment := f.fragmentID(t, oldFile, "CursorSelect")
	newFragment := f.fragmentID(t, newFile, "CursorSelect")

	callee := f.procedureID(t, f.fileID(t, "/src/b.sql"), "Callee")
	f.relation(t, "sql_procedure", oldProc, "sql_procedure", callee, "calls_procedure", 3)
	f.relation(t, "sql_procedure", newProc, "sql_procedure", callee, "calls_procedure", 4)
	f.relation(t, "query_fragment", oldFragment, "sql_table", 1, "references_table", 5)
	f.relation(t, "query_fragment", newFragment, "sql_table", 1, "references_table", 6)

	counts, err := f.db.PurgeRelationSourcesByFileIDs(ctx, []int64{oldFile})
	if err != nil {
		t.Fatalf("PurgeRelationSourcesByFileIDs: %v", err)
	}
	if counts["sql_procedure"] != 1 || counts["query_fragment"] != 1 {
		t.Fatalf("purge counts = %#v, want sql_procedure=1 query_fragment=1", counts)
	}
	if got := f.relationsExist(t, "source_id = $1", oldProc); got != 0 {
		t.Fatalf("old-source edges = %d, want 0", got)
	}
	if got := f.relationsExist(t, "source_id = $1", oldFragment); got != 0 {
		t.Fatalf("old-fragment edges = %d, want 0", got)
	}
	if got := f.relationsExist(t, "source_id IN ($1, $2)", newProc, newFragment); got != 2 {
		t.Fatalf("new-generation edges = %d, want 2", got)
	}
}

// TestPurgeRelationSources_Indirect (задача 2.3): report_field/report_param/
// vb_function достигают files через report_forms — purge покрывает и их.
func TestPurgeRelationSources_Indirect(t *testing.T) {
	f := openReconcileFixture(t)
	ctx := context.Background()

	oldFile := f.fileID(t, "/src/report.tpr")
	newFile := f.fileID(t, "/src/report.tpr")
	oldForm := f.reportFormID(t, oldFile, "Rpt")
	newForm := f.reportFormID(t, newFile, "Rpt")
	oldField := f.reportFieldID(t, oldForm, "Amount")
	newField := f.reportFieldID(t, newForm, "Amount")
	oldVB := f.vbFunctionID(t, oldForm, "Calc")
	newVB := f.vbFunctionID(t, newForm, "Calc")

	f.relation(t, "report_form", oldForm, "report_field", oldField, "has_field", 10)
	f.relation(t, "report_form", newForm, "report_field", newField, "has_field", 11)
	f.relation(t, "vb_function", oldVB, "query_fragment", 1, "executes_query", 12)
	f.relation(t, "vb_function", newVB, "query_fragment", 2, "executes_query", 13)

	counts, err := f.db.PurgeRelationSourcesByFileIDs(ctx, []int64{oldFile})
	if err != nil {
		t.Fatalf("PurgeRelationSourcesByFileIDs: %v", err)
	}
	if counts["report_form"] != 1 || counts["vb_function"] != 1 {
		t.Fatalf("purge counts = %#v, want report_form=1 vb_function=1", counts)
	}
	if got := f.relationsExist(t, "source_id IN ($1, $2)", oldForm, oldVB); got != 0 {
		t.Fatalf("old indirect-source edges = %d, want 0", got)
	}
	if got := f.relationsExist(t, "source_id IN ($1, $2)", newForm, newVB); got != 2 {
		t.Fatalf("new indirect-source edges = %d, want 2", got)
	}
}

// TestRemapRelationTargets_SQLProcedure (задача 2.4): изменение тела
// процедуры и дубли имён — входящие рёбра указывают на новую max-id
// сущность, дублей кортежей нет.
func TestRemapRelationTargets_SQLProcedure(t *testing.T) {
	f := openReconcileFixture(t)
	ctx := context.Background()

	oldFile := f.fileID(t, "/src/callee.sql")
	newFile := f.fileID(t, "/src/callee.sql")
	oldCallee := f.procedureID(t, oldFile, "Callee")
	// Дубль имени в новом поколении: max id — третья строка.
	f.procedureID(t, newFile, "Callee")
	latestCallee := f.procedureID(t, newFile, "Callee")

	callerFile := f.fileID(t, "/src/caller.sql")
	caller := f.procedureID(t, callerFile, "Caller")
	f.relation(t, "sql_procedure", caller, "sql_procedure", oldCallee, "calls_procedure", 7)

	counts, err := f.db.RemapRelationTargetsByFileIDs(ctx, []int64{oldFile})
	if err != nil {
		t.Fatalf("RemapRelationTargetsByFileIDs: %v", err)
	}
	if counts["sql_procedure"] != 1 {
		t.Fatalf("remap counts = %#v, want sql_procedure=1", counts)
	}
	if got := f.relationsExist(t, "source_id=$1 AND target_id=$2 AND line_number=7", caller, latestCallee); got != 1 {
		t.Fatalf("edge to latest callee = %d, want 1", got)
	}
	if got := f.relationsExist(t, "target_id = $1", oldCallee); got != 0 {
		t.Fatalf("edges to old callee = %d, want 0", got)
	}
	if got := f.relationsExist(t, "source_id=$1 AND relation_type='calls_procedure'", caller); got != 1 {
		t.Fatalf("caller edges = %d, want 1 (no duplicates)", got)
	}
}

// TestRemapRelationTargets_SQLTable (задача 2.5): все пять ссылочных
// семейств живых источников переносятся на новую сущность таблицы.
func TestRemapRelationTargets_SQLTable(t *testing.T) {
	f := openReconcileFixture(t)
	ctx := context.Background()

	oldFile := f.fileID(t, "/src/tables.sql")
	newFile := f.fileID(t, "/src/tables.sql")
	oldTable := f.tableID(t, oldFile, "tAccount")
	newTable := f.tableID(t, newFile, "tAccount")

	liveFile := f.fileID(t, "/src/live.sql")
	liveProc := f.procedureID(t, liveFile, "LiveProc")
	liveFragment := f.fragmentID(t, liveFile, "Frag")

	for _, family := range []string{"references_table", "selects_from", "inserts_into", "updates", "deletes_from"} {
		f.relation(t, "sql_procedure", liveProc, "sql_table", oldTable, family, 20)
	}
	f.relation(t, "query_fragment", liveFragment, "sql_table", oldTable, "references_table", 21)

	counts, err := f.db.RemapRelationTargetsByFileIDs(ctx, []int64{oldFile})
	if err != nil {
		t.Fatalf("RemapRelationTargetsByFileIDs: %v", err)
	}
	if counts["sql_table"] != 6 {
		t.Fatalf("remap counts = %#v, want sql_table=6", counts)
	}
	if got := f.relationsExist(t, "target_id = $1", newTable); got != 6 {
		t.Fatalf("edges to new table = %d, want 6", got)
	}
	if got := f.relationsExist(t, "target_id = $1", oldTable); got != 0 {
		t.Fatalf("edges to old table = %d, want 0", got)
	}
}

// TestRemapRelationTargets_APIContract (задача 2.6): identity — имя + kind:
// контракт того же имени, но другого kind, кандидатом не является.
func TestRemapRelationTargets_APIContract(t *testing.T) {
	f := openReconcileFixture(t)
	ctx := context.Background()

	oldFile := f.fileID(t, "/src/contract.xml")
	newFile := f.fileID(t, "/src/contract.xml")
	oldContract := f.contractID(t, oldFile, "FCD_X_Get", "service")
	newContract := f.contractID(t, newFile, "FCD_X_Get", "service")
	// Тот же путь и имя, но другой kind — не должен участвовать в remap.
	f.contractID(t, newFile, "FCD_X_Get", "event")

	liveFile := f.fileID(t, "/src/impl.sql")
	liveProc := f.procedureID(t, liveFile, "Impl")
	f.relation(t, "sql_procedure", liveProc, "api_contract", oldContract, "implements_contract", 30)

	counts, err := f.db.RemapRelationTargetsByFileIDs(ctx, []int64{oldFile})
	if err != nil {
		t.Fatalf("RemapRelationTargetsByFileIDs: %v", err)
	}
	if counts["api_contract"] != 1 {
		t.Fatalf("remap counts = %#v, want api_contract=1", counts)
	}
	if got := f.relationsExist(t, "target_id=$1 AND relation_type='implements_contract'", newContract); got != 1 {
		t.Fatalf("edge to new contract = %d, want 1", got)
	}
}

// TestDeleteUnresolvedRelationTargets (задача 2.7): удалённый файл и
// переименованный путь — remap-карты нет, рёбра на старые id удаляются,
// пока старые строки ещё живы (критерий — old file_id, не NOT EXISTS).
func TestDeleteUnresolvedRelationTargets(t *testing.T) {
	f := openReconcileFixture(t)
	ctx := context.Background()

	// Удалённый файл: новой генерации нет вообще.
	removedFile := f.fileID(t, "/src/removed.sql")
	removedProc := f.procedureID(t, removedFile, "Gone")
	// Переименованный файл: новая генерация по другому пути — identity
	// матчится по files.path, совпадения нет.
	renamedOld := f.fileID(t, "/src/old-name.sql")
	renamedProc := f.procedureID(t, renamedOld, "Moved")
	renamedNewFile := f.fileID(t, "/src/new-name.sql")
	movedProc := f.procedureID(t, renamedNewFile, "Moved")

	liveFile := f.fileID(t, "/src/live.sql")
	liveProc := f.procedureID(t, liveFile, "Live")
	f.relation(t, "sql_procedure", liveProc, "sql_procedure", removedProc, "calls_procedure", 40)
	f.relation(t, "sql_procedure", liveProc, "sql_procedure", renamedProc, "calls_procedure", 41)
	f.relation(t, "sql_procedure", liveProc, "sql_procedure", movedProc, "calls_procedure", 42)

	oldIDs := []int64{removedFile, renamedOld}
	if _, err := f.db.RemapRelationTargetsByFileIDs(ctx, oldIDs); err != nil {
		t.Fatalf("remap: %v", err)
	}
	counts, err := f.db.DeleteUnresolvedRelationTargets(ctx, oldIDs)
	if err != nil {
		t.Fatalf("DeleteUnresolvedRelationTargets: %v", err)
	}
	if counts["sql_procedure"] != 2 {
		t.Fatalf("deleted_unresolved counts = %#v, want sql_procedure=2", counts)
	}
	if got := f.relationsExist(t, "target_id IN ($1, $2)", removedProc, renamedProc); got != 0 {
		t.Fatalf("edges to unresolved targets = %d, want 0", got)
	}
	if got := f.relationsExist(t, "target_id = $1", movedProc); got != 1 {
		t.Fatalf("edges to renamed-new target = %d, want 1", got)
	}
}

// TestReconcileIdempotency (задача 2.8): повторный вызов purge/remap/delete на
// тех же oldIDs не меняет набор рёбер, счётчики второго вызова нулевые.
func TestReconcileIdempotency(t *testing.T) {
	f := openReconcileFixture(t)
	ctx := context.Background()

	oldFile := f.fileID(t, "/src/a.sql")
	newFile := f.fileID(t, "/src/a.sql")
	oldProc := f.procedureID(t, oldFile, "Caller")
	newProc := f.procedureID(t, newFile, "Caller")
	calleeOld := f.procedureID(t, oldFile, "Callee")
	calleeNew := f.procedureID(t, newFile, "Callee")
	f.relation(t, "sql_procedure", oldProc, "sql_procedure", calleeOld, "calls_procedure", 50)
	f.relation(t, "sql_procedure", newProc, "sql_procedure", calleeNew, "calls_procedure", 51)
	live := f.procedureID(t, f.fileID(t, "/src/live.sql"), "Live")
	f.relation(t, "sql_procedure", live, "sql_procedure", calleeOld, "calls_procedure", 52)

	run := func() (purged, remapped, unresolved int) {
		p, err := f.db.PurgeRelationSourcesByFileIDs(ctx, []int64{oldFile})
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		m, err := f.db.RemapRelationTargetsByFileIDs(ctx, []int64{oldFile})
		if err != nil {
			t.Fatalf("remap: %v", err)
		}
		u, err := f.db.DeleteUnresolvedRelationTargets(ctx, []int64{oldFile})
		if err != nil {
			t.Fatalf("unresolved: %v", err)
		}
		return int(p["sql_procedure"]), int(m["sql_procedure"]), int(u["sql_procedure"])
	}

	p1, m1, u1 := run()
	if p1 != 1 || m1 != 1 || u1 != 0 {
		t.Fatalf("first run = purge %d remap %d unresolved %d, want 1/1/0", p1, m1, u1)
	}
	var afterFirst int
	if err := f.db.QueryRow(`SELECT count(*) FROM relations`).Scan(&afterFirst); err != nil {
		t.Fatalf("count: %v", err)
	}

	p2, m2, u2 := run()
	if p2 != 0 || m2 != 0 || u2 != 0 {
		t.Fatalf("second run = purge %d remap %d unresolved %d, want 0/0/0", p2, m2, u2)
	}
	var afterSecond int
	if err := f.db.QueryRow(`SELECT count(*) FROM relations`).Scan(&afterSecond); err != nil {
		t.Fatalf("count: %v", err)
	}
	if afterFirst != afterSecond {
		t.Fatalf("relations changed on rerun: %d -> %d", afterFirst, afterSecond)
	}
	// Инвариант: живой вызыватель перенесён на новую цель Callee.
	if got := f.relationsExist(t, "source_id=$1 AND target_id=$2 AND line_number=52", live, calleeNew); got != 1 {
		t.Fatalf("live caller edge after rerun = %d, want 1", got)
	}
}
