//go:build integration

package indexer

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// Фикстура testdata/reconcile: caller.sql (CallerA → CalleeB + select из
// tAccount), callee.sql (CalleeB), helper.sql (Helper), tables.sql (create
// table tAccount), impl.sql (API_CREATE_PROC(FCD_X_Get) →
// implements_contract), doomed.sql (DoomedCaller → CalleeB, файл для
// удаления), XML-контракт FCD_X_Get (service). Сценарии задач 4.1–4.13
// change relations-incremental-consistency.
//
// Модель графа: proc→sql_table рёбра same-file (цель — occurrence-строка в
// файле-источнике), межфайловые ссылки на таблицы идут от query_fragment
// (глобальный резолв постобработки), вызовы процедур и контракты —
// межфайловые по имени.

func newReconcileIndexer(t *testing.T) (*Indexer, string) {
	t.Helper()
	db := testutil.Open(t)
	root := t.TempDir()
	src := filepath.Join("testdata", "reconcile")
	if err := copyTree(src, root); err != nil {
		t.Fatalf("copy testdata: %v", err)
	}
	idx := &Indexer{
		db: db,
		config: &config.Config{Indexer: config.IndexerConfig{
			Parallel:        1,
			BatchSize:       100,
			IncludePatterns: []string{"*.sql", "*.xml"},
		}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}
	return idx, root
}

// assertNoDeadRelations — инвариант целостности (задача 4.10, Q6/Q7-эквивалент):
// ни одно ребро не ссылается на отсутствующий источник или цель.
func assertNoDeadRelations(t *testing.T, idx *Indexer) {
	t.Helper()
	type binding struct {
		entityType string
		table      string
		viaReport  bool
	}
	bindings := []binding{
		{"sql_procedure", "sql_procedures", false},
		{"sql_table", "sql_tables", false},
		{"pas_method", "pas_methods", false},
		{"js_function", "js_functions", false},
		{"query_fragment", "query_fragments", false},
		{"dfm_form", "dfm_forms", false},
		{"smf_instrument", "smf_instruments", false},
		{"report_form", "report_forms", false},
		{"api_contract", "api_contracts", false},
		{"spec_capability", "spec_capabilities", false},
		{"spec_requirement", "spec_requirements", false},
		{"spec_scenario", "spec_scenarios", false},
		{"spec_usecase", "spec_usecases", false},
		{"spec_change", "spec_changes", false},
		{"report_field", "report_fields", true},
		{"report_param", "report_params", true},
		{"vb_function", "vb_functions", true},
	}
	for _, b := range bindings {
		for _, side := range []string{"source", "target"} {
			query := fmt.Sprintf(`
				SELECT count(*)
				FROM relations r
				WHERE r.%s_type = $1
				  AND NOT EXISTS (SELECT 1 FROM %s e WHERE e.id = r.%s_id%s)
			`, side, b.table, side, reportFormsJoin(b.viaReport))
			var n int
			if err := idx.db.QueryRow(query, b.entityType).Scan(&n); err != nil {
				t.Fatalf("dead %s check (%s): %v", side, b.entityType, err)
			}
			if n != 0 {
				t.Fatalf("invariant violated: %d relations with dead %s %s", n, side, b.entityType)
			}
		}
	}
}

func reportFormsJoin(viaReport bool) string {
	if !viaReport {
		return ""
	}
	return " AND EXISTS (SELECT 1 FROM report_forms rf WHERE rf.id = e.report_form_id)"
}

func assertSingleGenerationPerPath(t *testing.T, idx *Indexer) {
	t.Helper()
	var n int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM (
			SELECT path FROM files GROUP BY path HAVING count(*) > 1
		) d
	`).Scan(&n); err != nil {
		t.Fatalf("duplicate generations check: %v", err)
	}
	if n != 0 {
		t.Fatalf("%d paths have multiple file generations", n)
	}
}

func latestProcID(t *testing.T, idx *Indexer, name string) int64 {
	t.Helper()
	ids, err := idx.db.FindLatestSQLProcedureIDsByNames(context.Background(), []string{name}, store.LookupScope{})
	if err != nil {
		t.Fatalf("lookup proc %s: %v", name, err)
	}
	if ids[strings.ToLower(name)] == 0 {
		t.Fatalf("proc %s not found", name)
	}
	return ids[strings.ToLower(name)]
}

func latestContractID(t *testing.T, idx *Indexer, name string) int64 {
	t.Helper()
	var id int64
	err := idx.db.QueryRow(`
		SELECT c.id
		FROM api_contracts c
		WHERE lower(c.contract_name) = lower($1) AND lower(c.contract_kind) = 'service'
		ORDER BY c.id DESC
		LIMIT 1
	`, name).Scan(&id)
	if err != nil {
		t.Fatalf("lookup contract %s: %v", name, err)
	}
	return id
}

func countRelations(t *testing.T, idx *Indexer, where string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := idx.db.QueryRow("SELECT count(*) FROM relations WHERE "+where, args...).Scan(&n); err != nil {
		t.Fatalf("count relations (%s): %v", where, err)
	}
	return n
}

// fragmentTableRefs — межфайловые references_table рёбра фрагментов на
// occurrence-строки таблицы с указанным именем: DISTINCT target_id + общее
// число рёбер.
func fragmentTableRefs(t *testing.T, idx *Indexer, tableName string) (targets []int64, edges int) {
	t.Helper()
	rows, err := idx.db.Query(`
		SELECT r.target_id, count(*)
		FROM relations r
		JOIN sql_tables tb ON tb.id = r.target_id
		WHERE r.relation_type = 'references_table'
		  AND r.source_type = 'query_fragment'
		  AND r.target_type = 'sql_table'
		  AND lower(tb.table_name) = lower($1)
		GROUP BY r.target_id
	`, tableName)
	if err != nil {
		t.Fatalf("fragment table refs query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var target int64
		var n int
		if err := rows.Scan(&target, &n); err != nil {
			t.Fatalf("fragment table refs scan: %v", err)
		}
		targets = append(targets, target)
		edges += n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("fragment table refs iterate: %v", err)
	}
	return targets, edges
}

// maxTableOccurrenceID — глобально последняя occurrence-строка таблицы
// (семантика FindLatestSQLTableIDsByNames в нулевом scope: max id).
func maxTableOccurrenceID(t *testing.T, idx *Indexer, tableName string) int64 {
	t.Helper()
	var id int64
	if err := idx.db.QueryRow(`SELECT max(id) FROM sql_tables WHERE lower(table_name) = lower($1)`, tableName).Scan(&id); err != nil {
		t.Fatalf("max table occurrence: %v", err)
	}
	if id == 0 {
		t.Fatalf("table %s not found", tableName)
	}
	return id
}

// callersOf — живые вызыватели процедуры (source сущность + файл существуют).
func callersOf(t *testing.T, idx *Indexer, targetProcID int64) map[string]string {
	t.Helper()
	rows, err := idx.db.Query(`
		SELECT sp.proc_name, f.path
		FROM relations r
		JOIN sql_procedures src ON src.id = r.source_id AND r.source_type = 'sql_procedure'
		JOIN sql_procedures sp ON sp.id = src.id
		JOIN files f ON f.id = src.file_id
		WHERE r.relation_type = 'calls_procedure' AND r.target_type = 'sql_procedure' AND r.target_id = $1
	`, targetProcID)
	if err != nil {
		t.Fatalf("callers query: %v", err)
	}
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var name, path string
		if err := rows.Scan(&name, &path); err != nil {
			t.Fatalf("callers scan: %v", err)
		}
		result[name] = path
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("callers iterate: %v", err)
	}
	return result
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func initReconcile(t *testing.T, idx *Indexer, root string) {
	t.Helper()
	stats, err := idx.Init(root, 1)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if stats.Errors != 0 {
		t.Fatalf("Init errors = %d", stats.Errors)
	}
}

// TestReconcile_CallerFileModified (задачи 4.1, 4.13): после переиндексации
// вызывателя у цели нет unknown/пустого файла, рёбра соответствуют новой
// версии; рёбра purged-источника не перепривязываются remap-ом.
func TestReconcile_CallerFileModified(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	calleeID := latestProcID(t, idx, "CalleeB")
	helperID := latestProcID(t, idx, "Helper")
	oldCaller := latestProcID(t, idx, "CallerA")

	writeFile(t, filepath.Join(root, "caller.sql"), "create procedure CallerA\nas\nbegin\n\texec CalleeB\n\texec Helper\n\tselect AccountID from tAccount\nend\n")
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}

	assertNoDeadRelations(t, idx)
	assertSingleGenerationPerPath(t, idx)

	if got := latestProcID(t, idx, "CalleeB"); got != calleeID {
		t.Fatalf("callee id changed without callee modification: %d -> %d", calleeID, got)
	}
	newCaller := latestProcID(t, idx, "CallerA")
	if newCaller == oldCaller {
		t.Fatal("CallerA must get a new generation id")
	}

	// 4.13: рёбра старого поколения CallerA удалены purge и не воскресли.
	if got := countRelations(t, idx, "source_type='sql_procedure' AND source_id = $1", oldCaller); got != 0 {
		t.Fatalf("old caller edges = %d, want 0", got)
	}
	if got := countRelations(t, idx, "source_type='sql_procedure' AND source_id=$1 AND target_type='sql_procedure' AND target_id=$2",
		oldCaller, calleeID); got != 0 {
		t.Fatalf("purged source must not be remapped: %d edges", got)
	}

	// 4.1: callers CalleeB — CallerA (новая версия, с файлом) и DoomedCaller,
	// без unknown и пустых файлов.
	callers := callersOf(t, idx, calleeID)
	if len(callers) != 2 {
		t.Fatalf("callers of CalleeB = %#v, want CallerA+DoomedCaller", callers)
	}
	for name, path := range callers {
		if path == "" || name == "unknown" {
			t.Fatalf("phantom caller leaked: name=%q path=%q", name, path)
		}
	}
	// Новая версия вызывателя: вызов Helper появился и резолвился.
	if got := countRelations(t, idx, "source_type='sql_procedure' AND source_id=$1 AND relation_type='calls_procedure' AND target_type='sql_procedure' AND target_id=$2",
		newCaller, helperID); got != 1 {
		t.Fatalf("new caller edge to Helper = %d, want 1", got)
	}
}

// TestReconcile_CalleeFileModified_CallerSurvives (задача 4.2): неизменённый
// вызыватель остаётся в callers цели — ребро переносится на новую сущность,
// line_number вызова не меняется.
func TestReconcile_CalleeFileModified_CallerSurvives(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	calleeOld := latestProcID(t, idx, "CalleeB")
	writeFile(t, filepath.Join(root, "callee.sql"), "create procedure CalleeB\nas\nbegin\n\tselect 42\nend\n")
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}

	assertNoDeadRelations(t, idx)
	assertSingleGenerationPerPath(t, idx)

	calleeNew := latestProcID(t, idx, "CalleeB")
	if calleeNew == calleeOld {
		t.Fatal("CalleeB must get a new generation id")
	}
	callers := callersOf(t, idx, calleeNew)
	if len(callers) != 2 {
		t.Fatalf("callers of new CalleeB = %#v, want CallerA+DoomedCaller (unmodified callers survive)", callers)
	}
	if _, ok := callers["CallerA"]; !ok {
		t.Fatalf("unmodified caller CallerA lost: %#v", callers)
	}
	if got := countRelations(t, idx, "source_type='sql_procedure' AND source_id=$1 AND target_type='sql_procedure' AND target_id=$2 AND line_number=4",
		latestProcID(t, idx, "CallerA"), calleeNew); got != 1 {
		t.Fatalf("remapped edge must keep caller line_number: %d", got)
	}
}

// TestReconcile_TableFileModified (задача 4.3): изменение файла с таблицами —
// межфайловые references_table рёбра живых фрагментов перенесены на новое
// поколение occurrence-строки; same-file рёбра процедур не тронуты.
func TestReconcile_TableFileModified(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	oldTargets, edgesBefore := fragmentTableRefs(t, idx, "tAccount")
	if edgesBefore == 0 {
		t.Fatal("precondition: no fragment references_table edges to tAccount")
	}
	callerID := latestProcID(t, idx, "CallerA")
	var sameFileTarget int64
	if err := idx.db.QueryRow(`
		SELECT r.target_id FROM relations r
		WHERE r.relation_type='selects_from' AND r.source_type='sql_procedure' AND r.source_id=$1
	`, callerID).Scan(&sameFileTarget); err != nil {
		t.Fatalf("precondition same-file selects_from: %v", err)
	}

	writeFile(t, filepath.Join(root, "tables.sql"), "create table tAccount (\n\tAccountID int,\n\tAccountNumber varchar(20),\n\tBranchID int\n)\n")
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}

	assertNoDeadRelations(t, idx)
	assertSingleGenerationPerPath(t, idx)

	newTargets, edgesAfter := fragmentTableRefs(t, idx, "tAccount")
	if edgesAfter != edgesBefore {
		t.Fatalf("fragment references_table edges = %d, want %d", edgesAfter, edgesBefore)
	}
	latest := maxTableOccurrenceID(t, idx, "tAccount")
	for _, target := range newTargets {
		if target != latest {
			t.Fatalf("fragment edge target %d, want remapped to latest %d", target, latest)
		}
	}
	for _, old := range oldTargets {
		if old == latest {
			continue
		}
		if got := countRelations(t, idx, "relation_type='references_table' AND target_type='sql_table' AND target_id=$1", old); got != 0 {
			t.Fatalf("edges to old occurrence %d = %d, want 0", old, got)
		}
	}
	// Same-file рёбра процедуры указывают на собственную occurrence-строку и
	// не перепривязываются на чужой файл.
	var sameFileTargetAfter int64
	if err := idx.db.QueryRow(`
		SELECT r.target_id FROM relations r
		WHERE r.relation_type='selects_from' AND r.source_type='sql_procedure' AND r.source_id=$1
	`, callerID).Scan(&sameFileTargetAfter); err != nil {
		t.Fatalf("same-file selects_from after update: %v", err)
	}
	if sameFileTargetAfter != sameFileTarget {
		t.Fatalf("same-file target changed: %d -> %d", sameFileTarget, sameFileTargetAfter)
	}
}

// TestReconcile_ContractFileModified (задача 4.4): implements_contract живого
// источника указывает на новое поколение контракта.
func TestReconcile_ContractFileModified(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	contractOld := latestContractID(t, idx, "FCD_X_Get")
	xmlPath := filepath.Join(root, "DSArchitectData", "BObject", "TestBO", "Service", "FCD_X_Get.xml")
	writeFile(t, xmlPath, "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Object>\n  <ObjectTypeID>1</ObjectTypeID>\n  <ObjectName>FCD_X_Get</ObjectName>\n  <Implemented>1</Implemented>\n  <ShortDescription>Changed description</ShortDescription>\n  <InputTables/>\n  <OutputTables/>\n  <ReturnValues/>\n</Object>\n")
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}

	assertNoDeadRelations(t, idx)
	assertSingleGenerationPerPath(t, idx)

	contractNew := latestContractID(t, idx, "FCD_X_Get")
	if contractNew == contractOld {
		t.Fatal("contract must get a new generation id")
	}
	if got := countRelations(t, idx, "relation_type='implements_contract' AND target_type='api_contract' AND target_id=$1", contractNew); got != 1 {
		t.Fatalf("implements_contract -> new contract = %d, want 1", got)
	}
	if got := countRelations(t, idx, "relation_type='implements_contract' AND target_type='api_contract' AND target_id=$1", contractOld); got != 0 {
		t.Fatalf("implements_contract -> old contract = %d, want 0", got)
	}
}

// TestReconcile_FileRemoved (задача 4.5): рёбра удалённого файла исчезают в
// обе стороны.
func TestReconcile_FileRemoved(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	doomed := latestProcID(t, idx, "DoomedCaller")
	callee := latestProcID(t, idx, "CalleeB")
	if got := countRelations(t, idx, "source_type='sql_procedure' AND source_id=$1 AND target_type='sql_procedure' AND target_id=$2 AND relation_type='calls_procedure'",
		doomed, callee); got != 1 {
		t.Fatalf("precondition: doomed edge = %d, want 1", got)
	}
	if err := os.Remove(filepath.Join(root, "doomed.sql")); err != nil {
		t.Fatalf("remove doomed.sql: %v", err)
	}
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}

	assertNoDeadRelations(t, idx)
	assertSingleGenerationPerPath(t, idx)

	if got := countRelations(t, idx, "source_type='sql_procedure' AND source_id = $1", doomed); got != 0 {
		t.Fatalf("outgoing edges of removed file = %d, want 0", got)
	}
	callers := callersOf(t, idx, latestProcID(t, idx, "CalleeB"))
	if _, ok := callers["DoomedCaller"]; ok {
		t.Fatalf("removed caller leaked into callers: %#v", callers)
	}
	if len(callers) != 1 {
		t.Fatalf("callers after removal = %#v, want only CallerA", callers)
	}
}

// TestReconcile_ProcedureRenamed (задача 4.6): рёбра на старое имя удалены,
// фантомов нет, новые вызовы резолвятся по общим правилам.
func TestReconcile_ProcedureRenamed(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	helperOld := latestProcID(t, idx, "Helper")
	writeFile(t, filepath.Join(root, "helper.sql"), "create procedure HelperRenamed\nas\nbegin\n\tselect 2\nend\n")
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}

	assertNoDeadRelations(t, idx)
	assertSingleGenerationPerPath(t, idx)

	if got := countRelations(t, idx, "target_type='sql_procedure' AND target_id = $1", helperOld); got != 0 {
		t.Fatalf("edges to renamed-away Helper = %d, want 0", got)
	}
	var helperCount int
	if err := idx.db.QueryRow(`SELECT count(*) FROM sql_procedures WHERE proc_name = 'Helper'`).Scan(&helperCount); err != nil || helperCount != 0 {
		t.Fatalf("old Helper rows = %d (err %v), want 0", helperCount, err)
	}
	// Новое имя резолвится по общим правилам (вызовов нет — рёбер нет).
	if got := countRelations(t, idx, "relation_type='calls_procedure' AND target_type='sql_procedure' AND target_id=$1",
		latestProcID(t, idx, "HelperRenamed")); got != 0 {
		t.Fatalf("edges to HelperRenamed = %d, want 0 (no callers)", got)
	}
}

// TestReconcile_FileRenamed (задача 4.7): переименованный файл не
// сопоставляется по имени между путями — несопоставленные цели удалены,
// мёртвых целей нет (осознанная потеря входящих рёбер, design D4).
func TestReconcile_FileRenamed(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	oldTargets, _ := fragmentTableRefs(t, idx, "tAccount")
	if len(oldTargets) == 0 {
		t.Fatal("precondition: no fragment refs to tAccount")
	}
	if err := os.Rename(filepath.Join(root, "tables.sql"), filepath.Join(root, "schema.sql")); err != nil {
		t.Fatalf("rename tables.sql: %v", err)
	}
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}

	assertNoDeadRelations(t, idx)
	assertSingleGenerationPerPath(t, idx)

	for _, old := range oldTargets {
		if got := countRelations(t, idx, "relation_type='references_table' AND target_type='sql_table' AND target_id=$1", old); got != 0 {
			t.Fatalf("edges to old-path occurrence %d = %d, want 0", old, got)
		}
	}
	// Новый путь проиндексирован; ссылки на tAccount остались только от
	// фрагментов нового файла (same-file), неизменённый caller.sql потерял
	// межфайловые ссылки (осознанная потеря, design D4).
	var callerRefs int
	if err := idx.db.QueryRow(`
		SELECT count(*)
		FROM relations r
		JOIN query_fragments qf ON qf.id = r.source_id AND r.source_type = 'query_fragment'
		JOIN files f ON f.id = qf.file_id
		JOIN sql_tables tb ON tb.id = r.target_id AND r.target_type = 'sql_table'
		WHERE r.relation_type = 'references_table'
		  AND lower(tb.table_name) = 'taccount'
		  AND f.path = $1
	`, filepath.ToSlash(filepath.Join(root, "caller.sql"))).Scan(&callerRefs); err != nil {
		t.Fatalf("caller refs after rename: %v", err)
	}
	if callerRefs != 0 {
		t.Fatalf("unmodified caller still references renamed-away table: %d", callerRefs)
	}
	latest := maxTableOccurrenceID(t, idx, "tAccount")
	if got := countRelations(t, idx, "relation_type='references_table' AND target_type='sql_table' AND target_id=$1", latest); got != 1 {
		t.Fatalf("new-file self reference = %d, want 1", got)
	}
}

// TestReconcile_ParseFailureNoEntities (задача 4.8): изменённый файл больше не
// содержит сущностей (эквивалент сбоя парсинга: новые сущности не созданы) —
// старые рёбра источника отсутствуют, инвариант целостности выполняется.
func TestReconcile_ParseFailureNoEntities(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	callerOld := latestProcID(t, idx, "CallerA")
	writeFile(t, filepath.Join(root, "caller.sql"), "-- nothing indexable here\nselect getdate()\n")
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}

	assertNoDeadRelations(t, idx)
	assertSingleGenerationPerPath(t, idx)
	if got := countRelations(t, idx, "source_type='sql_procedure' AND source_id = $1", callerOld); got != 0 {
		t.Fatalf("edges of vanished entities = %d, want 0", got)
	}
	callers := callersOf(t, idx, latestProcID(t, idx, "CalleeB"))
	if len(callers) != 1 {
		t.Fatalf("callers after caller vanished = %#v, want only DoomedCaller", callers)
	}
}

// TestReconcile_BranchPathIsolation (задача 4.9): remap не выходит за пределы
// пути файла — изменение таблиц в одном каталоге не трогает рёбра другого.
func TestReconcile_BranchPathIsolation(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	branchB := filepath.Join(root, "branchB")
	if err := os.MkdirAll(branchB, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(branchB, "tables.sql"), "create table tBranch (\n\tBranchID int\n)\n")
	writeFile(t, filepath.Join(branchB, "caller.sql"), "create procedure BranchCaller\nas\nbegin\n\tselect BranchID from tBranch\nend\n")
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update add branchB: %v", err)
	}

	branchTargets, branchEdges := fragmentTableRefs(t, idx, "tBranch")
	if branchEdges == 0 || len(branchTargets) != 1 {
		t.Fatalf("precondition: branchB tBranch refs = %v/%d, want single target", branchTargets, branchEdges)
	}
	branchTarget := branchTargets[0]

	writeFile(t, filepath.Join(root, "tables.sql"), "create table tAccount (\n\tAccountID int,\n\tAccountNumber varchar(20),\n\tNote text\n)\n")
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update modify root tables: %v", err)
	}

	assertNoDeadRelations(t, idx)
	assertSingleGenerationPerPath(t, idx)

	// Рёбра branchB не перепривязались: цель и количество не изменились.
	branchTargetsAfter, branchEdgesAfter := fragmentTableRefs(t, idx, "tBranch")
	if branchEdgesAfter != branchEdges {
		t.Fatalf("branchB edges = %d, want %d untouched", branchEdgesAfter, branchEdges)
	}
	if len(branchTargetsAfter) != 1 || branchTargetsAfter[0] != branchTarget {
		t.Fatalf("branchB target = %v, want [%d] (remap must stay within path)", branchTargetsAfter, branchTarget)
	}
	// Root-рёбра перенесены на новое поколение.
	latest := maxTableOccurrenceID(t, idx, "tAccount")
	rootTargets, _ := fragmentTableRefs(t, idx, "tAccount")
	for _, target := range rootTargets {
		if target != latest {
			t.Fatalf("root fragment target %d, want latest %d", target, latest)
		}
	}
}

// TestReconcile_NoopUpdateZeroCounters (задачи 3.2, 3.3-нуль): no-op без
// изменённых/удалённых файлов не выполняет запросов — счётчики нулевые,
// длительность близка к нулю; набор рёбер не меняется.
func TestReconcile_NoopUpdateZeroCounters(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	before := countRelations(t, idx, "TRUE")

	start := time.Now()
	result, err := idx.reconcileRelations(context.Background(), nil, nil, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("no-op reconcile: %v", err)
	}
	if result.OldFileGenerations != 0 || reconcileCounterTotal(result.Purged) != 0 ||
		reconcileCounterTotal(result.Remapped) != 0 || reconcileCounterTotal(result.DeletedUnresolved) != 0 {
		t.Fatalf("no-op counters = %+v, want all zero", result)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("no-op reconcile took %v, want <50ms", elapsed)
	}

	stats, err := idx.Update(root, true, 1)
	if err != nil {
		t.Fatalf("no-op Update: %v", err)
	}
	if stats.FilesUpdated != 0 || stats.FilesDeleted != 0 {
		t.Fatalf("no-op update touched files: updated=%d deleted=%d", stats.FilesUpdated, stats.FilesDeleted)
	}
	if got := countRelations(t, idx, "TRUE"); got != before {
		t.Fatalf("relations changed on no-op: %d -> %d", before, got)
	}
	assertNoDeadRelations(t, idx)
}

// TestReconcile_SafetyNetCleansCrashWindow (задача 3.5): имитация падения в
// окне reconcile → удаление старых поколений (второе поколение вставлено,
// старое не удалено) — следующий Update с safety-net очищает дубли, в БД
// остаётся одно поколение на путь, инвариант выполняется.
func TestReconcile_SafetyNetCleansCrashWindow(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	callerPath := filepath.ToSlash(filepath.Join(root, "caller.sql"))
	// Дубликат поколения: копия строки files (включая encoding — иначе
	// GetLatestFilesByRootPath падает на NULL) + копия процедур, как если бы
	// прогон упал после вставки нового поколения, но до удаления старого.
	var newFileID int64
	if err := idx.db.QueryRow(`
		INSERT INTO files (scan_run_id, ds_product_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		SELECT scan_run_id, ds_product_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, COALESCE(encoding, ''), language
		FROM files WHERE path = $1
		RETURNING id
	`, callerPath).Scan(&newFileID); err != nil {
		t.Fatalf("insert duplicate file generation: %v", err)
	}
	if _, err := idx.db.Exec(`
		INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end)
		SELECT $1, proc_name, line_start, line_end FROM sql_procedures p
		JOIN files f ON f.id = p.file_id WHERE f.path = $2
	`, newFileID, callerPath); err != nil {
		t.Fatalf("insert duplicate procedures: %v", err)
	}
	// Прогон успел создать рёбра нового поколения до падения.
	var newCallerID int64
	if err := idx.db.QueryRow(`SELECT id FROM sql_procedures WHERE file_id=$1 AND proc_name='CallerA'`, newFileID).Scan(&newCallerID); err != nil {
		t.Fatalf("lookup new-gen caller: %v", err)
	}
	calleeID := latestProcID(t, idx, "CalleeB")
	if _, err := idx.db.Exec(`
		INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type, confidence, line_number)
		VALUES ('sql_procedure', $1, 'sql_procedure', $2, 'calls_procedure', 'regex', 4)
	`, newCallerID, calleeID); err != nil {
		t.Fatalf("insert new-gen edge: %v", err)
	}

	var dupPaths int
	if err := idx.db.QueryRow(`SELECT count(*) FROM (SELECT path FROM files GROUP BY path HAVING count(*) > 1) d`).Scan(&dupPaths); err != nil || dupPaths != 1 {
		t.Fatalf("precondition duplicates = %d (err %v), want 1", dupPaths, err)
	}

	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update after simulated crash: %v", err)
	}

	assertSingleGenerationPerPath(t, idx)
	assertNoDeadRelations(t, idx)

	// Рёбра вызывающих сторон указывают на актуальное поколение CallerA.
	callers := callersOf(t, idx, latestProcID(t, idx, "CalleeB"))
	if len(callers) != 2 {
		t.Fatalf("callers after safety-net = %#v, want CallerA+DoomedCaller", callers)
	}
}

// TestReconcile_IncrementalEqualsFullRebuild (задачи 4.11, 4.12): серия
// incremental update даёт тот же набор рёбер (по identity без id), что
// полная пересборка итогового состояния; дублей полного кортежа нет.
func TestReconcile_IncrementalEqualsFullRebuild(t *testing.T) {
	idx, root := newReconcileIndexer(t)
	initReconcile(t, idx, root)

	steps := []func(){
		func() {
			writeFile(t, filepath.Join(root, "caller.sql"), "create procedure CallerA\nas\nbegin\n\texec CalleeB\n\texec Helper\n\tselect AccountID from tAccount\nend\n")
		},
		func() {
			writeFile(t, filepath.Join(root, "callee.sql"), "create procedure CalleeB\nas\nbegin\n\tselect 42\nend\n")
		},
		func() {
			writeFile(t, filepath.Join(root, "tables.sql"), "create table tAccount (\n\tAccountID int,\n\tAccountNumber varchar(20),\n\tBranchID int\n)\n")
		},
		func() {
			if err := os.Remove(filepath.Join(root, "doomed.sql")); err != nil {
				t.Fatalf("remove doomed: %v", err)
			}
		},
	}
	for i, step := range steps {
		step()
		if _, err := idx.Update(root, true, 1); err != nil {
			t.Fatalf("Update step %d: %v", i+1, err)
		}
		assertNoDeadRelations(t, idx)
	}

	assertNoDuplicateRelationTuples(t, idx)
	incremental := relationIdentitySet(t, idx)

	// Полная пересборка итогового состояния.
	if _, err := idx.db.ResetCodebaseTables(context.Background()); err != nil {
		t.Fatalf("reset tables: %v", err)
	}
	if _, err := idx.Init(root, 1); err != nil {
		t.Fatalf("full rebuild Init: %v", err)
	}
	assertNoDuplicateRelationTuples(t, idx)
	rebuilt := relationIdentitySet(t, idx)

	missing := make([]string, 0)
	extra := make([]string, 0)
	for key := range incremental {
		if _, ok := rebuilt[key]; !ok {
			missing = append(missing, key)
		}
	}
	for key := range rebuilt {
		if _, ok := incremental[key]; !ok {
			extra = append(extra, key)
		}
	}
	if len(missing) != 0 || len(extra) != 0 {
		sort.Strings(missing)
		sort.Strings(extra)
		t.Fatalf("incremental != full rebuild\nmissing in rebuild:\n  %s\nextra in rebuild:\n  %s",
			strings.Join(missing, "\n  "), strings.Join(extra, "\n  "))
	}
}

// assertNoDuplicateRelationTuples (задача 4.12): полный кортеж
// (source_type, source_id, target_type, target_id, relation_type, line_number)
// уникален после remap.
func assertNoDuplicateRelationTuples(t *testing.T, idx *Indexer) {
	t.Helper()
	var n int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM (
			SELECT source_type, source_id, target_type, target_id, relation_type, line_number, count(*) AS c
			FROM relations
			GROUP BY source_type, source_id, target_type, target_id, relation_type, line_number
			HAVING count(*) > 1
		) d
	`).Scan(&n); err != nil {
		t.Fatalf("duplicate tuples check: %v", err)
	}
	if n != 0 {
		t.Fatalf("%d duplicate relation tuples", n)
	}
}

// relationIdentitySet — множество рёбер в id-агностичном виде: идентификаторы
// заменены на identity (имя сущности + путь файла / query_hash), поэтому
// наборы incremental и full rebuild сравнимы напрямую.
func relationIdentitySet(t *testing.T, idx *Indexer) map[string]struct{} {
	t.Helper()
	keys := map[string]func(id int64) string{
		"sql_procedure": func(id int64) string {
			var name, path string
			if err := idx.db.QueryRow(`SELECT p.proc_name, f.path FROM sql_procedures p JOIN files f ON f.id=p.file_id WHERE p.id=$1`, id).Scan(&name, &path); err != nil {
				t.Fatalf("proc identity %d: %v", id, err)
			}
			return strings.ToLower(name) + "@" + path
		},
		// sql_table-цели: identity — только имя. Конкретная occurrence-строка
		// (в каком файле) выбирается резолвером как глобальный max id и
		// зависит от порядка вставки поколений — различие между incremental
		// и rebuild в выборе строки-представителя является существующим
		// поведением резолвера и выходит за рамки change (Non-Goals).
		"sql_table": func(id int64) string {
			var name string
			if err := idx.db.QueryRow(`SELECT table_name FROM sql_tables WHERE id=$1`, id).Scan(&name); err != nil {
				t.Fatalf("table identity %d: %v", id, err)
			}
			return strings.ToLower(name)
		},
		"api_contract": func(id int64) string {
			var name, kind, path string
			if err := idx.db.QueryRow(`SELECT c.contract_name, c.contract_kind, f.path FROM api_contracts c JOIN files f ON f.id=c.file_id WHERE c.id=$1`, id).Scan(&name, &kind, &path); err != nil {
				t.Fatalf("contract identity %d: %v", id, err)
			}
			return strings.ToLower(name) + "|" + strings.ToLower(kind) + "@" + path
		},
		"query_fragment": func(id int64) string {
			var hash string
			var line int
			if err := idx.db.QueryRow(`SELECT COALESCE(query_hash,''), line_number FROM query_fragments WHERE id=$1`, id).Scan(&hash, &line); err != nil {
				t.Fatalf("fragment identity %d: %v", id, err)
			}
			return hash + "#" + fmt.Sprint(line)
		},
	}

	rows, err := idx.db.Query(`SELECT source_type, source_id, target_type, target_id, relation_type, line_number FROM relations`)
	if err != nil {
		t.Fatalf("relations query: %v", err)
	}
	defer rows.Close()
	result := make(map[string]struct{})
	for rows.Next() {
		var sourceType, targetType, relationType string
		var sourceID, targetID int64
		var line int
		if err := rows.Scan(&sourceType, &sourceID, &targetType, &targetID, &relationType, &line); err != nil {
			t.Fatalf("relations scan: %v", err)
		}
		sourceKey, ok := keys[sourceType]
		if !ok {
			t.Fatalf("unexpected source_type %s in fixture relations", sourceType)
		}
		targetKey, ok := keys[targetType]
		if !ok {
			t.Fatalf("unexpected target_type %s in fixture relations", targetType)
		}
		identity := sourceType + "|" + sourceKey(sourceID) + "|" + targetType + "|" + targetKey(targetID) + "|" + relationType + "|" + fmt.Sprint(line)
		result[identity] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("relations iterate: %v", err)
	}
	return result
}
