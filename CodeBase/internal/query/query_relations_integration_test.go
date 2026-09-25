//go:build integration

package query

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func insertRelationsFile(t *testing.T, db *store.DB, scanID int64, path string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, $2, $3, 'sql', 1, $4, NOW(), 'UTF8', 'SQL')
		RETURNING id
	`, scanID, path, path, path).Scan(&id)
	if err != nil {
		t.Fatalf("insert file %s: %v", path, err)
	}
	return id
}

func insertRelationsProc(t *testing.T, db *store.DB, fileID int64, name string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end)
		VALUES ($1, $2, 1, 2)
		RETURNING id
	`, fileID, name).Scan(&id)
	if err != nil {
		t.Fatalf("insert proc %s: %v", name, err)
	}
	return id
}

func insertRelationsTable(t *testing.T, db *store.DB, fileID int64, name string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO sql_tables (file_id, table_name, context, line_number)
		VALUES ($1, $2, 'select', 1)
		RETURNING id
	`, fileID, name).Scan(&id)
	if err != nil {
		t.Fatalf("insert table %s: %v", name, err)
	}
	return id
}

func insertRelationsEdge(t *testing.T, db *store.DB, sourceType string, sourceID int64, targetType string, targetID int64, relationType string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type, confidence, line_number)
		VALUES ($1, $2, $3, $4, $5, 'certain', 1)
		RETURNING id
	`, sourceType, sourceID, targetType, targetID, relationType).Scan(&id)
	if err != nil {
		t.Fatalf("insert relation %s->%s: %v", sourceType, targetType, err)
	}
	return id
}

type relationsFixture struct {
	db      *store.DB
	myProc  int64
	myExt   int64
	tTarget int64
	pPort   int64
	other   int64
	edgeIDs map[string]int64
}

func seedRelationsFixture(t *testing.T) relationsFixture {
	t.Helper()
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	fileID := insertRelationsFile(t, db, scanID, "/repo/relations.sql")

	myProc := insertRelationsProc(t, db, fileID, "MyProc")
	myExt := insertRelationsProc(t, db, fileID, "MyProcExtended")
	tTarget := insertRelationsTable(t, db, fileID, "tTarget")
	pPort := insertRelationsTable(t, db, fileID, "pPortObject")
	other := insertRelationsTable(t, db, fileID, "tOther")

	edges := map[string]int64{}
	edges["myproc->ttarget"] = insertRelationsEdge(t, db, "sql_procedure", myProc, "sql_table", tTarget, "selects_from")
	edges["myext->ttarget"] = insertRelationsEdge(t, db, "sql_procedure", myExt, "sql_table", tTarget, "selects_from")
	edges["myproc->other"] = insertRelationsEdge(t, db, "sql_procedure", myProc, "sql_table", other, "selects_from")
	edges["myproc->pport"] = insertRelationsEdge(t, db, "sql_procedure", myProc, "sql_table", pPort, "selects_from")

	return relationsFixture{db: db, myProc: myProc, myExt: myExt, tTarget: tTarget, pPort: pPort, other: other, edgeIDs: edges}
}

func relationIDSet(relations []RelationResult) []int64 {
	ids := make([]int64, 0, len(relations))
	for _, r := range relations {
		ids = append(ids, r.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func relationSourceNames(relations []RelationResult) []string {
	names := make([]string, 0, len(relations))
	for _, r := range relations {
		names = append(names, r.Source.Name)
	}
	sort.Strings(names)
	return names
}

func TestSearchRelations_ExactNamePreferred(t *testing.T) {
	fx := seedRelationsFixture(t)
	q := New(fx.db)

	relations, err := q.SearchRelations(context.Background(), "", "MyProc", "", "", "", 50)
	if err != nil {
		t.Fatalf("SearchRelations: %v", err)
	}

	want := []int64{fx.edgeIDs["myproc->ttarget"], fx.edgeIDs["myproc->other"], fx.edgeIDs["myproc->pport"]}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if got := relationIDSet(relations); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("exact-first ids = %v, want %v", got, want)
	}
	for _, name := range relationSourceNames(relations) {
		if name == "MyProcExtended" {
			t.Fatalf("exact match must exclude substring match MyProcExtended")
		}
	}
}

func TestSearchRelations_SubstringFallback(t *testing.T) {
	fx := seedRelationsFixture(t)
	q := New(fx.db)

	relations, err := q.SearchRelations(context.Background(), "", "yPro", "", "", "", 50)
	if err != nil {
		t.Fatalf("SearchRelations: %v", err)
	}

	names := relationSourceNames(relations)
	wantNames := []string{"MyProc", "MyProc", "MyProc", "MyProcExtended"}
	if fmt.Sprint(names) != fmt.Sprint(wantNames) {
		t.Fatalf("fallback source names = %v, want %v", names, wantNames)
	}
}

func TestSearchRelations_IntersectionWithTwoNames(t *testing.T) {
	fx := seedRelationsFixture(t)
	q := New(fx.db)

	relations, err := q.SearchRelations(context.Background(), "", "MyProc", "", "tTarget", "", 50)
	if err != nil {
		t.Fatalf("SearchRelations: %v", err)
	}

	want := []int64{fx.edgeIDs["myproc->ttarget"]}
	if got := relationIDSet(relations); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("intersection ids = %v, want %v", got, want)
	}
}

func TestSearchRelations_NameWithoutTypeResolvesAnyEntity(t *testing.T) {
	fx := seedRelationsFixture(t)
	q := New(fx.db)

	relations, err := q.SearchRelations(context.Background(), "", "", "", "pPortObject", "", 50)
	if err != nil {
		t.Fatalf("SearchRelations: %v", err)
	}

	if len(relations) != 1 {
		t.Fatalf("relations count = %d, want 1", len(relations))
	}
	if relations[0].Target.Type != "sql_table" || relations[0].Target.Name != "pPortObject" {
		t.Fatalf("target = %+v, want sql_table pPortObject", relations[0].Target)
	}
}

func TestSearchRelations_TypeOnlyFilter(t *testing.T) {
	fx := seedRelationsFixture(t)
	q := New(fx.db)

	relations, err := q.SearchRelations(context.Background(), "sql_procedure", "", "sql_table", "", "selects_from", 50)
	if err != nil {
		t.Fatalf("SearchRelations: %v", err)
	}
	if len(relations) != 4 {
		t.Fatalf("type-only relations count = %d, want 4", len(relations))
	}
}

// TestSearchRelations_SubstringMatchesOldBehavior проверяет, что для имён без
// точного совпадения подстрочный фолбэк возвращает тот же набор id, что и
// прежняя коррелированная EXISTS-логика (нерегрессия результата).
func TestSearchRelations_SubstringMatchesOldBehavior(t *testing.T) {
	fx := seedRelationsFixture(t)
	q := New(fx.db)
	ctx := context.Background()

	relations, err := q.SearchRelations(ctx, "", "yPro", "", "", "", 50)
	if err != nil {
		t.Fatalf("SearchRelations: %v", err)
	}

	rows, err := fx.db.QueryContext(ctx, `
		SELECT r.id
		FROM relations r
		WHERE (
			(r.source_type = 'sql_procedure' AND EXISTS (SELECT 1 FROM sql_procedures n WHERE n.id = r.source_id AND n.proc_name ILIKE $1)) OR
			(r.source_type = 'sql_table' AND EXISTS (SELECT 1 FROM sql_tables n WHERE n.id = r.source_id AND n.table_name ILIKE $1))
		)
		ORDER BY r.id DESC
		LIMIT $2
	`, "%yPro%", 50)
	if err != nil {
		t.Fatalf("reference query: %v", err)
	}
	defer rows.Close()
	reference := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan reference id: %v", err)
		}
		reference = append(reference, id)
	}
	sort.Slice(reference, func(i, j int) bool { return reference[i] < reference[j] })

	if got := relationIDSet(relations); fmt.Sprint(got) != fmt.Sprint(reference) {
		t.Fatalf("fallback ids = %v, want old-behavior ids %v", got, reference)
	}
}

// TestSearchRelations_IndexedLookupPlan проверяет, что выборка id связей
// опирается на индекс по relations, а не на полное сканирование таблицы.
// enable_seqscan выключен на выделенном соединении, чтобы проверка не
// зависела от размера таблицы.
func TestSearchRelations_IndexedLookupPlan(t *testing.T) {
	fx := seedRelationsFixture(t)
	ctx := context.Background()

	queryText, args := buildRelationIDsQuery(
		[]relationEntityMatch{{Type: "sql_procedure", ID: fx.myProc}},
		[]relationEntityMatch{{Type: "sql_table", ID: fx.tTarget}},
		"", "", "", 50,
	)

	conn, err := fx.db.DB.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire connection: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET enable_seqscan = off"); err != nil {
		t.Fatalf("disable seqscan: %v", err)
	}

	rows, err := conn.QueryContext(ctx, "EXPLAIN "+queryText, args...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	plan := make([]string, 0)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		plan = append(plan, line)
	}
	joined := strings.Join(plan, "\n")
	if strings.Contains(joined, "Seq Scan on relations") {
		t.Fatalf("plan must not seq-scan relations:\n%s", joined)
	}
	if !strings.Contains(joined, "relations") {
		t.Fatalf("plan must include relations:\n%s", joined)
	}
}
