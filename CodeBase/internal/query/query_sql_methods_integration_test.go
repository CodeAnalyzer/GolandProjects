//go:build integration

package query

import (
	"context"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func insertPasUnit(t *testing.T, db *store.DB, fileID int64, name string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO pas_units (file_id, unit_name, line_start, line_end)
		VALUES ($1, $2, 1, 2)
		RETURNING id
	`, fileID, name).Scan(&id)
	if err != nil {
		t.Fatalf("insert unit %s: %v", name, err)
	}
	return id
}

func insertPasClass(t *testing.T, db *store.DB, fileID, unitID int64, name string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO pas_classes (file_id, unit_id, class_name, line_start, line_end)
		VALUES ($1, $2, $3, 1, 2)
		RETURNING id
	`, fileID, unitID, name).Scan(&id)
	if err != nil {
		t.Fatalf("insert class %s: %v", name, err)
	}
	return id
}

func insertPasMethod(t *testing.T, db *store.DB, fileID, classID, unitID int64, name string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO pas_methods (file_id, class_id, unit_id, method_name, line_number)
		VALUES ($1, $2, $3, $4, 1)
		RETURNING id
	`, fileID, classID, unitID, name).Scan(&id)
	if err != nil {
		t.Fatalf("insert method %s: %v", name, err)
	}
	return id
}

func insertQueryFragment(t *testing.T, db *store.DB, fileID, parentID int64, name string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO query_fragments
			(file_id, parent_type, parent_id, component_name, component_type, query_text, context, line_number)
		VALUES ($1, 'pas_method', $2, $3, 'pas', 'SELECT * FROM tAim', 'select', 1)
		RETURNING id
	`, fileID, parentID, name).Scan(&id)
	if err != nil {
		t.Fatalf("insert query fragment %s: %v", name, err)
	}
	return id
}

// seedMethodsByTableFixture создаёт метод с двумя query-фрагментами на одну
// таблицу (fan-out) и второй метод с одним фрагментом на ту же таблицу.
func seedMethodsByTableFixture(t *testing.T) (db *store.DB, methodFanOut, methodSingle int64) {
	t.Helper()
	db = testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	fileID := insertRelationsFile(t, db, scanID, "/repo/E_ObjClsRelation_Grid_Aim.pas")

	unitID := insertPasUnit(t, db, fileID, "E_ObjClsRelation_Grid_Aim")
	classID := insertPasClass(t, db, fileID, unitID, "TE_ObjClsRelation_Grid_Aim")

	tableID := insertRelationsTable(t, db, fileID, "tAim")

	methodFanOut = insertPasMethod(t, db, fileID, classID, unitID, "GetFilterQueryText")
	frag1 := insertQueryFragment(t, db, fileID, methodFanOut, "GetFilterQueryText_1")
	frag2 := insertQueryFragment(t, db, fileID, methodFanOut, "GetFilterQueryText_2")
	insertRelationsEdge(t, db, "pas_method", methodFanOut, "query_fragment", frag1, "builds_query")
	insertRelationsEdge(t, db, "pas_method", methodFanOut, "query_fragment", frag2, "builds_query")
	// Второй множитель: тот же фрагмент продублирован связью executes_query.
	insertRelationsEdge(t, db, "pas_method", methodFanOut, "query_fragment", frag1, "executes_query")
	insertRelationsEdge(t, db, "query_fragment", frag1, "sql_table", tableID, "references_table")
	insertRelationsEdge(t, db, "query_fragment", frag2, "sql_table", tableID, "references_table")

	methodSingle = insertPasMethod(t, db, fileID, classID, unitID, "GetFilterQueryTextSingle")
	frag3 := insertQueryFragment(t, db, fileID, methodSingle, "GetFilterQueryTextSingle_1")
	insertRelationsEdge(t, db, "pas_method", methodSingle, "query_fragment", frag3, "builds_query")
	insertRelationsEdge(t, db, "query_fragment", frag3, "sql_table", tableID, "references_table")

	return db, methodFanOut, methodSingle
}

func TestFindMethodsByTable_DeduplicatesMethodsWithMultipleFragments(t *testing.T) {
	db, methodFanOut, methodSingle := seedMethodsByTableFixture(t)
	q := New(db)

	results, err := q.FindMethodsByTable(context.Background(), "tAim", 50)
	if err != nil {
		t.Fatalf("FindMethodsByTable: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("row count = %d, want 2 (one row per unique method): %+v", len(results), results)
	}

	seen := map[int64]int{}
	for _, r := range results {
		seen[r.ID]++
	}
	if seen[methodFanOut] != 1 {
		t.Fatalf("method with two fragments returned %d times, want 1", seen[methodFanOut])
	}
	if seen[methodSingle] != 1 {
		t.Fatalf("single-fragment method returned %d times, want 1", seen[methodSingle])
	}
	if results[0].TableUsage != "tAim" {
		t.Fatalf("TableUsage = %q, want tAim", results[0].TableUsage)
	}
}

func TestFindMethodsByTable_UnknownTableReturnsEmpty(t *testing.T) {
	db, _, _ := seedMethodsByTableFixture(t)
	q := New(db)

	results, err := q.FindMethodsByTable(context.Background(), "tMissing", 50)
	if err != nil {
		t.Fatalf("FindMethodsByTable: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("row count = %d, want 0", len(results))
	}
}
