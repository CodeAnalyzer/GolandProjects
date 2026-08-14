//go:build integration

package indexer

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store/testutil"
)

func newTestIndexer(t *testing.T) (*Indexer, string) {
	t.Helper()
	db := testutil.Open(t)
	root := t.TempDir()
	src := filepath.Join("testdata", "mini")
	if err := copyTree(src, root); err != nil {
		t.Fatalf("copy testdata: %v", err)
	}
	idx := &Indexer{
		db:          db,
		config: &config.Config{Indexer: config.IndexerConfig{
			Parallel:        1,
			BatchSize:       100,
			IncludePatterns: []string{"*.sql", "*.js", "*.xml", "*.h"},
		}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}
	return idx, root
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		return os.WriteFile(target, data, 0644)
	})
}

func countRelation(t *testing.T, idx *Indexer, relType, sourceType string) int {
	t.Helper()
	var n int
	q := `SELECT count(*) FROM relations WHERE relation_type=$1`
	args := []interface{}{relType}
	if sourceType != "" {
		q += ` AND source_type=$2`
		args = append(args, sourceType)
	}
	if err := idx.db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("count %s/%s: %v", relType, sourceType, err)
	}
	return n
}

func TestInitMiniTree_BuildsCallsAndCallbacks(t *testing.T) {
	idx, root := newTestIndexer(t)
	stats, err := idx.Init(root, 1)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if stats.Errors != 0 {
		t.Fatalf("Init errors = %d", stats.Errors)
	}

	ids, err := idx.db.FindLatestSQLProcedureIDsByNames([]string{"CallerA", "CalleeB"})
	if err != nil {
		t.Fatalf("lookup procs: %v", err)
	}
	if ids["callera"] == 0 || ids["calleeb"] == 0 {
		t.Fatalf("missing procedures: %#v", ids)
	}
	if got := countRelation(t, idx, "calls_procedure", "sql_procedure"); got != 1 {
		t.Fatalf("sql calls_procedure = %d, want 1", got)
	}
	if got := countRelation(t, idx, "calls_procedure", "js_function"); got != 1 {
		t.Fatalf("js calls_procedure = %d, want 1", got)
	}
	if got := countRelation(t, idx, "subscribes_to_event", ""); got != 1 {
		t.Fatalf("subscribes_to_event = %d, want 1", got)
	}

	var msg string
	if err := idx.db.QueryRow(`SELECT message FROM ds_return_codes WHERE ret_code=10001`).Scan(&msg); err != nil {
		t.Fatalf("retcode: %v", err)
	}
	if msg != "Test error message" {
		t.Fatalf("retcode message = %q, want resolved define", msg)
	}
}

func TestUpdateOnlyModified_RebuildsStableRelations(t *testing.T) {
	idx, root := newTestIndexer(t)
	if _, err := idx.Init(root, 1); err != nil {
		t.Fatalf("Init: %v", err)
	}
	sqlCalls := countRelation(t, idx, "calls_procedure", "sql_procedure")
	jsCalls := countRelation(t, idx, "calls_procedure", "js_function")
	callbacks := countRelation(t, idx, "subscribes_to_event", "")

	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update unchanged: %v", err)
	}
	if got := countRelation(t, idx, "calls_procedure", "sql_procedure"); got != sqlCalls {
		t.Fatalf("sql calls after noop update = %d, want %d", got, sqlCalls)
	}
	if got := countRelation(t, idx, "calls_procedure", "js_function"); got != jsCalls {
		t.Fatalf("js calls after noop update = %d, want %d", got, jsCalls)
	}
	if got := countRelation(t, idx, "subscribes_to_event", ""); got != callbacks {
		t.Fatalf("callbacks after noop update = %d, want %d", got, callbacks)
	}

	aPath := filepath.Join(root, "a.sql")
	if err := os.WriteFile(aPath, []byte("create procedure CallerA\nas\nbegin\n\texec CalleeB\n\tselect 1\nend\n"), 0644); err != nil {
		t.Fatalf("rewrite a.sql: %v", err)
	}
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update modified: %v", err)
	}
	ids2, err := idx.db.FindLatestSQLProcedureIDsByNames([]string{"CallerA"})
	if err != nil {
		t.Fatalf("lookup after modify: %v", err)
	}
	newCallerID := ids2["callera"]
	if newCallerID == 0 {
		t.Fatal("CallerA not found after modify")
	}
	var newCallExists bool
	if err := idx.db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM relations WHERE source_type='sql_procedure' AND source_id=$1 AND relation_type='calls_procedure')`,
		newCallerID,
	).Scan(&newCallExists); err != nil {
		t.Fatalf("check new call relation: %v", err)
	}
	if !newCallExists {
		t.Fatal("new CallerA must have calls_procedure relation")
	}

	if err := os.Remove(filepath.Join(root, "c.js")); err != nil {
		t.Fatalf("remove c.js: %v", err)
	}
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update after delete: %v", err)
	}
	var jsFuncExists bool
	if err := idx.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM js_functions WHERE function_name='CallCallee')`).Scan(&jsFuncExists); err != nil {
		t.Fatalf("check js function: %v", err)
	}
	if jsFuncExists {
		t.Fatal("CallCallee js function must be deleted after file removal")
	}
}

func TestInitMiniTree_ParallelMatchesSerial(t *testing.T) {
	serial, root1 := newTestIndexer(t)
	if _, err := serial.Init(root1, 1); err != nil {
		t.Fatalf("serial Init: %v", err)
	}
	parallel, root2 := newTestIndexer(t)
	if _, err := parallel.Init(root2, 4); err != nil {
		t.Fatalf("parallel Init: %v", err)
	}
	for _, rel := range []struct {
		kind, src string
	}{
		{"calls_procedure", "sql_procedure"},
		{"calls_procedure", "js_function"},
		{"subscribes_to_event", ""},
	} {
		want := countRelation(t, serial, rel.kind, rel.src)
		got := countRelation(t, parallel, rel.kind, rel.src)
		if got != want {
			t.Fatalf("%s/%s serial=%d parallel=%d", rel.kind, rel.src, want, got)
		}
	}
}
