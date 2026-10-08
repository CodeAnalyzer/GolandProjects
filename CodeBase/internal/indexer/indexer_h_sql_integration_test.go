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

// TestHFileSQLOutgoingRelations: .h с процедурой (exec + insert + select)
// должен давать исходящие calls_procedure, табличные relations и
// query_fragments с parent-binding к процедуре; Defines из SQL-разбора
// не дублируются; .h без маркеров не даёт SQL-сущностей.
func TestHFileSQLOutgoingRelations(t *testing.T) {
	db := testutil.Open(t)
	idx := &Indexer{
		db: db,
		config: &config.Config{Indexer: config.IndexerConfig{
			Parallel:        1,
			BatchSize:       100,
			IncludePatterns: []string{"*.h", "*.sql"},
		}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}

	root := t.TempDir()
	writeFile := func(rel, content string) {
		t.Helper()
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(full, []byte(content), 0644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	writeFile("procs.h", `#define MAX_ROWS 1000
DCL_PROC_BEGIN(HSourceProc)
as
  __BEGIN_PROCEDURE__(HSourceProc)
  exec HTargetProc
  insert into pHTable (ID) values (1)
  select * from pHReadTable
__END_PROCEDURE__(HSourceProc)
`)
	writeFile("target.sql", `DCL_PROC_BEGIN(HTargetProc)
as
  select 1
__END_PROCEDURE__(HTargetProc)
`)
	writeFile("plain.h", "#define OTHER_CONST 5\n")

	stats, err := idx.Init(root, 1)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if stats.Errors != 0 {
		t.Fatalf("Init errors = %d", stats.Errors)
	}

	queryInt := func(q string, args ...interface{}) int {
		t.Helper()
		var n int
		if err := idx.db.QueryRow(q, args...).Scan(&n); err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		return n
	}

	// Исходящий calls_procedure: HSourceProc -> HTargetProc
	n := queryInt(`SELECT count(*) FROM relations r
		JOIN sql_procedures src ON src.id = r.source_id AND r.source_type = 'sql_procedure'
		JOIN sql_procedures dst ON dst.id = r.target_id AND r.target_type = 'sql_procedure'
		WHERE r.relation_type = 'calls_procedure'
		AND LOWER(src.proc_name) = 'hsourceproc' AND LOWER(dst.proc_name) = 'htargetproc'`)
	if n != 1 {
		t.Fatalf("calls_procedure HSourceProc->HTargetProc = %d, want 1", n)
	}

	// Табличные relations из тела .h-процедуры: inserts_into и selects_from
	n = queryInt(`SELECT count(*) FROM relations r
		JOIN sql_procedures src ON src.id = r.source_id AND r.source_type = 'sql_procedure'
		JOIN sql_tables tbl ON tbl.id = r.target_id AND r.target_type = 'sql_table'
		WHERE r.relation_type = 'inserts_into'
		AND LOWER(src.proc_name) = 'hsourceproc' AND LOWER(tbl.table_name) = 'phtable'`)
	if n < 1 {
		t.Fatalf("inserts_into HSourceProc->pHTable = %d, want >= 1", n)
	}
	n = queryInt(`SELECT count(*) FROM relations r
		JOIN sql_procedures src ON src.id = r.source_id AND r.source_type = 'sql_procedure'
		JOIN sql_tables tbl ON tbl.id = r.target_id AND r.target_type = 'sql_table'
		WHERE r.relation_type = 'selects_from'
		AND LOWER(src.proc_name) = 'hsourceproc' AND LOWER(tbl.table_name) = 'phreadtable'`)
	if n < 1 {
		t.Fatalf("selects_from HSourceProc->pHReadTable = %d, want >= 1", n)
	}

	// Фрагменты с parent-binding к процедуре
	n = queryInt(`SELECT count(*) FROM query_fragments qf
		JOIN files f ON f.id = qf.file_id
		WHERE f.path LIKE '%procs.h' AND qf.parent_type = 'sql_procedure' AND qf.parent_id > 0`)
	if n < 1 {
		t.Fatalf("query_fragments from procs.h with sql_procedure parent = %d, want >= 1", n)
	}

	// Defines не дублируются: MAX_ROWS один раз (H-парсер), OTHER_CONST есть
	if n := queryInt(`SELECT count(*) FROM h_files_defines WHERE define_name = 'MAX_ROWS'`); n != 1 {
		t.Fatalf("h_files_defines MAX_ROWS = %d, want 1 (no dupes from SQL parse)", n)
	}
	if n := queryInt(`SELECT count(*) FROM h_files_defines WHERE define_name = 'OTHER_CONST'`); n != 1 {
		t.Fatalf("h_files_defines OTHER_CONST = %d, want 1", n)
	}

	// .h без маркеров: SQL-сущностей нет (pre-check)
	n = queryInt(`SELECT count(*) FROM sql_procedures p
		JOIN files f ON f.id = p.file_id
		WHERE f.path LIKE '%plain.h'`)
	if n != 0 {
		t.Fatalf("sql_procedures from plain.h = %d, want 0", n)
	}
	n = queryInt(`SELECT count(*) FROM query_fragments qf
		JOIN files f ON f.id = qf.file_id
		WHERE f.path LIKE '%plain.h'`)
	if n != 0 {
		t.Fatalf("query_fragments from plain.h = %d, want 0", n)
	}
}
