//go:build integration

package review

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/indexer"
	"github.com/codebase/internal/store/testutil"
)

const memUniqueLSTFixture = `[FA-Test]
pMemUniq;TEST
pMemApi;TEST
`

// Уникальный XPK у pMemUniq (в памяти), неуникальный XIE и уникальный XPK у
// tOther (НЕ в памяти) — разделяет триггеры «уникальный индекс» и «таблица в памяти».
const memUniqueDefsFixture = `M_CRT_INDEX('UNIQUE', 'XPKpMemUniq', 'pMemUniq', 'SPID, KeyCol')
M_CRT_INDEX('', 'XIE1pMemUniq', 'pMemUniq', 'PlainCol')
M_CRT_INDEX('UNIQUE', 'XPKtOther', 'tOther', 'KeyCol')
`

const memUniqueTargetFixture = `create procedure x_ReviewTarget
as
begin
  update pMemUniq set KeyCol = @KeyCol where SPID = @@SPID
  update pMemUniq set PlainCol = @PlainCol where SPID = @@SPID
  update x set KeyCol = @KeyCol from pMemUniq x where x.SPID = @@SPID
  update tOther set KeyCol = @KeyCol
  update #tmpMem set KeyCol = @KeyCol
  update dbo.[pMemUniq] set KeyCol = @KeyCol where SPID = @@SPID
end
`

func writeMemUniqueFile(t *testing.T, root, rel, content string) string {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
	return full
}

// TestMemoryUniqueIndexUpdateRuleE2E: полный путь данных правила — индексация
// фикстур (ModuleTableInMemory.lst → memory_tables; M_CRT_INDEX('UNIQUE',...) →
// sql_index_definitions + поля) и review целевого файла: finding только на
// уникальных колонках in-memory таблицы (прямая и через алиас), отсутствие —
// на неуникальной колонке, чужой таблице и #таблице.
func TestMemoryUniqueIndexUpdateRuleE2E(t *testing.T) {
	db := testutil.Open(t)
	root := t.TempDir()

	writeMemUniqueFile(t, root, filepath.Join("DBMaintenance_Ext", "Server", "ModuleTableInMemory.lst"), memUniqueLSTFixture)
	writeMemUniqueFile(t, root, filepath.Join("Server", "pMemUniq_TmpTbl.sql"), memUniqueDefsFixture)
	targetPath := writeMemUniqueFile(t, root, filepath.Join("Server", "review_target.sql"), memUniqueTargetFixture)

	idx := indexer.New(db, &config.Config{Indexer: config.IndexerConfig{
		Parallel:        1,
		BatchSize:       100,
		IncludePatterns: []string{"*.lst", "*.sql"},
	}})
	if _, err := idx.InitCtx(context.Background(), root, 1); err != nil {
		t.Fatalf("InitCtx: %v", err)
	}

	// Review требует непустой ds_product_id у целевого файла
	var productID int64
	if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ('fa-test') RETURNING id`).Scan(&productID); err != nil {
		t.Fatalf("insert ds_products: %v", err)
	}
	if _, err := db.Exec(`UPDATE files SET ds_product_id = $1 WHERE path LIKE '%review_target.sql'`, productID); err != nil {
		t.Fatalf("update files: %v", err)
	}

	r := NewRunner(db)
	res, err := r.RunSQLFileCtx(context.Background(), targetPath, Options{Rules: []RuleID{RuleMemoryUniqueIndexUpdate}})
	if err != nil {
		t.Fatalf("RunSQLFileCtx: %v", err)
	}

	if len(res.Findings) != 3 {
		t.Fatalf("findings = %d, want 3: %+v", len(res.Findings), res.Findings)
	}
	for i, wantLine := range []int{4, 6, 9} {
		f := res.Findings[i]
		if f.Rule != RuleMemoryUniqueIndexUpdate || f.Severity != SeverityDeployStopper {
			t.Fatalf("finding[%d] = %+v, want memoryUniqueIndexUpdate/severity 1", i, f)
		}
		if !strings.EqualFold(f.Object, "pMemUniq") {
			t.Fatalf("finding[%d].Object = %q, want pMemUniq", i, f.Object)
		}
		if f.Line != wantLine {
			t.Fatalf("finding[%d].Line = %d, want %d", i, f.Line, wantLine)
		}
		if !strings.Contains(f.Message, "XPKpMemUniq") || !strings.Contains(strings.ToLower(f.Message), "keycol") {
			t.Fatalf("finding[%d].Message = %q, want XPKpMemUniq and keycol", i, f.Message)
		}
	}
	if res.Summary.Total != 3 || res.Summary.ByRule[RuleMemoryUniqueIndexUpdate] != 3 {
		t.Fatalf("summary = %+v, want total 3 / memoryUniqueIndexUpdate 3", res.Summary)
	}
}
