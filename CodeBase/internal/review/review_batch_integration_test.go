//go:build integration

package review

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/indexer"
	"github.com/codebase/internal/store/testutil"
)

const batchProcFixture = `create procedure %s
as
begin
  select * from %s
end
`

func TestRunSQLFilesCtxMatchesSingleAndIsolatesErrors(t *testing.T) {
	db := testutil.Open(t)
	root := t.TempDir()

	aPath := writeMemUniqueFile(t, root, filepath.Join("A", "ProcA.sql"), fmt.Sprintf(batchProcFixture, "ProcA", "tA"))
	bPath := writeMemUniqueFile(t, root, filepath.Join("B", "ProcB.sql"), fmt.Sprintf(batchProcFixture, "ProcB", "tB"))

	idx := indexer.New(db, &config.Config{Indexer: config.IndexerConfig{
		Parallel:        1,
		BatchSize:       100,
		IncludePatterns: []string{"*.sql"},
	}})
	if _, err := idx.InitCtx(context.Background(), root, 1); err != nil {
		t.Fatalf("InitCtx: %v", err)
	}

	for _, pair := range []struct {
		product string
		path    string
	}{
		{"fa-batch-a", aPath},
		{"fa-batch-b", bPath},
	} {
		var productID int64
		if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ($1) RETURNING id`, pair.product).Scan(&productID); err != nil {
			t.Fatalf("insert ds_products %s: %v", pair.product, err)
		}
		if _, err := db.Exec(`UPDATE files SET ds_product_id = $1 WHERE path = $2`, productID, normalizePath(pair.path)); err != nil {
			t.Fatalf("update file %s: %v", pair.path, err)
		}
	}

	ctx := context.Background()
	opts := Options{Rules: []RuleID{RuleUseSelectAll}}

	singleA, err := NewRunner(db).RunSQLFileCtx(ctx, aPath, opts)
	if err != nil {
		t.Fatalf("single A: %v", err)
	}
	singleB, err := NewRunner(db).RunSQLFileCtx(ctx, bPath, opts)
	if err != nil {
		t.Fatalf("single B: %v", err)
	}
	if len(singleA.Findings) == 0 || len(singleB.Findings) == 0 {
		t.Fatalf("fixture must trigger useSelectAll: A=%d B=%d", len(singleA.Findings), len(singleB.Findings))
	}

	missingPath := filepath.Join(root, "Missing.sql")
	batch, err := NewRunner(db).RunSQLFilesCtx(ctx, []string{aPath, missingPath, bPath}, opts, BatchHooks{})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}

	if batch.Total != 3 || batch.Succeeded != 2 || batch.Failed != 1 {
		t.Fatalf("batch counters = total:%d succeeded:%d failed:%d", batch.Total, batch.Succeeded, batch.Failed)
	}
	if len(batch.Files) != 3 {
		t.Fatalf("files = %d, want 3", len(batch.Files))
	}
	if batch.Files[1].Status != "error" || batch.Files[1].Error == nil || batch.Files[1].Error.Code != "not_indexed" {
		t.Fatalf("middle file result = %#v", batch.Files[1])
	}

	batchedA := findingsForFile(batch.Findings, singleA.Findings[0].File)
	batchedB := findingsForFile(batch.Findings, singleB.Findings[0].File)
	if !reflect.DeepEqual(batchedA, singleA.Findings) {
		t.Fatalf("A findings differ:\nbatch=%#v\nsingle=%#v", batchedA, singleA.Findings)
	}
	if !reflect.DeepEqual(batchedB, singleB.Findings) {
		t.Fatalf("B findings differ:\nbatch=%#v\nsingle=%#v", batchedB, singleB.Findings)
	}
}

func findingsForFile(findings []Finding, file string) []Finding {
	out := make([]Finding, 0, len(findings))
	for _, finding := range findings {
		if finding.File == file {
			out = append(out, finding)
		}
	}
	return out
}
