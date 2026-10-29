//go:build integration

package indexer

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// TestPostProcessSQLCallsResolveInSourceBranchScope — сценарии дельты
// relations-postprocessing: вызов резолвится в копию ветки источника; имя,
// существующее только в чужой ветке того же продукта, остаётся неразрешённым.
func TestPostProcessSQLCallsResolveInSourceBranchScope(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	productID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-scope")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName fa-scope: %v", err)
	}
	otherProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-scope-other")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName fa-scope-other: %v", err)
	}

	insertBranch := func(product int64, name, key, path string, isDefault bool) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			product, name, key, path, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", key, err)
		}
		return id
	}
	devBranch := insertBranch(productID, "develop", "develop", "/repo/fa-scope/develop", true)
	spBranch := insertBranch(productID, "FA-Scope-11-01-7-SP", "fa-scope-11-01-7", "/repo/fa-scope-11017", false)
	otherBranch := insertBranch(otherProductID, "develop", "develop", "/repo/fa-scope-other/develop", true)

	insertFile := func(path string, product, branch int64) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, hash_sha256, modified_at)
			VALUES ($1, $2, NULLIF($3, 0), $4, $4, 'sql', 'h', NOW()) RETURNING id`,
			scanID, product, branch, path).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	insertProc := func(fileID int64, name string) int64 {
		var id int64
		if err := db.QueryRow(
			`INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end) VALUES ($1, $2, 1, 100) RETURNING id`,
			fileID, name).Scan(&id); err != nil {
			t.Fatalf("insert procedure %s: %v", name, err)
		}
		return id
	}

	// Копии целей: MyProc в develop и SP (SP позже — больший id), OnlySP —
	// только в SP, UtilX — только в другом продукте.
	devTargetsFile := insertFile("/repo/fa-scope/develop/targets.sql", productID, devBranch)
	spTargetsFile := insertFile("/repo/fa-scope-11017/targets.sql", productID, spBranch)
	otherFile := insertFile("/repo/fa-scope-other/develop/util.sql", otherProductID, otherBranch)
	devMyProcID := insertProc(devTargetsFile, "SCOPE_MyProc")
	spOnlyID := insertProc(spTargetsFile, "SCOPE_OnlySP")
	spMyProcID := insertProc(spTargetsFile, "SCOPE_MyProc")
	otherUtilID := insertProc(otherFile, "SCOPE_UtilX")

	// Источники: вызывающая процедура в develop и своя в SP.
	devCallerFile := insertFile("/repo/fa-scope/develop/caller.sql", productID, devBranch)
	spCallerFile := insertFile("/repo/fa-scope-11017/caller.sql", productID, spBranch)
	devCallerID := insertProc(devCallerFile, "SCOPE_DevCaller")
	spCallerID := insertProc(spCallerFile, "SCOPE_SpCaller")

	idx := &Indexer{
		db:          db,
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
		config: &config.Config{Indexer: config.IndexerConfig{
			Parallel:  1,
			BatchSize: 100,
		}},
	}
	idx.addPendingSQLCalls(devCallerFile, "caller.sql",
		[]*model.SQLProcedure{{ProcName: "SCOPE_DevCaller", LineStart: 1, LineEnd: 100}},
		map[string]int64{"scope_devcaller": devCallerID},
		[]*model.SQLProcedureCall{
			{CallerName: "SCOPE_DevCaller", CalleeName: "SCOPE_MyProc", LineNumber: 10},
			{CallerName: "SCOPE_DevCaller", CalleeName: "SCOPE_OnlySP", LineNumber: 20},
			{CallerName: "SCOPE_DevCaller", CalleeName: "SCOPE_UtilX", LineNumber: 30},
		})
	idx.addPendingSQLCalls(spCallerFile, "caller.sql",
		[]*model.SQLProcedure{{ProcName: "SCOPE_SpCaller", LineStart: 1, LineEnd: 100}},
		map[string]int64{"scope_spcaller": spCallerID},
		[]*model.SQLProcedureCall{
			{CallerName: "SCOPE_SpCaller", CalleeName: "SCOPE_MyProc", LineNumber: 10},
			{CallerName: "SCOPE_SpCaller", CalleeName: "SCOPE_OnlySP", LineNumber: 20},
		})

	collector := &statsCollector{}
	idx.postProcessSQLProcedureCallRelations(ctx, collector, 1)

	// Проверка рёбер: (источник, цель, линия).
	targetOf := func(sourceID int64) map[int64]int64 {
		rows, err := db.Query(`
			SELECT target_id, line_number FROM relations
			WHERE relation_type='calls_procedure' AND source_type='sql_procedure' AND source_id=$1`,
			sourceID)
		if err != nil {
			t.Fatalf("select relations for %d: %v", sourceID, err)
		}
		defer rows.Close()
		result := map[int64]int64{}
		for rows.Next() {
			var targetID, line int64
			if err := rows.Scan(&targetID, &line); err != nil {
				t.Fatalf("scan relation: %v", err)
			}
			result[targetID] = line
		}
		return result
	}

	// Develop-источник: MyProc → develop-копия; OnlySP НЕ резолвится (чужая
	// ветка того же продукта); UtilX → другой продукт (третий ярус).
	devEdges := targetOf(devCallerID)
	if devEdges[devMyProcID] == 0 {
		t.Fatalf("dev caller must call develop MyProc copy: %+v", devEdges)
	}
	if _, exists := devEdges[spMyProcID]; exists {
		t.Fatalf("dev caller must not call SP MyProc copy: %+v", devEdges)
	}
	if _, exists := devEdges[spOnlyID]; exists {
		t.Fatalf("dev caller must not resolve SCOPE_OnlySP: %+v", devEdges)
	}
	if devEdges[otherUtilID] == 0 {
		t.Fatalf("dev caller must call SCOPE_UtilX from other product: %+v", devEdges)
	}
	if len(devEdges) != 2 {
		t.Fatalf("dev caller edges = %d, want 2: %+v", len(devEdges), devEdges)
	}

	// SP-источник: MyProc → SP-копия (своя ветка), OnlySP резолвится.
	spEdges := targetOf(spCallerID)
	if spEdges[spMyProcID] == 0 {
		t.Fatalf("sp caller must call SP MyProc copy: %+v", spEdges)
	}
	if _, exists := spEdges[devMyProcID]; exists {
		t.Fatalf("sp caller must not call develop MyProc copy: %+v", spEdges)
	}
	if spEdges[spOnlyID] == 0 {
		t.Fatalf("sp caller must resolve SCOPE_OnlySP in its branch: %+v", spEdges)
	}
	if len(spEdges) != 2 {
		t.Fatalf("sp caller edges = %d, want 2: %+v", len(spEdges), spEdges)
	}

	// Batch-механика: обе группы (develop и SP) резолвились в одном прогоне.
	if n := countRelation(t, idx, "calls_procedure", "sql_procedure"); n != 4 {
		t.Fatalf("calls_procedure relations = %d, want 4", n)
	}
	_ = store.LookupScope{}
}
