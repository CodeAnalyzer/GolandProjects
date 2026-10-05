//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store/testutil"
)

func TestDescriptionSearchSchemaColumnsAndIndexes(t *testing.T) {
	db := testutil.Open(t)
	requiredColumns := []struct {
		table string
		col   string
	}{
		{"sql_procedures", "description"},
		{"sql_procedures", "search_vector"},
		{"api_contracts", "search_vector"},
	}
	for _, tc := range requiredColumns {
		var exists bool
		if err := db.QueryRow(`
			SELECT EXISTS(SELECT 1 FROM information_schema.columns
			WHERE table_name = $1 AND column_name = $2)`, tc.table, tc.col).Scan(&exists); err != nil {
			t.Fatalf("check column %s.%s: %v", tc.table, tc.col, err)
		}
		if !exists {
			t.Fatalf("missing column %s.%s", tc.table, tc.col)
		}
	}

	requiredIndexes := []string{
		"idx_sql_procedures_fts",
		"idx_api_contracts_fts",
		"idx_sql_procedures_description_trgm",
	}
	for _, name := range requiredIndexes {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)`, name).Scan(&exists); err != nil {
			t.Fatalf("check index %s: %v", name, err)
		}
		if !exists {
			t.Fatalf("missing index %s", name)
		}
	}
}

func TestBackfillAPIContractSearchVectors(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	var scanRunID, fileID, contractID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/API_CCred_BindClassifier.xml', 'API_CCred_BindClassifier.xml', '.xml', 'hash', NOW()) RETURNING id`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO api_contracts (file_id, contract_name, contract_kind, short_description, full_description)
		VALUES ($1, 'API_CCred_BindClassifier', 'service',
			'Привязка кредитных договоров к классификатору',
			'Метод осуществляет привязку кредитных договоров к классификатору.')
		RETURNING id`, fileID).Scan(&contractID); err != nil {
		t.Fatalf("insert contract: %v", err)
	}

	// Бэкфилл заполняет вектор без переиндексации
	if err := db.BackfillAPIContractSearchVectors(ctx); err != nil {
		t.Fatalf("BackfillAPIContractSearchVectors: %v", err)
	}
	var vectorText string
	if err := db.QueryRow(`SELECT search_vector::text FROM api_contracts WHERE id = $1`, contractID).Scan(&vectorText); err != nil {
		t.Fatalf("read vector: %v", err)
	}
	if vectorText == "" {
		t.Fatal("search_vector is empty after backfill")
	}
	if err := db.QueryRow(`
		SELECT count(*) FROM api_contracts
		WHERE id = $1 AND search_vector @@ plainto_tsquery('russian', 'классификатор')`, contractID).Scan(&vectorText); err != nil {
		t.Fatalf("match query: %v", err)
	}
	if vectorText == "0" {
		t.Fatal("описание не находится полнотекстовым запросом после бэкфилла")
	}

	// Идемпотентность: повторный бэкфилл не трогает заполненные векторы
	if _, err := db.Exec(`UPDATE api_contracts SET search_vector = to_tsvector('simple', 'sentinel') WHERE id = $1`, contractID); err != nil {
		t.Fatalf("set sentinel vector: %v", err)
	}
	if err := db.BackfillAPIContractSearchVectors(ctx); err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	if err := db.QueryRow(`SELECT search_vector::text FROM api_contracts WHERE id = $1`, contractID).Scan(&vectorText); err != nil {
		t.Fatalf("read sentinel vector: %v", err)
	}
	if vectorText != "'sentinel':1" {
		t.Fatalf("sentinel vector was overwritten: %q", vectorText)
	}
}

func TestEnsureDescriptionSearchVectors_ProcedureNameOverDescription(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	var scanRunID, fileID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/proc.sql', 'proc.sql', '.sql', 'hash', NOW()) RETURNING id`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	procedures := []*model.SQLProcedure{
		{FileID: fileID, ProcName: "BindClassifier_Main", Description: "обычная процедура без ключевых слов"},
		{FileID: fileID, ProcName: "Unrelated_Proc", Description: "здесь упоминается классификатор в описании"},
	}
	if err := db.BatchInsertSQLProcedures(ctx, procedures, 100); err != nil {
		t.Fatalf("BatchInsertSQLProcedures: %v", err)
	}
	if err := db.EnsureDescriptionSearchVectors(ctx, fileID); err != nil {
		t.Fatalf("EnsureDescriptionSearchVectors: %v", err)
	}

	var nameRank, descRank float64
	if err := db.QueryRow(`
		SELECT ts_rank(search_vector, plainto_tsquery('russian', 'классификатор'))
		FROM sql_procedures WHERE proc_name = 'BindClassifier_Main'`).Scan(&nameRank); err != nil {
		t.Fatalf("name proc rank: %v", err)
	}
	if err := db.QueryRow(`
		SELECT ts_rank(search_vector, plainto_tsquery('russian', 'классификатор'))
		FROM sql_procedures WHERE proc_name = 'Unrelated_Proc'`).Scan(&descRank); err != nil {
		t.Fatalf("desc proc rank: %v", err)
	}
	if nameRank >= descRank {
		t.Fatalf("имя и описание в разных записях: name=%v desc=%v — веса A/B не применены", nameRank, descRank)
	}
}

func TestPublishDescLSAGeneration_Rotation(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	publish := func(generation string, entityID int64) {
		t.Helper()
		terms := []model.SpecVocabTerm{{Generation: generation, Term: "term_" + generation, DocFreq: 1, IDF: 1}}
		embeddings := []model.DescEmbedding{{
			Generation: generation, EntityType: "procedure", EntityID: entityID,
			EmbedText: "text", Embedding: []float64{0.1, 0.2}, EmbedMethod: "tfidf-lsa", EmbedDim: 2,
		}}
		if err := db.PublishDescLSAGeneration(ctx, generation, terms, embeddings); err != nil {
			t.Fatalf("publish %s: %v", generation, err)
		}
	}
	publish("G1", 1)
	publish("G2", 2)
	publish("G3", 3)
	if err := db.DeleteDescLSAGenerationsExcept(ctx, "G3", "G2"); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT count(DISTINCT generation) FROM desc_embeddings`).Scan(&count); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if count != 2 {
		t.Fatalf("generations = %d, want 2 (текущее+предыдущее)", count)
	}
	var g1Exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM desc_embeddings WHERE generation = 'G1')`).Scan(&g1Exists); err != nil {
		t.Fatalf("check G1: %v", err)
	}
	if g1Exists {
		t.Fatal("G1 должна быть удалена ротацией")
	}
	hasG3, err := db.HasDescLSAGeneration(ctx, "G3")
	if err != nil {
		t.Fatalf("HasDescLSAGeneration: %v", err)
	}
	if !hasG3 {
		t.Fatal("HasDescLSAGeneration(G3) = false, want true")
	}
}

func TestBatchInsertSQLProcedures_StoresDescription(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	var scanRunID, fileID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/ReturnCashFund_Insert.sql', 'ReturnCashFund_Insert.sql', '.sql', 'hash', NOW()) RETURNING id`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	procedures := []*model.SQLProcedure{
		{FileID: fileID, ProcName: "ReturnCashFund_Insert", Description: "возврат сумм из Банка-партнера"},
		{FileID: fileID, ProcName: "NoDesc_Proc"},
	}
	if err := db.BatchInsertSQLProcedures(ctx, procedures, 100); err != nil {
		t.Fatalf("BatchInsertSQLProcedures: %v", err)
	}
	var desc string
	if err := db.QueryRow(`SELECT description FROM sql_procedures WHERE proc_name = 'ReturnCashFund_Insert'`).Scan(&desc); err != nil {
		t.Fatalf("read description: %v", err)
	}
	if desc != "возврат сумм из Банка-партнера" {
		t.Fatalf("description = %q", desc)
	}
	if err := db.QueryRow(`SELECT coalesce(description,'') FROM sql_procedures WHERE proc_name = 'NoDesc_Proc'`).Scan(&desc); err != nil {
		t.Fatalf("read empty description: %v", err)
	}
	if desc != "" {
		t.Fatalf("empty description = %q, want ''", desc)
	}
}
