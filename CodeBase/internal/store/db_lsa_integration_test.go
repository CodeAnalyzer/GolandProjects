//go:build integration

package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedLSACaps заполняет files → spec_configs → spec_capabilities → spec_requirements →
// spec_scenarios минимальным набором для проверки загрузчика LSA-корпуса.
// Возвращает id capability.
func seedLSACaps(t *testing.T, db *store.DB) int64 {
	t.Helper()
	ctx := context.Background()

	var scanRunID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO scan_runs (root_path, status) VALUES ('/test/lsa', 'ok') RETURNING id`,
	).Scan(&scanRunID); err != nil {
		t.Fatalf("seed scan_run: %v", err)
	}

	var fileID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at)
		VALUES ($1, '/test/lsa/specs/cap/spec.md', 'specs/cap/spec.md', '.md', 100, 'h1', NOW())
		RETURNING id`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	var cfgID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'test-product') RETURNING id`,
		fileID).Scan(&cfgID); err != nil {
		t.Fatalf("seed spec_config: %v", err)
	}

	var capID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title, purpose, notes)
		VALUES ($1, $2, 'cap', 'Заголовок', NULL, 'Примечание') RETURNING id`,
		fileID, cfgID).Scan(&capID); err != nil {
		t.Fatalf("seed capability: %v", err)
	}

	// Два требования с req_order 2 и 1 — вставляем в обратном порядке,
	// чтобы проверить сортировку string_agg по req_order.
	reqs := []struct {
		name  string
		body  string
		order int
	}{
		{"Второе требование", "Текст второго", 2},
		{"Первое требование", "Текст первого", 1},
	}
	reqIDs := make(map[string]int64, len(reqs))
	for _, r := range reqs {
		var id int64
		if err := db.QueryRowContext(ctx, `
			INSERT INTO spec_requirements (file_id, capability_id, requirement_name, body_text, req_order)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			fileID, capID, r.name, r.body, r.order).Scan(&id); err != nil {
			t.Fatalf("seed requirement %s: %v", r.name, err)
		}
		reqIDs[r.name] = id
	}

	// Сценарии: у второго требования scn_order 2 и 1 (вставка в обратном порядке),
	// у первого — сценарий с NULL-текстами (проверка concat_ws).
	type scn struct {
		req   string
		name  string
		given interface{}
		when  interface{}
		then  interface{}
		order int
	}
	scns := []scn{
		{"Второе требование", "Сценарий бэта", "дано бэта", "когда бэта", "тогда бэта", 2},
		{"Второе требование", "Сценарий альфа", "дано альфа", "когда альфа", "тогда альфа", 1},
		{"Первое требование", "Сценарий с NULL", "дано NULL", nil, nil, 1},
	}
	for _, s := range scns {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO spec_scenarios (file_id, requirement_id, scenario_name, given_text, when_text, then_text, scn_order)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			fileID, reqIDs[s.req], s.name, s.given, s.when, s.then, s.order); err != nil {
			t.Fatalf("seed scenario %s: %v", s.name, err)
		}
	}
	return capID
}

// TestLoadAllSpecCapabilitiesWithReqsForLSA — детерминированная агрегация:
// полнота (все req/scn), порядок (req_order, scn_order), NULL-безопасность.
func TestLoadAllSpecCapabilitiesWithReqsForLSA(t *testing.T) {
	db := testutil.Open(t)
	capID := seedLSACaps(t, db)

	caps, err := db.LoadAllSpecCapabilitiesWithReqsForLSA(context.Background())
	if err != nil {
		t.Fatalf("LoadAllSpecCapabilitiesWithReqsForLSA: %v", err)
	}
	if len(caps) != 1 {
		t.Fatalf("expected 1 capability, got %d", len(caps))
	}
	c := caps[0]
	if c.ID != capID {
		t.Errorf("capability id = %d, want %d", c.ID, capID)
	}
	if c.Title != "Заголовок" || c.Purpose != "" || c.Notes != "Примечание" {
		t.Errorf("fields: title=%q purpose=%q notes=%q", c.Title, c.Purpose, c.Notes)
	}

	// Порядок требований: req_order 1, затем 2. Сценарии: req1/scn1 (с NULL),
	// затем req2/scn1, req2/scn2 — как в SQL ORDER BY req.req_order, s.scn_order, s.id.
	want := "Первое требование Текст первого Сценарий с NULL дано NULL " +
		"Второе требование Текст второго Сценарий альфа дано альфа когда альфа тогда альфа " +
		"Сценарий бэта дано бэта когда бэта тогда бэта"
	if c.LSAText != want {
		t.Errorf("LSAText mismatch:\n got: %q\nwant: %q", c.LSAText, want)
	}
	if strings.Contains(c.LSAText, "  ") {
		t.Errorf("LSAText contains double spaces (NULL handling broken): %q", c.LSAText)
	}

	// Детерминизм: повторная загрузка даёт тот же текст
	again, err := db.LoadAllSpecCapabilitiesWithReqsForLSA(context.Background())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if again[0].LSAText != c.LSAText {
		t.Errorf("aggregation not deterministic: %q vs %q", again[0].LSAText, c.LSAText)
	}
}

func TestPublishSpecLSAGeneration(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	var scanRunID, fileID, configID, capabilityID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'completed') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/spec.md', 'spec.md', '.md', 'hash', NOW()) RETURNING id`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'product') RETURNING id`, fileID).Scan(&configID); err != nil {
		t.Fatalf("insert spec config: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title)
		VALUES ($1, $2, 'capability', 'Capability') RETURNING id`, fileID, configID).Scan(&capabilityID); err != nil {
		t.Fatalf("insert capability: %v", err)
	}

	terms := []model.SpecVocabTerm{{Term: "term", DocFreq: 1, IDF: 1}}
	embeddings := []model.SpecEmbedding{{SpecID: capabilityID, EmbedLevel: "spec", EmbedText: "text", Embedding: []float64{1}, EmbedMethod: "tfidf-lsa", EmbedDim: 1}}
	if err := db.PublishSpecLSAGeneration(ctx, "gen-current", terms, embeddings); err != nil {
		t.Fatalf("publish current generation: %v", err)
	}
	if ok, err := db.HasSpecLSAGeneration(ctx, "gen-current"); err != nil || !ok {
		t.Fatalf("current generation exists = %v, err=%v", ok, err)
	}

	invalidEmbeddings := []model.SpecEmbedding{{SpecID: capabilityID + 1000, EmbedLevel: "spec", EmbedText: "invalid", Embedding: []float64{1}, EmbedMethod: "tfidf-lsa", EmbedDim: 1}}
	if err := db.PublishSpecLSAGeneration(ctx, "gen-invalid", terms, invalidEmbeddings); err == nil {
		t.Fatal("invalid embedding SpecID must fail")
	}
	var vocabCount, embeddingCount int
	if err := db.QueryRow(`SELECT count(*) FROM spec_vocab WHERE generation = 'gen-invalid'`).Scan(&vocabCount); err != nil {
		t.Fatalf("count invalid vocab: %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM spec_embeddings WHERE generation = 'gen-invalid'`).Scan(&embeddingCount); err != nil {
		t.Fatalf("count invalid embeddings: %v", err)
	}
	if vocabCount != 0 || embeddingCount != 0 {
		t.Fatalf("rollback left invalid generation rows: vocab=%d embeddings=%d", vocabCount, embeddingCount)
	}

	for _, generation := range []string{"gen-previous", "gen-old"} {
		if err := db.PublishSpecLSAGeneration(ctx, generation, terms, embeddings); err != nil {
			t.Fatalf("publish %s: %v", generation, err)
		}
	}
	if err := db.DeleteSpecLSAGenerationsExcept(ctx, "gen-current", "gen-previous"); err != nil {
		t.Fatalf("delete stale generations: %v", err)
	}
	for _, tc := range []struct {
		generation string
		want       bool
	}{
		{generation: "gen-current", want: true},
		{generation: "gen-previous", want: true},
		{generation: "gen-old", want: false},
	} {
		got, err := db.HasSpecLSAGeneration(ctx, tc.generation)
		if err != nil {
			t.Fatalf("check %s: %v", tc.generation, err)
		}
		if got != tc.want {
			t.Fatalf("generation %s exists = %v, want %v", tc.generation, got, tc.want)
		}
	}
}
