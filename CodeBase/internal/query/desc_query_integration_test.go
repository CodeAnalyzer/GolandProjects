//go:build integration

package query

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/specfts"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func setupDescSearchFixture(t *testing.T) (*store.DB, context.Context) {
	t.Helper()
	db := testutil.Open(t)
	ctx := context.Background()

	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	insertDescFile := func(path string) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
			VALUES ($1, $2, $2, '.sql', 'hash-' || $2, NOW()) RETURNING id`, scanID, path).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	insertProc := func(fileID int64, name, description string) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRow(`
			INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end, description)
			VALUES ($1, $2, 1, 2, $3) RETURNING id`, fileID, name, description).Scan(&id); err != nil {
			t.Fatalf("insert proc %s: %v", name, err)
		}
		return id
	}
	insertContract := func(fileID int64, name, kind, shortDesc, fullDesc string) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRow(`
			INSERT INTO api_contracts (file_id, contract_name, contract_kind, short_description, full_description, line_start, line_end)
			VALUES ($1, $2, $3, $4, $5, 1, 1) RETURNING id`, fileID, name, kind, shortDesc, fullDesc).Scan(&id); err != nil {
			t.Fatalf("insert contract %s: %v", name, err)
		}
		return id
	}

	sqlFile := insertDescFile("Server/Accrual/ReturnCashFund_Insert.sql")
	xmlFile := insertDescFile("DSArchitectData/BObject/ContractCredit/API_CCred_BindClassifier.xml")
	extraFile := insertDescFile("Server/Misc/Unrelated_Proc.sql")

	_ = insertProc(sqlFile, "ReturnCashFund_Insert",
		"ReturnCashFund_Insert - возврат сумм из Банка-партнера на счет клиента\n\n@ContractID - идентификатор договора")
	_ = insertProc(extraFile, "Unrelated_Proc",
		"здесь упоминается классификатор в тексте описания процедуры")
	// Совпадение ключевого слова в имени (вес A) — для проверки весов
	_ = insertProc(extraFile, "Проверка классификатора", "")
	bindClassifierProc := insertProc(xmlFile, "API_CCred_BindClassifier",
		"Метод осуществляет привязку кредитных договоров к классификатору.")
	_ = insertContract(xmlFile, "API_CCred_BindClassifier", "service",
		"Привязка кредитных договоров к классификатору",
		"Метод осуществляет привязку кредитных договоров к классификатору.")

	// Связь implements_contract: пара «контракт вытесняет процедуру»
	var contractID int64
	if err := db.QueryRow(`SELECT id FROM api_contracts WHERE contract_name = 'API_CCred_BindClassifier'`).Scan(&contractID); err != nil {
		t.Fatalf("read contract id: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type)
		VALUES ('sql_procedure', $1, 'api_contract', $2, 'implements_contract')`, bindClassifierProc, contractID); err != nil {
		t.Fatalf("insert relation: %v", err)
	}

	// Векторы: через штатный per-file ensure
	for _, fid := range []int64{sqlFile, xmlFile, extraFile} {
		if err := db.EnsureDescriptionSearchVectors(ctx, fid); err != nil {
			t.Fatalf("ensure vectors: %v", err)
		}
	}
	return db, ctx
}

func TestSearchDescriptions_HitByContractDescription(t *testing.T) {
	db, ctx := setupDescSearchFixture(t)
	q := New(db)

	items, hasMore, err := q.SearchDescriptions(ctx, "привязка кредитных договоров к классификатору", nil, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions: %v", err)
	}
	if hasMore {
		t.Fatal("hasMore = true, want false")
	}
	if len(items) == 0 {
		t.Fatal("пустая выдача, ожидается контракт")
	}
	if items[0].Name != "API_CCred_BindClassifier" {
		t.Fatalf("top = %s, want API_CCred_BindClassifier", items[0].Name)
	}
	if items[0].Kind != "service" {
		t.Fatalf("kind = %s, want service", items[0].Kind)
	}
	// Дедуп: реализующая процедура вытеснена контрактом
	for _, it := range items {
		if it.Kind == "procedure" && it.Name == "API_CCred_BindClassifier" {
			t.Fatal("процедура-реализация не вытеснена контрактом")
		}
	}
}

func TestSearchDescriptions_HitByProcedureDescription(t *testing.T) {
	db, ctx := setupDescSearchFixture(t)
	q := New(db)

	items, _, err := q.SearchDescriptions(ctx, "возврат сумм из Банка-партнера", nil, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("пустая выдача, ожидается процедура")
	}
	if items[0].Name != "ReturnCashFund_Insert" {
		t.Fatalf("top = %s, want ReturnCashFund_Insert", items[0].Name)
	}
	if items[0].Kind != "procedure" {
		t.Fatalf("kind = %s, want procedure", items[0].Kind)
	}
	if items[0].Snippet == "" {
		t.Fatal("сниппет пуст")
	}
	if items[0].Source != "exact" {
		t.Fatalf("source = %s, want exact", items[0].Source)
	}
}

func TestSearchDescriptions_NameRanksAboveDescription(t *testing.T) {
	db, ctx := setupDescSearchFixture(t)
	q := New(db)

	// Прямая проверка весов A/B: одна запись с совпадением в имени,
	// другая — только в описании
	var nameRank, descRank float64
	if err := db.QueryRow(`
		SELECT ts_rank(search_vector, plainto_tsquery('russian', 'классификатор'))
		FROM sql_procedures WHERE proc_name = $1`, "Проверка классификатора").Scan(&nameRank); err != nil {
		t.Fatalf("name rank: %v", err)
	}
	if err := db.QueryRow(`
		SELECT ts_rank(search_vector, plainto_tsquery('russian', 'классификатор'))
		FROM sql_procedures WHERE proc_name = 'Unrelated_Proc'`).Scan(&descRank); err != nil {
		t.Fatalf("desc rank: %v", err)
	}
	if nameRank <= descRank {
		t.Fatalf("имя (%v) должно ранжироваться выше описания (%v)", nameRank, descRank)
	}

	items, _, err := q.SearchDescriptions(ctx, "классификатор", nil, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions: %v", err)
	}
	if len(items) < 2 {
		t.Fatalf("выдача = %d записей, want >= 2", len(items))
	}
	if items[0].Name != "Проверка классификатора" {
		t.Fatalf("top = %s, want запись с совпадением по имени", items[0].Name)
	}
	if items[0].Rank <= items[1].Rank {
		t.Fatalf("rank имени (%v) <= rank описания (%v)", items[0].Rank, items[1].Rank)
	}
}

func TestSearchDescriptions_KindFilter(t *testing.T) {
	db, ctx := setupDescSearchFixture(t)
	q := New(db)

	items, _, err := q.SearchDescriptions(ctx, "классификатор", []string{"procedure"}, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions(procedure): %v", err)
	}
	for _, it := range items {
		if it.Kind != "procedure" {
			t.Fatalf("kind = %s, want только procedure", it.Kind)
		}
	}
	if len(items) == 0 {
		t.Fatal("пустая выдача для kind=procedure, ожидается Unrelated_Proc")
	}

	items, _, err = q.SearchDescriptions(ctx, "классификатор", []string{"service", "event"}, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions(service,event): %v", err)
	}
	for _, it := range items {
		if it.Kind != "service" && it.Kind != "event" {
			t.Fatalf("kind = %s, want только service/event", it.Kind)
		}
	}
	if len(items) == 0 {
		t.Fatal("пустая выдача для kind=service,event, ожидается контракт")
	}
}

func TestSearchDescriptions_FallbackWhenContractDescriptionEmpty(t *testing.T) {
	db, ctx := setupDescSearchFixture(t)
	q := New(db)

	// Опустошаем описание контракта — пара должна показать процедуру
	if _, err := db.Exec(`UPDATE api_contracts SET short_description = NULL, full_description = NULL WHERE contract_name = 'API_CCred_BindClassifier'`); err != nil {
		t.Fatalf("clear contract descriptions: %v", err)
	}
	if _, err := db.Exec(`UPDATE api_contracts SET search_vector = NULL WHERE contract_name = 'API_CCred_BindClassifier'`); err != nil {
		t.Fatalf("reset contract vector: %v", err)
	}
	if err := db.BackfillAPIContractSearchVectors(ctx); err != nil {
		t.Fatalf("rebackfill: %v", err)
	}

	items, _, err := q.SearchDescriptions(ctx, "привязка кредитных договоров к классификатору", nil, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions: %v", err)
	}
	found := false
	for _, it := range items {
		if it.Kind == "procedure" && it.Name == "API_CCred_BindClassifier" {
			found = true
		}
	}
	if !found {
		t.Fatalf("fallback не сработал: процедура должна появиться при пустом описании контракта, выдача: %+v", items)
	}
}

func TestSearchDescriptions_LimitAndTruncation(t *testing.T) {
	db, ctx := setupDescSearchFixture(t)
	q := New(db)

	items, hasMore, err := q.SearchDescriptions(ctx, "классификатор", nil, 1)
	if err != nil {
		t.Fatalf("SearchDescriptions: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	if !hasMore {
		t.Fatal("hasMore = false, want true (совпадений больше лимита)")
	}
}

func TestSearchDescriptions_EmptyResult(t *testing.T) {
	db, ctx := setupDescSearchFixture(t)
	q := New(db)

	items, hasMore, err := q.SearchDescriptions(ctx, "xyzzy qwerty фырц", nil, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("items = %d, want 0", len(items))
	}
	if hasMore {
		t.Fatal("hasMore = true, want false")
	}
	if items == nil {
		t.Fatal("items = nil, want пустой слайс")
	}
}

func TestValidateDescriptionSearchKinds(t *testing.T) {
	if _, err := ValidateDescriptionSearchKinds([]string{"Procedure", "EVENT"}); err != nil {
		t.Fatalf("valid kinds rejected: %v", err)
	}
	if _, err := ValidateDescriptionSearchKinds([]string{"bogus"}); err == nil {
		t.Fatal("bogus kind accepted")
	}
	got, err := ValidateDescriptionSearchKinds(nil)
	if err != nil || got != nil {
		t.Fatalf("nil kinds: %v %v", got, err)
	}
}

// trainDescLSAForTest собирает и публикует desc-LSA-модель напрямую через
// specfts (мини-корпус, K=2) — без участия индексера.
func trainDescLSAForTest(t *testing.T, db *store.DB) {
	t.Helper()
	ctx := context.Background()

	rows, err := db.LoadDescriptionsForLSA(ctx)
	if err != nil {
		t.Fatalf("LoadDescriptionsForLSA: %v", err)
	}
	if len(rows) < 4 {
		t.Fatalf("corpus = %d docs, want >= 4", len(rows))
	}
	docs := make([]specfts.Document, len(rows))
	for i, row := range rows {
		docs[i] = specfts.Document{ID: row.ID, Text: specfts.LSADocText(row.Name, "", "", row.Description)}
	}
	vocab := specfts.BuildVocab(docs, 1, 1)
	tfidf := vocab.TFIDFMatrix(docs)
	u, s, vt, err := specfts.SVD(tfidf, 2)
	if err != nil {
		t.Fatalf("SVD: %v", err)
	}
	generation := specfts.CorpusFingerprint(docs, specfts.LSAParams{MinDF: 1, MaxDF: 1, K: 2})
	terms := make([]model.SpecVocabTerm, len(vocab.Terms))
	for i, term := range vocab.Terms {
		terms[i] = model.SpecVocabTerm{Generation: generation, Term: term, DocFreq: vocab.DocFreq[i], IDF: vocab.IDF[i]}
	}
	embeddings := make([]model.DescEmbedding, len(rows))
	for i, row := range rows {
		embeddings[i] = model.DescEmbedding{
			Generation: generation, EntityType: row.EntityType, EntityID: row.ID,
			EmbedText: specfts.LSADocText(row.Name, "", "", row.Description),
			Embedding: specfts.DocEmbedding(u, s, i), EmbedMethod: "tfidf-lsa", EmbedDim: len(s),
		}
	}
	if err := db.PublishDescLSAGeneration(ctx, generation, terms, embeddings); err != nil {
		t.Fatalf("PublishDescLSAGeneration: %v", err)
	}
	lsaModel := &specfts.LSAModel{Generation: generation, Vocab: vocab, VT: vt, Singulars: s, K: len(s), NumDocs: len(rows), NumTerms: len(vocab.Terms)}
	tempPath, err := specfts.WriteLSAModelTemp(lsaModel, config.DescLSAModelPath())
	if err != nil {
		t.Fatalf("WriteLSAModelTemp: %v", err)
	}
	if err := specfts.ActivateLSAModel(tempPath, config.DescLSAModelPath()); err != nil {
		t.Fatalf("ActivateLSAModel: %v", err)
	}
}

// setupDescLSAEnv — фикстура + конфиг desc-LSA (sidecar во временном каталоге).
func setupDescLSAEnv(t *testing.T) (*store.DB, context.Context) {
	t.Helper()
	db, ctx := setupDescSearchFixture(t)

	oldConfigFile := config.GetConfigFile()
	oldCfg := config.Get()
	var oldCfgCopy config.Config
	if oldCfg != nil {
		oldCfgCopy = *oldCfg
	}
	root := t.TempDir()
	config.CreateDefault(root)
	cfg := config.Get()
	descEnabled := true
	cfg.DescLSA.LSAEnabled = &descEnabled
	cfg.DescLSA.LSAMinDF = 1
	cfg.DescLSA.LSAMaxDF = 1
	cfg.DescLSA.LSAK = 2
	cfg.DescLSA.LSAMinCorpus = 1
	minCosine := 0.0
	cutoff := 0.0
	cfg.DescLSA.LSAMinCosine = &minCosine
	cfg.DescLSA.LSARelativeCutoff = &cutoff
	config.SetConfigFile(filepath.Join(root, "codebase.toml"))
	t.Cleanup(func() {
		config.SetConfigFile(oldConfigFile)
		if current := config.Get(); current != nil {
			if oldCfg != nil {
				*current = oldCfgCopy
			}
		}
	})
	return db, ctx
}

// Семантический хит: запрос «возврат классификатор» не проходит exact-слой
// (plainto требует оба термина в одном документе), но LSA находит процедуры
// по латентной близости.
func TestSearchDescriptions_SemanticHit(t *testing.T) {
	db, ctx := setupDescLSAEnv(t)
	trainDescLSAForTest(t, db)
	q := New(db)

	items, _, err := q.SearchDescriptions(ctx, "возврат классификатор", nil, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions: %v", err)
	}
	semanticFound := false
	for _, it := range items {
		if it.Source == "semantic" {
			semanticFound = true
			if it.Rank <= 0 {
				t.Fatalf("semantic rank = %v, want > 0", it.Rank)
			}
		}
	}
	if !semanticFound {
		t.Fatalf("semantic-хиты отсутствуют, выдача: %+v", items)
	}
}

// Объединение слоёв: сущность, найденная в обоих, представлена один раз с
// источником exact; дедуп «контракт вытесняет процедуру» действует и на
// semantic-слой.
func TestSearchDescriptions_MergeExactAndSemantic(t *testing.T) {
	db, ctx := setupDescLSAEnv(t)
	trainDescLSAForTest(t, db)
	q := New(db)

	items, _, err := q.SearchDescriptions(ctx, "классификатор", nil, 20)
	if err != nil {
		t.Fatalf("SearchDescriptions: %v", err)
	}
	seen := map[string]int{}
	for _, it := range items {
		key := it.Kind + "|" + it.Name
		seen[key]++
		if seen[key] > 1 {
			t.Fatalf("дубль в объединённой выдаче: %s", key)
		}
		if it.Kind == "procedure" && it.Name == "API_CCred_BindClassifier" {
			t.Fatal("процедура-реализация не вытеснена контрактом (semantic-слой)")
		}
	}
	if len(items) == 0 {
		t.Fatal("пустая выдача")
	}
}

// Fallback: без файла модели поиск возвращает exact-хиты без ошибки.
func TestSearchDescriptions_FallbackWithoutModel(t *testing.T) {
	db, ctx := setupDescLSAEnv(t)
	trainDescLSAForTest(t, db)
	q := New(db)

	// Убираем sidecar-модель
	if err := os.Remove(config.DescLSAModelPath()); err != nil {
		t.Fatalf("remove model: %v", err)
	}

	items, _, err := q.SearchDescriptions(ctx, "классификатор", nil, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions без модели: %v", err)
	}
	for _, it := range items {
		if it.Source != "exact" {
			t.Fatalf("source = %s, want только exact без модели", it.Source)
		}
	}
	if len(items) == 0 {
		t.Fatal("пустая выдача в fallback-режиме, ожидается exact-хит")
	}
}
