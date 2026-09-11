package indexer

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/codebase/internal/parser/openspecmd"
	"github.com/codebase/internal/specfts"
)

// loadLSACorpus парсит синтетический корпус testdata/lsacorpus/specs/*/spec.md
// и строит документы как продакшн-конвейер: openspec-парсер → детерминированная
// агрегация req/scn (порядок как в SQL-загрузчике: req_order, scn_order) → LSADocText.
// Возвращает документы и метки (title) для человекочитаемых golden-файлов.
func loadLSACorpus(t *testing.T) ([]specfts.Document, []string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "lsacorpus", "specs", "*", "spec.md"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(files) < 120 {
		t.Fatalf("expected >=120 spec files (LSAMinCorpus=100), got %d", len(files))
	}
	sort.Strings(files)

	docs := make([]specfts.Document, 0, len(files))
	labels := make([]string, 0, len(files))
	for idx, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		parsed := openspecmd.ParseSpecFile(string(data))
		if parsed.Title == "" {
			t.Fatalf("%s: title not parsed", f)
		}
		if len(parsed.Requirements) == 0 {
			t.Fatalf("%s: requirements not parsed", f)
		}
		for _, req := range parsed.Requirements {
			if req.BodyText == "" {
				t.Fatalf("%s: requirement %q body empty", f, req.Name)
			}
			if len(req.Scenarios) == 0 {
				t.Fatalf("%s: requirement %q has no scenarios", f, req.Name)
			}
		}
		lsaText := aggregateLSAText(parsed)
		if lsaText == "" {
			t.Fatalf("%s: aggregated lsa text empty", f)
		}
		docs = append(docs, specfts.Document{
			ID:   int64(idx + 1),
			Text: specfts.LSADocText(parsed.Title, parsed.Purpose, parsed.Notes, lsaText),
		})
		labels = append(labels, parsed.Title)
	}
	return docs, labels
}

// aggregateLSAText повторяет детерминированную агрегацию SQL-загрузчика
// LoadAllSpecCapabilitiesWithReqsForLSA: имя+тело требования, затем имя+GWT сценариев.
func aggregateLSAText(p *openspecmd.ParsedSpec) string {
	parts := make([]string, 0, 8)
	for _, req := range p.Requirements {
		parts = append(parts, strings.TrimSpace(req.Name+" "+req.BodyText))
		for _, scn := range req.Scenarios {
			parts = append(parts, strings.TrimSpace(scn.Name+" "+scn.Given+" "+scn.When+" "+scn.Then))
		}
	}
	return strings.Join(parts, " ")
}

// TestLSADocComposition — состав документа: title×2, тексты требований и сценариев.
func TestLSADocComposition(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "lsacorpus", "specs", "cap-001", "spec.md"))
	if err != nil {
		t.Fatal(err)
	}
	parsed := openspecmd.ParseSpecFile(string(data))
	text := specfts.LSADocText(parsed.Title, parsed.Purpose, parsed.Notes, aggregateLSAText(parsed))

	counts := specfts.TokenizeToStemCounts(text)
	if counts["арест"] != 3 {
		t.Errorf("stem 'арест': expected count 3 (title×2 + 'ареста' в purpose), got %d", counts["арест"])
	}
	if counts["счет"] < 2 {
		t.Errorf("stem 'счет': expected >=2 (title×2 'счёта'→'счет' после ё→е), got %d", counts["счет"])
	}
	if counts["блокировк"] < 2 {
		t.Errorf("stem 'блокировк': expected >=2 (purpose + requirement), got %d", counts["блокировк"])
	}
	if !strings.Contains(text, "Арестованный счёт") {
		t.Errorf("requirement name missing in doc text: %q", text)
	}
	if !strings.Contains(text, "счёт клиента арестован") {
		t.Errorf("scenario given text missing in doc text")
	}
}

// TestLSAGolden — golden-тест обучения на синтетическом корпусе:
// словарь (состав терминов) и ранжирование контрольных запросов.
// Обновление golden: CODEBASE_UPDATE_GOLDEN=1 go test ./internal/indexer/ -run TestLSAGolden
func TestLSAGolden(t *testing.T) {
	docs, labels := loadLSACorpus(t)
	params := specfts.LSAParams{MinDF: 3, MaxDF: 0.3, K: 128}

	vocab := specfts.BuildVocab(docs, params.MinDF, params.MaxDF)
	if len(vocab.Terms) == 0 {
		t.Fatal("empty vocab")
	}
	tfidf := vocab.TFIDFMatrix(docs)
	u, s, vt, err := specfts.SVD(tfidf, params.K)
	if err != nil || u == nil || s == nil || vt == nil {
		t.Fatalf("SVD: %v", err)
	}
	// Эмбеддинги считаются напрямую из u/s; для запросов нужны только Vocab и VT.
	model := &specfts.LSAModel{Vocab: vocab, VT: vt}

	// Эмбеддинги документов — как в postProcessSpecLSA: U[i]·diag(S).
	embeddings := make([][]float64, len(docs))
	for i := range docs {
		embeddings[i] = specfts.DocEmbedding(u, s, i)
	}

	// Инварианты корпуса (не зависят от golden-файлов)
	for _, term := range []string{"арест", "блокировк", "заметк", "счет"} {
		if _, ok := vocab.Index[term]; !ok {
			t.Errorf("term %q must be in vocab (df=3)", term)
		}
	}
	if _, ok := vocab.Index["клиент"]; ok {
		t.Error("term 'клиент' must be filtered by maxDF (df > 30% corpus)")
	}

	// Golden: словарь
	goldenVocabPath := filepath.Join("testdata", "golden", "lsa_vocab.txt")
	if os.Getenv("CODEBASE_UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(goldenVocabPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenVocabPath, []byte(strings.Join(vocab.Terms, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	wantTerms, err := os.ReadFile(goldenVocabPath)
	if err != nil {
		t.Fatalf("read golden vocab: %v (run with CODEBASE_UPDATE_GOLDEN=1)", err)
	}
	if gotTerms := strings.Join(vocab.Terms, "\n") + "\n"; gotTerms != string(wantTerms) {
		t.Errorf("vocab mismatch:\n--- got ---\n%s\n--- want ---\n%s", gotTerms, wantTerms)
	}

	// Golden: ранжирование контрольных запросов (топ-5, cosine 4 знака)
	queries := []string{
		"блокировка счёта",   // ко-встречаемость: capability про арест
		"Заметка по клиенту", // редкий термин из требования: «Учёт юридических признаков»
		"инкассация",         // наполнитель
	}

	var sb strings.Builder
	for _, q := range queries {
		fmt.Fprintf(&sb, "# query: %s\n", q)
		qv := model.Vocab.ProjectQuery(q, model.VT)
		type scored struct {
			label string
			rank  float64
		}
		ranked := make([]scored, len(docs))
		for i := range docs {
			ranked[i] = scored{labels[i], specfts.CosineSimilarity(qv, embeddings[i])}
		}
		sort.SliceStable(ranked, func(a, b int) bool { return ranked[a].rank > ranked[b].rank })
		for _, r := range ranked[:5] {
			fmt.Fprintf(&sb, "%.4f  %s\n", r.rank, r.label)
		}
	}
	goldenQueriesPath := filepath.Join("testdata", "golden", "lsa_queries.txt")
	if os.Getenv("CODEBASE_UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(goldenQueriesPath, []byte(sb.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	wantQ, err := os.ReadFile(goldenQueriesPath)
	if err != nil {
		t.Fatalf("read golden queries: %v (run with CODEBASE_UPDATE_GOLDEN=1)", err)
	}
	if sb.String() != string(wantQ) {
		t.Errorf("query ranking mismatch:\n--- got ---\n%s\n--- want ---\n%s", sb.String(), wantQ)
	}

	// Главный сценарий change: «Заметка по клиенту» — топ-1 «Учёт юридических признаков»,
	// шумовые «Отчётность по клиенту» не в топ-3.
	qv := model.Vocab.ProjectQuery("Заметка по клиенту", model.VT)
	type scored struct {
		label string
		rank  float64
	}
	ranked := make([]scored, len(docs))
	for i := range docs {
		ranked[i] = scored{labels[i], specfts.CosineSimilarity(qv, embeddings[i])}
	}
	sort.SliceStable(ranked, func(a, b int) bool { return ranked[a].rank > ranked[b].rank })
	if !strings.HasPrefix(ranked[0].label, "Учёт юридических признаков") {
		t.Errorf("top-1 = %q (%.4f), expected 'Учёт юридических признаков'", ranked[0].label, ranked[0].rank)
	}
	for _, r := range ranked[:3] {
		if strings.HasPrefix(r.label, "Отчётность по клиенту") {
			t.Errorf("noise capability in top-3: %q (%.4f)", r.label, r.rank)
		}
	}
}
