package specfts

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLSADocText_TitleDoubled(t *testing.T) {
	text := LSADocText("Арест счёта", "Ограничение операций", "Примечание", "Требование один")
	if strings.Count(text, "Арест счёта") != 2 {
		t.Errorf("expected title duplicated twice, got %q", text)
	}
	parts := strings.SplitN(text, " ", -1)
	if len(parts) < 8 {
		t.Errorf("expected all parts included, got %q", text)
	}
	if !strings.Contains(text, "Требование один") || !strings.Contains(text, "Ограничение операций") {
		t.Errorf("expected purpose and req text included, got %q", text)
	}
}

func TestTokenizeToStemCounts_Multiplicity(t *testing.T) {
	counts := TokenizeToStemCounts("арест ареста блокировка")
	if counts["арест"] != 2 {
		t.Errorf("expected stem 'арест' count 2 (арест+ареста → один стем), got %v", counts)
	}
	if counts["блокировк"] != 1 {
		t.Errorf("expected stem 'блокировк' count 1, got %v", counts)
	}
}

func TestTokenize_YoNormalization(t *testing.T) {
	// «счёт» и «счет» должны давать один стем после нормализации ё→е
	stems1 := TokenizeToStems("счёт счёт")
	stems2 := TokenizeToStems("счет счет")
	if len(stems1) != 1 || len(stems2) != 1 {
		t.Fatalf("expected single stem each, got %v and %v", stems1, stems2)
	}
	if stems1[0] != stems2[0] {
		t.Errorf("stems differ after ё→е: %q vs %q", stems1[0], stems2[0])
	}

	// падежные формы с ё и е совпадают
	stems3 := TokenizeToStems("счёта")
	stems4 := TokenizeToStems("счета")
	if len(stems3) != 1 || len(stems4) != 1 || stems3[0] != stems4[0] {
		t.Errorf("stems of счёта/счета differ: %v vs %v", stems3, stems4)
	}

	// существующие категории токенов не затронуты
	tech := TokenizeToStems("вызов CON_STP_MassAccrual и API_Test")
	foundTech, foundAPI := false, false
	for _, s := range tech {
		if s == "CON_STP_MassAccrual" {
			foundTech = true
		}
		if s == "API_Test" {
			foundAPI = true
		}
	}
	if !foundTech || !foundAPI {
		t.Errorf("tech tokens broken after ё→е: %v", tech)
	}
}

func TestCorpusFingerprint_Determinism(t *testing.T) {
	docs := []Document{
		{ID: 1, Text: "текст один"},
		{ID: 2, Text: "текст два"},
	}
	params := LSAParams{MinDF: 3, MaxDF: 0.3, K: 128}
	fp1 := CorpusFingerprint(docs, params)
	fp2 := CorpusFingerprint(docs, params)
	if fp1 != fp2 {
		t.Errorf("fingerprint not deterministic: %s vs %s", fp1, fp2)
	}
	if len(fp1) != 64 { // SHA-256 hex
		t.Errorf("expected sha256 hex, got %q", fp1)
	}
}

func TestCorpusFingerprint_SensitiveToText(t *testing.T) {
	params := LSAParams{MinDF: 3, MaxDF: 0.3, K: 128}
	base := CorpusFingerprint([]Document{{ID: 1, Text: "текст"}}, params)

	changedText := CorpusFingerprint([]Document{{ID: 1, Text: "текст изменён"}}, params)
	if base == changedText {
		t.Error("fingerprint must change when text changes")
	}

	changedID := CorpusFingerprint([]Document{{ID: 2, Text: "текст"}}, params)
	if base == changedID {
		t.Error("fingerprint must change when doc id changes")
	}

	// разделитель NUL защищает от коллизий конкатенаций:
	// ("ab","c") и ("a","bc") дают разный хеш
	fp1 := CorpusFingerprint([]Document{{ID: 1, Text: "ab"}, {ID: 2, Text: "c"}}, params)
	fp2 := CorpusFingerprint([]Document{{ID: 1, Text: "a"}, {ID: 2, Text: "bc"}}, params)
	if fp1 == fp2 {
		t.Error("concatenation collision: NUL separators must prevent it")
	}
}

func TestCorpusFingerprint_SensitiveToParamsAndVersions(t *testing.T) {
	docs := []Document{{ID: 1, Text: "текст"}}
	base := CorpusFingerprint(docs, LSAParams{MinDF: 3, MaxDF: 0.3, K: 128})

	if CorpusFingerprint(docs, LSAParams{MinDF: 2, MaxDF: 0.3, K: 128}) == base {
		t.Error("fingerprint must change when min_df changes")
	}
	if CorpusFingerprint(docs, LSAParams{MinDF: 3, MaxDF: 0.15, K: 128}) == base {
		t.Error("fingerprint must change when max_df changes")
	}
	if CorpusFingerprint(docs, LSAParams{MinDF: 3, MaxDF: 0.3, K: 64}) == base {
		t.Error("fingerprint must change when k changes")
	}
}

func TestLSAState_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spec_lsa_state.json")

	// Отсутствие файла — не ошибка
	st, err := LoadLSAState(path)
	if err != nil || st != nil {
		t.Fatalf("expected (nil, nil) for missing file, got (%v, %v)", st, err)
	}

	orig := &LSAState{
		Fingerprint: "abc",
		Pending:     7,
		Params:      LSAParams{MinDF: 3, MaxDF: 0.3, K: 128},
		NumDocs:     120,
	}
	if err := SaveLSAState(path, orig); err != nil {
		t.Fatalf("SaveLSAState: %v", err)
	}

	loaded, err := LoadLSAState(path)
	if err != nil {
		t.Fatalf("LoadLSAState: %v", err)
	}
	if loaded.Fingerprint != orig.Fingerprint || loaded.Pending != orig.Pending ||
		loaded.Params != orig.Params || loaded.NumDocs != orig.NumDocs {
		t.Errorf("round-trip mismatch: %+v vs %+v", loaded, orig)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("tmp file must be cleaned after rename")
	}
}

func TestLSAState_CorruptedFileReturnsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spec_lsa_state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := LoadLSAState(path)
	if err == nil || st != nil {
		t.Errorf("expected error for corrupted state, got (%v, %v)", st, err)
	}
}

// TestSVD_VTCoversAllTerms — регрессия бага обрезки VT:
// при nTerms > nDocs матрица Vᵀ обязана покрывать ВСЕ термины (cols = nTerms),
// иначе термины с индексом ≥ nDocs выпадают из проекции запроса.
func TestSVD_VTCoversAllTerms(t *testing.T) {
	// 6 документов; 55 «шумовых» техимён (латиница сортируется раньше кириллицы)
	// гарантируют nTerms > nDocs и высокий алфавитный индекс «заметк».
	docs := []Document{
		{ID: 1, Text: "заметка арбитраж"},
		{ID: 2, Text: "заметка арбитраж"},
		{ID: 3, Text: "заметка арбитраж"},
		{ID: 4, Text: "заметка арбитраж"},
		{ID: 5, Text: "заметка арбитраж"},
		{ID: 6, Text: "заметка арбитраж"},
	}
	for i := 0; i < 55; i++ {
		term := fmt.Sprintf("AANOISE%02d", i)
		for d := range docs {
			docs[d] = Document{ID: docs[d].ID, Text: docs[d].Text + " " + term}
		}
	}

	vocab := BuildVocab(docs, 3, 1.0)
	if len(vocab.Terms) <= len(docs) {
		t.Fatalf("precondition failed: need nTerms(%d) > nDocs(%d)", len(vocab.Terms), len(docs))
	}
	tfidf := vocab.TFIDFMatrix(docs)
	_, _, vt, err := SVD(tfidf, 4)
	if err != nil || vt == nil {
		t.Fatalf("SVD: %v", err)
	}
	if vt.Cols() != len(vocab.Terms) {
		t.Fatalf("VT cols = %d, want %d (all terms)", vt.Cols(), len(vocab.Terms))
	}

	// Индекс «заметк» гарантированно ≥ nDocs (латинские техимёна сортируются раньше)
	zidx := vocab.Index["заметк"]
	if zidx < len(docs) {
		t.Fatalf("precondition: index of 'заметк' (%d) must be >= nDocs (%d)", zidx, len(docs))
	}

	// Проекция запроса по «заметке» обязана быть ненулевой
	q := vocab.ProjectQuery("заметка", vt)
	var norm float64
	for _, v := range q {
		norm += v * v
	}
	if norm == 0 {
		t.Fatal("query projection is zero: high-index term invisible after SVD truncation")
	}
}
