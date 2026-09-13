package specfts

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadLSAModel(t *testing.T) {
	docs := []Document{
		{ID: 1, Text: "арест счёта должника"},
		{ID: 2, Text: "блокировка счёта клиента"},
		{ID: 3, Text: "выдача кредита заёмщику"},
		{ID: 4, Text: "погашение задолженности"},
		{ID: 5, Text: "арест имущества"},
	}
	vocab := BuildVocab(docs, 1, 1.0)
	m := vocab.TFIDFMatrix(docs)

	u, s, vt, err := SVD(m, 2)
	if err != nil {
		t.Fatalf("SVD failed: %v", err)
	}

	model := &LSAModel{
		Generation: "gen-test",
		Vocab:      vocab,
		VT:         vt,
		Singulars:  s,
		K:          2,
		NumDocs:    len(docs),
		NumTerms:   len(vocab.Terms),
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "spec_lsa_model.bin")

	if err := SaveLSAModel(model, path); err != nil {
		t.Fatalf("SaveLSAModel failed: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("model file not created: %v", err)
	}
	if err := SaveLSAModel(model, path); err != nil {
		t.Fatalf("second SaveLSAModel failed: %v", err)
	}
	if tempPaths, err := filepath.Glob(path + ".tmp-*"); err != nil || len(tempPaths) != 0 {
		t.Fatalf("temporary model files = %v, err=%v", tempPaths, err)
	}

	loaded, err := LoadLSAModel(path)
	if err != nil {
		t.Fatalf("LoadLSAModel failed: %v", err)
	}

	if loaded.Generation != model.Generation {
		t.Errorf("Generation mismatch: %q vs %q", loaded.Generation, model.Generation)
	}
	if loaded.K != model.K {
		t.Errorf("K mismatch: %d vs %d", loaded.K, model.K)
	}
	if loaded.NumDocs != model.NumDocs {
		t.Errorf("NumDocs mismatch: %d vs %d", loaded.NumDocs, model.NumDocs)
	}
	if len(loaded.Singulars) != len(model.Singulars) {
		t.Errorf("Singulars length mismatch: %d vs %d", len(loaded.Singulars), len(model.Singulars))
	}
	if len(loaded.Vocab.Terms) != len(model.Vocab.Terms) {
		t.Errorf("Vocab terms mismatch: %d vs %d", len(loaded.Vocab.Terms), len(model.Vocab.Terms))
	}

	// Проверяем что проекция запроса работает с загруженной моделью
	qVec := loaded.Vocab.ProjectQuery("блокировка счёта", loaded.VT)
	if len(qVec) != loaded.K {
		t.Fatalf("expected %d-dim query, got %d", loaded.K, len(qVec))
	}

	// Сравниваем эмбеддинги из оригинальной и загруженной модели
	emb1 := DocEmbedding(u, s, 0)
	_ = emb1 // просто проверяем что не падает

	// Проверяем что Vᵀ совпадает
	for i := 0; i < model.VT.Rows(); i++ {
		for j := 0; j < model.VT.Cols(); j++ {
			if model.VT.At(i, j) != loaded.VT.At(i, j) {
				t.Errorf("VT mismatch at (%d,%d): %f vs %f", i, j, model.VT.At(i, j), loaded.VT.At(i, j))
			}
		}
	}
}

func TestLSAModelTempActivation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spec_lsa_model.bin")
	model := &LSAModel{Vocab: &Vocab{}, VT: NewDenseMatrix(1, 1), Singulars: []float64{1}, K: 1, NumDocs: 1, NumTerms: 1}

	tempPath, err := WriteLSAModelTemp(model, path)
	if err != nil {
		t.Fatalf("WriteLSAModelTemp: %v", err)
	}
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("temp model file: %v", err)
	}
	if err := ActivateLSAModel(tempPath, path); err != nil {
		t.Fatalf("ActivateLSAModel: %v", err)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("activated temp file still exists: %v", err)
	}

	tempPath, err = WriteLSAModelTemp(model, path)
	if err != nil {
		t.Fatalf("WriteLSAModelTemp for invalid target: %v", err)
	}
	invalidTarget := filepath.Join(dir, "missing", "spec_lsa_model.bin")
	if err := ActivateLSAModel(tempPath, invalidTarget); err == nil {
		t.Fatal("ActivateLSAModel with invalid target must fail")
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("failed activation temp file still exists: %v", err)
	}
}

func TestLoadLSAModel_LegacyGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spec_lsa_model.bin")
	legacy := struct {
		Vocab     *Vocab
		VT        *DenseMatrix
		Singulars []float64
		K         int
		NumDocs   int
		NumTerms  int
	}{
		Vocab:     &Vocab{},
		VT:        NewDenseMatrix(1, 1),
		Singulars: []float64{1},
		K:         1,
		NumDocs:   1,
		NumTerms:  1,
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := gob.NewEncoder(f).Encode(legacy); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadLSAModel(path)
	if err != nil {
		t.Fatalf("LoadLSAModel: %v", err)
	}
	if loaded.Generation != LegacyGeneration {
		t.Fatalf("Generation = %q, want %q", loaded.Generation, LegacyGeneration)
	}
}
