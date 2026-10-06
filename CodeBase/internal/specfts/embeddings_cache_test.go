package specfts

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// testCacheRow — строка кэша для тестов generic-хелпера.
type testCacheRow struct {
	Name     string    `json:"n"`
	Product  string    `json:"p"`
	Dim      int       `json:"d"`
	FloatsLE []float32 `json:"-"`
}

func (r *testCacheRow) EmbeddingDim() int           { return r.Dim }
func (r *testCacheRow) EmbeddingFloats() []float32  { return r.FloatsLE }
func (r *testCacheRow) SetEmbedding(dim int, f []float32) {
	r.Dim = dim
	r.FloatsLE = f
}

func makeTestRows() []*testCacheRow {
	return []*testCacheRow{
		{Name: "alpha", Product: "fa-cards", Dim: 3, FloatsLE: []float32{0.5, -1.25, 2.0}},
		{Name: "beta", Product: "fa-payments", Dim: 3, FloatsLE: []float32{-0.75, 0.125, 3.5}},
	}
}

func TestEmbeddingsCacheRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.bin")
	rows := makeTestRows()
	if err := WriteEmbeddingsCache(path, "TESTC1", "gen-1", rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	loaded, err := ReadEmbeddingsCache[*testCacheRow](path, "TESTC1", "gen-1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("got %d rows, want 2", len(loaded))
	}
	for i, want := range rows {
		got := loaded[i]
		if got.Name != want.Name || got.Product != want.Product || got.Dim != 3 {
			t.Errorf("row %d meta mismatch: %+v vs %+v", i, got, want)
		}
		for j := range want.FloatsLE {
			if got.FloatsLE[j] != want.FloatsLE[j] {
				t.Errorf("row %d float %d: got %v want %v", i, j, got.FloatsLE[j], want.FloatsLE[j])
			}
		}
	}
	// каждая строка — собственный слайс эмбеддинга
	if &loaded[0].FloatsLE[0] == &loaded[1].FloatsLE[0] {
		t.Error("rows share embedding backing array")
	}
}

func TestEmbeddingsCacheGenerationMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.bin")
	if err := WriteEmbeddingsCache(path, "TESTC1", "gen-1", makeTestRows()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := ReadEmbeddingsCache[*testCacheRow](path, "TESTC1", "gen-2"); err == nil {
		t.Fatal("expected generation mismatch error")
	} else if !errors.Is(err, os.ErrNotExist) {
		// ошибка промаха, но не «файл отсутствует» — вызывающий пересоберёт
		t.Logf("mismatch error: %v", err)
	}
}

func TestEmbeddingsCacheMagicMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.bin")
	if err := WriteEmbeddingsCache(path, "TESTC1", "gen-1", makeTestRows()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := ReadEmbeddingsCache[*testCacheRow](path, "OTHER1", "gen-1"); err == nil {
		t.Fatal("expected magic mismatch error")
	}
}

func TestEmbeddingsCacheMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "cache.bin")
	if _, err := ReadEmbeddingsCache[*testCacheRow](path, "TESTC1", "gen-1"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

func TestEmbeddingsCacheCorrupted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache.bin")
	if err := WriteEmbeddingsCache(path, "TESTC1", "gen-1", makeTestRows()); err != nil {
		t.Fatalf("write: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// усечение хвоста (блок float32)
	if err := os.WriteFile(path, data[:len(data)-8], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEmbeddingsCache[*testCacheRow](path, "TESTC1", "gen-1"); err == nil {
		t.Fatal("expected truncated floats error")
	}
}

func TestEmbeddingsCacheEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.bin")
	if err := WriteEmbeddingsCache[*testCacheRow](path, "TESTC1", "gen-1", nil); err != nil {
		t.Fatalf("write empty: %v", err)
	}
	loaded, err := ReadEmbeddingsCache[*testCacheRow](path, "TESTC1", "gen-1")
	if err != nil {
		t.Fatalf("read empty: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("got %d rows, want 0", len(loaded))
	}
}
