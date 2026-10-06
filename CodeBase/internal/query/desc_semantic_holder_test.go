package query

import (
	"sync"
	"testing"

	"github.com/codebase/internal/specfts"
)

func TestDescSemanticHolder_StoreSnapshot(t *testing.T) {
	var h descSemanticHolder
	if gen, model, rows := h.snapshot(); gen != "" || model != nil || rows != nil {
		t.Fatal("пустой holder должен возвращать пустой снапшот")
	}

	m1 := &specfts.LSAModel{Generation: "g1", K: 2}
	r1 := []*descEmbeddingRow{{Name: "a", Dim: 2, FloatsLE: []float32{1, 2}}}
	h.store("g1", m1, r1)

	gen, model, rows := h.snapshot()
	if gen != "g1" || model != m1 || len(rows) != 1 || rows[0] != r1[0] {
		t.Fatalf("снапшот после store: gen=%s model=%p rows=%d", gen, model, len(rows))
	}
	if h.loadCount() != 1 {
		t.Fatalf("loadCount = %d, want 1", h.loadCount())
	}

	// смена поколения замещает слот
	m2 := &specfts.LSAModel{Generation: "g2", K: 3}
	r2 := []*descEmbeddingRow{{Name: "b"}, {Name: "c"}}
	h.store("g2", m2, r2)
	gen, model, rows = h.snapshot()
	if gen != "g2" || model != m2 || len(rows) != 2 {
		t.Fatalf("снапшот после замещения: gen=%s model=%p rows=%d", gen, model, len(rows))
	}
	if h.loadCount() != 2 {
		t.Fatalf("loadCount = %d, want 2", h.loadCount())
	}
}

func TestDescSemanticHolder_ConcurrentSnapshot(t *testing.T) {
	var h descSemanticHolder
	m := &specfts.LSAModel{Generation: "g1", K: 2}
	rows := []*descEmbeddingRow{{Name: "a"}}
	h.store("g1", m, rows)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gen, model, gotRows := h.snapshot()
			if gen != "g1" || model == nil || len(gotRows) != 1 {
				t.Error("конкурентный снапшот некорректен")
			}
		}()
	}
	wg.Wait()
}
