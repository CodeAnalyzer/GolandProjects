package specsvc

import (
	"testing"

	"github.com/codebase/internal/specfts"
)

func hit(id int64, rank float64) SpecSearchHit {
	return SpecSearchHit{CapabilityID: id, Rank: rank}
}

func ranks(hits []SpecSearchHit) []float64 {
	out := make([]float64, len(hits))
	for i, h := range hits {
		out[i] = h.Rank
	}
	return out
}

func equalRanks(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestFilterSemanticHits — табличный тест чистой функции фильтрации:
// абсолютный порог → сортировка DESC → relative cutoff → limit.
func TestFilterSemanticHits(t *testing.T) {
	tests := []struct {
		name           string
		hits           []SpecSearchHit
		minCosine      float64
		relativeCutoff float64
		limit          int
		want           []float64
	}{
		{
			name:           "пустой список",
			hits:           nil,
			minCosine:      0.15,
			relativeCutoff: 0.5,
			limit:          10,
			want:           []float64{},
		},
		{
			name:           "отсечение по абсолютному порогу до сортировки",
			hits:           []SpecSearchHit{hit(1, 0.05), hit(2, 0.8), hit(3, 0.1)},
			minCosine:      0.15,
			relativeCutoff: 0,
			limit:          10,
			want:           []float64{0.8},
		},
		{
			name:           "сортировка DESC",
			hits:           []SpecSearchHit{hit(1, 0.4), hit(2, 0.9), hit(3, 0.6)},
			minCosine:      0.15,
			relativeCutoff: 0,
			limit:          10,
			want:           []float64{0.9, 0.6, 0.4},
		},
		{
			name:           "relative cutoff отсекает до limit: 0.3 < 0.5×0.8",
			hits:           []SpecSearchHit{hit(1, 0.8), hit(2, 0.5), hit(3, 0.3)},
			minCosine:      0.15,
			relativeCutoff: 0.5,
			limit:          10,
			want:           []float64{0.8, 0.5},
		},
		{
			name:           "limit применяется последним",
			hits:           []SpecSearchHit{hit(1, 0.9), hit(2, 0.85), hit(3, 0.8)},
			minCosine:      0.15,
			relativeCutoff: 0.5,
			limit:          2,
			want:           []float64{0.9, 0.85},
		},
		{
			name:           "все ниже абсолютного порога — пусто",
			hits:           []SpecSearchHit{hit(1, 0.05), hit(2, 0.1)},
			minCosine:      0.15,
			relativeCutoff: 0.5,
			limit:          10,
			want:           []float64{},
		},
		{
			name:           "минус-нулевые и нулевые ранги отсеиваются",
			hits:           []SpecSearchHit{hit(1, 0), hit(2, -0.2), hit(3, 0.7)},
			minCosine:      0.15,
			relativeCutoff: 0,
			limit:          10,
			want:           []float64{0.7},
		},
		{
			name:           "cutoff 0 — без relative-фильтрации",
			hits:           []SpecSearchHit{hit(1, 0.8), hit(2, 0.2)},
			minCosine:      0.15,
			relativeCutoff: 0,
			limit:          10,
			want:           []float64{0.8, 0.2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterSemanticHits(tt.hits, tt.minCosine, tt.relativeCutoff, tt.limit)
			if !equalRanks(ranks(got), tt.want) {
				t.Errorf("filterSemanticHits = %v, want %v", ranks(got), tt.want)
			}
		})
	}
}

// TestOOVQueryStems — стемы запроса вне словаря LSA.
func TestOOVQueryStems(t *testing.T) {
	// форма «клиенту» стеммится в «клиент» (снятие окончания -у)
	docs := []specfts.Document{
		{ID: 1, Text: "арест счёта блокировка"},
		{ID: 2, Text: "арест счета ограничение"},
		{ID: 3, Text: "блокировка расходных операций счёт"},
		{ID: 4, Text: "клиенту банка"},
	}
	vocab := specfts.BuildVocab(docs, 1, 1.0)

	oov := oovQueryStems("заметка по клиенту", vocab)
	if len(oov) != 1 || oov[0] != "заметк" {
		t.Errorf("expected oov ['заметк'], got %v", oov)
	}

	oovAll := oovQueryStems("арест счёта", vocab)
	if len(oovAll) != 0 {
		t.Errorf("expected no oov, got %v", oovAll)
	}
}

// TestBuildSemanticMeta — диагностика пустой semantic-секции и OOV-подсказка.
func TestBuildSemanticMeta(t *testing.T) {
	// словарь знает «клиент», не знает «заметк»
	vocab := specfts.BuildVocab([]specfts.Document{
		{ID: 1, Text: "клиенту банка"},
		{ID: 2, Text: "клиенту отчет"},
		{ID: 3, Text: "клиенту тариф"},
	}, 1, 1.0)

	t.Run("пусто после порога, без OOV", func(t *testing.T) {
		meta := buildSemanticMeta("клиенту", vocab, 12, 0.15, 0)
		if meta == nil || meta.Reason != "threshold" || meta.FilteredOut != 12 || meta.Threshold != 0.15 {
			t.Errorf("unexpected meta: %+v", meta)
		}
		if meta.Hint != "" {
			t.Errorf("no hint expected without oov, got %q", meta.Hint)
		}
	})

	t.Run("частичный OOV и пустая секция", func(t *testing.T) {
		meta := buildSemanticMeta("заметка по клиенту", vocab, 12, 0.15, 0)
		if meta == nil || meta.Reason != "threshold" {
			t.Errorf("unexpected meta: %+v", meta)
		}
		if len(meta.OOVTerms) != 1 || meta.OOVTerms[0] != "заметк" {
			t.Errorf("expected oov ['заметк'], got %v", meta.OOVTerms)
		}
		if meta.Hint == "" {
			t.Error("expected hint referencing exact layer")
		}
	})

	t.Run("полный OOV — причина oov", func(t *testing.T) {
		meta := buildSemanticMeta("заметка", vocab, 0, 0.15, 0)
		if meta == nil || meta.Reason != "oov" {
			t.Errorf("unexpected meta: %+v", meta)
		}
		if meta.Hint == "" || meta.OOVTerms == nil {
			t.Errorf("expected hint and oov terms: %+v", meta)
		}
	})

	t.Run("есть хиты и нет OOV — мета не возвращается", func(t *testing.T) {
		if meta := buildSemanticMeta("клиенту", vocab, 3, 0.15, 5); meta != nil {
			t.Errorf("expected nil meta, got %+v", meta)
		}
	})

	t.Run("есть хиты, но частичный OOV — мета с oov_terms", func(t *testing.T) {
		meta := buildSemanticMeta("заметка по клиенту", vocab, 0, 0.15, 5)
		if meta == nil || len(meta.OOVTerms) != 1 || meta.Reason != "" {
			t.Errorf("expected meta with oov terms only, got %+v", meta)
		}
	})
}
