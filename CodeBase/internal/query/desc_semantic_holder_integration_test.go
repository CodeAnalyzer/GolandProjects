//go:build integration

package query

import (
	"context"
	"os"
	"testing"

	"github.com/codebase/internal/config"
)

// Две последовательные загрузки — одно чтение: второй вызов semantic-слоя
// переиспользует модель и эмбеддинги из holder (loads не растёт),
// результаты идентичны.
func TestSearchDescriptions_HolderReusesModelAndRows(t *testing.T) {
	db, ctx := setupDescLSAEnv(t)
	trainDescLSAForTest(t, db)
	descSemanticCache.reset()
	q := New(db)

	first, _, err := q.SearchDescriptions(ctx, "возврат классификатор", nil, 10)
	if err != nil {
		t.Fatalf("first SearchDescriptions: %v", err)
	}
	loadsAfterFirst := descSemanticCache.loadCount()
	if loadsAfterFirst != 1 {
		t.Fatalf("после первого вызова loads = %d, want 1", loadsAfterFirst)
	}

	second, _, err := q.SearchDescriptions(ctx, "возврат классификатор", nil, 10)
	if err != nil {
		t.Fatalf("second SearchDescriptions: %v", err)
	}
	if got := descSemanticCache.loadCount(); got != loadsAfterFirst {
		t.Fatalf("повторный вызов перечитал модель/кэш: loads = %d, want %d", got, loadsAfterFirst)
	}
	if len(first) != len(second) {
		t.Fatalf("выдачи различаются: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Name != second[i].Name || first[i].Source != second[i].Source {
			t.Fatalf("строка %d различается: %+v vs %+v", i, first[i], second[i])
		}
	}

	// новый Query (как MCP на каждый вызов инструмента) — тот же holder
	q2 := New(db)
	if _, _, err := q2.SearchDescriptions(ctx, "возврат классификатор", nil, 10); err != nil {
		t.Fatalf("q2 SearchDescriptions: %v", err)
	}
	if got := descSemanticCache.loadCount(); got != loadsAfterFirst {
		t.Fatalf("новый Query перечитал модель/кэш: loads = %d, want %d", got, loadsAfterFirst)
	}
}

// Смена поколения замещает слот holder: перевыпуск поколения с тем же
// содержимым невозможен (fingerprint детерминирован), поэтому проверяем
// замену через удаление поколения из БД — holder перезагружается и
// переходит в exact-only.
func TestSearchDescriptions_HolderGenerationInvalidation(t *testing.T) {
	db, ctx := setupDescLSAEnv(t)
	trainDescLSAForTest(t, db)
	descSemanticCache.reset()
	q := New(db)

	if _, _, err := q.SearchDescriptions(ctx, "классификатор", nil, 10); err != nil {
		t.Fatalf("first: %v", err)
	}
	if descSemanticCache.loadCount() != 1 {
		t.Fatalf("loads = %d, want 1", descSemanticCache.loadCount())
	}
	gen, model, _ := descSemanticCache.snapshot()
	if model == nil || gen == "" {
		t.Fatal("holder не заполнен после первого вызова")
	}

	// поколение исчезло из БД (переиндексация) → holder перезагружается
	if _, err := db.Exec(`DELETE FROM desc_embeddings WHERE generation = $1`, gen); err != nil {
		t.Fatalf("delete embeddings: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM desc_vocab WHERE generation = $1`, gen); err != nil {
		t.Fatalf("delete vocab: %v", err)
	}

	items, _, err := q.SearchDescriptions(ctx, "классификатор", nil, 10)
	if err != nil {
		t.Fatalf("after invalidation: %v", err)
	}
	for _, it := range items {
		if it.Source == "semantic" {
			t.Fatal("semantic-хит после инвалидации поколения")
		}
	}
}

// Fallback без модели: файл модели отсутствует до первого вызова —
// повторные вызовы возвращают exact-only без загрузки кэша.
func TestSearchDescriptions_FallbackRepeatedWithoutModel(t *testing.T) {
	db, _ := setupDescLSAEnv(t)
	trainDescLSAForTest(t, db)
	if err := os.Remove(config.DescLSAModelPath()); err != nil {
		t.Fatalf("remove model: %v", err)
	}
	descSemanticCache.reset()
	q := New(db)

	ctx := context.Background()
	for call := 0; call < 3; call++ {
		items, _, err := q.SearchDescriptions(ctx, "классификатор", nil, 10)
		if err != nil {
			t.Fatalf("call %d: %v", call, err)
		}
		if len(items) == 0 {
			t.Fatalf("call %d: пустая выдача, ожидается exact-хит", call)
		}
		for _, it := range items {
			if it.Source != "exact" {
				t.Fatalf("call %d: source = %s, want exact", call, it.Source)
			}
		}
	}
	if got := descSemanticCache.loadCount(); got != 0 {
		t.Fatalf("loads = %d, want 0 (модель отсутствует)", got)
	}
}
