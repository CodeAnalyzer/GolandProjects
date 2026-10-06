package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/specfts"
)

// descSemanticHolder — кэш в памяти semantic-слоя desc-поиска: модель
// (desc_lsa_model.bin) и эмбеддинги поколения загружаются один раз и
// переиспользуются между вызовами процесса (MCP-сервер живёт долго).
// Инвалидация — по поколению: при каждом обращении выполняется дешёвая
// EXISTS-проверка поколения в БД; смена поколения (переобучение после
// переиндексации) замещает слот. Загрузка сериализована (singleflight):
// параллельные промахи не дают двойного чтения модели/кэша.
type descSemanticHolder struct {
	mu    sync.RWMutex
	gen   string
	model *specfts.LSAModel
	rows  []*descEmbeddingRow

	loadMu sync.Mutex
	loads  int // число загрузок модели+эмбеддингов (диагностика/тесты)
}

// descSemanticCache — глобальный держатель кэша desc-semantic-слоя
// (Query создаётся на каждый вызов инструмента, кэш — на процесс).
var descSemanticCache descSemanticHolder

func (h *descSemanticHolder) snapshot() (string, *specfts.LSAModel, []*descEmbeddingRow) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.gen, h.model, h.rows
}

func (h *descSemanticHolder) store(gen string, model *specfts.LSAModel, rows []*descEmbeddingRow) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen = gen
	h.model = model
	h.rows = rows
	h.loads++
}

// loadCount возвращает число выполненных загрузок (для тестов).
func (h *descSemanticHolder) loadCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.loads
}

// reset сбрасывает слот и счётчик загрузок (для тестов).
func (h *descSemanticHolder) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen, h.model, h.rows = "", nil, nil
	h.loads = 0
}

// get возвращает модель и эмбеддинги актуального поколения.
// Модель не обучена (файл отсутствует / поколение без эмбеддингов) —
// (nil, nil, nil): graceful exact-only.
func (h *descSemanticHolder) get(ctx context.Context, q *Query) (*specfts.LSAModel, []*descEmbeddingRow, error) {
	// быстрый путь: закэшированное поколение ещё актуально
	if gen, model, rows := h.snapshot(); model != nil {
		has, err := q.db.HasDescLSAGeneration(ctx, gen)
		if err != nil {
			return nil, nil, fmt.Errorf("check desc LSA generation: %w", err)
		}
		if has {
			return model, rows, nil
		}
	}

	// медленный путь: единственный загрузчик, остальные ждут и переиспользуют
	h.loadMu.Lock()
	defer h.loadMu.Unlock()

	if gen, model, rows := h.snapshot(); model != nil {
		has, err := q.db.HasDescLSAGeneration(ctx, gen)
		if err != nil {
			return nil, nil, fmt.Errorf("check desc LSA generation: %w", err)
		}
		if has {
			return model, rows, nil
		}
	}

	model, err := specfts.LoadLSAModel(config.DescLSAModelPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil // модель не обучена — exact-only
		}
		return nil, nil, fmt.Errorf("load desc LSA model: %w", err)
	}
	if model.Vocab == nil || model.VT == nil || model.Generation == "" || model.K <= 0 {
		return nil, nil, fmt.Errorf("invalid desc LSA model")
	}
	has, err := q.db.HasDescLSAGeneration(ctx, model.Generation)
	if err != nil {
		return nil, nil, fmt.Errorf("check desc LSA generation: %w", err)
	}
	if !has {
		return nil, nil, nil
	}
	rows, err := q.loadDescEmbeddingsCached(ctx, model.Generation)
	if err != nil {
		return nil, nil, err
	}
	h.store(model.Generation, model, rows)
	return model, rows, nil
}
