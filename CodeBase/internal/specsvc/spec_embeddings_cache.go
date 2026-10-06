package specsvc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/specfts"
	"github.com/codebase/internal/store"
	"github.com/lib/pq"
)

// specEmbeddingRow — эмбеддинг capability spec-корпуса с метаданными для
// выдачи semantic-слоя. Product хранится в кэше: фильтрация semantic-поиска
// по продукту выполняется в памяти, без отдельного запроса к БД.
type specEmbeddingRow struct {
	CapabilityID   int64     `json:"i"`
	CapabilityName string    `json:"n"`
	Title          string    `json:"t"`
	Purpose        string    `json:"p"`
	LineStart      int       `json:"l"`
	LineEnd        int       `json:"e"`
	Snippet        string    `json:"s"` // embed_text документа
	Product        string    `json:"pr"`
	Dim            int       `json:"d"`
	FloatsLE       []float32 `json:"-"`
}

// Интерфейс строки кэша specfts.EmbeddingRow (generic-хелпер кэша).
func (r *specEmbeddingRow) EmbeddingDim() int          { return r.Dim }
func (r *specEmbeddingRow) EmbeddingFloats() []float32 { return r.FloatsLE }
func (r *specEmbeddingRow) SetEmbedding(dim int, floats []float32) {
	r.Dim = dim
	r.FloatsLE = floats
}

// specEmbeddingsCacheMagic — маркер формата/владельца кэша spec-корпуса.
const specEmbeddingsCacheMagic = "SPEMBC1"

// specEmbeddingsCachePath — путь к бинарному кэшу эмбеддингов spec-корпуса
// (sidecar spec-LSA: spec_lsa_model.bin / spec_lsa_state.json /
// spec_lsa_embeddings.bin). Инвалидация — по поколению в заголовке файла.
// Кэш исключает pull spec_embeddings из БД с парсингом float8[]-как-текст
// на каждый semantic-вызов.
func specEmbeddingsCachePath() string {
	return config.SpecLSAEmbeddingsPath()
}

// loadSpecEmbeddingsCached возвращает эмбеддинги поколения: из бинарного кэша
// (быстрый путь, поколение сверяется по заголовку) или из БД с последующей
// атомарной перезаписью кэша.
func loadSpecEmbeddingsCached(ctx context.Context, db *store.DB, generation string) ([]*specEmbeddingRow, error) {
	cachePath := specEmbeddingsCachePath()
	if rows, err := specfts.ReadEmbeddingsCache[*specEmbeddingRow](cachePath, specEmbeddingsCacheMagic, generation); err == nil {
		return rows, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		// повреждённый/устаревший кэш не фатален — пересоберём из БД
		_ = os.Remove(cachePath)
	}

	rows, err := loadSpecEmbeddingsFromDB(ctx, db, generation)
	if err != nil {
		return nil, err
	}
	if err := specfts.WriteEmbeddingsCache(cachePath, specEmbeddingsCacheMagic, generation, rows); err != nil {
		// ошибка записи кэша не ломает поиск
		_ = os.Remove(cachePath)
	}
	return rows, nil
}

// loadSpecEmbeddingsFromDB читает эмбеддинги поколения (embed_level='spec')
// вместе с метаданными capability и именем продукта.
func loadSpecEmbeddingsFromDB(ctx context.Context, db *store.DB, generation string) ([]*specEmbeddingRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT se.spec_id, se.embedding, sc.capability_name, sc.title,
		       COALESCE(sc.purpose, ''), sc.line_start, sc.line_end, se.embed_text,
		       COALESCE(dp.product_name, '')
		FROM spec_embeddings se
		JOIN spec_capabilities sc ON se.spec_id = sc.id
		LEFT JOIN ds_products dp ON dp.id = sc.ds_product_id
		WHERE se.generation = $1 AND se.embed_level = 'spec'
		ORDER BY se.spec_id`, generation)
	if err != nil {
		return nil, fmt.Errorf("spec semantic load embeddings: %w", err)
	}
	defer rows.Close()

	out := make([]*specEmbeddingRow, 0, 256)
	for rows.Next() {
		row := &specEmbeddingRow{}
		var emb pq.Float64Array
		if err := rows.Scan(&row.CapabilityID, &emb, &row.CapabilityName, &row.Title,
			&row.Purpose, &row.LineStart, &row.LineEnd, &row.Snippet, &row.Product); err != nil {
			return nil, fmt.Errorf("spec semantic scan embeddings: %w", err)
		}
		row.FloatsLE = make([]float32, len(emb))
		for i, v := range emb {
			row.FloatsLE[i] = float32(v)
		}
		row.Dim = len(row.FloatsLE)
		out = append(out, row)
	}
	return out, rows.Err()
}

// specSemanticHolder — кэш в памяти semantic-слоя spec-поиска: модель
// (spec_lsa_model.bin) и эмбеддинги поколения загружаются один раз и
// переиспользуются между вызовами процесса. Инвалидация — по поколению
// (дешёвая EXISTS-проверка в БД при каждом обращении); загрузка
// сериализована (singleflight).
type specSemanticHolder struct {
	mu    sync.RWMutex
	gen   string
	model *specfts.LSAModel
	rows  []*specEmbeddingRow

	loadMu sync.Mutex
	loads  int // число загрузок модели+эмбеддингов (диагностика/тесты)
}

// specSemanticCache — глобальный держатель кэша spec-semantic-слоя
// (searchSpecSemantic — пакетовая функция, кэш — на процесс).
var specSemanticCache specSemanticHolder

func (h *specSemanticHolder) snapshot() (string, *specfts.LSAModel, []*specEmbeddingRow) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.gen, h.model, h.rows
}

func (h *specSemanticHolder) store(gen string, model *specfts.LSAModel, rows []*specEmbeddingRow) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen = gen
	h.model = model
	h.rows = rows
	h.loads++
}

// loadCount возвращает число выполненных загрузок (для тестов).
func (h *specSemanticHolder) loadCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.loads
}

// reset сбрасывает слот и счётчик загрузок (для тестов).
func (h *specSemanticHolder) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen, h.model, h.rows = "", nil, nil
	h.loads = 0
}

// get возвращает модель и эмбеддинги актуального поколения.
// Семантика ошибок прежняя: модель не обучена — errs.ErrSpecModelNotFound;
// поколение без эмбеддингов — ошибка.
func (h *specSemanticHolder) get(ctx context.Context, db *store.DB) (*specfts.LSAModel, []*specEmbeddingRow, error) {
	// быстрый путь: закэшированное поколение ещё актуально
	if gen, model, rows := h.snapshot(); model != nil {
		has, err := db.HasSpecLSAGeneration(ctx, gen)
		if err != nil {
			return nil, nil, fmt.Errorf("check spec LSA generation: %w", err)
		}
		if has {
			return model, rows, nil
		}
	}

	// медленный путь: единственный загрузчик, остальные ждут и переиспользуют
	h.loadMu.Lock()
	defer h.loadMu.Unlock()

	if gen, model, rows := h.snapshot(); model != nil {
		has, err := db.HasSpecLSAGeneration(ctx, gen)
		if err != nil {
			return nil, nil, fmt.Errorf("check spec LSA generation: %w", err)
		}
		if has {
			return model, rows, nil
		}
	}

	model, err := specfts.LoadLSAModel(config.SpecLSAModelPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, errs.ErrSpecModelNotFound
		}
		return nil, nil, fmt.Errorf("load spec LSA model: %w", err)
	}
	if model.Vocab == nil || model.VT == nil || model.Generation == "" || model.K <= 0 {
		return nil, nil, fmt.Errorf("invalid spec LSA model")
	}
	has, err := db.HasSpecLSAGeneration(ctx, model.Generation)
	if err != nil {
		return nil, nil, fmt.Errorf("check spec LSA generation: %w", err)
	}
	if !has {
		return nil, nil, fmt.Errorf("spec LSA generation %q has no embeddings", model.Generation)
	}
	rows, err := loadSpecEmbeddingsCached(ctx, db, model.Generation)
	if err != nil {
		return nil, nil, err
	}
	h.store(model.Generation, model, rows)
	return model, rows, nil
}
