//go:build integration

package specsvc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/specfts"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// Локальные фикстуры (внешний specsvc_test недоступен из внутреннего пакета).

func cacheTestInsertFile(t *testing.T, db *store.DB, path string) int64 {
	t.Helper()
	var scanID int64
	if err := db.QueryRow(
		`INSERT INTO scan_runs (root_path, status) VALUES ('/test', 'done') RETURNING id`,
	).Scan(&scanID); err != nil {
		t.Fatalf("insert scan_run: %v", err)
	}
	var fileID int64
	if err := db.QueryRow(
		`INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		 VALUES ($1, $2, $2, 'md', 'h', NOW()) RETURNING id`,
		scanID, path,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert file %s: %v", path, err)
	}
	return fileID
}

func cacheTestConfigureModel(t *testing.T, modelPath string) {
	t.Helper()
	oldConfigFile := config.GetConfigFile()
	oldCfg := config.Get()
	var oldCfgCopy config.Config
	if oldCfg != nil {
		oldCfgCopy = *oldCfg
	}
	config.CreateDefault(filepath.Dir(modelPath))
	cfg := config.Get()
	cfg.Spec.LSAModelPath = modelPath
	cfg.Spec.LSAEnabled = true
	config.SetConfigFile(filepath.Join(filepath.Dir(modelPath), "codebase.toml"))
	t.Cleanup(func() {
		config.SetConfigFile(oldConfigFile)
		if current := config.Get(); current != nil {
			if oldCfg != nil {
				*current = oldCfgCopy
			} else {
				current.Spec.LSAEnabled = false
			}
		}
	})
}

func cacheTestWriteModel(t *testing.T, path, generation string) {
	t.Helper()
	vocab := &specfts.Vocab{
		Terms:   []string{"арест"},
		Index:   map[string]int{"арест": 0},
		DocFreq: []int{1},
		IDF:     []float64{1},
	}
	vt := specfts.NewDenseMatrix(1, 1)
	vt.Set(0, 0, 1)
	m := &specfts.LSAModel{Generation: generation, Vocab: vocab, VT: vt, Singulars: []float64{1}, K: 1, NumDocs: 1, NumTerms: 1}
	if err := specfts.SaveLSAModel(m, path); err != nil {
		t.Fatalf("SaveLSAModel: %v", err)
	}
}

func cacheTestInsertCapability(t *testing.T, db *store.DB, fileID int64, capName string) int64 {
	t.Helper()
	var cfgID int64
	if err := db.QueryRow(
		`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'fa-contracts') RETURNING id`,
		fileID,
	).Scan(&cfgID); err != nil {
		t.Fatalf("insert spec_config: %v", err)
	}
	var capID int64
	if err := db.QueryRow(
		`INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title)
		 VALUES ($1, $2, $3, 'Title') RETURNING id`,
		fileID, cfgID, capName,
	).Scan(&capID); err != nil {
		t.Fatalf("insert spec_capability %s: %v", capName, err)
	}
	return capID
}

// cacheTestInsertCapabilityWithProduct — spec_config + ds_product +
// spec_capability с заданным продуктом (фильтр semantic-слоя в памяти).
func cacheTestInsertCapabilityWithProduct(t *testing.T, db *store.DB, fileID int64, capName, product string) int64 {
	t.Helper()
	var cfgID int64
	if err := db.QueryRow(
		`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, $2) RETURNING id`,
		fileID, product,
	).Scan(&cfgID); err != nil {
		t.Fatalf("insert spec_config: %v", err)
	}
	var prodID int64
	if err := db.QueryRow(
		`INSERT INTO ds_products (product_name) VALUES ($1) RETURNING id`,
		product,
	).Scan(&prodID); err != nil {
		t.Fatalf("insert ds_product: %v", err)
	}
	var capID int64
	if err := db.QueryRow(
		`INSERT INTO spec_capabilities (file_id, spec_config_id, ds_product_id, capability_name, title)
		 VALUES ($1, $2, $3, $4, 'Title') RETURNING id`,
		fileID, cfgID, prodID, capName,
	).Scan(&capID); err != nil {
		t.Fatalf("insert spec_capability %s: %v", capName, err)
	}
	return capID
}

// cacheTestPublishEmbeddings — строки spec_embeddings поколения одним
// вызовом (публикация перезаписывает поколение целиком).
func cacheTestPublishEmbeddings(t *testing.T, db *store.DB, generation string, embeddings ...model.SpecEmbedding) {
	t.Helper()
	if err := db.PublishSpecLSAGeneration(context.Background(), generation, nil, embeddings); err != nil {
		t.Fatalf("PublishSpecLSAGeneration: %v", err)
	}
}

// cacheTestSetupEnv — БД + модель + sidecar-пути во временном каталоге,
// holder сброшен. Возвращает fileID для вставки capability.
func cacheTestSetupEnv(t *testing.T, generation string) (*store.DB, int64) {
	t.Helper()
	db := testutil.Open(t)
	// отдельный каталог с ретраем уборки: Windows RemoveAll падает, если
	// файл кэша ещё удерживается (AV/индексатор) сразу после rename
	dir, err := os.MkdirTemp("", "spec-semantic-cache-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for i := 0; i < 10; i++ {
			if err := os.RemoveAll(dir); err == nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
	modelPath := filepath.Join(dir, "spec-model.bin")
	cacheTestWriteModel(t, modelPath, generation)
	cacheTestConfigureModel(t, modelPath)
	specSemanticCache.reset()
	fileID := cacheTestInsertFile(t, db, "specs/semantic-cache/spec.md")
	return db, fileID
}

// Первый вызов создаёт файловый кэш, повторный — переиспользует модель и
// эмбеддинги из holder (loads не растёт), результаты идентичны.
func TestSpecSemanticCache_FileCreatedAndReused(t *testing.T) {
	db, fileID := cacheTestSetupEnv(t, "spec-cache-gen-1")
	ctx := context.Background()
	capA := cacheTestInsertCapability(t, db, fileID, "cap-a")
	capB := cacheTestInsertCapability(t, db, fileID, "cap-b")
	cacheTestPublishEmbeddings(t, db, "spec-cache-gen-1",
		model.SpecEmbedding{SpecID: capA, EmbedLevel: "spec", EmbedText: "арест счёта", Embedding: []float64{1}, EmbedMethod: "tfidf-lsa", EmbedDim: 1},
		model.SpecEmbedding{SpecID: capB, EmbedLevel: "spec", EmbedText: "арест имущества", Embedding: []float64{1}, EmbedMethod: "tfidf-lsa", EmbedDim: 1},
	)

	first, err := ExecuteSpecSearch(ctx, db, "арест", "", "", "semantic", 10)
	if err != nil {
		t.Fatalf("first search: %v", err)
	}
	if len(first.Semantic) != 2 {
		t.Fatalf("semantic hits = %d, want 2: %+v", len(first.Semantic), first.Semantic)
	}
	cachePath := config.SpecLSAEmbeddingsPath()
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("кэш не создан после первого вызова: %v", err)
	}
	if got := specSemanticCache.loadCount(); got != 1 {
		t.Fatalf("после первого вызова loads = %d, want 1", got)
	}

	second, err := ExecuteSpecSearch(ctx, db, "арест", "", "", "semantic", 10)
	if err != nil {
		t.Fatalf("second search: %v", err)
	}
	if got := specSemanticCache.loadCount(); got != 1 {
		t.Fatalf("повторный вызов перечитал модель/кэш: loads = %d, want 1", got)
	}
	if len(first.Semantic) != len(second.Semantic) {
		t.Fatalf("выдачи различаются: %d vs %d", len(first.Semantic), len(second.Semantic))
	}
	for i := range first.Semantic {
		if first.Semantic[i].CapabilityName != second.Semantic[i].CapabilityName {
			t.Fatalf("хит %d различается: %s vs %s", i,
				first.Semantic[i].CapabilityName, second.Semantic[i].CapabilityName)
		}
	}
}

// Фильтр по продукту применяется в памяти: в выдаче только capability
// запрошенного продукта.
func TestSpecSemanticCache_ProductFilterInMemory(t *testing.T) {
	db, fileID := cacheTestSetupEnv(t, "spec-cache-gen-filter")
	ctx := context.Background()
	capA := cacheTestInsertCapabilityWithProduct(t, db, fileID, "cap-cards", "fa-cards")
	capB := cacheTestInsertCapabilityWithProduct(t, db, fileID, "cap-payments", "fa-payments")
	cacheTestPublishEmbeddings(t, db, "spec-cache-gen-filter",
		model.SpecEmbedding{SpecID: capA, EmbedLevel: "spec", EmbedText: "арест счёта", Embedding: []float64{1}, EmbedMethod: "tfidf-lsa", EmbedDim: 1},
		model.SpecEmbedding{SpecID: capB, EmbedLevel: "spec", EmbedText: "арест имущества", Embedding: []float64{1}, EmbedMethod: "tfidf-lsa", EmbedDim: 1},
	)

	res, err := ExecuteSpecSearch(ctx, db, "арест", "fa-cards", "", "semantic", 10)
	if err != nil {
		t.Fatalf("search with product: %v", err)
	}
	if len(res.Semantic) != 1 {
		t.Fatalf("semantic hits = %d, want 1: %+v", len(res.Semantic), res.Semantic)
	}
	if res.Semantic[0].CapabilityName != "cap-cards" {
		t.Fatalf("hit = %s, want cap-cards", res.Semantic[0].CapabilityName)
	}
	if res.Semantic[0].Product != "fa-cards" {
		t.Fatalf("hit product = %s, want fa-cards", res.Semantic[0].Product)
	}
}

// Повреждённый кэш не фатален: пересборка из БД и перезапись файла.
func TestSpecSemanticCache_CorruptedCacheRebuild(t *testing.T) {
	db, fileID := cacheTestSetupEnv(t, "spec-cache-gen-corrupt")
	ctx := context.Background()
	capA := cacheTestInsertCapability(t, db, fileID, "cap-corrupt")
	cacheTestPublishEmbeddings(t, db, "spec-cache-gen-corrupt",
		model.SpecEmbedding{SpecID: capA, EmbedLevel: "spec", EmbedText: "арест счёта", Embedding: []float64{1}, EmbedMethod: "tfidf-lsa", EmbedDim: 1},
	)

	if _, err := ExecuteSpecSearch(ctx, db, "арест", "", "", "semantic", 10); err != nil {
		t.Fatalf("first search: %v", err)
	}
	// портим файловый кэш и сбрасываем holder — следующая загрузка с диска
	if err := os.WriteFile(config.SpecLSAEmbeddingsPath(), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	specSemanticCache.reset()

	res, err := ExecuteSpecSearch(ctx, db, "арест", "", "", "semantic", 10)
	if err != nil {
		t.Fatalf("search after cache corruption: %v", err)
	}
	if len(res.Semantic) != 1 || res.Semantic[0].CapabilityName != "cap-corrupt" {
		t.Fatalf("semantic hits after rebuild: %+v", res.Semantic)
	}
	// кэш перезаписан валидным файлом
	rows, err := loadSpecEmbeddingsCached(ctx, db, "spec-cache-gen-corrupt")
	if err != nil || len(rows) != 1 {
		t.Fatalf("кэш не восстановлен: rows=%d err=%v", len(rows), err)
	}
}

// Исчезновение поколения: holder перезагружается, отсутствие эмбеддингов
// поколения — прежняя ошибка.
func TestSpecSemanticCache_GenerationInvalidation(t *testing.T) {
	db, fileID := cacheTestSetupEnv(t, "spec-cache-gen-inval")
	ctx := context.Background()
	capA := cacheTestInsertCapability(t, db, fileID, "cap-inval")
	cacheTestPublishEmbeddings(t, db, "spec-cache-gen-inval",
		model.SpecEmbedding{SpecID: capA, EmbedLevel: "spec", EmbedText: "арест счёта", Embedding: []float64{1}, EmbedMethod: "tfidf-lsa", EmbedDim: 1},
	)

	if _, err := ExecuteSpecSearch(ctx, db, "арест", "", "", "semantic", 10); err != nil {
		t.Fatalf("first search: %v", err)
	}
	if specSemanticCache.loadCount() != 1 {
		t.Fatalf("loads = %d, want 1", specSemanticCache.loadCount())
	}

	// поколение исчезло из БД (переиндексация) → holder перезагружается
	if _, err := db.Exec(`DELETE FROM spec_embeddings WHERE generation = $1`, "spec-cache-gen-inval"); err != nil {
		t.Fatalf("delete embeddings: %v", err)
	}
	_, err := ExecuteSpecSearch(ctx, db, "арест", "", "", "semantic", 10)
	if err == nil {
		t.Fatal("ожидается ошибка отсутствия эмбеддингов поколения")
	}
}
