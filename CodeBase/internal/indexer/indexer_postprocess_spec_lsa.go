package indexer

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/specfts"
)

// postProcessSpecLSA выполняет LSA-обучение и вставку vocab/embeddings.
// Вызывается после всех параллельных постпроцессоров и профилирования продукта.
func (idx *Indexer) postProcessSpecLSA(ctx context.Context, collector *statsCollector) {
	cfg := config.Get()
	if cfg == nil || !cfg.Spec.LSAEnabled {
		return
	}

	stats := collector.Snapshot()

	// Подсчёт изменённых capability
	changedCount := stats.SpecCapabilities

	// Загрузка всех capability
	caps, err := idx.db.LoadAllSpecCapabilitiesForLSA(ctx)
	if err != nil {
		idx.logError("<post-processing>", "spec-lsa: error loading capabilities: %v", err)
		return
	}

	nDocs := len(caps)
	if nDocs == 0 {
		return
	}

	// Проверка минимального размера корпуса
	if nDocs < cfg.Spec.LSAMinCorpus {
		return
	}

	modelPath := cfg.Spec.LSAModelPath
	if modelPath == "" {
		modelPath = filepath.Join(filepath.Dir(config.GetConfigFile()), "spec_lsa_model.bin")
	}

	// Проверка порога пересчёта (только для update, не init)
	// При init (changedCount == nDocs) — всегда обучаем
	// При update — если changedCount >= threshold
	if _, err := os.Stat(modelPath); err == nil && changedCount < nDocs && changedCount < cfg.Spec.LSARetrainThreshold {
		return
	}

	// Сборка документов
	docs := make([]specfts.Document, nDocs)
	for i, c := range caps {
		text := strings.Join([]string{c.Title, c.Purpose, c.Notes}, " ")
		docs[i] = specfts.Document{
			ID:   c.ID,
			Text: text,
		}
	}

	// Построение словаря
	vocab := specfts.BuildVocab(docs, cfg.Spec.LSAMinDF, cfg.Spec.LSAMaxDF)
	if len(vocab.Terms) == 0 {
		return
	}

	// TF-IDF матрица
	tfidf := vocab.TFIDFMatrix(docs)

	// SVD
	k := cfg.Spec.LSAK
	u, s, vt, err := specfts.SVD(tfidf, k)
	if err != nil {
		idx.logError("<post-processing>", "spec-lsa: SVD error: %v", err)
		return
	}
	if u == nil || s == nil || vt == nil {
		idx.logError("<post-processing>", "spec-lsa: SVD returned nil")
		return
	}
	actualK := len(s)

	// Сохранение модели в файл
	lsaModel := &specfts.LSAModel{
		Vocab:     vocab,
		VT:        vt,
		Singulars: s,
		K:         actualK,
		NumDocs:   nDocs,
		NumTerms:  len(vocab.Terms),
	}

	if err := specfts.SaveLSAModel(lsaModel, modelPath); err != nil {
		idx.logError("<post-processing>", "spec-lsa: save model error: %v", err)
		return
	}
	// Очистка и вставка vocab
	if err := idx.db.ClearSpecVocab(ctx); err != nil {
		idx.logError("<post-processing>", "spec-lsa: clear vocab error: %v", err)
		return
	}

	vocabTerms := make([]model.SpecVocabTerm, len(vocab.Terms))
	for i, term := range vocab.Terms {
		vocabTerms[i] = model.SpecVocabTerm{
			Term:    term,
			DocFreq: vocab.DocFreq[i],
			IDF:     vocab.IDF[i],
		}
	}
	if err := idx.db.InsertSpecVocabBatch(ctx, vocabTerms); err != nil {
		idx.logError("<post-processing>", "spec-lsa: insert vocab error: %v", err)
		return
	}
	// Очистка и вставка embeddings
	if err := idx.db.ClearSpecEmbeddings(ctx); err != nil {
		idx.logError("<post-processing>", "spec-lsa: clear embeddings error: %v", err)
		return
	}

	embeddings := make([]model.SpecEmbedding, nDocs)
	for i, c := range caps {
		emb := specfts.DocEmbedding(u, s, i)
		embedText := strings.Join([]string{c.Title, c.Purpose, c.Notes}, " ")
		embeddings[i] = model.SpecEmbedding{
			SpecID:      c.ID,
			EmbedLevel:  "spec",
			EmbedText:   embedText,
			Embedding:   emb,
			EmbedMethod: "tfidf-lsa",
			EmbedDim:    actualK,
		}
	}
	if err := idx.db.InsertSpecEmbeddingsBatch(ctx, embeddings); err != nil {
		idx.logError("<post-processing>", "spec-lsa: insert embeddings error: %v", err)
		return
	}
	// Проверка что файл модели существует
	if _, err := os.Stat(modelPath); err != nil {
		idx.logError("<post-processing>", "spec-lsa: model file not found after save: %v", err)
	}
}
