package indexer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/specfts"
)

// postProcessDescLSA обучает LSA-модель корпуса описаний (процедуры +
// API-контракты) и публикует поколение в desc_vocab/desc_embeddings.
// Корпус независим от спекового (D8 change add-description-search):
// собственные sidecar-файлы desc_lsa_model.bin / desc_lsa_state.json и
// собственная машина переобучения; spec-модель не затрагивается.
func (idx *Indexer) postProcessDescLSA(ctx context.Context, collector *statsCollector) {
	cfg := config.Get()
	if cfg == nil || !cfg.DescLSA.Enabled() {
		return
	}

	params := specfts.LSAParams{
		MinDF: cfg.DescLSA.LSAMinDF,
		MaxDF: cfg.DescLSA.LSAMaxDF,
		K:     cfg.DescLSA.LSAK,
	}

	modelPath := config.DescLSAModelPath()
	statePath := config.DescLSAStatePath()

	// Стадия выставляется до загрузки state/модели: загрузка большой модели
	// может занять больше тика репортера, и пока Stage пуст, в паузе между
	// spec-lsa и desc-lsa репортер вернёт строку счётчиков и продублирует её.
	setStage := func(stage string) {
		collector.Add(func(stats *model.ScanStats) { stats.Stage = stage })
	}
	defer setStage("") // сброс стадии для прогресс-репортера при любом выходе

	setStage("desc-lsa: load corpus")

	// Изменённые за прогон сущности корпуса описаний: процедуры + контракты.
	stats := collector.Snapshot()
	corpusDelta := stats.Procedures + stats.APIContracts

	loadedState, err := specfts.LoadLSAState(statePath)
	if err != nil {
		idx.logError("<post-processing>", "desc-lsa: state load error (treating as missing): %v", err)
		loadedState = nil
	}
	var loadedModel *specfts.LSAModel
	hasGeneration := false
	if loadedState != nil {
		var modelErr error
		loadedModel, modelErr = specfts.LoadLSAModel(modelPath)
		if modelErr != nil {
			idx.logError("<post-processing>", "desc-lsa: model load error (treating publication as missing): %v", modelErr)
			loadedModel = nil
		} else {
			var generationErr error
			hasGeneration, generationErr = idx.db.HasDescLSAGeneration(ctx, loadedModel.Generation)
			if generationErr != nil {
				idx.logError("<post-processing>", "desc-lsa: generation check error (treating publication as missing): %v", generationErr)
				hasGeneration = false
			}
		}
	}
	state, previousGeneration := selectLSADecisionState(loadedState, loadedModel, hasGeneration)

	rows, err := idx.db.LoadDescriptionsForLSA(ctx)
	if err != nil {
		idx.logError("<post-processing>", "desc-lsa: error loading corpus: %v", err)
		return
	}

	nDocs := len(rows)
	if nDocs == 0 {
		return
	}
	if nDocs < cfg.DescLSA.LSAMinCorpus {
		return
	}

	// Сборка документов: имя×2 + описание (по образцу spec-варианта LSADocText).
	docs := make([]specfts.Document, nDocs)
	for i, row := range rows {
		docs[i] = specfts.Document{
			ID:   row.ID,
			Text: specfts.LSADocText(row.Name, "", "", row.Description),
		}
	}
	fingerprint := specfts.CorpusFingerprint(docs, params)

	deletedDocs := 0
	if state != nil && state.NumDocs > nDocs {
		deletedDocs = state.NumDocs - nDocs
	}

	decision, newPending := decideLSARetrain(lsaDecisionInput{
		State:       state,
		Fingerprint: fingerprint,
		Params:      params,
		CorpusDelta: corpusDelta,
		DeletedCaps: deletedDocs,
		Threshold:   cfg.DescLSA.RetrainThreshold(),
	})

	switch decision {
	case lsaDecisionSkipIdempotent:
		return
	case lsaDecisionDefer:
		state.Pending = newPending
		state.RetryFingerprint = ""
		if err := specfts.SaveLSAState(statePath, state); err != nil {
			idx.logError("<post-processing>", "desc-lsa: state save error: %v", err)
		}
		return
	}

	// Маркер повторной попытки: при ошибке обучения ниже следующий прогон повторит.
	retryState := &specfts.LSAState{
		Fingerprint:      "",
		Generation:       "",
		RetryFingerprint: fingerprint,
		Pending:          0,
		Params:           params,
		Algorithm:        specfts.AlgorithmVersion,
		NumDocs:          nDocs,
	}
	if state != nil {
		copyState := *state
		retryState = &copyState
		retryState.RetryFingerprint = fingerprint
	}
	if err := specfts.SaveLSAState(statePath, retryState); err != nil {
		idx.logError("<post-processing>", "desc-lsa: retry marker save error: %v", err)
		return
	}

	// Построение словаря
	setStage("desc-lsa: build vocab")
	vocab := specfts.BuildVocab(docs, cfg.DescLSA.LSAMinDF, cfg.DescLSA.LSAMaxDF)
	if len(vocab.Terms) == 0 {
		return
	}

	// TF-IDF матрица
	setStage(fmt.Sprintf("desc-lsa: tf-idf matrix (%d docs x %d terms)", nDocs, len(vocab.Terms)))
	tfidf := vocab.TFIDFMatrix(docs)

	// SVD
	k := cfg.DescLSA.LSAK
	setStage(fmt.Sprintf("desc-lsa: svd (%d docs x %d terms, k=%d; first run may take a long time)", nDocs, len(vocab.Terms), k))
	u, s, vt, err := specfts.SVD(tfidf, k)
	if err != nil {
		idx.logError("<post-processing>", "desc-lsa: SVD error: %v", err)
		return
	}
	if u == nil || s == nil || vt == nil {
		idx.logError("<post-processing>", "desc-lsa: SVD returned nil")
		return
	}
	actualK := len(s)

	// Финализация в отвязанном контексте: SVD — часы вычислений, публикация —
	// секунды. Ctrl+C, попавший в финализацию, не должен обнулять результат
	// обучения (инцидент 2026-10-05: "publish generation error: context canceled"
	// после многочасового SVD).
	finalizeCtx := context.WithoutCancel(ctx)

	// Сохранение модели
	setStage("desc-lsa: save model + vocab + embeddings")
	generation := fingerprint
	lsaModel := &specfts.LSAModel{
		Generation: generation,
		Vocab:      vocab,
		VT:         vt,
		Singulars:  s,
		K:          actualK,
		NumDocs:    nDocs,
		NumTerms:   len(vocab.Terms),
	}

	vocabTerms := make([]model.SpecVocabTerm, len(vocab.Terms))
	for i, term := range vocab.Terms {
		vocabTerms[i] = model.SpecVocabTerm{
			Generation: generation,
			Term:       term,
			DocFreq:    vocab.DocFreq[i],
			IDF:        vocab.IDF[i],
		}
	}

	embeddings := make([]model.DescEmbedding, nDocs)
	for i, row := range rows {
		emb := specfts.DocEmbedding(u, s, i)
		embeddings[i] = model.DescEmbedding{
			Generation:  generation,
			EntityType:  row.EntityType,
			EntityID:    row.ID,
			EmbedText:   specfts.LSADocText(row.Name, "", "", row.Description),
			Embedding:   emb,
			EmbedMethod: "tfidf-lsa",
			EmbedDim:    actualK,
		}
	}

	tempPath, err := specfts.WriteLSAModelTemp(lsaModel, modelPath)
	if err != nil {
		idx.logError("<post-processing>", "desc-lsa: save model temp error: %v", err)
		return
	}
	if err := idx.db.PublishDescLSAGeneration(finalizeCtx, generation, vocabTerms, embeddings); err != nil {
		_ = os.Remove(tempPath)
		idx.logError("<post-processing>", "desc-lsa: publish generation error: %v", err)
		return
	}
	if err := specfts.ActivateLSAModel(tempPath, modelPath); err != nil {
		idx.logError("<post-processing>", "desc-lsa: activate model error: %v", err)
		return
	}
	// State пишется последним: при ошибке выше state остаётся прежним.
	if err := specfts.SaveLSAState(statePath, &specfts.LSAState{
		Fingerprint:      fingerprint,
		Generation:       generation,
		RetryFingerprint: "",
		Pending:          0,
		Params:           params,
		Algorithm:        specfts.AlgorithmVersion,
		NumDocs:          nDocs,
		TrainedAt:        time.Now(), // локальное время оператора
	}); err != nil {
		idx.logError("<post-processing>", "desc-lsa: state save error: %v", err)
		return
	}
	if err := idx.db.DeleteDescLSAGenerationsExcept(finalizeCtx, generation, previousGeneration); err != nil {
		idx.logError("<post-processing>", "desc-lsa: stale generation cleanup error: %v", err)
	}
}
