package indexer

import (
	"context"
	"os"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/specfts"
)

// lsaRetrainDecision — решение машины состояний переобучения LSA.
type lsaRetrainDecision int

const (
	lsaDecisionRetrain        lsaRetrainDecision = iota // переобучить на текущем корпусе
	lsaDecisionSkipIdempotent                           // fingerprint совпал — модель актуальна
	lsaDecisionDefer                                    // корпус изменился, порог не достигнут — отложить
)

// lsaDecisionInput — входные данные решения о переобучении.
type lsaDecisionInput struct {
	State       *specfts.LSAState // nil — state отсутствует (первый запуск или миграция)
	Fingerprint string            // fingerprint текущего корпуса
	Params      specfts.LSAParams // текущие параметры модели из конфига
	CorpusDelta int               // изменённые сущности спек за прогон (cap + req + scn)
	DeletedCaps int               // оценка удалённых capability: state.NumDocs - nDocs (>= 0)
	Threshold   int               // порог накопленных изменений; 0 = переобучать при любом изменении
}

func lsaPublicationReady(state *specfts.LSAState, model *specfts.LSAModel, hasGeneration bool) bool {
	return state != nil && model != nil && state.Generation != "" &&
		model.Generation != "" && state.Generation == model.Generation && hasGeneration
}

func selectLSADecisionState(loadedState *specfts.LSAState, model *specfts.LSAModel, hasGeneration bool) (*specfts.LSAState, string) {
	if loadedState == nil {
		return nil, ""
	}
	if lsaPublicationReady(loadedState, model, hasGeneration) {
		return loadedState, loadedState.Generation
	}
	if loadedState.RetryFingerprint != "" {
		return loadedState, ""
	}
	return nil, ""
}

// decideLSARetrain — чистая функция принятия решения о переобучении (тестируется без БД).
//
// Особенность конвейера: удаление устаревших файлов применяется ДО пост-обработки,
// поэтому в прогоне удаления корпус в БД уже отражает актуальное состояние,
// а отличие fingerprint приводит к переобучению в том же прогоне.
func decideLSARetrain(in lsaDecisionInput) (lsaRetrainDecision, int) {
	if in.State == nil {
		// Нет state: модель не обучена либо миграция со старой версии — обучаем.
		return lsaDecisionRetrain, 0
	}
	if in.State.RetryFingerprint != "" {
		return lsaDecisionRetrain, 0
	}
	if in.State.Algorithm != specfts.AlgorithmVersion {
		// Смена алгоритма обучения (код) — переобучаем безусловно,
		// амортизация порогом на это не распространяется.
		return lsaDecisionRetrain, 0
	}
	if in.State.Params != in.Params {
		// Смена параметров модели (min_df/max_df/k) — действие оператора:
		// применяем немедленно, амортизация предназначена только для дрейфа корпуса.
		return lsaDecisionRetrain, 0
	}
	if in.State.Fingerprint == in.Fingerprint {
		// Корпус и параметры идентичны обученным — идемпотентный пропуск.
		return lsaDecisionSkipIdempotent, in.State.Pending
	}
	delta := in.CorpusDelta + in.DeletedCaps
	if delta < 1 {
		// Fingerprint отличается — минимум одно изменение есть (например,
		// удаление capability, не зафиксированное в статистике прогона).
		delta = 1
	}
	if in.State.Pending+delta >= in.Threshold {
		return lsaDecisionRetrain, 0
	}
	return lsaDecisionDefer, in.State.Pending + delta
}

// postProcessSpecLSA выполняет LSA-обучение и вставку vocab/embeddings.
// Вызывается после всех параллельных постпроцессоров и профилирования продукта.
// Политика переобучения: fingerprint корпуса (тексты + параметры + версии) и
// кумулятивный порог изменений — см. decideLSARetrain и openspec change
// lsa-quality-improvement (design D2).
func (idx *Indexer) postProcessSpecLSA(ctx context.Context, collector *statsCollector) {
	cfg := config.Get()
	if cfg == nil || !cfg.Spec.LSAEnabled {
		return
	}

	stats := collector.Snapshot()
	params := specfts.LSAParams{
		MinDF: cfg.Spec.LSAMinDF,
		MaxDF: cfg.Spec.LSAMaxDF,
		K:     cfg.Spec.LSAK,
	}

	modelPath := config.SpecLSAModelPath()
	statePath := config.SpecLSAStatePath()

	// Изменённые за прогон сущности спек: capability + requirements + scenarios.
	corpusDelta := stats.SpecCapabilities + stats.SpecRequirements + stats.SpecScenarios

	loadedState, err := specfts.LoadLSAState(statePath)
	if err != nil {
		idx.logError("<post-processing>", "spec-lsa: state load error (treating as missing): %v", err)
		loadedState = nil
	}
	var loadedModel *specfts.LSAModel
	hasGeneration := false
	if loadedState != nil {
		var modelErr error
		loadedModel, modelErr = specfts.LoadLSAModel(modelPath)
		if modelErr != nil {
			idx.logError("<post-processing>", "spec-lsa: model load error (treating publication as missing): %v", modelErr)
			loadedModel = nil
		} else {
			var generationErr error
			hasGeneration, generationErr = idx.db.HasSpecLSAGeneration(ctx, loadedModel.Generation)
			if generationErr != nil {
				idx.logError("<post-processing>", "spec-lsa: generation check error (treating publication as missing): %v", generationErr)
				hasGeneration = false
			}
		}
	}
	state, previousGeneration := selectLSADecisionState(loadedState, loadedModel, hasGeneration)

	setStage := func(stage string) {
		collector.Add(func(stats *model.ScanStats) { stats.Stage = stage })
	}
	defer setStage("") // сброс стадии для прогресс-репортера при любом выходе

	setStage("spec-lsa: load corpus")
	caps, err := idx.db.LoadAllSpecCapabilitiesWithReqsForLSA(ctx)
	if err != nil {
		idx.logError("<post-processing>", "spec-lsa: error loading capabilities: %v", err)
		return
	}

	nDocs := len(caps)
	if nDocs == 0 {
		return
	}
	if nDocs < cfg.Spec.LSAMinCorpus {
		return
	}

	// Сборка документов: title×2 + purpose + notes + тексты требований и сценариев.
	docs := make([]specfts.Document, nDocs)
	for i, c := range caps {
		docs[i] = specfts.Document{
			ID:   c.ID,
			Text: specfts.LSADocText(c.Title, c.Purpose, c.Notes, c.LSAText),
		}
	}
	fingerprint := specfts.CorpusFingerprint(docs, params)

	deletedCaps := 0
	if state != nil && state.NumDocs > nDocs {
		deletedCaps = state.NumDocs - nDocs
	}

	decision, newPending := decideLSARetrain(lsaDecisionInput{
		State:       state,
		Fingerprint: fingerprint,
		Params:      params,
		CorpusDelta: corpusDelta,
		DeletedCaps: deletedCaps,
		Threshold:   cfg.Spec.RetrainThreshold(),
	})

	switch decision {
	case lsaDecisionSkipIdempotent:
		return
	case lsaDecisionDefer:
		state.Pending = newPending
		state.RetryFingerprint = ""
		if err := specfts.SaveLSAState(statePath, state); err != nil {
			idx.logError("<post-processing>", "spec-lsa: state save error: %v", err)
		}
		return
	}

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
		idx.logError("<post-processing>", "spec-lsa: retry marker save error: %v", err)
		return
	}

	// Построение словаря
	setStage("spec-lsa: build vocab")
	vocab := specfts.BuildVocab(docs, cfg.Spec.LSAMinDF, cfg.Spec.LSAMaxDF)
	if len(vocab.Terms) == 0 {
		return
	}

	// TF-IDF матрица
	setStage("spec-lsa: tf-idf matrix")
	tfidf := vocab.TFIDFMatrix(docs)

	// SVD
	setStage("spec-lsa: svd (may take 1-2 min)")
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
	setStage("spec-lsa: save model + vocab + embeddings")
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

	embeddings := make([]model.SpecEmbedding, nDocs)
	for i, c := range caps {
		emb := specfts.DocEmbedding(u, s, i)
		embedText := specfts.LSADocText(c.Title, c.Purpose, c.Notes, c.LSAText)
		embeddings[i] = model.SpecEmbedding{
			Generation:  generation,
			SpecID:      c.ID,
			EmbedLevel:  "spec",
			EmbedText:   embedText,
			Embedding:   emb,
			EmbedMethod: "tfidf-lsa",
			EmbedDim:    actualK,
		}
	}

	tempPath, err := specfts.WriteLSAModelTemp(lsaModel, modelPath)
	if err != nil {
		idx.logError("<post-processing>", "spec-lsa: save model temp error: %v", err)
		return
	}
	if err := idx.db.PublishSpecLSAGeneration(ctx, generation, vocabTerms, embeddings); err != nil {
		_ = os.Remove(tempPath)
		idx.logError("<post-processing>", "spec-lsa: publish generation error: %v", err)
		return
	}
	if err := specfts.ActivateLSAModel(tempPath, modelPath); err != nil {
		idx.logError("<post-processing>", "spec-lsa: activate model error: %v", err)
		return
	}
	// Модель и её состояние сохранены атомарно по факту успеха: state пишется последним,
	// при ошибке обучения выше state остаётся прежним и следующий прогон повторит попытку.
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
		idx.logError("<post-processing>", "spec-lsa: state save error: %v", err)
		return
	}
	if err := idx.db.DeleteSpecLSAGenerationsExcept(ctx, generation, previousGeneration); err != nil {
		idx.logError("<post-processing>", "spec-lsa: stale generation cleanup error: %v", err)
	}
}
