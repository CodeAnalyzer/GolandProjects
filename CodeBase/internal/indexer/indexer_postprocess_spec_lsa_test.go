package indexer

import (
	"testing"

	"github.com/codebase/internal/specfts"
)

// TestDecideLSARetrain покрывает машину состояний переобучения (design D2).
func TestDecideLSARetrain(t *testing.T) {
	fp := "fingerprint-a"
	params := specfts.LSAParams{MinDF: 3, MaxDF: 0.3, K: 128}

	tests := []struct {
		name     string
		input    lsaDecisionInput
		want     lsaRetrainDecision
		wantPend int
	}{
		{
			name:     "первый запуск: state отсутствует — обучение",
			input:    lsaDecisionInput{State: nil, Fingerprint: fp, Threshold: 0},
			want:     lsaDecisionRetrain,
			wantPend: 0,
		},
		{
			name: "идемпотентный прогон: fingerprint совпал",
			input: lsaDecisionInput{
				State:       &specfts.LSAState{Fingerprint: fp, Pending: 5, Params: params, Algorithm: specfts.AlgorithmVersion},
				Fingerprint: fp, Params: params, CorpusDelta: 0, Threshold: 50,
			},
			want:     lsaDecisionSkipIdempotent,
			wantPend: 5,
		},
		{
			name: "текст изменился, порог не достигнут — pending растёт",
			input: lsaDecisionInput{
				State:       &specfts.LSAState{Fingerprint: fp, Pending: 0, NumDocs: 100, Params: params, Algorithm: specfts.AlgorithmVersion},
				Fingerprint: "fingerprint-b", Params: params, CorpusDelta: 10, Threshold: 50,
			},
			want:     lsaDecisionDefer,
			wantPend: 10,
		},
		{
			name: "накопление через прогоны: pending 45 + 5 ≥ 50 — переобучение",
			input: lsaDecisionInput{
				State:       &specfts.LSAState{Fingerprint: fp, Pending: 45, NumDocs: 100, Params: params, Algorithm: specfts.AlgorithmVersion},
				Fingerprint: "fingerprint-b", Params: params, CorpusDelta: 5, Threshold: 50,
			},
			want:     lsaDecisionRetrain,
			wantPend: 0,
		},
		{
			name: "нулевой порог: любое отличие fingerprint — переобучение",
			input: lsaDecisionInput{
				State:       &specfts.LSAState{Fingerprint: fp, Pending: 0, NumDocs: 100, Params: params, Algorithm: specfts.AlgorithmVersion},
				Fingerprint: "fingerprint-b", Params: params, CorpusDelta: 1, Threshold: 0,
			},
			want:     lsaDecisionRetrain,
			wantPend: 0,
		},
		{
			name: "нулевой порог и корпус изменился без статистики прогона (удаление в прошлом прогоне)",
			input: lsaDecisionInput{
				State:       &specfts.LSAState{Fingerprint: fp, Pending: 0, NumDocs: 105, Params: params, Algorithm: specfts.AlgorithmVersion},
				Fingerprint: "fingerprint-b", CorpusDelta: 0, DeletedCaps: 5, Threshold: 0,
			},
			want:     lsaDecisionRetrain,
			wantPend: 0,
		},
		{
			name: "порог не достигнут, delta=0, fingerprint отличается — минимум 1 изменение",
			input: lsaDecisionInput{
				State:       &specfts.LSAState{Fingerprint: fp, Pending: 0, NumDocs: 100, Params: params, Algorithm: specfts.AlgorithmVersion},
				Fingerprint: "fingerprint-b", Params: params, CorpusDelta: 0, Threshold: 50,
			},
			want:     lsaDecisionDefer,
			wantPend: 1,
		},
		{
			name: "изменение параметра модели (k) — переобучение безусловно, порог не действует",
			input: lsaDecisionInput{
				State:       &specfts.LSAState{Fingerprint: fp, Pending: 0, NumDocs: 100, Params: specfts.LSAParams{MinDF: 3, MaxDF: 0.3, K: 128}, Algorithm: specfts.AlgorithmVersion},
				Fingerprint: "fingerprint-b", Params: specfts.LSAParams{MinDF: 3, MaxDF: 0.3, K: 512}, CorpusDelta: 0, Threshold: 1000,
			},
			want:     lsaDecisionRetrain,
			wantPend: 0,
		},
		{
			name: "смена версии алгоритма — переобучение безусловно, амортизация не действует",
			input: lsaDecisionInput{
				State:       &specfts.LSAState{Fingerprint: fp, Pending: 0, NumDocs: 100, Params: params, Algorithm: specfts.AlgorithmVersion - 1},
				Fingerprint: fp, Params: params, CorpusDelta: 0, Threshold: 1000,
			},
			want:     lsaDecisionRetrain,
			wantPend: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, pend := decideLSARetrain(tt.input)
			if got != tt.want {
				t.Errorf("decision = %v, want %v", got, tt.want)
			}
			if pend != tt.wantPend {
				t.Errorf("pending = %d, want %d", pend, tt.wantPend)
			}
		})
	}
}

func TestLSAPublicationReady(t *testing.T) {
	state := &specfts.LSAState{Generation: specfts.LegacyGeneration}
	model := &specfts.LSAModel{Generation: specfts.LegacyGeneration}
	currentState := &specfts.LSAState{Generation: "gen-current"}
	currentModel := &specfts.LSAModel{Generation: "gen-current"}

	for _, tc := range []struct {
		name          string
		state         *specfts.LSAState
		model         *specfts.LSAModel
		hasGeneration bool
		want          bool
	}{
		{name: "nil state", state: nil, model: model, hasGeneration: true},
		{name: "nil model", state: state, model: nil, hasGeneration: true},
		{name: "mismatched generation", state: state, model: currentModel, hasGeneration: true},
		{name: "missing database generation", state: currentState, model: currentModel, hasGeneration: false},
		{name: "valid legacy", state: state, model: model, hasGeneration: true, want: true},
		{name: "valid current", state: currentState, model: currentModel, hasGeneration: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := lsaPublicationReady(tc.state, tc.model, tc.hasGeneration); got != tc.want {
				t.Fatalf("lsaPublicationReady() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSelectLSADecisionState(t *testing.T) {
	validState := &specfts.LSAState{Generation: "gen-current"}
	validModel := &specfts.LSAModel{Generation: "gen-current"}
	markerState := &specfts.LSAState{Generation: "gen-current", RetryFingerprint: "fp-new"}
	invalidModel := &specfts.LSAModel{Generation: "gen-other"}

	for _, tc := range []struct {
		name          string
		loadedState   *specfts.LSAState
		model         *specfts.LSAModel
		hasGeneration bool
		wantState     *specfts.LSAState
		wantPrevious  string
	}{
		{name: "valid publication without marker", loadedState: validState, model: validModel, hasGeneration: true, wantState: validState, wantPrevious: "gen-current"},
		{name: "invalid publication without marker", loadedState: validState, model: invalidModel, hasGeneration: true},
		{name: "missing model with marker", loadedState: markerState, model: nil, hasGeneration: false, wantState: markerState},
		{name: "invalid publication with marker", loadedState: markerState, model: invalidModel, hasGeneration: true, wantState: markerState},
		{name: "valid publication with marker", loadedState: markerState, model: validModel, hasGeneration: true, wantState: markerState, wantPrevious: "gen-current"},
		{name: "missing generation with marker", loadedState: markerState, model: validModel, hasGeneration: false, wantState: markerState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotState, gotPrevious := selectLSADecisionState(tc.loadedState, tc.model, tc.hasGeneration)
			if gotState != tc.wantState || gotPrevious != tc.wantPrevious {
				t.Fatalf("selectLSADecisionState() = (%p, %q), want (%p, %q)", gotState, gotPrevious, tc.wantState, tc.wantPrevious)
			}
		})
	}
}

func TestLSARetrainWithRetryMarkerIgnoresThreshold(t *testing.T) {
	params := specfts.LSAParams{MinDF: 1, MaxDF: 1, K: 1}
	decision, pending := decideLSARetrain(lsaDecisionInput{
		State: &specfts.LSAState{
			Fingerprint:      "fp-old",
			RetryFingerprint: "fp-new",
			Params:           params,
			Algorithm:        specfts.AlgorithmVersion,
		},
		Fingerprint: "fp-old",
		Params:      params,
		CorpusDelta: 0,
		Threshold:   1000,
	})
	if decision != lsaDecisionRetrain || pending != 0 {
		t.Fatalf("decision = %v, pending = %d, want retrain/0", decision, pending)
	}
}

func TestLSARetrainWhenPublicationIsNotReady(t *testing.T) {
	params := specfts.LSAParams{MinDF: 1, MaxDF: 1, K: 1}
	state := &specfts.LSAState{Generation: "gen-state", Fingerprint: "same", Params: params, Algorithm: specfts.AlgorithmVersion}
	model := &specfts.LSAModel{Generation: "gen-model"}
	if lsaPublicationReady(state, model, true) {
		t.Fatal("mismatched publication must not be ready")
	}
	decision, pending := decideLSARetrain(lsaDecisionInput{
		State:       nil,
		Fingerprint: "same",
		Params:      params,
		CorpusDelta: 0,
		Threshold:   100,
	})
	if decision != lsaDecisionRetrain || pending != 0 {
		t.Fatalf("decision = %v, pending = %d, want retrain/0", decision, pending)
	}
}

func TestLSARetrainAfterCorpusDeletionWithoutDelta(t *testing.T) {
	params := specfts.LSAParams{MinDF: 1, MaxDF: 1, K: 1}
	oldFingerprint := specfts.CorpusFingerprint([]specfts.Document{{ID: 1, Text: "one"}, {ID: 2, Text: "two"}}, params)
	currentFingerprint := specfts.CorpusFingerprint([]specfts.Document{{ID: 1, Text: "one"}}, params)
	decision, pending := decideLSARetrain(lsaDecisionInput{
		State:       &specfts.LSAState{Fingerprint: oldFingerprint, NumDocs: 2, Params: params, Algorithm: specfts.AlgorithmVersion},
		Fingerprint: currentFingerprint,
		Params:      params,
		CorpusDelta: 0,
		DeletedCaps: 1,
		Threshold:   0,
	})
	if decision != lsaDecisionRetrain || pending != 0 {
		t.Fatalf("decision = %v, pending = %d, want retrain/0", decision, pending)
	}
}
