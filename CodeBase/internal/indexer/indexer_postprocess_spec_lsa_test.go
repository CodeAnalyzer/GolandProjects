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
		name       string
		input      lsaDecisionInput
		want       lsaRetrainDecision
		wantPend   int
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
