package specfts

// SemanticDiagnostics — общая диагностика semantic-слоя поиска.
// Используется spec-search и desc-search, чтобы причина пустой semantic-секции
// и метрики покрытия/порогов были единообразны в CLI и MCP.
type SemanticDiagnostics struct {
	FilteredOut       int      `json:"filtered_out,omitempty"`       // сколько кандидатов отсеяно порогами (только threshold/oov)
	Threshold         float64  `json:"threshold,omitempty"`          // применённый абсолютный порог cosine
	Coverage          *float64 `json:"coverage,omitempty"`           // доля распознанных слов запроса (query_coverage)
	CoverageThreshold *float64 `json:"coverage_threshold,omitempty"` // порог покрытия (query_coverage)
	Reason            string   `json:"reason,omitempty"`             // threshold | oov | query_coverage | unavailable
	OOVTerms          []string `json:"oov_terms,omitempty"`          // OOV-стемы словаря или нераспознанные слова запроса
	Hint              string   `json:"hint,omitempty"`               // подсказка для пользователя
}

// OOVStems возвращает уникальные стемы запроса, отсутствующие в словаре LSA.
// Пустой/неподходящий словарь даёт nil: диагностика OOV в этом случае
// не строится.
func OOVStems(query string, vocab *Vocab) []string {
	if vocab == nil {
		return nil
	}
	stems := TokenizeToStems(query)
	var oov []string
	for _, stem := range stems {
		if _, ok := vocab.Index[stem]; !ok {
			oov = append(oov, stem)
		}
	}
	return oov
}
