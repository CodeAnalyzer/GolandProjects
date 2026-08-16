package rti

import (
	"context"
	"regexp"
	"strings"

	"github.com/codebase/internal/query"
)

// ClientLookup — узкий интерфейс для обогащения клиентских событий данными
// из CodeBase (PAS-методы/классы, DFM-формы, query_fragments). Реализуется
// *query.Query.
type ClientLookup interface {
	FindPASMethodsByName(ctx context.Context, methodName string, like bool, limit int) ([]query.MethodResult, error)
	SearchDFMForm(ctx context.Context, name string, like bool, limit int) ([]query.DFMFormResult, error)
	SearchQueryFragment(ctx context.Context, text string, limit int) ([]query.QueryFragmentResult, error)
}

// ClientEnrichment — результат обогащения клиентского события цепочкой
// DFM-форма → PAS-метод → SQL/exec → серверная процедура.
type ClientEnrichment struct {
	ClassName  string `json:"class_name"`
	MethodName string `json:"method_name"`
	Unit       string `json:"unit,omitempty"`
	SourceFile string `json:"source_file,omitempty"`
	LineNumber int    `json:"line_number,omitempty"`
	Found      bool   `json:"found"`

	DFMFormName string `json:"dfm_form_name,omitempty"`
	DFMCaption  string `json:"dfm_caption,omitempty"`

	// Только для sql_block/exec-событий: где найден похожий текст запроса.
	QueryFragmentFile string `json:"query_fragment_file,omitempty"`
	QueryFragmentLine int    `json:"query_fragment_line,omitempty"`
	OriginMethod      string `json:"origin_method,omitempty"`
}

// clientEnrichLookupLimit — сколько кандидатов запрашивать у CodeBase на
// каждый шаг поиска (класс/метод, форма, фрагмент запроса).
const clientEnrichLookupLimit = 5

// reExecParamNameOnly — используется при нормализации exec-сегмента: убирает
// конкретные значения параметров, оставляя только имена (`@Param = <val>` → `@Param`),
// чтобы сравнение с текстом query_fragments (шаблоном с плейсхолдерами) было устойчивее.
var reExecParamNameOnly = regexp.MustCompile(`(?i)@(\w+)\s*=\s*[^,\r\n]+`)

// EnrichClientEvents обогащает клиентские события данными из CodeBase.
// Возвращает map: "ClassName.MethodName" → enrichment (дедупликация запросов
// по этому ключу, аналогично EnrichCalls для серверных вызовов).
func EnrichClientEvents(ctx context.Context, q ClientLookup, events []*RTIClientEvent) map[string]*ClientEnrichment {
	result := make(map[string]*ClientEnrichment)
	for _, ev := range events {
		if ev.ClassName == "" && ev.MethodName == "" {
			continue
		}
		key := ev.ClassName + "." + ev.MethodName
		if _, ok := result[key]; ok {
			continue
		}
		result[key] = EnrichClientEvent(ctx, q, ev)
	}
	return result
}

// EnrichClientEvent обогащает одно клиентское событие цепочкой
// DFM-форма → PAS-метод → (для sql_block) query_fragment.
// Best-effort: при отсутствии совпадений возвращает Found=false без ошибки.
func EnrichClientEvent(ctx context.Context, q ClientLookup, ev *RTIClientEvent) *ClientEnrichment {
	enrich := &ClientEnrichment{
		ClassName:  ev.ClassName,
		MethodName: ev.MethodName,
	}
	if q == nil {
		return enrich
	}

	// Шаг 1: класс/метод -> pas_methods/pas_units.
	if ev.MethodName != "" {
		methods, err := q.FindPASMethodsByName(ctx, ev.MethodName, false, clientEnrichLookupLimit)
		if err == nil {
			for _, m := range methods {
				if ev.ClassName != "" && !strings.EqualFold(m.ClassName, ev.ClassName) {
					continue
				}
				enrich.Unit = m.UnitName
				enrich.SourceFile = m.File
				enrich.LineNumber = m.LineNumber
				enrich.Found = true
				break
			}
		}
	}

	// Шаг 2: DFM-форма, если класс соответствует UI-форме (инфраструктурные
	// классы вроде DsADORecordset/TCodeProtection формой не являются — это
	// ожидаемо и не считается ошибкой).
	if ev.ClassName != "" {
		forms, err := q.SearchDFMForm(ctx, ev.ClassName, false, 1)
		if err == nil && len(forms) > 0 {
			enrich.DFMFormName = forms[0].FormName
			enrich.DFMCaption = forms[0].Caption
		}
	}

	// Шаг 3: для exec-блоков — поиск похожего текста в query_fragments,
	// чтобы найти PAS-метод/JS-функцию, сгенерировавшую этот SQL-вызов.
	if ev.Kind == "sql_block" && ev.SQL != nil && ev.SQL.ExecProcedure != "" {
		fragments, err := q.SearchQueryFragment(ctx, ev.SQL.ExecProcedure, clientEnrichLookupLimit)
		if err == nil {
			normalizedLog := normalizeExecSegmentForCompare(ev.SQL.Text)
			for _, f := range fragments {
				if !strings.Contains(strings.ToLower(f.QueryText), strings.ToLower(ev.SQL.ExecProcedure)) {
					continue
				}
				normalizedFragment := normalizeExecSegmentForCompare(f.QueryText)
				if normalizedLog == "" || normalizedFragment == "" || execParamSetsOverlap(normalizedLog, normalizedFragment) {
					enrich.QueryFragmentFile = f.File
					enrich.QueryFragmentLine = f.LineNumber
					enrich.OriginMethod = f.ComponentName
					break
				}
			}
		}
	}

	return enrich
}

// normalizeExecSegmentForCompare убирает конкретные значения параметров exec-вызова,
// оставляя только набор имён параметров — для устойчивого сравнения фактического
// вызова (из лога, с реальными значениями) с шаблоном в query_fragments.
func normalizeExecSegmentForCompare(text string) string {
	if text == "" {
		return ""
	}
	return reExecParamNameOnly.ReplaceAllString(text, "@$1")
}

// execParamSetsOverlap — грубая эвристика совпадения двух нормализованных
// exec-вызовов: считаем совпадением, если хотя бы половина имён параметров
// из лога присутствует в тексте фрагмента.
func execParamSetsOverlap(logText, fragmentText string) bool {
	logParams := reExecParamLine.FindAllStringSubmatch(logText, -1)
	if len(logParams) == 0 {
		return true
	}
	matched := 0
	fragmentLower := strings.ToLower(fragmentText)
	for _, m := range logParams {
		if strings.Contains(fragmentLower, "@"+strings.ToLower(m[1])) {
			matched++
		}
	}
	return matched*2 >= len(logParams)
}
