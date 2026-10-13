package mcp

import (
	"context"
	"log"
	"time"
)

// warmupProbe — одна проба прогрева: реплей tool-хендлера с дешёвыми
// представительными аргументами. Аргументы обязаны проходить required-валидацию
// хендлеров (непустые строки), иначе вызов уйдёт в ошибку до касания БД.
type warmupProbe struct {
	tool string
	args map[string]interface{}
}

// warmupProbesByProfile — состав проб по профилям. Только `query`: его горячие
// пути (буферы PostgreSQL + ленивые LSA/embeddings-кэши) дают холодный старт
// 5–30 с на первый вызов инструмента. Порядок проб фиксирован (design.md п.5):
// 1) семантические поиски (in-process LSA/embeddings — самые дорогие и не
// подвержены вытеснению), 2) callers (полосы листьев индексов relations +
// PK sql_procedures/files), 3) лёгкие name-пробы, 4) sql_fragment — последней
// (GIN-скан query_fragments может вытеснить прогретые полосы).
//
// Имя процедуры для callers — константа (средняя полоса fan-out по данным
// живой БД FA: 1090 прямых + 2904 косвенных строк relations).
var warmupProbesByProfile = map[string][]warmupProbe{
	"query": {
		{tool: "codebase_query_desc_search", args: map[string]interface{}{"text": "договор", "limit": 1}},
		{tool: "codebase_query_spec_search", args: map[string]interface{}{"query": "договор", "layer": "semantic", "limit": 1}},
		{tool: "codebase_query_callers", args: map[string]interface{}{"procedure": "FCD_GL_Audit_Insert", "limit": 1}},
		{tool: "codebase_query_symbol", args: map[string]interface{}{"name": "Insert", "like": true, "limit": 1}},
		{tool: "codebase_query_relations", args: map[string]interface{}{"relation_type": "references_table", "limit": 1}},
		{tool: "codebase_query_api_contract", args: map[string]interface{}{"name": "API", "like": true, "limit": 1}},
		{tool: "codebase_query_table", args: map[string]interface{}{"name": "tConsSalePortfolio"}},
		{tool: "codebase_query_form_component", args: map[string]interface{}{"name": "dl", "like": true, "limit": 1}},
		{tool: "codebase_query_sql_fragment", args: map[string]interface{}{"text": "select", "limit": 1}},
	},
}

// warmupForProfile выполняет последовательность реплей-вызовов tool-хендлеров
// из собранного registry для прогрева горячих путей (буферы БД + ленивые
// LSA/embeddings-кэши). Best-effort: ошибки отдельных проб игнорируются
// (учитываются счётчиком), probe не влияет на транспорт и результаты вызовов.
// timeoutSec > 0 ограничивает всю пачку; 0 — без ограничения.
func warmupForProfile(ctx context.Context, profile string, registry map[string]registeredTool, logger *log.Logger, timeoutSec int) {
	probes := warmupProbesByProfile[profile]
	// Пустой профиль = все инструменты, включая полный набор query: к нему
	// применяется тот же список проб, что и к профилю query.
	if profile == "" {
		probes = warmupProbesByProfile["query"]
	}
	if len(probes) == 0 || logger == nil {
		return
	}

	if timeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
		defer cancel()
	}

	start := time.Now()
	done, errs := 0, 0
	for _, probe := range probes {
		tool, ok := registry[probe.tool]
		if !ok {
			continue
		}
		if _, err := tool.Handler(ctx, probe.args); err != nil {
			errs++
			continue
		}
		done++
	}

	// Пустой профиль в логах обозначается как "all" (конвенция logMCPToolCall).
	logProfile := profile
	if logProfile == "" {
		logProfile = "all"
	}
	logger.Printf(
		"profile=%s warmup probes=%d done=%d elapsed=%s errors=%d",
		logProfile, len(probes), done, time.Since(start).Round(time.Millisecond), errs,
	)
}
