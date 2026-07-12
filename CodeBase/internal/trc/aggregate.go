package trc

import "sort"

// TRCProcAgg — агрегированная статистика по одной процедуре среди событий
// сессии: количество вызовов и min/max/avg/total длительность (мс, из
// TRCEvent.DurationMs).
type TRCProcAgg struct {
	Procedure string  `json:"procedure"`
	Count     int     `json:"count"`
	TotalMs   int64   `json:"total_ms"`
	MinMs     int64   `json:"min_ms"`
	MaxMs     int64   `json:"max_ms"`
	AvgMs     float64 `json:"avg_ms"`
	// Enriched fields (заполняются из CodeBase, см. enrich.go)
	SourceFile string `json:"source_file,omitempty"`
}

// AggregateByProcedure агрегирует события с непустым Procedure по имени
// процедуры, отсортированные по TotalMs по убыванию (самые "дорогие"
// процедуры первыми). События без Procedure (не exec-вызовы) игнорируются.
func AggregateByProcedure(events []TRCEvent) []TRCProcAgg {
	type acc struct {
		count   int
		total   int64
		min     int64
		max     int64
		hasDur  bool
	}
	byProc := make(map[string]*acc)
	var order []string
	for _, ev := range events {
		if ev.Procedure == "" {
			continue
		}
		a, ok := byProc[ev.Procedure]
		if !ok {
			a = &acc{min: -1}
			byProc[ev.Procedure] = a
			order = append(order, ev.Procedure)
		}
		a.count++
		if ev.DurationMs > 0 {
			a.hasDur = true
			a.total += ev.DurationMs
			if a.min < 0 || ev.DurationMs < a.min {
				a.min = ev.DurationMs
			}
			if ev.DurationMs > a.max {
				a.max = ev.DurationMs
			}
		}
	}

	result := make([]TRCProcAgg, 0, len(order))
	for _, proc := range order {
		a := byProc[proc]
		agg := TRCProcAgg{Procedure: proc, Count: a.count, TotalMs: a.total}
		if a.hasDur {
			agg.MinMs = a.min
			agg.MaxMs = a.max
			agg.AvgMs = float64(a.total) / float64(a.count)
		}
		result = append(result, agg)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TotalMs > result[j].TotalMs })
	return result
}

// EnrichAggregates заполняет SourceFile для каждой агрегированной процедуры
// из уже посчитанного enrichMap (см. EnrichEvents).
func EnrichAggregates(aggs []TRCProcAgg, enrichMap map[string]*ProcedureEnrichment) {
	for i := range aggs {
		if enrich, ok := enrichMap[aggs[i].Procedure]; ok && enrich != nil && enrich.Found {
			aggs[i].SourceFile = enrich.SourceFile
		}
	}
}
