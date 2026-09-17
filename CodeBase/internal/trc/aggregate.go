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

// AggregateByProcedure агрегирует завершённые вызовы процедур по имени,
// отсортированные по TotalMs по убыванию.
func AggregateByProcedure(events []TRCEvent) []TRCProcAgg {
	type acc struct {
		count int
		total int64
		min   int64
		max   int64
	}
	byProc := make(map[string]*acc)
	var order []string
	for _, ev := range events {
		if ev.EventName != "SP:Completed" || ev.Procedure == "" {
			continue
		}
		a, ok := byProc[ev.Procedure]
		if !ok {
			a = &acc{min: ev.DurationMs, max: ev.DurationMs}
			byProc[ev.Procedure] = a
			order = append(order, ev.Procedure)
		}
		a.count++
		a.total += ev.DurationMs
		if ev.DurationMs < a.min {
			a.min = ev.DurationMs
		}
		if ev.DurationMs > a.max {
			a.max = ev.DurationMs
		}
	}

	result := make([]TRCProcAgg, 0, len(order))
	for _, proc := range order {
		a := byProc[proc]
		result = append(result, TRCProcAgg{
			Procedure: proc,
			Count:     a.count,
			TotalMs:   a.total,
			MinMs:     a.min,
			MaxMs:     a.max,
			AvgMs:     float64(a.total) / float64(a.count),
		})
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
