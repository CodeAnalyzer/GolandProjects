package trc

import "sort"

// TRCProcAgg — агрегированная статистика по одной процедуре среди событий
// сессии: количество вызовов и min/max/avg/total длительность (мс, из
// TRCEvent.DurationMs). SPID заполняется только при группировке (spid,
// procedure) — см. AggregateOptions.GroupBySPID.
type TRCProcAgg struct {
	SPID      int     `json:"spid,omitempty"`
	Procedure string  `json:"procedure"`
	Count     int     `json:"count"`
	TotalMs   int64   `json:"total_ms"`
	MinMs     int64   `json:"min_ms"`
	MaxMs     int64   `json:"max_ms"`
	AvgMs     float64 `json:"avg_ms"`
	// Enriched fields (заполняются из CodeBase, см. enrich.go)
	SourceFile string `json:"source_file,omitempty"`
}

// EventSPID возвращает SPID события из декодированной колонки 12.
// Второе возвращаемое значение false — SPID отсутствует (аналог NULL).
func EventSPID(ev TRCEvent) (int, bool) {
	v, ok := ev.Columns[12].(int32)
	return int(v), ok
}

// AggregateByProcedure агрегирует вызовы процедур по правилам AggregateOptions
// (см. LoadProceduresAggregated): EventNames nil → только SP:Completed,
// группировка по procedure или (spid, procedure) с исключением событий без
// SPID, детерминированная сортировка (метрика DESC, procedure ASC, spid ASC)
// и Top после агрегации.
func AggregateByProcedure(events []TRCEvent, opts AggregateOptions) []TRCProcAgg {
	eventNames := make(map[string]bool, len(opts.EventNames))
	for _, name := range opts.AggregateEventNames() {
		eventNames[name] = true
	}
	spids := make(map[int]bool, len(opts.SPIDs))
	for _, s := range opts.SPIDs {
		spids[s] = true
	}

	type acc struct {
		count int
		total int64
		min   int64
		max   int64
	}
	type groupKey struct {
		spid      int
		procedure string
	}
	byGroup := make(map[groupKey]*acc)
	var order []groupKey
	for _, ev := range events {
		if !eventNames[ev.EventName] || ev.Procedure == "" {
			continue
		}
		key := groupKey{procedure: ev.Procedure}
		if len(opts.SPIDs) > 0 || opts.GroupBySPID {
			spid, ok := EventSPID(ev)
			if len(opts.SPIDs) > 0 {
				if !ok || !spids[spid] {
					continue
				}
			}
			if opts.GroupBySPID {
				if !ok {
					// NULL spid исключается из агрегата при группировке по SPID
					continue
				}
				key.spid = spid
			}
		}
		a, ok := byGroup[key]
		if !ok {
			a = &acc{min: ev.DurationMs, max: ev.DurationMs}
			byGroup[key] = a
			order = append(order, key)
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
	for _, key := range order {
		a := byGroup[key]
		result = append(result, TRCProcAgg{
			SPID:      key.spid,
			Procedure: key.procedure,
			Count:     a.count,
			TotalMs:   a.total,
			MinMs:     a.min,
			MaxMs:     a.max,
			AvgMs:     float64(a.total) / float64(a.count),
		})
	}

	metric := func(a TRCProcAgg) float64 {
		switch opts.SortBy {
		case "avg_ms":
			return a.AvgMs
		case "max_ms":
			return float64(a.MaxMs)
		case "count":
			return float64(a.Count)
		default:
			return float64(a.TotalMs)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		mi, mj := metric(result[i]), metric(result[j])
		if mi != mj {
			return mi > mj
		}
		if result[i].Procedure != result[j].Procedure {
			return result[i].Procedure < result[j].Procedure
		}
		return result[i].SPID < result[j].SPID
	})

	if opts.Top > 0 && len(result) > opts.Top {
		result = result[:opts.Top]
	}
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
