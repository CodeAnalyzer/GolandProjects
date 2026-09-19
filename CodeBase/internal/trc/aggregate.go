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

// --- Сравнение процедур focus/peer SPID ---

// CompareOptions — параметры сравнения: FocusSPID определяет Top-N,
// CompareSPIDs — нормализованный список peer (без focus, порядок сохранён),
// EventNames при nil — только SP:Completed, Top 1..100, SortBy как в
// AggregateOptions.
type CompareOptions struct {
	FocusSPID    int
	CompareSPIDs []int
	EventNames   []string
	Top          int
	SortBy       string
}

// FocusMetrics — метрики процедуры в focus SPID; топ происходит из её
// агрегатов, поэтому значения не nullable.
type FocusMetrics struct {
	Count   int     `json:"count"`
	TotalMs int64   `json:"total_ms"`
	MinMs   int64   `json:"min_ms"`
	MaxMs   int64   `json:"max_ms"`
	AvgMs   float64 `json:"avg_ms"`
}

// PeerMetrics — метрики процедуры в одном peer SPID; отсутствие вызовов
// отличимо от валидной нулевой длительности через nullable min/max/avg.
type PeerMetrics struct {
	SPID    int       `json:"spid"`
	Count   int64     `json:"count"`
	TotalMs int64     `json:"total_ms"`
	MinMs   *int64    `json:"min_ms"`
	MaxMs   *int64    `json:"max_ms"`
	AvgMs   *float64  `json:"avg_ms"`
}

// CombinedMetrics — взвешенные метрики по сырым вызовам всех peer вместе:
// avg = sum(duration)/count, а не среднее от средних SPID.
type CombinedMetrics struct {
	Count   int64    `json:"count"`
	TotalMs int64    `json:"total_ms"`
	MinMs   *int64   `json:"min_ms"`
	MaxMs   *int64   `json:"max_ms"`
	AvgMs   *float64 `json:"avg_ms"`
}

// CompareRatios — отношения focus к peer_combined; null при нулевом
// знаменателе или отсутствии peer-вызовов.
type CompareRatios struct {
	AvgVsPeers   *float64 `json:"avg_vs_peers"`
	MaxVsPeers   *float64 `json:"max_vs_peers"`
	CountVsPeers *float64 `json:"count_vs_peers"`
}

// CompareProcedure — одна строка сравнения: rank в Top-N focus, метрики
// focus, каждого peer в исходном порядке, peer_combined и ratios.
type CompareProcedure struct {
	Rank         int             `json:"rank"`
	Procedure    string          `json:"procedure"`
	Focus        FocusMetrics    `json:"focus"`
	Peers        []PeerMetrics   `json:"peers"`
	PeerCombined CombinedMetrics `json:"peer_combined"`
	Ratios       CompareRatios   `json:"ratios"`
}

// CompareResult — результат сравнения focus/peer SPID.
type CompareResult struct {
	FocusSPID    int                `json:"focus_spid"`
	CompareSPIDs []int              `json:"compare_spids"`
	SortBy       string             `json:"sort_by"`
	Top          int                `json:"top"`
	Procedures   []CompareProcedure `json:"procedures"`
}

// CompareProceduresRows собирает сравнение из агрегатов (spid, procedure),
// полученных серверно (LoadProceduresAggregated с GroupBySPID) или в памяти
// (AggregateByProcedure). Top-N определяется только агрегатами focus SPID.
func CompareProceduresRows(rows []TRCProcAgg, opts CompareOptions) CompareResult {
	result := CompareResult{
		FocusSPID:    opts.FocusSPID,
		CompareSPIDs: opts.CompareSPIDs,
		SortBy:       opts.SortBy,
		Top:          opts.Top,
	}

	focusByProc := make(map[string]*TRCProcAgg)
	for i := range rows {
		if rows[i].SPID == opts.FocusSPID {
			r := rows[i]
			focusByProc[r.Procedure] = &r
		}
	}
	focusList := make([]TRCProcAgg, 0, len(focusByProc))
	for _, r := range focusByProc {
		focusList = append(focusList, *r)
	}
	sort.Slice(focusList, func(i, j int) bool {
		mi, mj := aggMetricValue(opts.SortBy, focusList[i]), aggMetricValue(opts.SortBy, focusList[j])
		if mi != mj {
			return mi > mj
		}
		return focusList[i].Procedure < focusList[j].Procedure
	})
	if opts.Top > 0 && len(focusList) > opts.Top {
		focusList = focusList[:opts.Top]
	}

	type peerKey struct {
		spid      int
		procedure string
	}
	peerByProc := make(map[peerKey]*TRCProcAgg)
	for i := range rows {
		if rows[i].SPID == opts.FocusSPID {
			continue
		}
		r := rows[i]
		peerByProc[peerKey{r.SPID, r.Procedure}] = &r
	}

	peerSet := make(map[int]bool, len(opts.CompareSPIDs))
	for _, s := range opts.CompareSPIDs {
		peerSet[s] = true
	}

	result.Procedures = make([]CompareProcedure, 0, len(focusList))
	for rank, f := range focusList {
		entry := CompareProcedure{
			Rank:      rank + 1,
			Procedure: f.Procedure,
			Focus: FocusMetrics{
				Count:   f.Count,
				TotalMs: f.TotalMs,
				MinMs:   f.MinMs,
				MaxMs:   f.MaxMs,
				AvgMs:   f.AvgMs,
			},
			Peers: make([]PeerMetrics, 0, len(opts.CompareSPIDs)),
		}

		var combCount int64
		var combTotal int64
		var combMin, combMax *int64

		for _, spid := range opts.CompareSPIDs {
			pm := PeerMetrics{SPID: spid}
			if r, ok := peerByProc[peerKey{spid, f.Procedure}]; ok {
				pm.Count = int64(r.Count)
				pm.TotalMs = r.TotalMs
				pm.MinMs = &r.MinMs
				pm.MaxMs = &r.MaxMs
				avg := r.AvgMs
				pm.AvgMs = &avg
			}
			entry.Peers = append(entry.Peers, pm)

			combCount += pm.Count
			combTotal += pm.TotalMs
			if pm.MinMs != nil && (combMin == nil || *pm.MinMs < *combMin) {
				min := *pm.MinMs
				combMin = &min
			}
			if pm.MaxMs != nil && (combMax == nil || *pm.MaxMs > *combMax) {
				max := *pm.MaxMs
				combMax = &max
			}
		}

		entry.PeerCombined = CombinedMetrics{Count: combCount, TotalMs: combTotal, MinMs: combMin, MaxMs: combMax}
		if combCount > 0 {
			avg := float64(combTotal) / float64(combCount)
			entry.PeerCombined.AvgMs = &avg
		}

		if entry.PeerCombined.AvgMs != nil && *entry.PeerCombined.AvgMs != 0 {
			v := f.AvgMs / *entry.PeerCombined.AvgMs
			entry.Ratios.AvgVsPeers = &v
		}
		if combMax != nil && *combMax != 0 {
			v := float64(f.MaxMs) / float64(*combMax)
			entry.Ratios.MaxVsPeers = &v
		}
		if combCount != 0 {
			v := float64(f.Count) / float64(combCount)
			entry.Ratios.CountVsPeers = &v
		}

		result.Procedures = append(result.Procedures, entry)
	}
	return result
}

// aggMetricValue возвращает значение метрики сортировки агрегата.
func aggMetricValue(sortBy string, a TRCProcAgg) float64 {
	switch sortBy {
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

// CompareProcedures собирает сравнение для file-mode: агрегирует события
// focus+peer в памяти и передаёт строки в CompareProceduresRows.
func CompareProcedures(events []TRCEvent, opts CompareOptions) CompareResult {
	all := make([]int, 0, len(opts.CompareSPIDs)+1)
	all = append(all, opts.FocusSPID)
	all = append(all, opts.CompareSPIDs...)
	rows := AggregateByProcedure(events, AggregateOptions{
		EventNames:  opts.EventNames,
		SPIDs:       all,
		GroupBySPID: true,
	})
	return CompareProceduresRows(rows, opts)
}
