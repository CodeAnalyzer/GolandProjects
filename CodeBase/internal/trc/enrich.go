package trc

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/codebase/internal/query"
)

// ProcedureLookup — узкий интерфейс для получения данных о процедуре из
// CodeBase. Реализуется *query.Query (тот же интерфейс, что и
// internal/rti/enrich.go — переиспользуется отдельным определением, а не
// импортом пакета rti, чтобы не создавать межпакетную зависимость
// internal/trc -> internal/rti).
type ProcedureLookup interface {
	GetProcedureResult(ctx context.Context, name string) (*query.SQLProcedureResult, error)
}

// ProcedureEnrichment — результат обогащения процедуры данными из CodeBase.
type ProcedureEnrichment struct {
	Procedure  string                  `json:"procedure"`
	SourceFile string                  `json:"source_file,omitempty"`
	LineStart  int                     `json:"line_start,omitempty"`
	LineEnd    int                     `json:"line_end,omitempty"`
	Params     []query.SQLParamResult  `json:"params,omitempty"`
	Found      bool                    `json:"found"`
}

// EnrichProcedure ищет процедуру в CodeBase DB и возвращает enrichment.
// Отбрасывает завершающий суффикс "#" от имени процедуры (встречается в
// TextData для параметризованных вызовов) перед поиском.
func EnrichProcedure(ctx context.Context, q ProcedureLookup, procName string) (*ProcedureEnrichment, error) {
	name := trimHashSuffix(procName)
	proc, err := q.GetProcedureResult(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("procedure %q not found: %w", procName, err)
	}
	return &ProcedureEnrichment{
		Procedure:  procName,
		SourceFile: proc.File,
		LineStart:  proc.LineStart,
		LineEnd:    proc.LineEnd,
		Params:     proc.Params,
		Found:      true,
	}, nil
}

// maxEnrichWorkers — лимит конкурентных SQL-запросов при обогащении.
// MaxOpenConns=25, оставляем запас для других запросов.
var maxEnrichWorkers = 16

// minProcsForParallelEnrich — порог для перехода на параллельный режим
// обогащения. При меньшем числе уникальных процедур накладные расходы
// на goroutines превысят выгоду.
var minProcsForParallelEnrich = 16

// trcSlowThresholdMs — порог медленности событий (мс).
var trcSlowThresholdMs = 100

// SetEnrichWorkers устанавливает лимит enrich-воркеров и порог параллельности.
func SetEnrichWorkers(max, minParallel int) {
	if max > 0 {
		maxEnrichWorkers = max
	}
	if minParallel > 0 {
		minProcsForParallelEnrich = minParallel
	}
}

// SetSlowThresholdMs устанавливает порог медленности событий (мс).
func SetSlowThresholdMs(ms int) {
	if ms > 0 {
		trcSlowThresholdMs = ms
	}
}

// GetSlowThresholdMs возвращает текущий порог медленности событий (мс).
func GetSlowThresholdMs() int {
	return trcSlowThresholdMs
}

// EnrichEvents обогащает события данными из CodeBase DB. Возвращает map:
// procedure name (как в TRCEvent.Procedure) → enrichment.
// Параллельно обрабатывает уникальные имена процедур через chunk-based
// паттерн (sync.WaitGroup + sync.Mutex для map), лимит — maxEnrichWorkers.
func EnrichEvents(ctx context.Context, q ProcedureLookup, events []TRCEvent) map[string]*ProcedureEnrichment {
	uniqueProcs := make(map[string]struct{})
	for _, ev := range events {
		if ev.Procedure != "" {
			uniqueProcs[ev.Procedure] = struct{}{}
		}
	}
	if len(uniqueProcs) == 0 {
		return map[string]*ProcedureEnrichment{}
	}

	procs := make([]string, 0, len(uniqueProcs))
	for name := range uniqueProcs {
		procs = append(procs, name)
	}

	if len(procs) < minProcsForParallelEnrich {
		result := make(map[string]*ProcedureEnrichment, len(procs))
		for _, procName := range procs {
			result[procName] = enrichSingle(ctx, q, procName)
		}
		return result
	}

	result := make(map[string]*ProcedureEnrichment, len(procs))
	var mu sync.Mutex

	workers := runtime.NumCPU()
	if workers > len(procs) {
		workers = len(procs)
	}
	if workers > maxEnrichWorkers {
		workers = maxEnrichWorkers
	}

	chunkSize := (len(procs) + workers - 1) / workers
	var wg sync.WaitGroup
	for i := 0; i < len(procs); i += chunkSize {
		end := i + chunkSize
		if end > len(procs) {
			end = len(procs)
		}
		wg.Add(1)
		go func(chunk []string) {
			defer wg.Done()
			for _, procName := range chunk {
				e := enrichSingle(ctx, q, procName)
				mu.Lock()
				result[procName] = e
				mu.Unlock()
			}
		}(procs[i:end])
	}
	wg.Wait()
	return result
}

// enrichSingle — helper для переиспользования между последовательным и
// параллельным путями EnrichEvents.
func enrichSingle(ctx context.Context, q ProcedureLookup, procName string) *ProcedureEnrichment {
	enrich, err := EnrichProcedure(ctx, q, procName)
	if err != nil {
		return &ProcedureEnrichment{
			Procedure:  procName,
			Found:      false,
			SourceFile: "(not found)",
		}
	}
	return enrich
}

// trimHashSuffix отбрасывает завершающий суффикс "#..." (маркер
// параметризованного/временного варианта имени), который может
// присутствовать в имени процедуры, извлечённом из TextData.
func trimHashSuffix(name string) string {
	if i := strings.IndexByte(name, '#'); i >= 0 {
		return name[:i]
	}
	return name
}
