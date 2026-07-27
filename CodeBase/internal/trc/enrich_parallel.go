package trc

import (
	"encoding/json"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

// minEventsForParallel — порог для перехода на параллельный режим.
// При меньшем числе событий накладные расходы на goroutines превысят выгоду.
const minEventsForParallel = 256

// enrichEventsParallel применяет enrichEvent ко всем событиям, используя
// chunk-based параллелизм (sync.WaitGroup + непересекающиеся диапазоны
// индексов в pre-allocated slice). Паттерн идентичен
// buildSQLProcedureCallRelationsParallel в indexer_postprocess_sql_calls.go.
func enrichEventsParallel(events []TRCEvent) {
	if len(events) < minEventsForParallel {
		for i := range events {
			enrichEvent(&events[i])
		}
		return
	}
	workers := runtime.NumCPU()
	if workers > len(events) {
		workers = len(events)
	}
	if workers < 1 {
		for i := range events {
			enrichEvent(&events[i])
		}
		return
	}
	chunkSize := (len(events) + workers - 1) / workers
	var wg sync.WaitGroup
	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize
		if end > len(events) {
			end = len(events)
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for j := start; j < end; j++ {
				enrichEvent(&events[j])
			}
		}(i, end)
	}
	wg.Wait()
}

// serializeParallel выполняет JSON-сериализацию columns и params для всех
// событий в параллельном режиме. Заполняет pre-allocated slices columnsJSONs
// и paramsJSONs по индексам. Возвращает первую ошибку сериализации (fail-fast).
func serializeParallel(events []TRCEvent, columnsJSONs [][]byte, paramsJSONs []interface{}) error {
	if len(events) < minEventsForParallel {
		return serializeSequential(events, columnsJSONs, paramsJSONs)
	}
	workers := runtime.NumCPU()
	if workers > len(events) {
		workers = len(events)
	}
	if workers < 1 {
		return serializeSequential(events, columnsJSONs, paramsJSONs)
	}
	chunkSize := (len(events) + workers - 1) / workers
	var firstErr atomic.Pointer[error]
	var wg sync.WaitGroup
	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize
		if end > len(events) {
			end = len(events)
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for j := start; j < end; j++ {
				b, err := marshalColumns(events[j].Columns)
				if err != nil {
					firstErr.CompareAndSwap(nil, &err)
					return
				}
				columnsJSONs[j] = b
				if len(events[j].Params) > 0 {
					pb, err := json.Marshal(sanitizeParams(events[j].Params))
					if err != nil {
						firstErr.CompareAndSwap(nil, &err)
						return
					}
					paramsJSONs[j] = string(pb)
				}
			}
		}(i, end)
	}
	wg.Wait()
	if p := firstErr.Load(); p != nil {
		return *p
	}
	return nil
}

// serializeSequential — последовательная версия serializeParallel для малых
// наборов данных и fallback.
func serializeSequential(events []TRCEvent, columnsJSONs [][]byte, paramsJSONs []interface{}) error {
	for i, ev := range events {
		b, err := marshalColumns(ev.Columns)
		if err != nil {
			return err
		}
		columnsJSONs[i] = b
		if len(ev.Params) > 0 {
			pb, err := json.Marshal(sanitizeParams(ev.Params))
			if err != nil {
				return err
			}
			paramsJSONs[i] = string(pb)
		}
	}
	return nil
}

// sanitizeParams возвращает копию params с очищенными от нулевых байтов (0x00)
// строковыми значениями. PostgreSQL jsonb не принимает \u0000 в строках.
func sanitizeParams(params []TRCParam) []TRCParam {
	out := make([]TRCParam, len(params))
	for i, p := range params {
		out[i] = TRCParam{
			Name:  strings.ReplaceAll(p.Name, "\x00", ""),
			Value: strings.ReplaceAll(p.Value, "\x00", ""),
		}
	}
	return out
}
