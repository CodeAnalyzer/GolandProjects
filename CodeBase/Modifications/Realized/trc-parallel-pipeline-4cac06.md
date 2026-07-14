# Параллелизация TRC-пайплайна (enrichEvent + marshalColumns + EnrichEvents)

Вынести CPU-bound и I/O-bound этапы пайплайна TRC в параллельное выполнение через `sync.WaitGroup` + chunk-based паттерн, уже используемый в CodeBase (indexer, review runner).

---

## Шаг 1: `enrichEventsParallel` — параллельный enrich событий

**Файлы:** `internal/trc/parser.go`, `internal/trc/xml_parser.go`, новый `internal/trc/enrich_parallel.go`

### Что делаем

1. Создать `internal/trc/enrich_parallel.go` с функцией `enrichEventsParallel(events []TRCEvent)`:
   - Паттерн: `sync.WaitGroup` + chunk-based деление по индексам (как `buildSQLProcedureCallRelationsParallel` в `indexer_postprocess_sql_calls.go:70-95`)
   - Worker count = `min(runtime.NumCPU(), len(events))`
   - Fallback на последовательный режим при `len(events) < 256` (накладные расходы на goroutines превысят выгоду)
   - Каждый воркер работает с непересекающимся диапазоном `events[start:end]`, вызывая `enrichEvent(&events[j])`
   - Pre-allocated slice — воркеры пишут по индексу, **нет shared state, нет mutex**

2. `parser.go:91` — убрать `enrichEvent(&ev)` из цикла декодирования
3. После цикла (строка 94, перед `return`) — добавить `enrichEventsParallel(events)`
4. `xml_parser.go:176` — убрать `enrichEvent(&ev)` из цикла
5. После цикла (строка 178, перед `return`) — добавить `enrichEventsParallel(events)`

### Код (скелет)

```go
// enrich_parallel.go
package trc

import (
	"runtime"
	"sync"
)

const minEventsForParallel = 256

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
```

### Инварианты
- `enrichEvent` — чистая функция, модифицирует только поля `*TRCEvent` (Procedure, Params, DurationMs). Нет shared state.
- `regexp.Regexp.FindStringSubmatch` — thread-safe для concurrent read.
- Порядок событий сохраняется — воркеры пишут в исходный slice по индексу.

---

## Шаг 2: `serializeParallel` — параллельная JSON-сериализация для INSERT

**Файлы:** `internal/trc/store.go`, `internal/trc/enrich_parallel.go`

### Что делаем

1. Добавить `serializeParallel(events []TRCEvent, columnsJSONs [][]byte, paramsJSONs []interface{}) error` в `enrich_parallel.go`:
   - Тот же паттерн: `sync.WaitGroup` + chunks
   - Fallback на последовательный режим при `len(events) < minEventsForParallel`
   - **Strict обработка ошибок**: первый воркер, столкнувшийся с ошибкой `marshalColumns` или `json.Marshal`, записывает её в `atomic.Pointer[error]` (или `sync.Once` + общую переменную) и продолжает; после `wg.Wait()` проверяется наличие ошибки
   - Каждый воркер заполняет `columnsJSONs[j]` и `paramsJSONs[j]` по своему диапазону индексов

2. Реструктурировать `insertTRCEvents` в `store.go:40-99`:
   - **Фаза 1**: pre-allocate `columnsJSONs := make([][]byte, len(events))` и `paramsJSONs := make([]interface{}, len(events))`, вызвать `serializeParallel`
   - **Фаза 2**: последовательный `COPY IN` цикл с готовыми JSON-строками (pq.CopyIn не потокобезопасен)
   - Обработка ошибок сериализации возвращается до начала транзакции (fail-fast)

### Код (скелет serializeParallel)

```go
import "sync/atomic"

func serializeParallel(events []TRCEvent, columnsJSONs [][]byte, paramsJSONs []interface{}) error {
	if len(events) < minEventsForParallel {
		return serializeSequential(events, columnsJSONs, paramsJSONs)
	}
	workers := runtime.NumCPU()
	if workers > len(events) {
		workers = len(events)
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
					pb, err := json.Marshal(events[j].Params)
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
```

### Инварианты
- `marshalColumns` и `json.Marshal` — thread-safe для разных значений.
- `pq.CopyIn` / `stmt.Exec` остаётся последовательным — порядок событий сохраняется.
- Ошибка сериализации возвращается до начала DB-транзакции (fail-fast, как в текущем коде).

---

## Шаг 3: Параллельный `EnrichEvents` (I/O-bound, SQL-запросы)

**Файлы:** `internal/trc/enrich.go`

### Что делаем

1. Переписать `EnrichEvents` (строки 50-71):
   - Сбор уникальных имён процедур — оставить последовательным (быстро, O(n))
   - Преобразовать `uniqueProcs` map в `[]string` slice для chunk-based деления
   - Параллельная обработка через `sync.WaitGroup` + chunks (как в indexer)
   - Worker count = `min(runtime.NumCPU(), len(uniqueProcs), 16)` — лимит 16, т.к. `MaxOpenConns=25` (оставляем запас для других запросов)
   - Fallback на последовательный режим при `len(uniqueProcs) < 16` (накладные расходы превысят выгоду при малом числе процедур)
   - Каждый воркер вызывает `EnrichProcedure(q, procName)` для своих процедур
   - Запись в общий `result map[string]*ProcedureEnrichment` через `sync.Mutex` (map не потокобезопасна для concurrent write)

2. **Без новых зависимостей** — `sync.WaitGroup` + `sync.Mutex`, без `errgroup`
3. Ошибки `EnrichProcedure` не останавливают другие воркеры (как в текущем коде — not found → запись с `Found: false`)

### Код (скелет)

```go
func EnrichEvents(q ProcedureLookup, events []TRCEvent) map[string]*ProcedureEnrichment {
	// Сбор уникальных имён (последовательно)
	uniqueProcs := make(map[string]struct{})
	for _, ev := range events {
		if ev.Procedure != "" {
			uniqueProcs[ev.Procedure] = struct{}{}
		}
	}
	if len(uniqueProcs) == 0 {
		return map[string]*ProcedureEnrichment{}
	}

	// Преобразуем в slice для chunk-based деления
	procs := make([]string, 0, len(uniqueProcs))
	for name := range uniqueProcs {
		procs = append(procs, name)
	}

	result := make(map[string]*ProcedureEnrichment, len(procs))
	var mu sync.Mutex

	workers := runtime.NumCPU()
	if workers > len(procs) {
		workers = len(procs)
	}
	if workers > 16 {
		workers = 16
	}
	if workers < 1 || len(procs) < 16 {
		// Последовательный режим для малого числа процедур
		for _, procName := range procs {
			result[procName] = enrichSingle(q, procName)
		}
		return result
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
				e := enrichSingle(q, procName)
				mu.Lock()
				result[procName] = e
				mu.Unlock()
			}
		}(procs[i:end])
	}
	wg.Wait()
	return result
}

// enrichSingle — extract helper для переиспользования между seq/parallel путями
func enrichSingle(q ProcedureLookup, procName string) *ProcedureEnrichment {
	enrich, err := EnrichProcedure(q, procName)
	if err != nil {
		return &ProcedureEnrichment{
			Procedure:  procName,
			Found:      false,
			SourceFile: "(not found)",
		}
	}
	return enrich
}
```

### Инварианты
- `*sql.DB` потокобезопасен для concurrent запросов. `MaxOpenConns=25`, лимит воркеров 16 — запас есть.
- `sync.Mutex` защищает только запись в `result` map (короткая критическая секция).
- `EnrichProcedure` — чистая функция (читает из БД через интерфейс `ProcedureLookup`, не модифицирует shared state).
- Поведение для not-found процедур идентично текущему (Found: false, SourceFile: "(not found)").
- Порядок ключей в map не детерминирован в Go (был и раньше) — результат идентичен по содержимому.

---

## Шаг 4: Тесты

**Файлы:** `internal/trc/enrich_parallel_test.go` (новый), `internal/trc/enrich_test.go`

### Новые тесты

1. **`TestEnrichEventsParallel_Deterministic`** — сравнение результатов параллельного и последовательного enrich на наборе из 500+ событий с разными процедурами. Проверка: те же `Procedure`, `Params`, `DurationMs` для каждого события.

2. **`TestEnrichEventsParallel_SmallDataset_Sequential`** — < 256 событий: проверка, что используется последовательный путь (результат корректен).

3. **`TestSerializeParallel_MatchesSequential`** — `serializeParallel` даёт те же `columnsJSONs`/`paramsJSONs`, что и последовательная сериализация. Проверка через `unmarshalColumns` (восстановление исходных типов).

4. **`TestSerializeParallel_ErrorPropagation`** — событие с невалидируемыми Columns (например, `func` тип) → `serializeParallel` возвращает ошибку.

5. **`TestEnrichEvents_ConcurrentSameResult`** — `EnrichEvents` с mockLookup на 20+ процедурах: результат (map содержимое) идентичен результату последовательного вызова. Проверка `Found`, `SourceFile`, `Procedure` для каждого ключа.

6. **`TestEnrichEvents_SmallDataset_Sequential`** — < 16 уникальных процедур: проверка корректности (последовательный путь).

### Обновление существующих тестов

- `TestEnrichEvents_Dedup` в `enrich_test.go` — должен продолжать работать без изменений (сигнатура `EnrichEvents` не меняется).

---

## Шаг 5: Сборка и проверка

1. `go build ./...` — без ошибок
2. `go test ./internal/trc/... -count=1` — все тесты PASS
3. `go vet ./internal/trc/...` — нет предупреждений о гонках
4. `go test -race ./internal/trc/... -count=1` — race detector чистый
5. (Опционально, по запросу пользователя) Бенчмарк до/после на реальном .trc файле

---

## Затронутые файлы

| Файл | Тип | Изменений |
|------|-----|-----------|
| `internal/trc/enrich_parallel.go` | новый | `enrichEventsParallel`, `serializeParallel`, `serializeSequential` (~80 строк) |
| `internal/trc/parser.go` | правка | Убрать `enrichEvent(&ev)` из цикла (стр. 91), добавить вызов `enrichEventsParallel` (~3 строки) |
| `internal/trc/xml_parser.go` | правка | Убрать `enrichEvent(&ev)` из цикла (стр. 176), добавить вызов `enrichEventsParallel` (~3 строки) |
| `internal/trc/store.go` | правка | Реструктурировать `insertTRCEvents`: pre-serialize + COPY IN (~20 строк изменений) |
| `internal/trc/enrich.go` | правка | Переписать `EnrichEvents` на chunk-based параллелизм + `enrichSingle` helper (~40 строк) |
| `internal/trc/enrich_parallel_test.go` | новый | 6 тестов (~120 строк) |
| `go.mod` | без изменений | Новые зависимости не нужны |

---

## Риски и митигация

| Риск | Митигация |
|------|-----------|
| Гонка данных в `enrichEvent` | Каждый воркер — непересекающийся диапазон индексов. `enrichEvent` модифицирует только поля `TRCEvent`. ✅ |
| `pq.CopyIn` не потокобезопасен | COPY IN остаётся последовательным. Параллелится только сериализация. ✅ |
| Конкуренция за БД в `EnrichEvents` | Лимит воркеров 16 при `MaxOpenConns=25`. ✅ |
| Порядок событий в БД | `stmt.Exec` вызывается последовательно в исходном порядке. ✅ |
| Запись в `result` map в `EnrichEvents` | `sync.Mutex` защищает запись. ✅ |
| Ошибки сериализации в `serializeParallel` | `atomic.Pointer[error]` для первой ошибки, fail-fast до транзакции. ✅ |
| Малые файлы — overhead goroutines | Fallback на последовательный режим при < 256 событий / < 16 процедур. ✅ |
| Race condition | `go test -race` на всех тестах. ✅ |

---

## Порядок выполнения

1. **Шаг 1** — `enrich_parallel.go` + правка `parser.go` + `xml_parser.go` → `go build` + `go test -race`
2. **Шаг 2** — `serializeParallel` + правка `store.go` → `go build` + `go test -race`
3. **Шаг 3** — правка `enrich.go` → `go build` + `go test -race`
4. **Шаг 4** — тесты → `go test -race ./internal/trc/...`
5. **Шаг 5** — финальная проверка: `go build ./...` + `go test -race ./internal/trc/... -count=1`

Каждый шаг — атомарная итерация с проверкой сборки и тестов.
