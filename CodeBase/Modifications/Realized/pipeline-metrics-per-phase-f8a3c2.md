# Информативные per-phase метрики пайплайна init/update

## Контекст проблемы

В `Init` (`runner.go:89-96`) воркеры делают `saveFile` и `processFile` в одном цикле (`processFilesWorkerPoolInit`, `indexer.go:187-203`). После `workersWG.Wait()` обе фазы завершены одновременно, поэтому `walkSaveDone` и `processDone` задаются в одну и ту же точку:

```go
workersWG.Wait()
walkSaveDone := time.Now()  // line 90
processDone  := time.Now()  // line 91 — та же точка!
```

Следствие: `ProcessMs == WalkSaveMs`, метрика неинформативна.

В `Update` (`runner.go:177-189`) этой проблемы нет — feeder делает `saveFile` (последовательно), воркеры — `processFile` (параллельно). Между `feederWG.Wait()` (line 177) и `workersWG.Wait()` (line 188) есть реальный разрыв. Но и там `ProcessMs` считается от `startedAt`, а не от `walkSaveDone`, что делает её cumulative, а не per-phase.

### Текущие метрики

| Поле | Описание | Init | Update |
|------|----------|------|--------|
| `WalkSaveMs` | wall-clock от старта до завершения walk+save | OK | OK |
| `ProcessMs` | wall-clock от старта до завершения пула | **== WalkSaveMs** (дубликат) | cumulative от старта (не per-phase) |
| `PostProcessMs` | пост-обработка relations | OK | OK |
| `CleanupMs` | удаление устаревших файлов | N/A | OK |

### Желаемые метрики

| Поле | Описание | Что показывает |
|------|----------|----------------|
| `WalkSaveMs` | wall-clock: старт → завершение walk+save (как сейчас) | общее время обхода ФС + сохранения файлов |
| `SaveMs` (новое) | **суммарное** время всех воркеров в `saveFile` | bottleneck DB INSERT vs парсинг |
| `ParseMs` (новое) | **суммарное** время всех воркеров в `processFile` | bottleneck парсинга vs DB INSERT |
| `ProcessMs` | wall-clock: завершение walk+save → завершение пула (только для Update) | реальное время парсинга без overlap с walk |
| `PostProcessMs` | как сейчас | без изменений |
| `CleanupMs` | как сейчас | без изменений |

**Почему `SaveMs`/`ParseMs` — суммарные, а не wall-clock:**
В параллельном пуле wall-clock одной фазы ≈ wall-clock другой (они выполняются concurrently в одном воркере). Суммарное время (`Σ per-worker`) показывает реальную стоимость каждой фазы и позволяет вычислить эффективность распараллеливания: `(SaveMs + ParseMs) / WalkSaveMs ≈ parallel` (в идеале).

---

## Затронутые файлы

- `internal/model/model.go` — новые поля `SaveMs`, `ParseMs` в `ScanStats`
- `internal/indexer/indexer.go` — таймеры в `processFilesWorkerPoolInit` и `processFilesWorkerPool`
- `internal/indexer/runner.go` — корректировка расчёта `ProcessMs` для Init и Update
- `cmd/init.go` — обновление `printPipelineTimings`
- `internal/indexer/indexer_helpers_test.go` — unit-тесты для аккумуляции `SaveMs`/`ParseMs`

---

## План (3 шага)

### Шаг 1: Новые поля в ScanStats (нулевой риск)

**Файл:** `internal/model/model.go:417-421`

**Изменение:**
```go
// Тайминги стадий пайплайна (миллисекунды).
WalkSaveMs    int64 // wall-clock: старт → завершение walk+save
SaveMs        int64 // суммарное время воркеров в saveFile (per-worker Σ)
ParseMs       int64 // суммарное время воркеров в processFile (per-worker Σ)
ProcessMs     int64 // wall-clock: завершение walk+save → завершение пула (Update only; 0 для Init)
PostProcessMs int64 // пост-обработка relations
CleanupMs     int64 // удаление устаревших/исчезнувших файлов (только update)
```

**Почему безопасно:** Новые поля int64 с zero-value. `mergeScanStats` уже суммирует все int-поля через `dst.Field += src.Field` — нужно убедиться, что `SaveMs` и `ParseMs` тоже суммируются (проверить `mergeScanStats`).

**Проверка:**
```
go build ./internal/model/...
```

---

### Шаг 2: Per-phase таймеры в воркерах (низкий риск)

#### 2a: `processFilesWorkerPoolInit` (Init)

**Файл:** `internal/indexer/indexer.go:179-208`

**Изменение:** Внутри цикла воркера — измерять `saveFile` и `processFile` независимо, аккумулировать через `collector.Add`:

```go
for file := range filesCh {
    collector.Add(func(stats *model.ScanStats) { stats.FilesScanned++ })

    saveStart := time.Now()
    fileID, err := idx.saveFile(file, scanRunID)
    saveElapsed := time.Since(saveStart).Milliseconds()
    collector.Add(func(stats *model.ScanStats) { stats.SaveMs += saveElapsed })
    if err != nil {
        idx.logError(file.Path, "Error saving file row: %v", err)
        collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
        continue
    }

    parseStart := time.Now()
    localStats := &model.ScanStats{}
    if err := idx.processFile(file, fileID, localStats); err != nil {
        idx.logError(file.Path, "Error processing file: %v", err)
        localStats.Errors++
    }
    parseElapsed := time.Since(parseStart).Milliseconds()
    localStats.ParseMs += parseElapsed
    collector.Add(func(stats *model.ScanStats) {
        mergeScanStats(stats, localStats)
    })
}
```

**Примечание:** `localStats.ParseMs` суммируется через `mergeScanStats` (которая делает `+=`), а `SaveMs` — напрямую через `collector.Add`, т.к. `saveFile` не возвращает `localStats`.

#### 2b: `processFilesWorkerPool` (Update)

**Файл:** `internal/indexer/indexer.go:210-232`

**Изменение:** В Update `saveFile` выполняется в feeder-горутине (последовательно), а `processFile` — в воркерах. Поэтому:
- `SaveMs` для Update — измеряется в feeder-горутине `runner.go:154` (вокруг `idx.saveFile`)
- `ParseMs` — измеряется в `processFilesWorkerPool` аналогично Init

```go
// в processFilesWorkerPool:
for job := range jobs {
    localStats := &model.ScanStats{}
    parseStart := time.Now()
    if err := idx.processFile(job.file, job.fileID, localStats); err != nil {
        idx.logError(job.file.Path, "Error processing file: %v", err)
        localStats.Errors++
    }
    localStats.ParseMs += time.Since(parseStart).Milliseconds()
    collector.Add(func(stats *model.ScanStats) {
        mergeScanStats(stats, localStats)
    })
}
```

```go
// в runner.go Update feeder (line 154):
saveStart := time.Now()
fileID, err := idx.saveFile(file, scanRunID)
saveElapsed := time.Since(saveStart).Milliseconds()
collector.Add(func(stats *model.ScanStats) { stats.SaveMs += saveElapsed })
```

**Проверка:**
```
go build ./...
go test ./internal/indexer/...
```

---

### Шаг 3: Корректировка ProcessMs и вывод (низкий риск)

#### 3a: `runner.go` — ProcessMs как per-phase (не cumulative)

**Файл:** `internal/indexer/runner.go`

**Init (lines 89-96):**
```go
workersWG.Wait()
walkSaveDone := time.Now()
// processDone не нужен для Init — saveFile и processFile в одном цикле
idx.runPostProcessingParallel(collector, parallel)
postProcessDone := time.Now()
stats := collector.Snapshot()
stats.WalkSaveMs = walkSaveDone.Sub(startedAt).Milliseconds()
stats.ProcessMs = 0 // Init: saveFile+processFile в одном цикле, per-phase через SaveMs/ParseMs
stats.PostProcessMs = postProcessDone.Sub(walkSaveDone).Milliseconds()
```

**Update (lines 177-211):**
```go
feederWG.Wait()
walkSaveDone := time.Now()

// ... batch delete ...

workersWG.Wait()
processDone := time.Now()
idx.runPostProcessingParallel(collector, parallel)
postProcessDone := time.Now()

// ... cleanup ...

stats := collector.Snapshot()
stats.WalkSaveMs = walkSaveDone.Sub(startedAt).Milliseconds()
stats.ProcessMs = processDone.Sub(walkSaveDone).Milliseconds() // per-phase, не cumulative
stats.PostProcessMs = postProcessDone.Sub(processDone).Milliseconds()
stats.CleanupMs = cleanupDone.Sub(postProcessDone).Milliseconds()
```

**Суть изменения для Update:** `ProcessMs` было `processDone.Sub(startedAt)` (cumulative, включало walk+save), станет `processDone.Sub(walkSaveDone)` (per-phase, только парсинг).

#### 3b: `cmd/init.go` — обновление `printPipelineTimings`

**Файл:** `cmd/init.go:21-29`

```go
func printPipelineTimings(stats *model.ScanStats) {
    fmt.Printf("\nStage timings:\n")
    fmt.Printf("  Walk+save files: %d ms (wall-clock)\n", stats.WalkSaveMs)
    if stats.SaveMs > 0 || stats.ParseMs > 0 {
        fmt.Printf("  Save (Σ workers):  %d ms\n", stats.SaveMs)
        fmt.Printf("  Parse (Σ workers): %d ms\n", stats.ParseMs)
        if stats.WalkSaveMs > 0 {
            ratio := float64(stats.SaveMs+stats.ParseMs) / float64(stats.WalkSaveMs)
            fmt.Printf("  Parallel efficiency: %.1fx (Σ / wall-clock)\n", ratio)
        }
    }
    if stats.ProcessMs > 0 {
        fmt.Printf("  Parse+insert:    %d ms (wall-clock, post-walk)\n", stats.ProcessMs)
    }
    fmt.Printf("  Post-processing: %d ms\n", stats.PostProcessMs)
    if stats.CleanupMs > 0 {
        fmt.Printf("  Cleanup:         %d ms\n", stats.CleanupMs)
    }
}
```

**Пример вывода Init:**
```
Stage timings:
  Walk+save files: 45000 ms (wall-clock)
  Save (Σ workers):  12000 ms
  Parse (Σ workers): 95000 ms
  Parallel efficiency: 2.4x (Σ / wall-clock)
  Post-processing: 8000 ms
```

**Пример вывода Update:**
```
Stage timings:
  Walk+save files: 5000 ms (wall-clock)
  Save (Σ workers):  800 ms
  Parse (Σ workers): 3200 ms
  Parse+insert:    2100 ms (wall-clock, post-walk)
  Post-processing: 1500 ms
  Cleanup:         200 ms
```

**Проверка:**
```
go build ./...
go test ./internal/indexer/...
```

Ручной тест:
```
codebase init D:\GITHUB\GolandProjects\FA --parallel 8
codebase update D:\GITHUB\GolandProjects\FA --parallel 8
```

---

## Порядок выполнения

1. **Шаг 1** (поля в model) → `go build`
2. **Шаг 2a** (таймеры в `processFilesWorkerPoolInit`) → `go build` + `go test ./internal/indexer/...`
3. **Шаг 2b** (таймеры в `processFilesWorkerPool` + feeder) → `go build` + `go test ./internal/indexer/...`
4. **Шаг 3a** (ProcessMs в runner.go) → `go build`
5. **Шаг 3b** (вывод в init.go) → `go build` + ручной тест

Шаги 1-2 можно комбинировать в один коммит. Шаг 3 — отдельный коммит.

---

## Проверка mergeScanStats

Перед реализацией убедиться, что `mergeScanStats` суммирует `SaveMs` и `ParseMs`. Если она использует named-field copy или `+=` по каждому полю — добавить новые поля. Если использует reflect — проверить автоматически.

**Файл:** `internal/indexer/indexer.go` (или `indexer_helpers.go`) — найти `mergeScanStats`.

---

## Тесты

В `internal/indexer/indexer_helpers_test.go` добавить:

1. **TestMergeScanStats_AccumulatesSaveMsParseMs** — проверить, что `SaveMs` и `ParseMs` суммируются при `mergeScanStats`
2. **TestStatsCollector_AccumulatesSaveMs** — проверить, что多次 `collector.Add` корректно накапливает `SaveMs`

---

## Ожидаемый эффект

| Метрика | До | После |
|---------|----|-------|
| `ProcessMs` (Init) | == `WalkSaveMs` (бесполезно) | 0 (неактуально для Init) |
| `SaveMs` (Init) | не было | суммарное время DB INSERT |
| `ParseMs` (Init) | не было | суммарное время парсинга |
| `ProcessMs` (Update) | cumulative от старта | per-phase (post-walk) |
| Parallel efficiency | не вычислялось | `(SaveMs + ParseMs) / WalkSaveMs` |

Позволяет диагностировать:
- `ParseMs >> SaveMs` → bottleneck в парсинге, нужно больше воркеров или оптимизация парсеров
- `SaveMs >> ParseMs` → bottleneck в DB INSERT, нужен batch-insert или больше соединений
- `Parallel efficiency < 1x` → воркеры простаивают (I/O bound), `> parallel` — невозможно (корректность измерения)
