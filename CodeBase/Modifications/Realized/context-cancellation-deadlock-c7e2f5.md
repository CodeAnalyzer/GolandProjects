# Устранение зависания индексатора при отмене через Ctrl+C

Конвейер `WalkParallel` → `InitCtx`/`UpdateCtx` не пробрасывает `context.Context` в отправки каналов. При отмене контекста (Ctrl+C / SIGTERM) workers прекращают чтение, но fswalk-горутины продолжают blocking-send в заполненные каналы → дедлок: процесс виснет и требует `kill -9`.

---

## Контекст проблемы

| # | Участок | Текущее поведение | Влияние |
|---|---------|-------------------|---------|
| 1 | `fswalk.go:191` — WalkDir goroutine | `fileQueue <- walkTask{...}` без `ctx.Done()` | При остановке read workers `fileQueue` заполняется → WalkDir блокируется навсегда |
| 2 | `fswalk.go:176,214` — read workers + pre-filter | `filesChan <- FileInfo{...}` без `ctx.Done()` | При остановке indexer workers `filesChan` заполняется → read workers блокируются |
| 3 | `fswalk.go:136,149,163,196,209` — error sends | `errorsChan <- ...` без `ctx.Done()` | Блокировка при полном буфере `errorsChan` (100) |
| 4 | `fswalk.go:230-234` — closer goroutine | `wg.Wait()` → `close(filesChan)` / `close(errorsChan)` | Никогда не выполняется: read workers зависли на шаге 2 |
| 5 | `runner.go:85,192` — main goroutine | `for err := range errsCh` | Блокируется: `errorsChan` никогда не закрывается (шаг 4) |
| 6 | `runner.go:187` — UpdateCtx feeder | `jobs <- indexedFileJob{...}` без `ctx.Done()` | При остановке `processFilesWorkerPool` workers `jobs` заполняется → feeder блокируется → `close(jobs)` не выполняется |
| 7 | `indexer.go:200,243` — workers receive | `for file := range filesCh` / `for job := range jobs` | `select { case <-ctx.Done(): ... default: }` проверяется **только между** файлами, не во время ожидания. Если канал пуст и не закрывается — worker зависает |

**Цепочка дедлока (InitCtx, Ctrl+C):**

```
workers выходят по ctx.Done() → filesChan не читается
  ← read workers зависают на filesChan <- (буфер полон)
    ← closer wg.Wait() ждёт read workers → close(filesChan)/close(errorsChan) не выполняется
      ← WalkDir зависает на fileQueue <- (буфер полон, read workers не читают)
      ← main goroutine зависает на for err := range errsCh
```

---

## Затронутые файлы

- `internal/fswalk/fswalk.go` — `WalkParallel`: добавить `context.Context`, обернуть все отправки в `select`
- `internal/fswalk/fswalk_test.go` — обновить вызовы `WalkParallel` (новая сигнатура), добавить тест отмены
- `internal/indexer/runner.go` — `InitCtx`/`UpdateCtx`: пробросить `ctx` в `WalkParallel`, обернуть `jobs <-` и `for err := range errsCh` в `select`
- `internal/indexer/indexer.go` — `processFilesWorkerPoolInit`/`processFilesWorkerPool`: заменить `for range` на `select` с `ctx.Done()`

---

## План (4 шага)

### Шаг 1: context.Context в WalkParallel (fswalk.go)

**Файл:** `internal/fswalk/fswalk.go:122-237`

**Изменение сигнатуры:**
```go
// Было:
func (w *Walker) WalkParallel(workers int) (<-chan FileInfo, <-chan error)

// Стало:
func (w *Walker) WalkParallelCtx(ctx context.Context, workers int) (<-chan FileInfo, <-chan error)
```

`Walk()` делегирует в `WalkParallelCtx(context.Background(), 1)`.

Для обратной совместимости можно оставить `WalkParallel(workers int)` как wrapper:
```go
func (w *Walker) WalkParallel(workers int) (<-chan FileInfo, <-chan error) {
    return w.WalkParallelCtx(context.Background(), workers)
}
```

**Обернуть все отправки в каналы в `select`:**

1. **WalkDir goroutine** — `fileQueue <-` (строка 191):
```go
select {
case fileQueue <- walkTask{path: path, relPath: relPath, info: info, ext: ext}:
case <-ctx.Done():
    return ctx.Err()
}
```
При `ctx.Err()` WalkDir прерывает обход (`filepath.WalkDir` возвращает ошибку → callback останавливается).

2. **WalkDir pre-filter** — `filesChan <-` (строка 176):
```go
select {
case filesChan <- FileInfo{...}:
case <-ctx.Done():
    return nil
}
```

3. **WalkDir error sends** — `errorsChan <-` (строки 136, 149, 163, 196):
```go
select {
case errorsChan <- err:
case <-ctx.Done():
    return nil
}
```

4. **Read workers** — `filesChan <-` (строка 214) и `errorsChan <-` (строка 209):
```go
select {
case filesChan <- FileInfo{...}:
case <-ctx.Done():
    return
}
```
```go
select {
case errorsChan <- fmt.Errorf(...):
case <-ctx.Done():
    return
}
```

5. **Closer goroutine** (строки 230-234) — без изменений: `wg.Wait()` дождётся выхода read workers (теперь они выходят по `ctx.Done()`), затем закроет каналы.

**Почему безопасно:**
- `context.Context` — стандартный паттерн Go; `select` на `ctx.Done()` не меняет нормальный путь (ctx не отменён → `default` не нужен, блокируется на `filesChan` как раньше)
- Порядок обхода и содержимое каналов не меняются при нормальной работе
- Существующие тесты (`TestWalkParallelReturnsIncludedFiles`, `TestWalkParallelPreFilter*`) продолжают работать через wrapper `WalkParallel` (использует `context.Background()`)

**Проверка:**
```
go build ./internal/fswalk/...
go test ./internal/fswalk/...
```

---

### Шаг 2: context-aware workers в indexer.go

**Файлы:** `internal/indexer/indexer.go:192-263`

**`processFilesWorkerPoolInit`** (строка 200) — заменить:
```go
// Было:
for file := range filesCh {
    select {
    case <-ctx.Done():
        return
    default:
    }
    // ... обработка файла
}

// Стало:
for {
    select {
    case <-ctx.Done():
        return
    case file, ok := <-filesCh:
        if !ok {
            return
        }
        // ... обработка файла (без изменений)
    }
}
```

**`processFilesWorkerPool`** (строка 243) — аналогично для `jobs`:
```go
for {
    select {
    case <-ctx.Done():
        return
    case job, ok := <-jobs:
        if !ok {
            return
        }
        // ... обработка job (без изменений)
    }
}
```

**Почему безопасно:**
- Логика обработки файла не меняется — меняется только способ получения из канала
- При нормальной работе (ctx не отменён) `select` выбирает `case file, ok := <-filesCh` как единственный готовый канал
- При отмене worker выходит немедленно, не дожидаясь следующего файла

**Проверка:**
```
go build ./internal/indexer/...
go test ./internal/indexer/...
```

---

### Шаг 3: context-aware feeder и error loop в runner.go

**Файл:** `internal/indexer/runner.go`

**3a. InitCtx — error loop** (строка 85):

Заменить:
```go
// Было:
for err := range errsCh {
    idx.logError(rootPath, "Walker error: %v", err)
    collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
}

// Стало:
for {
    select {
    case err, ok := <-errsCh:
        if !ok {
            goto workersDone
        }
        idx.logError(rootPath, "Walker error: %v", err)
        collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
    case <-ctx.Done():
        // Дождаться workers (они тоже выйдут по ctx.Done), затем выйти.
        // errorsChan может быть не закрыт, но workersWG.Wait() гарантирует
        // завершение processing. Оставшиеся ошибки в буфере теряются — это
        // нормально при отмене.
        goto workersDone
    }
}
workersDone:
```

Также: заменить `walker.WalkParallel(parallel)` → `walker.WalkParallelCtx(ctx, parallel)` (строка 74).

**3b. UpdateCtx — feeder** (строка 187):

Заменить:
```go
// Было:
jobs <- indexedFileJob{file: file, fileID: fileID}

// Стало:
select {
case jobs <- indexedFileJob{file: file, fileID: fileID}:
case <-ctx.Done():
    close(jobs)
    return
}
```

И в начале feeder-горутины — `defer func() { close(jobs) }()` заменить на явный `close(jobs)` перед каждым `return` (или оставить `defer` + `return` после `close`).

**3c. UpdateCtx — error loop** (строка 192):

Аналогично 3a — `for err := range errsCh` → `select` с `ctx.Done()`.

Также: заменить `walker.WalkParallel(parallel)` → `walker.WalkParallelCtx(ctx, parallel)` (строка 138).

**Почему безопасно:**
- При нормальной работе (ctx не отменён) `select` всегда выбирает `case err, ok := <-errsCh`
- При отмене main goroutine выходит из error loop и переходит к `workersWG.Wait()` — workers тоже получат `ctx.Done()` и выйдут
- Feeder при отмене закрывает `jobs` и выходит — workers дочитают остаток `jobs` и выходят по `!ok`

**Проверка:**
```
go build ./...
go test ./internal/indexer/... ./internal/fswalk/...
```

---

### Шаг 4: Тест отмены

**Файлы:** `internal/fswalk/fswalk_test.go`, `internal/indexer/indexer_test.go` (если есть) или `internal/indexer/runner_test.go` (если есть)

**Тест в fswalk_test.go:**

```go
func TestWalkParallelCtx_CancelStopsPipeline(t *testing.T) {
    root := t.TempDir()
    // Создаём много файлов, чтобы заполнить буфер каналов
    for i := 0; i < 200; i++ {
        writeTestFile(t, filepath.Join(root, fmt.Sprintf("file_%03d.sql", i)), "select 1")
    }

    ctx, cancel := context.WithCancel(context.Background())
    w := NewWalker(root, []string{"*.sql"}, nil)
    filesChan, errorsChan := w.WalkParallelCtx(ctx, 2)

    // Читаем первый файл, затем отменяем
    <-filesChan
    cancel()

    // Должны получить закрытые каналы (не зависнуть)
    done := make(chan struct{})
    go func() {
        for range filesChan {
        }
        for range errorsChan {
        }
        close(done)
    }()

    select {
    case <-done:
        // OK — каналы закрылись после отмены
    case <-time.After(5 * time.Second):
        t.Fatal("WalkParallelCtx hung after context cancellation")
    }
}
```

**Проверка:**
```
go test ./internal/fswalk/... -run TestWalkParallelCtx -count=1 -timeout 30s
go test ./internal/indexer/... ./internal/fswalk/... -count=1
```

---

## Порядок выполнения

1. **Шаг 1** (WalkParallelCtx) → `go build ./internal/fswalk/...` + `go test ./internal/fswalk/...`
2. **Шаг 2** (workers select) → `go build ./internal/indexer/...` + `go test ./internal/indexer/...`
3. **Шаг 3** (runner feeder + error loop) → `go build ./...` + `go test ./internal/indexer/... ./internal/fswalk/...`
4. **Шаг 4** (тест отмены) → `go test ./internal/fswalk/... -run TestWalkParallelCtx -count=1 -timeout 30s`

Каждый шаг компилируется и тестируется независимо. Шаги 1-3 можно коммитить раздельно.

---

## Ожидаемый результат

| Сценарий | Было | Стало |
|---|---|---|
| `codebase init` + Ctrl+C на 50K файлов | Зависание, `kill -9` | Корректное завершение за <2с |
| `codebase update` + Ctrl+C | Зависание (feeder + error loop) | Корректное завершение за <2с |
| Нормальная работа (без отмены) | Без изменений | Без изменений (select выбирает data-case) |
