# Параллельный walker + ModTime/Size pre-filter

Ускорение фазы walk (чтение+хэширование файлов) в 5-10× за счёт параллельного I/O и пропуска неизменённых файлов по mtime+size без чтения содержимого.

---

## Контекст проблемы

| # | Участок | Текущее поведение | Влияние |
|---|---------|-------------------|---------|
| 1 | `fswalk.go:92-174` | Одна горутина последовательно: `os.ReadFile` + `sha256` для каждого файла | 100K файлов × ~7 мс = ~688 сек wall-clock (Update: 705 сек, из них SaveMs=17 сек) |
| 2 | `fswalk.go:144-149` | Все файлы читаются с диска и хэшируются, даже если не изменились | Update: 87K из 100K файлов (87%) читаются + хэшируются только чтобы узнать, что они не изменились |

**Текущий поток данных:**
```
WalkDir (1 поток)
  └─ для каждого файла:
       ├─ os.ReadFile(path)      ← I/O, блокирует ~5 мс
       ├─ computeHashBytes(...)   ← CPU, блокирует ~2 мс
       └─ filesChan <- FileInfo{Content: content}  ← весь контент в канале
```

**Целевой поток данных:**
```
WalkDir (1 поток, только метаданные)
  └─ для каждого файла:
       ├─ d.Info()               ← быстро, из кэша ReadDir
       ├─ фильтр include/exclude ← быстро
       ├─ [Update] pre-filter: mtime+size совпадает? → пропустить (без чтения)
       └─ fileQueue <- {path, info, relPath, ext}  ← метаданные, без контента

Воркеры чтения+хэширования (N):
  for item := range fileQueue:
       ├─ os.ReadFile(item.path)      ← I/O, параллельно
       ├─ computeHashBytes(content)    ← CPU, параллельно
       └─ filesChan <- FileInfo{..., Content: content}
```

---

## Затронутые файлы

- `internal/fswalk/fswalk.go` — параллельный walker, pre-filter
- `internal/fswalk/fswalk_test.go` — тесты параллельности + pre-filter
- `internal/indexer/runner.go` — передача `existing` в walker для pre-filter (Update only)
- `internal/model/model.go` — без изменений (`File` уже содержит `SizeBytes`, `ModifiedAt`, `HashSHA256`)

---

## План (3 шага)

### Шаг 1: Параллельный walker — чтение+хэширование в N горутинах (умеренный риск)

**Файл:** `internal/fswalk/fswalk.go`

**Изменение:**

1. Новый метод `WalkParallel(workers int)` (или параметр `workers` в `Walk`):
   ```go
   func (w *Walker) WalkParallel(workers int) (<-chan FileInfo, <-chan error) {
       filesChan := make(chan FileInfo, workers*50)
       errorsChan := make(chan error, 100)
       fileQueue := make(chan walkTask, workers*50)

       type walkTask struct {
           path    string
           relPath string
           info    fs.FileInfo
           ext     string
       }

       // Обходчик каталогов — 1 горутина (лёгкая операция, без чтения файлов)
       go func() {
           defer close(fileQueue)
           filepath.WalkDir(w.rootPath, func(path string, d fs.DirEntry, err error) error {
               // ... те же фильтры isExcluded/isIncluded ...
               info, err := d.Info()
               // ... отправка walkTask в fileQueue (без чтения контента) ...
               fileQueue <- walkTask{path, relPath, info, ext}
               return nil
           })
       }()

       // Воркеры чтения+хэширования — N горутин
       var wg sync.WaitGroup
       for i := 0; i < workers; i++ {
           wg.Add(1)
           go func() {
               defer wg.Done()
               for task := range fileQueue {
                   content, err := os.ReadFile(task.path)
                   if err != nil {
                       errorsChan <- fmt.Errorf("failed to read %s: %w", task.path, err)
                       continue
                   }
                   hash := computeHashBytes(content)
                   encoding, language := getEncodingAndLanguage(task.ext)
                   filesChan <- FileInfo{
                       Path:       filepath.ToSlash(task.path),
                       RelPath:    task.relPath,
                       Extension:  task.ext,
                       Size:       task.info.Size(),
                       Hash:       hash,
                       ModifiedAt: task.info.ModTime(),
                       Encoding:   encoding,
                       Language:   language,
                       Content:    content,
                   }
               }
           }()
       }

       // Закрытие channels после завершения всех воркеров
       go func() {
           wg.Wait()
           close(filesChan)
           close(errorsChan)
       }()

       return filesChan, errorsChan
   }
   ```

2. `Walk()` остаётся как есть (делегирует в `WalkParallel(1)`) — обратная совместимость для тестов.

3. Вызывающий код в `runner.go`:
   - `Init`: `walker.Walk()` → `walker.WalkParallel(parallel)`
   - `Update`: `walker.Walk()` → `walker.WalkParallel(parallel)`

**Почему безопасно:**
- `filepath.WalkDir` остаётся однопоточным — порядок обхода каталогов не меняется
- Порядок файлов в `filesChan` нарушается, но Init/Update не зависят от порядка
- `os.ReadFile` — thread-safe (каждый воркер читает свой файл)
- `filesChan` буфер `workers*50` — воркеры не блокируются на отправке
- Существующие тесты (`TestWalkReturnsIncludedFiles`) проверяют содержимое, а не порядок — будут работать

**Влияние на метрики прогресса (`scanned`/`indexed`):**
- `scanned` (FilesScanned) — инкрементируется в feeder для **каждого** файла из `filesChan`, включая pre-filtered (с `Hash == ""`). Число не изменится: 100K.
- `indexed` (FilesIndexed) — инкрементируется только в `processFile` после успешного парсинга. Pre-filtered файлы до `processFile` не доходят. Число не изменится: 13K.
- Прогресс-репортер `update / scanned=N indexed=M` работает без изменений.
- Единственная разница: `scanned` будет расти быстрее, т.к. pre-filtered файлы поступают в `filesChan` мгновенно (без I/O).

**Риски:**
- Пиковая память: `workers` × средний_размер_файла. Для Diasoft 5NT (1-500 КБ) при workers=12: ~6 МБ — безопасно
- На HDD ускорение меньше (2-3x) из-за seek overhead. На SSD — линейное до 4-8 потоков

**Проверка:**
```
go build ./internal/fswalk/...
go test ./internal/fswalk/...
```

---

### Шаг 2: ModTime+Size pre-filter для Update (низкий риск)

**Файлы:** `internal/fswalk/fswalk.go`, `internal/indexer/runner.go`

**Концепция:**
Вместо чтения файла и вычисления SHA-256, чтобы узнать, изменился ли он, — сравнить `mtime` и `size` с предыдущим индексированием. Если оба совпадают, файл считается неизменённым (как `git status` без `--no-renames`).

Это **эвристика** — теоретически файл может быть изменён с сохранением размера и mtime. На практике:
- Копирование файла сохраняет mtime, но меняет содержимое — редкий случай
- Ручное редактирование меняет и размер, и mtime
- `touch` меняет mtime, но не содержимое — будет ложное срабатывание (файл перечитается, но хэш совпадёт → пропуск в feeder)

**Изменение в `fswalk.go`:**

1. Новое поле в `Walker`:
   ```go
   type Walker struct {
       rootPath        string
       includePatterns []string
       excludePatterns []string
       includeRegexps  []*regexp.Regexp
       excludeRegexps  []*regexp.Regexp
       // preFilter — карта известных файлов для пропуска по mtime+size (Update only)
       // Ключ: нормализованный path. Значение: {size, modTime}
       preFilter map[string]fileFingerprint
   }

   type fileFingerprint struct {
       size    int64
       modTime time.Time
   }
   ```

2. Новый конструктор / setter:
   ```go
   func (w *Walker) SetPreFilter(existing map[string]fileFingerprint) {
       w.preFilter = existing
   }
   ```

3. В обходчике (до отправки в `fileQueue`):
   ```go
   // Pre-filter: если mtime+size совпадают — пропускаем чтение файла
   if w.preFilter != nil {
       if fp, ok := w.preFilter[normalizedPath]; ok {
           if fp.size == info.Size() && fp.modTime.Equal(info.ModTime()) {
               // Файл не изменился — отправляем метаданные без контента
               // Hash будет пустой — feeder поймёт, что это pre-filtered
               filesChan <- FileInfo{
                   Path:       filepath.ToSlash(path),
                   RelPath:    relPath,
                   Extension:  ext,
                   Size:       info.Size(),
                   Hash:       "",  // без хэша — не читали
                   ModifiedAt: info.ModTime(),
                   // Content: nil — не читали
               }
               continue
           }
       }
   }
   ```

   **Важно:** pre-filtered файлы отправляются напрямую в `filesChan`, минуя `fileQueue` и воркеры. Это файлы, которые **не нужно читать** — отправка из обходчика быстрее, чем через воркер.

**Изменение в `runner.go` (Update only):**

1. Преобразовать `existing` map в `fileFingerprint` map:
   ```go
   fingerprints := make(map[string]fswalk.FileFingerprint, len(existing))
   for path, f := range existing {
       fingerprints[path] = fswalk.FileFingerprint{Size: f.SizeBytes, ModTime: f.ModifiedAt}
   }
   walker.SetPreFilter(fingerprints)
   ```

2. В feeder-горутине — обработка pre-filtered файлов:
   ```go
   for file := range filesCh {
       normalizedPath := filepath.ToSlash(strings.TrimSpace(file.Path))
       seen[normalizedPath] = struct{}{}
       collector.Add(func(stats *model.ScanStats) { stats.FilesScanned++ })

       prev := existing[normalizedPath]
       // Pre-filtered файл (Hash пустой) — считаем неизменённым
       if file.Hash == "" && prev != nil {
           continue
       }
       // Обычная проверка хэша
       if onlyModified && prev != nil && prev.HashSHA256 == file.Hash {
           continue
       }
       // ... saveFile + jobs ...
   }
   ```

**Init не использует pre-filter** — при первом индексировании `existing` пуст.

**Почему безопасно:**
- Pre-filter — оптимистичная эвристика. False negative (файл изменился, но mtime+size совпали) — крайне редкий случай. Даже если произойдёт, содержимое будет неактуально до следующего `update` с изменённым mtime.
- False positive (файл не изменился, но mtime изменился — `touch`) — файл будет перечитан, хэш совпадёт, feeder пропустит. Стоимость — одно лишнее чтение.
- `Hash == ""` — надёжный маркер pre-filtered файла (реальные хэши всегда 64-char hex)
- `FileFingerprint` — экспортируемый тип, чтобы `runner.go` мог его создать

**Проверка:**
```
go build ./...
go test ./internal/fswalk/... ./internal/indexer/...
```

Ручной тест:
```
codebase update D:\GITHUB\GolandProjects\FA --parallel 8
```
Сравнить `WalkSaveMs` до/после. Ожидание: 705 сек → ~10-20 сек (87K файлов не читаются).

---

### Шаг 3: Метрика WalkMs — разделить walk и read+hash (низкий риск)

**Файлы:** `internal/fswalk/fswalk.go`, `internal/model/model.go`, `internal/indexer/runner.go`, `cmd/init.go`

**Изменение:**

1. Новое поле в `ScanStats`:
   ```go
   WalkMs    int64 // wall-clock: обход каталогов (WalkDir только метаданные)
   ReadHashMs int64 // wall-clock: чтение файлов + хэширование (воркеры)
   ```

2. В `WalkParallel` — два таймера:
   ```go
   walkStart := time.Now()
   // ... WalkDir ...
   walkDone := time.Now()
   ```

   Однако `WalkParallel` возвращает каналы, а не `ScanStats`. Поэтому таймеры измеряются в вызывающем коде (`runner.go`):
   - `WalkMs` — от старта до закрытия `fileQueue` (обход каталогов завершён)
   - `ReadHashMs` — от старта до закрытия `filesChan` (все файлы прочитаны)

   Для этого `WalkParallel` может вернуть дополнительный канал `<-chan struct{}` (сигнал завершения обхода каталогов) или просто использовать `WalkSaveMs` как сейчас (wall-clock всей фазы) и `ReadHashMs` как `WalkSaveMs - WalkMs`.

   **Альтернатива (проще):** Не разделять, оставить `WalkSaveMs` как есть. Pre-filter уже даст радикальное ускорение. Разделение — nice-to-have, не блокирующее.

**Решение:** Отложить шаг 3 до подтверждения эффекта шагов 1-2. Если `WalkSaveMs` останется интересной для анализа — добавить разделение в следующей итерации.

---

## Порядок выполнения

1. **Шаг 1** (параллельный walker) → сборка + тесты fswalk + ручной тест Init
2. **Шаг 2** (pre-filter) → сборка + тесты fswalk + indexer + ручной тест Update
3. **Шаг 3** (метрики) — опционально, по результатам

Шаги 1-2 можно комбинировать в один коммит (шаг 2 зависит от шага 1 — `WalkParallel` должен существовать).

---

## Ожидаемый эффект

| Сценарий | Метрика | Сейчас | После шага 1 | После шага 1+2 |
|----------|---------|--------|--------------|----------------|
| Init (100K файлов, parallel=12) | WalkSaveMs | ~688 сек | ~86 сек (×8) | ~86 сек (нет pre-filter) |
| Update (100K файлов, 13K изменено) | WalkSaveMs | 705 сек | ~86 сек | ~11 сек (87K не читаются) |
| Update (100K файлов, 13K изменено) | Files read from disk | 100K | 100K | ~13K |
| Update (100K файлов, 0 изменено) | WalkSaveMs | 705 сек | ~86 сек | ~2 сек (все pre-filtered) |

**Допущения:**
- SSD, средний файл 7 КБ, `os.ReadFile` ~5 мс, `sha256` ~2 мс
- Параллельное чтение на SSD: линейное ускорение до 4-8 потоков, далее I/O saturation
- Pre-filter hit rate: 87% (на основе Update от 2026-08-14: 100K scanned, 13K indexed)
