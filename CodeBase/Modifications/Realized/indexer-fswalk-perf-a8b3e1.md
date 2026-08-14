# Оптимизация indexer pipeline: параллельный saveFile, WalkDir, hex-хэш

Устранение узких мест в пайплайне `codebase init`: последовательные INSERT-ы файлов в один поток, избыточные syscalls при обходе ФС, лишние аллокации при хэшировании.

---

## Контекст проблемы

| # | Участок | Текущее поведение | Влияние |
|---|---------|-------------------|---------|
| 1 | `runner.go:84-98` (Init feeder) | Один feeder-поток делает синхронный `INSERT INTO files ... RETURNING id` для каждого файла | 50K+ roundtrip'ов в PostgreSQL последовательно; воркеры простаивают |
| 2 | `fswalk.go:97` | `filepath.Walk` → `os.Lstat` per entry | Лишний syscall на каждый файл/директорию (на Windows — `GetFileAttributesEx`) |
| 3 | `fswalk.go:201-204` | `fmt.Sprintf("%x", sum[:])` для SHA-256 хэша | Reflection + лишние аллокации; в 2.5–3× медленнее `hex.EncodeToString` |

---

## Затронутые файлы

- `internal/fswalk/fswalk.go` — WalkDir, hex.EncodeToString
- `internal/fswalk/fswalk_test.go` — обновление/добавление тестов
- `internal/indexer/runner.go` — перенос saveFile в воркеры (Init only)
- `internal/indexer/indexer.go` — signature saveFile (без изменений логики)
- `internal/indexer/indexer_test.go` — новый тест конкурентности (при необходимости)

---

## План (3 шага)

### Шаг 1: hex.EncodeToString вместо fmt.Sprintf (нулевой риск)

**Файл:** `internal/fswalk/fswalk.go:201-204`

**Изменение:**
- Добавить `"encoding/hex"` в imports
- Заменить `fmt.Sprintf("%x", sum[:])` → `hex.EncodeToString(sum[:])`
- Убрать `"fmt"` из imports, если больше не используется в этом файле (остаётся — используется в `Walk` для `fmt.Errorf`)

**Почему безопасно:** Идентичный вывод для тех же входных данных. Тест `TestComputeHashBytes` (`fswalk_test.go:65-71`) уже верифицирует хэш `ba7816bf...` для `"abc"`.

**Проверка:**
```
go build ./internal/fswalk/...
go test ./internal/fswalk/...
```

---

### Шаг 2: filepath.WalkDir вместо filepath.Walk (низкий риск)

**Файл:** `internal/fswalk/fswalk.go:86-167`

**Изменение:**
- Заменить `filepath.Walk(w.rootPath, func(path string, info os.FileInfo, err error) error {` на `filepath.WalkDir(w.rootPath, func(path string, d fs.DirEntry, err error) error {`
- Добавить `"io/fs"` в imports
- `info.IsDir()` → `d.IsDir()`
- `info.Name()` → `d.Name()`
- Для `info.Size()` и `info.ModTime()` — вызвать `d.Info()` один раз и сохранить в переменную:
  ```go
  info, err := d.Info()
  if err != nil {
      errorsChan <- fmt.Errorf("failed to get file info: %w", err)
      return nil
  }
  ```
  Вызов `d.Info()` на Windows возвращает данные из кэша `ReadDir` (`WIN32_FIND_DATA`) без дополнительного syscall.
- `filepath.SkipDir` — поддерживается WalkDir без изменений

**Почему безопасно:**
- `WalkDir` — стандартная функция `path/filepath`, API-stable с Go 1.16
- Порядок обхода (lexical) не меняется
- `fs.DirEntry` предоставляет `IsDir()`, `Name()`, `Type()` без extra stat
- Тест `TestWalkReturnsIncludedFiles` (`fswalk_test.go:110-144`) покрывает: include/exclude, скрытые директории, содержимое файлов, metadata (encoding/language)

**Проверка:**
```
go build ./internal/fswalk/...
go test ./internal/fswalk/...
```

---

### Шаг 3: Параллельный saveFile в воркерах для Init (умеренный риск)

**Файлы:** `internal/indexer/runner.go:60-124` (Init), `internal/indexer/indexer.go:179-201` (processFilesWorkerPool), `internal/indexer/indexer.go:1480-1512` (saveFile)

**Текущий поток данных (Init):**
```
walker → filesCh → feeder: saveFile (1 поток, sequential INSERT) → jobs{file, fileID} → workers: processFile
```

**Новый поток данных (Init):**
```
walker → filesCh → workers: saveFile + processFile (N потоков, parallel INSERT)
```

**Изменения в `runner.go` (Init only, `Update` не трогаем):**

1. Убрать feeder-горутину с последовательным `saveFile`
2. Изменить канал `jobs` → `filesCh` напрямую (воркеры читают `fswalk.FileInfo`)
3. В `processFilesWorkerPool` (или новом `processFilesWorkerPoolInit`) — каждый воркер:
   ```go
   for file := range filesCh {
       fileID, err := idx.saveFile(file, scanRunID)
       if err != nil {
           idx.logError(file.Path, "Error saving file row: %v", err)
           collector.Add(func(s *model.ScanStats) { s.Errors++ })
           continue
       }
       localStats := &model.ScanStats{}
       if err := idx.processFile(file, fileID, localStats); err != nil {
           idx.logError(file.Path, "Error processing file: %v", err)
           localStats.Errors++
       }
       collector.Add(func(s *model.ScanStats) { mergeScanStats(s, localStats) })
   }
   ```
4. `collector.Add(FilesScanned++)` перенести в воркеры (перед `saveFile`)
5. `close(jobs)` → убрать (воркеры читают напрямую из `filesCh`, который закрывает walker)
6. `feederWG` → убрать (не нужен)
7. `workersWG` запускается сразу, без промежуточной горутины-обёртки

**Update не меняется** — там feeder делает больше чем `saveFile` (проверка `onlyModified`, трекинг `seen`, `modifiedPaths`, `newFileIDs`), и модифицированных файлов обычно немного, bottleneck менее острый.

**Безопасность конкурентного доступа:**
- `idx.db` (обёртка над `*sql.DB`) — безопасен для конкурентного использования (connection pool)
- `idx.shared` (`indexerSharedState`) — уже защищён `pendingMu`
- `idx.shared.dsProductCache` — уже защищён `dsProductMu`
- `saveFile` вызывает `resolveDSProductID` (cache + mutex) и `db.QueryRow` — оба thread-safe
- `processFile` уже работает параллельно в текущей реализации

**Риск:** `saveFile` и `processFile` в одном воркере увеличивают время жизни воркера. Если `saveFile` медленнее `processFile` (маленькие файлы), воркеры могут простаивать на I/O. Однако на реальной нагрузке (Diasoft 5NT: крупные SQL/PAS файлы) `processFile` доминирует над `saveFile`, так что баланс сохраняется.

**Проверка:**
```
go build ./...
go test ./internal/indexer/... ./internal/fswalk/...
```

Дополнительно — ручной тест на реальной кодовой базе:
```
codebase init D:\GITHUB\GolandProjects\FA --parallel 8
```
Сравнить `WalkSaveMs` и `ProcessMs` в выводе статистики до/после.

---

## Порядок выполнения

1. **Шаг 1** (hex) → сборка + тесты fswalk
2. **Шаг 2** (WalkDir) → сборка + тесты fswalk
3. **Шаг 3** (saveFile в воркеры) → сборка + тесты indexer + ручной тест на FA

Каждый шаг независим. Шаги 1-2 можно комбинировать (один PR/коммит), шаг 3 — отдельный коммит.

---

## Ожидаемый эффект

| Шаг | Метрика | Ожидание |
|-----|---------|----------|
| 1 | Аллокации на хэш | −2× (убрать reflection) |
| 2 | Syscalls при обходе | −30-50% (на Windows, зависит от структуры каталогов) |
| 3 | WalkSaveMs (Init, 50K файлов) | −3-7× (1 поток → N потоков INSERT) |
