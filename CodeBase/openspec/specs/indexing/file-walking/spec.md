# File Walking

## Purpose

Обход файловой системы проекта Diasoft 5NT, хеширование файлов для инкрементального обновления, фильтрация по include/exclude patterns, параллельное сканирование с worker pool.

## Requirements

### Requirement: Сканирование директорий

Система SHALL рекурсивно обходить директории от корневого пути проекта, используя `filepath.WalkDir` (одна горутина-обходчик, только метаданные `fs.DirEntry`, без `stat`-вызовов на каждый файл). Обходчик применяется как «feeder»: он фильтрует файлы по include/exclude и передаёт подходящие в очередь задач. Тяжёлая часть — чтение файла и вычисление SHA-256 — выполняется N воркерами параллельно (см. требование «Параллельное сканирование»). Все отправки в каналы обёрнуты в `select` с `ctx.Done()`, что предотвращает зависание горутин при отмене.

#### Scenario: Полное сканирование

- **GIVEN** корневой путь проекта, содержащий файлы поддерживаемых типов
- **WHEN** выполняется `codebase init`
- **THEN** все файлы, соответствующие `include_patterns`, найдены и переданы в indexer
- **AND** файлы из `exclude_patterns` пропущены

#### Scenario: Инкрементальное обновление

- **GIVEN** ранее проиндексированный проект с сохранёнными хешами файлов
- **WHEN** выполняется `codebase update`
- **THEN** только файлы с изменённым хешем (SHA-256) передаются в indexer
- **AND** неизменённые файлы пропускаются

### Requirement: Фильтрация по шаблонам

Система SHALL поддерживать настройку `include_patterns` и `exclude_patterns` в секции `[indexer]` конфигурационного файла для гибкого управления составом индексируемых файлов.

#### Scenario: Include patterns по умолчанию

- **GIVEN** конфигурация без явного указания `include_patterns`
- **WHEN** выполняется инициализация индекса
- **THEN** используются шаблоны по умолчанию: `*.sql`, `*.h`, `*.pas`, `*.inc`, `*.js`, `*.smf`, `*.dfm`, `*.tpr`, `*.rpt`, `*.xml`

#### Scenario: Опциональные .t01

- **GIVEN** конфигурация с `*.t01` в `include_patterns`
- **WHEN** выполняется индексация
- **THEN** препроцессированные `.t01` файлы индексируются как SQL-like layer

### Requirement: Хеширование файлов

Система SHALL вычислять SHA-256 хеш содержимого каждого файла для определения изменений при инкрементальном обновлении, используя `hex.EncodeToString` для эффективного преобразования. Для файлов, пропущенных pre-filter-ом (см. «Pre-filter по fingerprint»), хеш не вычисляется — в `FileInfo` передаётся пустой `Hash` как маркер «файл не читался»; такие файлы помечаются как pre-filtered в `ScanStats` и не передаются в indexer.

#### Scenario: Детекция изменения файла

- **GIVEN** файл `Procedure.sql` с ранее сохранённым хешем `abc123`
- **WHEN** содержимое файла изменено и выполняется `codebase update`
- **THEN** новый хеш отличается от сохранённого
- **AND** файл помечен как изменённый и передаётся в indexer

#### Scenario: Файл без чтения (pre-filtered)

- **GIVEN** файл `Form.pas` с прежними `size` и `mtime` в индексе
- **WHEN** выполняется `codebase update` и pre-filter определяет совпадение fingerprint
- **THEN** файл не читается с диска, его хеш не вычисляется
- **AND** в поток передаётся `FileInfo` с пустым `Hash` (маркер «не читался»)
- **AND** счётчик `PreFilteredFiles` в `ScanStats` увеличивается

### Requirement: Pre-filter по fingerprint (mtime + size)

Система SHALL при инкрементальном обновлении (`codebase update`) пропускать чтение и хеширование файлов, у которых `size` и `mtime` совпадают с предыдущей индексацией. Карта известных fingerprint-ов (`map[path]FileFingerprint{Size, ModTime}`) загружается из индекса и устанавливается в walker через `SetPreFilter` перед обходом. Сравнение `mtime` выполняется с допуском 1мс (`modTimeMatch`), чтобы компенсировать потерю точности между PostgreSQL `TIMESTAMPTZ` (микросекунды) и `os.Stat` на Windows (100нс). Pre-filter применяется только при наличии предыдущего состояния (Init не использует pre-filter). Число пропущенных файлов отражается в метрике `PreFilteredFiles` `ScanStats` и печатается в выводе `codebase update` (`Pre-filtered: N`).

#### Scenario: Файл не изменился — пропущен pre-filter-ом

- **GIVEN** ранее проиндексированный файл `Form.pas` с тем же размером и `mtime`, что в индексе
- **WHEN** выполняется `codebase update` с установленным pre-filter
- **THEN** файл не читается с диска и не хешируется
- **AND** метрика `PreFilteredFiles` увеличивается на 1
- **AND** файл не передаётся в indexer

#### Scenario: Файл изменился по mtime — читается и хешируется

- **GIVEN** ранее проиндексированный файл `Proc.sql`, у которого изменилось содержимое (а значит и `mtime`/`size`)
- **WHEN** выполняется `codebase update`
- **THEN** fingerprint не совпадает, файл читается и хешируется
- **AND** файл передаётся в indexer для повторного парсинга

#### Scenario: Init не использует pre-filter

- **GIVEN** первичная индексация проекта, предыдущего состояния в индексе нет
- **WHEN** выполняется `codebase init`
- **THEN** pre-filter отключён, все подходящие файлы читаются и хешируются

### Requirement: Параллельное сканирование

Система SHALL поддерживать параллельную обработку файлов с настраиваемым количеством workers через флаг `-j` / `--parallel` или параметр `indexer.parallel` в конфигурации.

#### Scenario: Параллельная индексация Init

- **GIVEN** проект с 1000 файлов и `parallel = 12`
- **WHEN** выполняется `codebase init`
- **THEN** файлы обрабатываются 12 воркерами параллельно
- **AND** каждый воркер читает файлы напрямую из канала и выполняет `saveFile` + `processFile`

#### Scenario: Параллельное обновление Update

- **GIVEN** проект с изменёнными файлами
- **WHEN** выполняется `codebase update`
- **THEN** изменённые файлы обрабатываются параллельно
- **AND** feeder-горутина фильтрует только изменённые файлы перед передачей воркерам

### Requirement: Повторная индексация изменённых файлов

Система SHALL при инкрементальном обновлении (`codebase update`) сохранять новую запись файла, затем удалять старую запись через `DeleteFilesByPathsExcept` (с сохранением нового `file_id`). Все таблицы сущностей имеют `ON DELETE CASCADE` на `file_id`, поэтому удаление старой записи файла каскадно удаляет все связанные сущности (процедуры, таблицы, колонки, symbols, relations, query_fragments и т.д.).

#### Scenario: Обновление изменённого файла

- **GIVEN** ранее проиндексированный файл `Proc.sql`, содержимое которого изменилось
- **WHEN** выполняется `codebase update`
- **THEN** создаётся новая запись в `files` с новым `file_id` и новым хешем
- **AND** старая запись файла удаляется через `DeleteFilesByPathsExcept` (с сохранением нового `file_id`)
- **AND** все сущности старого файла каскадно удаляются (procedures, columns, symbols, relations, fragments)
- **AND** новые сущности из обновлённого файла сохраняются

### Requirement: Удаление отсутствующих файлов

Система SHALL при инкрементальном обновлении определять файлы, отсутствующие в walker, но присутствующие в индексе, и удалять их через `DeleteFilesByPaths` batch-ем (по 500 путей за раз). Удаление записи файла каскадно удаляет все связанные сущности.

#### Scenario: Удалённый файл

- **GIVEN** ранее проиндексированный файл `OldProc.sql`, который был удалён с диска
- **WHEN** выполняется `codebase update`
- **THEN** файл не найден walker'ом
- **AND** путь добавлен в `removedPaths`
- **AND** запись файла удалена через `DeleteFilesByPaths`
- **AND** все связанные сущности каскадно удалены

### Requirement: Порядок постобработки

Система SHALL выполнять постобработку после завершения индексации всех файлов. Сначала удаляются все `subscribes_to_event` relations (`DeleteSubscribesToEventRelations`), затем запускаются 5 параллельных постпроцессоров: PAS-DFM links, SQL procedure call relations, callback event relations, retcode constants, fragment relations.

#### Scenario: Постобработка после update

- **GIVEN** завершённая индексация изменённых файлов
- **WHEN** выполняется `runPostProcessingParallel`
- **THEN** сначала удалены все `subscribes_to_event` relations
- **AND** затем 5 постпроцессоров запущены параллельно

### Requirement: Отмена pipeline

Система SHALL поддерживать отмену pipeline через context cancellation. `context.Context` пробрасывается в walker через `WalkParallelCtx(ctx, workers)` — все `select`-циклы обходчика и воркеров содержат ветку `case <-ctx.Done()`, что предотвращает зависание горутин при отмене (Ctrl+C / SIGTERM). При отмене используется свежий контекст с timeout 10 секунд для финализации `scan_run` со статусом `canceled`.

#### Scenario: Отмена через Ctrl+C

- **GIVEN** запущенный `codebase init` или `codebase update`
- **WHEN** пользователь нажимает Ctrl+C (context cancelled)
- **THEN** обходчик и воркеры прекращают обработку (выход через `ctx.Done()` без зависания на отправке в каналы)
- **AND** `scan_run` финализируется со статусом `canceled` через свежий контекст

## Related code

- `internal/fswalk/fswalk.go` — `Walker`, `Walk` (однопоточный), `WalkParallel`, `WalkParallelCtx` (параллельный с context), `computeHashBytes`, `FileFingerprint`, `SetPreFilter`, `modTimeMatch`, `getEncodingAndLanguage`
- `internal/indexer/runner.go` — `InitCtx`, `UpdateCtx`, worker pool pipeline, `runPostProcessingParallel`, загрузка fingerprint-ов и `walker.SetPreFilter`
- `internal/indexer/indexer.go` — `processFilesWorkerPoolInit`, `processFilesWorkerPool`, `mergeScanStats` (суммирует `PreFilteredFiles`)
- `internal/model/model.go` — `ScanStats.PreFilteredFiles`
- `cmd/update.go` — печать `Pre-filtered: N` в сводке
- `internal/store/db_files.go` — `DeleteFilesByPath`, `DeleteFilesByPaths`, `DeleteFilesByPathsExcept`, `GetLatestFilesByRootPath`
- `internal/store/db_schema.go` — `ON DELETE CASCADE` на всех таблицах сущностей
- `internal/config/config.go` — `IndexerConfig.IncludePatterns`, `ExcludePatterns`, `Parallel`

## Notes

- `.t01` не входит в дефолтные `include_patterns` — требует явного добавления в конфиг
- `filepath.WalkDir` используется вместо `filepath.Walk` для эффективности (DirEntry вместо FileInfo)
- Прогресс-бар обновляется с настраиваемым интервалом (`progress_interval_ms`)
- Архитектура walker-а: 1 горутина-обходчик (WalkDir, лёгкие метаданные) + N воркеров чтение+хеширование (тяжёлый I/O+CPU). Context cancellation через `ctx.Done()` в каждом `select`-цикле — устраняет дедлок при отмене (доработка «context-cancellation-deadlock»).
- Допуск 1мс в `modTimeMatch` компенсирует разницу точности `PostgreSQL TIMESTAMPTZ` (мкс) и `os.Stat` на Windows (100нс); точное `time.Equal` не работает после round-trip через БД.
- Pre-filter — оптимизация только для `codebase update`; `codebase init` всегда читает все файлы.
- Execution-слой `internal/indexer` используется и CLI (`cmd/init.go`, `cmd/update.go`), и потенциально MCP; ход walker-а специфицирован здесь.
