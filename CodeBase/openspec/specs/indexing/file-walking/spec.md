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

Система SHALL поддерживать настройку `include_patterns` и `exclude_patterns` в секции `[indexer]` конфигурационного файла для гибкого управления составом индексируемых файлов. Шаблоны по умолчанию: `*.sql`, `*.h`, `*.pas`, `*.inc`, `*.js`, `*.smf`, `*.dfm`, `*.tpr`, `*.rpt`, `*.xml`, `*.md`, `*.yaml`. Для markdown-файлов, не относящихся к openspec-корням, действует тот же механизм include/exclude: владелец конфигурации может исключить их явным паттерном (например `docs/*`), не отключая индексацию спек.

#### Scenario: Include patterns по умолчанию

- **GIVEN** конфигурация без явного указания `include_patterns`
- **WHEN** выполняется инициализация индекса
- **THEN** используются шаблоны по умолчанию: `*.sql`, `*.h`, `*.pas`, `*.inc`, `*.js`, `*.smf`, `*.dfm`, `*.tpr`, `*.rpt`, `*.xml`, `*.md`, `*.yaml`

#### Scenario: Опциональные .t01

- **GIVEN** конфигурация с `*.t01` в `include_patterns`
- **WHEN** выполняется индексация
- **THEN** препроцессированные `.t01` файлы индексируются как SQL-like layer

#### Scenario: Markdown вне openspec исключается паттерном

- **GIVEN** конфигурация с `docs/*` в `exclude_patterns` и деревом, содержащим openspec-спеки и посторонние `docs/*.md`
- **WHEN** выполняется индексация
- **THEN** markdown-спеки openspec-корней индексируются, файлы `docs/*.md` пропущены

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

Система SHALL при инкрементальном обновлении (`codebase update`, флаг `--modified=true` — значение по умолчанию) пропускать чтение и хеширование файлов, у которых `size` и `mtime` совпадают с предыдущей индексацией. Карта известных fingerprint-ов (`map[path]FileFingerprint{Size, ModTime}`) загружается из индекса и устанавливается в walker через `SetPreFilter` перед обходом. Сравнение `mtime` выполняется с допуском 1мс (`modTimeMatch`), чтобы компенсировать потерю точности между PostgreSQL `TIMESTAMPTZ` (микросекунды) и `os.Stat` на Windows (100нс). Pre-filter применяется только при наличии предыдущего состояния (Init не использует pre-filter). Число пропущенных файлов отражается в метрике `PreFilteredFiles` `ScanStats` и печатается в выводе `codebase update` (`Pre-filtered: N`).

Pre-filter устанавливается в walker только при `--modified=true`. При `--modified=false` pre-filter НЕ устанавливается: все файлы читаются и хешируются, но прогон выполняется как полная пересборка индекса через init-пайплайн (см. требование «Полная пересборка индекса (--modified=false)»), а не по per-file update-ветке. Флаг `--modified=false` SHALL отключать как mtime+size pre-filter, так и hash-сравнение (`onlyModified`).

#### Scenario: Файл не изменился — пропущен pre-filter-ом

- **GIVEN** ранее проиндексированный файл `Form.pas` с тем же размером и `mtime`, что в индексе
- **WHEN** выполняется `codebase update` (или `codebase update --modified=true`) с установленным pre-filter
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

#### Scenario: --modified=false — полный перепрох без pre-filter

- **GIVEN** ранее проиндексированный проект, в индексе есть файлы с неизменными `size` и `mtime`
- **WHEN** выполняется `codebase update --modified=false`
- **THEN** pre-filter не устанавливается, все подходящие файлы читаются и хешируются независимо от совпадения fingerprint и hash
- **AND** прогон выполняется как полная пересборка индекса (сброс таблиц кодовой базы + init-пайплайн), а не по per-file update-ветке
- **AND** счётчик `PreFilteredFiles` не растёт за счёт пропуска по fingerprint

### Requirement: Полная пересборка индекса (--modified=false)

Система SHALL при `codebase update --modified=false` выполнять полную пересборку индекса кодовой базы вместо перепрохода по update-ветке: сброс таблиц, заполняемых парсерами кодовой базы (`ResetCodebaseTables`, см. `infrastructure/database-schema`), удаление LSA-sidecar файлов (`spec_lsa_model.bin`, `spec_lsa_state.json`) и запуск штатного init-пайплайна (walk без pre-filter → парсинг → batch insert → постобработка → пересчёт LSA-модели). Прогресс-строка прогона помечается меткой `rebuild`. RTI- и TRC-сессии, история `scan_runs` и `schema_migrations` сохраняются. Прерванная (например, Ctrl+C) пересборка возобновляется повторным запуском `codebase update --modified=false` — полным рестартом пересборки. Инкрементальный run (`--modified=true`) после прерванной пересборки восстанавливает сущности уже обработанных файлов (pre-filter пропускает их по fingerprint), но НЕ восстанавливает relations-граф: pending-ссылки постпроцессоров накапливаются в памяти при парсинге и не переживают перезапуск процесса, поэтому для доигрывания пересборки инкрементальный run использовать нельзя.

#### Scenario: Полная пересборка с сохранением RTI/TRC и истории сканов

- **GIVEN** проиндексированный проект, в БД есть RTI- и TRC-сессии и история `scan_runs`
- **WHEN** выполняется `codebase update --modified=false`
- **THEN** таблицы кодовой базы сброшены и заполнены заново init-пайплайном
- **AND** сессии `rti_sessions`/`trc_sessions` и строки `rti_*`/`trc_*` остались на месте
- **AND** строки истории `scan_runs` сохранились, добавлена новая запись прогона

#### Scenario: Метка прогресса rebuild

- **GIVEN** выполняется `codebase update --modified=false`
- **WHEN** прогресс-репортер печатает строку прогресса
- **THEN** строка содержит метку `rebuild`, а не `init` или `update`

#### Scenario: Прерванная пересборка возобновляется повторным rebuild

- **GIVEN** пересборка `codebase update --modified=false` прервана после обработки части файлов (индекс частично заполнен)
- **WHEN** выполняется повторный `codebase update --modified=false`
- **THEN** пересборка стартует заново с чистого состояния и завершается полностью
- **AND** итоговый индекс эквивалентен непрерванной пересборке: сущности и relations-граф в единственном экземпляре

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

Система SHALL выполнять постобработку после завершения индексации всех файлов. Сначала удаляются все `subscribes_to_event` relations (`DeleteSubscribesToEventRelations`), затем запускаются параллельные постпроцессоры: PAS-DFM links, SQL procedure call relations, callback event relations, retcode constants, fragment relations и spec-постпроцессор (резолв `spec_code_mentions` → `references_code`, построение `depends_on_capability`, иерархия capability, `change_modifies`; поведение — см. `indexing/openspec-parsing`). Если индексируемых спек нет, spec-постпроцессор завершается без изменений. Пересчёт LSA-модели полнотекстового слоя (см. `query/spec-search`) выполняется после завершения всех постпроцессоров, по порогу изменений.

#### Scenario: Постобработка после update

- **GIVEN** завершённая индексация изменённых файлов
- **WHEN** выполняется `runPostProcessingParallel`
- **THEN** сначала удалены все `subscribes_to_event` relations
- **AND** затем постпроцессоры, включая spec-постпроцессор, запущены параллельно

#### Scenario: Спек в дереве отсутствует

- **GIVEN** индексация проекта без openspec-корней
- **WHEN** выполняется постобработка
- **THEN** spec-постпроцессор не создаёт записей и не мешает прочим постпроцессорам

### Requirement: Отмена pipeline

Система SHALL поддерживать отмену pipeline через context cancellation. `context.Context` пробрасывается в walker через `WalkParallelCtx(ctx, workers)` — все `select`-циклы обходчика и воркеров содержат ветку `case <-ctx.Done()`, что предотвращает зависание горутин при отмене (Ctrl+C / SIGTERM). При отмене используется свежий контекст с timeout 10 секунд для финализации `scan_run` со статусом `canceled`.

#### Scenario: Отмена через Ctrl+C

- **GIVEN** запущенный `codebase init` или `codebase update`
- **WHEN** пользователь нажимает Ctrl+C (context cancelled)
- **THEN** обходчик и воркеры прекращают обработку (выход через `ctx.Done()` без зависания на отправке в каналы)
- **AND** `scan_run` финализируется со статусом `canceled` через свежий контекст

### Requirement: Маркировка генерируемых копий файлов

Система SHALL при сохранении файла в индекс вычислять и сохранять признак генерируемой копии `is_generated`: файл помечается как генерируемая копия, если его относительный путь содержит сегмент каталога `UPLOAD` (регистр символов сегмента не значим) или расширение файла равно `t01`. Признак не влияет на включение файла в индекс и на его парсинг — файлы-копии индексируются как обычно, но несут метку для приоритезации в name-based lookup'ах и для последующей фильтрации в отчётах. Канонические исходники (в том числе расположенные в каталогах `SERVER`/`Server`) MUST NOT помечаться как генерируемые копии.

#### Scenario: Файл в каталоге UPLOAD помечается копией

- **GIVEN** дерево проекта содержит `fa-contracts/LoanBureau/Server/UPLOAD/BaseAlg_ConsMinRest.sql`
- **WHEN** выполняется индексация файла
- **THEN** запись файла в индексе имеет `is_generated = true`

#### Scenario: Препроцессированный t01 помечается копией

- **GIVEN** дерево проекта содержит `fa-contracts/API_Credit/Server/UPLOAD/BaseAlgAmrtCostSinglePmnt.t01`
- **WHEN** выполняется индексация файла
- **THEN** запись файла в индексе имеет `is_generated = true`

#### Scenario: Канонический исходник не помечается

- **GIVEN** дерево проекта содержит `fa-contracts/Consumer/SERVER/Accrual/BaseAlg_ConsMinRest.sql`
- **WHEN** выполняется индексация файла
- **THEN** запись файла в индексе имеет `is_generated = false`
- **AND** файл проиндексирован и распарсен как обычно

#### Scenario: Регистр сегмента UPLOAD не значим

- **GIVEN** дерево проекта содержит файлы в каталогах `Server/UPLOAD` и `Server/upload`
- **WHEN** выполняется индексация обоих файлов
- **THEN** обе записи имеют `is_generated = true`

### Requirement: Детекция кодировки по содержимому в walk-воркере

Система SHALL определять кодировку single-byte legacy-форматов (`.sql`, `.h`, `.tpr`, `.pas`, `.inc`, `.js`, `.smf`, `.dfm`, `.rpt`) по содержимому файла в walk-воркере — после чтения файла и вычисления SHA-256, по уже прочитанному буферу (прецедент — `.md`). Карта расширений используется как prior (см. `infrastructure/encoding-cli`, требование «Детекция кодировок»). Детекция выполняется одним линейным проходом счётчиков по сырым байтам; декодирование выполняется ровно один раз с детектированной кодировкой (без decode-repair-redecode). Файл без байт `> 0x7F` пропускает детекцию (ASCII fast-path, кодировка — prior, декод — identity). Детектированная кодировка записывается в `FileInfo.Encoding` и сохраняется в `files.encoding` БД. Число файлов, у которых детектированная кодировка отличается от prior карты, отражается в метрике `EncodingRefined` `ScanStats` и печатается в сводке прогона. SHA-256 вычисляется по сырым байтам — детекция не влияет на hash и pre-filter. Pre-filtered файлы (mtime+size совпали, содержимое не читается) не проходят детекцию и не декодируются — их кодировка в БД остаётся с предыдущего прогона. Форматы `.md`, `.xml`, `.yaml`, `.t01` обрабатываются прежними правилами (DetectMarkdownEncoding / DetectXMLEncoding / UTF-8 / CP866).

#### Scenario: CP1251 SQL-файл детектируется и декодируется корректно

- **GIVEN** дерево проекта содержит `fa-administrator/.../Proc.SQL` в кодировке CP1251 (карта даёт prior CP866)
- **WHEN** walk-воркер читает файл
- **THEN** детекция по содержимому возвращает WIN1251
- **AND** `FileInfo.Encoding` = WIN1251, значение сохранено в `files.encoding`
- **AND** метрика `EncodingRefined` увеличена на 1
- **AND** header-описание процедуры декодировано читаемо, без mojibake

#### Scenario: CP866 SQL-файл с рамками в комментариях остаётся CP866

- **GIVEN** CP866 SQL-файл, комментарии которого содержат псевдографику (байты 0xC0–0xDF) и кириллицу (0x80–0x9F, 0xA0–0xAF)
- **WHEN** walk-воркер читает файл
- **THEN** cp866Score перевешивает, кодировка остаётся CP866
- **AND** метрика `EncodingRefined` не увеличивается

#### Scenario: ASCII-файл пропускает детекцию

- **GIVEN** SQL-файл, содержащий только байты `≤ 0x7F`
- **WHEN** walk-воркер читает файл
- **THEN** детекция по содержимому не выполняется (fast-path), кодировка — prior (CP866)
- **AND** декодирование — identity, содержимое не изменяется

#### Scenario: PAS-файл в CP866 детектируется

- **GIVEN** PAS-файл в кодировке CP866 (карта даёт prior WIN1251)
- **WHEN** walk-воркер читает файл
- **THEN** детекция определяет CP866, файл декодирован корректно
- **AND** метрика `EncodingRefined` увеличена

#### Scenario: Pre-filtered файл не проходит детекцию

- **GIVEN** ранее проиндексированный файл с совпадающими size и mtime
- **WHEN** выполняется `codebase update` с pre-filter
- **THEN** файл не читается, детекция не выполняется, файл не декодируется
- **AND** кодировка в БД остаётся с предыдущего прогона

#### Scenario: Сводка прогона показывает масштаб перекодировок

- **GIVEN** прогон `codebase init`, в котором 100 файлов получили кодировку, отличную от prior карты
- **WHEN** прогон завершается
- **THEN** в сводке прогона напечатана метрика EncodingRefined: 100

#### Scenario: Производительность детекции не деградирует индексацию

- **GIVEN** полное дерево FA
- **WHEN** выполняется индексация с детекцией по содержимому
- **THEN** детекция добавляет один линейный проход счётчиков по горячему буферу на файл (без дополнительного декодирования)
- **AND** выполняется параллельно в существующих walk-воркерах
- **AND** суммарное время прогона в пределах шума относительно прогона без детекции

## Related code

- `internal/fswalk/fswalk.go` — `Walker`, `Walk` (однопоточный), `WalkParallel`, `WalkParallelCtx` (параллельный с context), `computeHashBytes`, `FileFingerprint`, `SetPreFilter`, `modTimeMatch`, `getEncodingAndLanguage`, `isContentDetectedExt` (single-byte расширения для детекции по содержимому через `encoding.DetectFromBytesWithPrior`)
- `internal/indexer/runner.go` — `InitCtx`, `UpdateCtx`, `runInitPipeline` (init-пайплайн с меткой прогресса), `fullRebuildCtx` (пересборка: reset → удаление LSA-sidecar → init-пайплайн), `removeLSASidecars`, worker pool pipeline, `runPostProcessingParallel`, загрузка fingerprint-ов и `walker.SetPreFilter`
- `internal/indexer/indexer.go` — `processFilesWorkerPoolInit`, `processFilesWorkerPool`, `mergeScanStats` (суммирует `PreFilteredFiles`, `EncodingRefined`)
- `internal/model/model.go` — `ScanStats.PreFilteredFiles`, `ScanStats.EncodingRefined`
- `cmd/update.go` — печать `Pre-filtered: N` в сводке
- `internal/store/db_files.go` — `DeleteFilesByPath`, `DeleteFilesByPaths`, `DeleteFilesByPathsExcept`, `GetLatestFilesByRootPath`
- `internal/store/db_reset.go` — `ResetCodebaseTables` (усечение таблиц кодовой базы для пересборки), `codebaseResetTables`
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
