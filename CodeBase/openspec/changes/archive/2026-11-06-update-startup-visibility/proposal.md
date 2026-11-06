# Proposal: update-startup-visibility

## Why

На рабочем ПК (80 FA-каталогов, ~624k файлов) между баннером `codebase update`
и первым кадром спиннера проходит минута и более полной тишины — прогресс
невидим, прогон выглядит зависшим (зарегистрирован Ctrl+C пользователем ровно
в середине этой фазы). Окно образуют подготовительные фазы `UpdateCtx` до
старта progress-репортера (runner.go:214):

1. **discoverGitSources** — последовательный цикл по 80 каталогам, по 3 спавна
   `git.exe` на каталог (240 запусков процессов; локально 190 мс/каталог, на
   рабочем ПК с корпоративным AV 0.5–1.5 с/спавн → 40–120 с только git);
2. **Три полных seq-scan таблицы `files`** (индекса на `path` нет):
   `BackfillFilesFromSources` (предикат `left(path, len)=prefix` не-sargable),
   `FindDuplicateFileGenerations` (`GROUP BY path`), `GetLatestFilesByRootPath`
   (`DISTINCT ON ... ORDER BY path` + передача всех ~624k строк клиенту).

Финальные `Stage timings` сегодня не атрибутируют это окно: `WalkSaveMs`
отсчитывается от старта `UpdateCtx` и включает все подготовительные фазы.

## What Changes

- Прогресс-репортер стартует сразу после баннера; ВСЕ подготовительные фазы
  (включая проверку/миграцию схемы БД) отображаются ОДНОЙ перезаписываемой
  строкой через существующий механизм `Stage` с общей фазой `preparing`:
  `preparing | schema check/migrations...` → `preparing | git detection 12/80`
  (счётчик — только детекция каталогов) → `preparing | source registry upsert`
  (upsert линий + backfill привязок) → `preparing | safety-net duplicates` →
  `preparing | load state files`; деталь сменяется на месте, без отдельных
  строк на каждую фазу; после подготовки — штатная строка счётчиков walk.
- Git-детекция каталогов выполняется пулом воркеров; размер пула — то же
  значение, что и для воркеров индексации (`[indexer] parallel` / `-j`, по
  умолчанию 4, у пользователя 12), нормализуется существующим
  `normalizeParallel`. Кэш детектора остаётся на время скана.
- Добавлен индекс `idx_files_path` на `files(path)` с opclass `text_pattern_ops`
  (колонка — `TEXT`); предикаты pre-walk запросов выравниваются в sargable-вид
  (убрана семантически мёртвая ветка `path = $1`; `left()` в
  `BackfillFilesFromSources` — на экранированный LIKE-префикс, единый хелпер
  экранирования для всех трёх запросов). Выигрыш индекса — узкопрефиксные
  параметризованные запросы (`update -p <подпуть>`); полнокорневая загрузка
  состояния и bulk-backfill привязок (VALUES-джойн: паттерн из колонки джойна
  не даёт индексного диапазона, префиксы покрывают всё дерево) остаются
  seq scan — их стоимость в передаче строк, не в доступе. Версия схемы
  бампается, существующие БД получают индекс при первом `update` штатной
  миграцией.
- `Stage timings` пополняются таймингами подготовительных фаз (discovery
  целиком, safety-net, load state, прочее prep — scan run + FTS-backfill):
  тайминги суммарно покрывают всё подготовительное окно; `WalkSaveMs`
  измеряется от фактического старта walk и больше не включает подготовительные
  фазы.
- Баннер прогона печатается до `InitSchema`, индикация схемы — первая деталь
  prep-строки: миграция (разовое `CREATE INDEX` на ~624k строк с блокировкой
  записи `files` на время построения) не создаёт новое тихое окно до баннера.

## Capabilities

### New Capabilities

- `indexing/progress-reporting`: отображение хода прогона init/update/rebuild —
  строка счётчиков, именованные стадии (включая подготовительные фазы до walk),
  тайминги фаз в финальном отчёте.

### Modified Capabilities

- `indexing/branch-sources`: требование «Discovery источников в init/update» —
  git-детекция каталогов выполняется параллельно пулом воркеров
  (размер = настройка параллельности индексации), с сохранением семантики
  «один набор вызовов git CLI на каталог» и кэша на время скана.
- `infrastructure/database-schema`: требование «Индексы для производительности» —
  добавлен индекс `files(path) text_pattern_ops` для LIKE-префиксных запросов
  по путям файлов; bump версии схемы.

## Impact

- `internal/indexer/runner.go` — ранний старт репортера, стадии подготовительных
  фаз, передача `parallel` в discovery, тайминги фаз.
- `internal/indexer/source_registry.go`, `source_git.go` — параллельный discovery
  (пул воркеров), счётчик каталогов для стадии, детектор за интерфейсом
  (тестируемость конкурентности).
- `internal/indexer/indexer.go` — запуск/остановка репортера до walk-фазы
  (механизм `Stage` переиспользуется без изменений).
- `internal/model/` — поля таймингов подготовительных фаз в `ScanStats`.
- `internal/store/db_schema.go` — DDL индекса, bump `CurrentSchemaVersion`.
- `internal/store/db_products.go` — sargable-предикат backfill.
- `internal/store/db_files.go`, `internal/store/db_reconcile_relations.go` —
  выравнивание предикатов и единое LIKE-экранирование (`escapeLikeAll`).
- `cmd/update.go`, `cmd/init.go` — баннер/индикация схемы до `InitSchema`,
  вывод новых таймингов в `printPipelineTimings`.
- БД пользователей: после замены бинаря read-only команды (`query`, `stats`,
  `health`, `review`, `rti`, `trc`) требуют однократного `codebase update`
  (ErrSchemaUpdateRequired до применения миграции v6 — цена bump'а версии);
  миграция выполняет разовое `CREATE INDEX` с SHARE-блокировкой записи `files`
  на время построения (~секунды на 624k строк).
