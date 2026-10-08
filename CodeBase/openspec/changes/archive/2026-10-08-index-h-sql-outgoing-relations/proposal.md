## Why

SQL-процедуры, определённые в H-файлах через `DCL_PROC_BEGIN`, индексируются, но их тела не участвуют в графе связей: у `.h`-процедур 0 исходящих `calls_procedure`, 0 табличных relations (`selects_from`/`inserts_into`/…) и 0 `query_fragments`. Граф вызовов и табличный анализ «слепы» к ~40 реальным процедурам в H-файлах FA, а `callers` не находит `.h`-процедуры как источники вызовов. Попутно SQL-парсер запускается безусловно для всех `.h` (по FA — 1954 файла при ~5% содержащих маркеры процедур), и define-ветки парсера потребляют только первую строку `#define`, из-за чего continuation-строки с `\` утекают в основной цикл и порождают джанк-сущности (сейчас гасятся пост-фильтром в индексере).

## What Changes

- SQL-парсер: define-ветки потребляют continuation-строки (пока строка заканчивается `\`) — многострочные `#define` перестают утекать в основной цикл; джанк-процедуры/фрагменты из тел макросов не возникают в принципе.
- Индексер `parseHFile`: дешёвый маркерный pre-check (`DCL_PROC_BEGIN(`/`__BEGIN_PROCEDURE__(`) перед SQL-парсингом `.h` — парсер запускается только для файлов с маркерами (~5% вместо 100%).
- Индексер `parseHFile`: репликация схемы `.sql`-пайплайна — вставка таблиц/колонок/`query_fragments` (с parent-binding к процедурам), `addPendingSQLCalls`, `buildSQLProcedureTableRelations` → у `.h`-процедур появляются исходящие связи в графе.
- Порядок реализации: сначала фикс парсера (чистый вывод без джанка), затем pre-check и связи.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/sql-parsing`: требование «SQL `#define` и include-директивы внутри SQL» расширяется — define-ветки парсера обязаны потреблять `\`-продолжения многострочных макросов целиком.
- `indexing/h-report-parsing`: требование «SQL-процедуры в H-файлах» расширяется — из тел `.h`-процедур строятся исходящие связи (`calls_procedure`, табличные relations) и `query_fragments`; SQL-парсинг `.h` выполняется только при наличии маркеров процедур.

## Impact

- `internal/parser/sql/sql_parser.go` — потребление `\`-продолжений в define-ветках (затрагивает и `.sql`-парсинг — нужен регрессионный замер по индексу FA).
- `internal/indexer/indexer.go` — `parseHFile`: pre-check, tables/columns/fragments batch, parent-binding, pending calls, table relations.
- `internal/indexer/indexer_sql_pas.go` — реюз существующих хелперов без изменений схемы БД.
- Тесты: `internal/indexer/indexer_h_test.go`, `internal/parser/sql/sql_parser_test.go`.
- Схема БД и публичные контракты CLI/MCP не меняются — появляется больше данных; новый контент требует полного перестроения (`init` / `update --modified=false`).
- `isLineInsideMacroDefinition` после фикса парсера становится технически избыточным (решение оставить/убрать — при реализации).
