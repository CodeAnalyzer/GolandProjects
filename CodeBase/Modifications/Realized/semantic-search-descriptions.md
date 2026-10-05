# Полнотекстовый и семантический поиск по описаниям процедур и API-контрактов

Предложение (не реализовано). Добавить поиск по человеческим описаниям SQL-процедур
(header-комментарий) и API-контрактов (`ShortDescription`/`FullDescription`) как точку
входа для доменных вопросов на естественном языке («каким образом привязать кредитный
договор к классификатору»), помимо существующего поиска по спекам OpenSpec.

---

## Контекст (что есть сейчас)

- **SQL-процедуры: описания не индексируются вообще.**
  - `sql_procedures` хранит только `file_id, proc_name, parameters, line_start, line_end, body_hash` (`internal/store/db_schema.go:67-75`).
  - В модели `SQLProcedure` поля описания нет (`internal/model/model.go:78-86`).
  - Парсер SQL использует комментарии только для пропуска (`isInsideBlockComment`, `commentRe` — `internal/parser/sql/sql_parser.go:54,1734+`), текст header-комментария не сохраняется.
  - При этом в исходниках FA описания есть почти у каждой процедуры
    (пример: `FA/fa-contracts/Consumer/Server/Accrual/ReturnCashFund_Insert.sql` —
    блок `/*...*/` до `__BEGIN_PROCEDURE__` с назначением и параметрами).

- **API-контракты: описания хранятся, но не ищутся.**
  - Парсинг: `internal/parser/dsxml/parser.go:51` (`FullDescription`), `:381-382` (TrimSpace-присвоение).
  - Схема: `api_contracts.short_description/full_description` (`internal/store/db_schema.go:308-309`).
  - Запись: `internal/store/api_store.go:177,183`; чтение: `internal/query/api_query.go:116,131`.
  - Поиск по контрактам фильтрует **только по имени**: `c.contract_name ILIKE $1`
    (`internal/query/api_query.go:300,315,346,361`). Текстового индекса на `api_contracts` нет.

- **FTS/семантика сегодня есть только у спек.**
  - tsvector `'russian'` + веса A/B: `internal/store/db_insert_spec.go:13-20`.
  - GIN-индексы: `internal/store/db_schema.go:890-895`.
  - LSA-слой: `internal/specfts/`, `internal/store/db_lsa.go`, sidecar `spec_lsa_model.bin`.

## Мотивация

Разбор кейса «привязка кредитного договора к классификатору» показал: точка входа
(`API_CCred_BindClassifier`) находится через OpenSpec-поиск + подбор имени (`BindClassifier`),
а текст `<FullDescription>` контракта, содержащий дословный ответ на вопрос, виден только
**после** попадания в контракт по имени. Описания процедур недоступны вовсе. Семантический
поиск по описаниям — это code-level discoverability для вопросов «как реализовано / где
точка входа», в дополнение к спекам («что должно делать»). Спеки остаются источником
истины для нормативного поведения и покрывают не весь код; описания покрывают всю базу.

## Предлагаемое решение

### Шаг 1. Извлечение header-описаний SQL-процедур

- `internal/parser/sql/`: при регистрации процедуры захватывать блок-комментарий,
  непосредственно предшествующий маркеру начала (`__BEGIN_PROCEDURE__`, `DCL_PROC_BEGIN`,
  `create procedure`). Обрезка по размеру (порядка 4-8 КБ), очистка декоративных линий
  (`-----`, `======`).
- `internal/model/model.go`: поле `Description string` в `SQLProcedure`.
- `internal/store/db_schema.go` + `db_insert_sql.go`: колонка `sql_procedures.description TEXT`
  (миграция `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`).

### Шаг 2. FTS-индексация описаний

По образцу spec-поиска (`db_insert_spec.go`):

- `sql_procedures.search_vector TSVECTOR` =
  `setweight(to_tsvector('russian', proc_name), 'A') || setweight(to_tsvector('russian', coalesce(description,'')), 'B')`
- `api_contracts.search_vector TSVECTOR` =
  `setweight(to_tsvector('russian', contract_name), 'A') || setweight(to_tsvector('russian', coalesce(short_description,'') || ' ' || coalesce(full_description,'')), 'B')`
- GIN-индексы на оба вектора; trgm-GIN на `sql_procedures.description` для частичных совпадений.
- Бэкфилл `search_vector` — SQL-`UPDATE` без переиндексации (как `EnsureSpecSearchVectors`,
  `db_insert_spec.go:371+`). Для `api_contracts` данные уже в БД, работает сразу.
  Для `sql_procedures` описание появится только после повторного парсинга (см. «Миграция»).

События и callback-контракты отдельной сущности не требуют — они уже лежат в
`api_contracts` (`contract_kind = event / callback_event`) и покрываются тем же вектором.

### Шаг 3. Единая точка поиска (svc + CLI + MCP)

- `internal/query/`: `SearchDescriptions(ctx, query, kinds, limit)` — единый UNION-запрос по
  `sql_procedures` + `api_contracts` (ts_rank, фильтр по kind: `procedure | service | event |
  callback_event | used_service`), в выдаче: тип, имя, файл:строка, ранг, сниппет описания.
- `internal/querysvc/` + `cmd/`: `codebase query desc-search --text "..." [--kind ...]`.
- `internal/mcp/tools.go`: инструмент `codebase_query_desc_search` (параметры `text`,
  `kind`, `limit`) с обычной пагинацией.
- `--json` envelope по machine-readable контракту.

### Шаг 4 (опционально, отдельным изменением). LSA-слой поверх описаний

Переиспользовать `internal/specfts/` (tokenizer/TF-IDF/LSA): второй sidecar для корпуса
«описания процедур+контрактов»; гибридный ранг exact+semantic как в `spec_search`.
Не блокирует шаги 1-3.

## Затронутые файлы

- `internal/parser/sql/sql_parser.go` — захват header-комментария
- `internal/model/model.go` — `SQLProcedure.Description`
- `internal/store/db_schema.go`, `internal/store/db_insert_sql.go` — колонка, вектор, индексы
- `internal/store/db_insert_spec.go` (или новый `db_insert_fts.go`) — выражения векторов и бэкфилл
- `internal/query/` (новый `desc_query.go`) — поисковый запрос
- `internal/querysvc/`, `cmd/` — сервис и CLI-команда
- `internal/mcp/tools.go` — MCP-инструмент

## Миграция и совместимость

- Новые колонки добавляются миграцией `IF NOT EXISTS`; для существующих индексов — `update`
  не перепарсит файлы (pre-filter по mtime+size, парсер не меняет файлы), поэтому
  `sql_procedures.description` заполнится только при полном `codebase init` (или
  `codebase update --modified=false`). Отразить в документации релиза.
- `api_contracts` полностью готов к поиску после бэкфилла вектора — без переиндексации.
- Публичные контракты существующих инструментов не меняются; добавляется новый инструмент
  и новая колонка в выдаче `query procedure` (обратная совместимость сохраняется).
- Спека `openspec/specs/query/` требует дельты (новая capability для поиска по описаниям) —
  реализацию сопровождать openspec-change.

## Тесты

- Парсер: извлечение описания (типовой header FA, header отсутствует, header после
  `#include`, описание длиннее лимита, CP1251-текст).
- Store: миграция колонки, бэкфилл вектора (по образцу `schema_integration_test.go`).
- Query: релевантность (`привязка кредитных договоров к классификатору` → `API_CCred_BindClassifier`
  в топе; `Возврат сумм из Банка-партнера` → `ReturnCashFund_Insert`), фильтр по kind, пустой
  результат = `[]`.
- MCP: регистрация инструмента, пагинация.

## Ожидаемый результат

| Метрика | Сейчас | После |
|---|---|---|
| Поиск по описанию процедуры | невозможен | FTS (+ опц. LSA) |
| Поиск по описанию API-контракта | невозможен (только по имени) | FTS (+ опц. LSA) |
| Запрос «привязка кредитного договора к классификатору» | спеки → имена → контракт (3+ шага) | прямой hit по описанию (1 шаг) |
| Точка входа для вопросов «как реализовано» | имена/спеки | описания + имена + спеки |

## Открытые вопросы

1. Включать ли в корпус `parameters`-часть header-комментария (описания `@Param`) — сейчас
   предлагается «да» (вес B).
2. Нужен ли поиск по описаниям SMF/DFM (captions) в этом же инструменте — предлагается
   вынести в отдельное изменение.
3. Шаг 4 (LSA по описаниям): сразу или после накопления опыта на FTS.
