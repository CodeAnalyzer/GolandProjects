# Единый поиск по описаниям процедур и API-контрактов

## Why

Точка входа для доменных вопросов на естественном языке («каким образом привязать
кредитный договор к классификатору») находится сегодня за 3+ шага: OpenSpec-поиск →
подбор имени → контракт. При этом:

- **SQL-процедуры: описания не индексируются вообще.** `sql_procedures` хранит
  только имя/параметры/строки/хэш; парсер использует комментарии лишь для пропуска.
  При этом header-описание есть у ~98% реальных процедур FA (~9.4 тыс. блоков
  «после `as`» в fa-contracts + fa-cards против ~9.5 тыс. процедур).
- **API-контракты: описания хранятся (`api_contracts.short_description` /
  `full_description`), но не ищутся** — все API-поиски фильтруют только по имени
  (`contract_name ILIKE`).

Описания — это code-level discoverability для вопросов «как реализовано / где точка
входа», в дополнение к спекам («что должно делать»).

## What Changes

- **Извлечение header-описаний SQL-процедур**: SQL-парсер захватывает первый
  блок-комментарий `/* ... */` в окне между строкой декларации процедуры и началом
  тела. Три стиля декларации покрываются одним правилом: `DCL_PROC_BEGIN(Name)`,
  `create proc Name`, `API_CREATE_PROC(Name)` (у последнего `as` и параметры
  отсутствуют). Очистка: срез первой строки-имени файла (`Name.sql`), декоративных
  линий `-----`/`=====`; лимит 8 КБ с обрезкой хвоста.
- **Модель и схема**: `SQLProcedure.Description`, колонка `sql_procedures.description`
  (миграция `ADD COLUMN IF NOT EXISTS`).
- **FTS-индексация**: `search_vector TSVECTOR` на `sql_procedures`
  (имя = вес A, описание = вес B) и `api_contracts` (имя = A, short+full описание = B),
  GIN-индексы, trgm-GIN на `description`, идемпотентный бэкфилл по образцу
  спекового (`EnsureSpecSearchVectors`).
- **Единая точка поиска**: `SearchDescriptions` — UNION-запрос по
  `sql_procedures` + `api_contracts` с фильтром по kind
  (`procedure | service | event | callback_event | used_service`).
- **Дедупликация API-пар**: процедура, реализующая контракт с непустым описанием,
  вытесняется контрактом в выдаче (LEFT JOIN на `relations implements_contract`);
  при пустом описании контракта — процедура остаётся (fallback). Хранение описаний
  при индексации — у всех процедур независимо.
- **LSA-слой (семантика)**: второй sidecar-модель (`desc_lsa_model.bin` + state) на
  отдельном корпусе «описания процедур + описания контрактов» (спековый корпус и
  существующая spec-модель не затрагиваются); документы = имя (вес ×2) + описание.
  Переиспользование `internal/specfts` (tokenizer/TF-IDF/SVD, fingerprint, машина
  переобучения); публикации генераций в новых таблицах `desc_vocab`/`desc_embeddings`.
  Гибридная выдача: exact-хиты + semantic-хиты по перефразировкам; без обученной
  модели — graceful fallback на exact.
- **CLI**: `codebase query desc-search --text "..." [--kind ...] [--limit N]`,
  `--json` envelope по machine-readable контракту.
- **MCP**: инструмент `codebase_query_desc_search` (`text`, `kind`, `limit`) с
  обычной пагинацией.
- **Обогащение выдачи** `query procedure` колонкой `description` (обратная
  совместимость: новые поля в существующих записях).

## Capabilities

### New Capabilities

- `query/description-search`: полнотекстовый и семантический (LSA) поиск по
  человеческим описаниям SQL-процедур (header-комментарии) и API-контрактов
  (ShortDescription/FullDescription): семантика ранжирования, фильтр по kind,
  правило «контракт вытесняет процедуру», гибрид exact+semantic с отдельной
  desc-моделью, CLI-команда и MCP-инструмент, поведение при пустых результатах.

### Modified Capabilities

- `indexing/sql-parsing`: добавлено требование об извлечении header-описаний
  SQL-процедур (окно захвата, три стиля декларации, очистка, лимит).
- `infrastructure/database-schema`: добавлено требование о FTS-индексации описаний
  (колонка `description`, векторы `search_vector`, GIN/trgm-индексы, бэкфилл).

## Impact

- **Парсер**: `internal/parser/sql/sql_parser.go` — захват блок-комментария
  (окно после декларации; комментарии уже вырезаются из потока на строках
  трекинга `/* */` — аккумулятор встаёт туда).
- **Модель/схема**: `internal/model/model.go`, `internal/store/db_schema.go`,
  `internal/store/db_insert_sql.go`, бэкфилл по образцу
  `internal/store/db_insert_spec.go`.
- **Запросы/сервисы**: `internal/query/` (новый файл), `internal/querysvc/`,
  `cmd/` (новая подкоманда `query desc-search`).
- **LSA**: `internal/specfts/` (переиспользование), `internal/store/db_lsa.go`
  (публикации desc-генераций), пост-обработка индексера (машина переобучения
  по образцу spec-LSA), `internal/config` (секция desc-LSA).
- **MCP**: `internal/mcp/tools.go` — регистрация нового инструмента.
- **Миграция данных**: `api_contracts` готовы к поиску сразу после бэкфилла;
  `sql_procedures.description` заполняется только при перепарсинге — полная
  пересборка (`codebase init` / `update --modified=false`) или инкрементальный
  update изменённых файлов.
- **Вне области**: индексация процедур из `.h`-файлов (отдельное предложение
  `Modifications/index-h-file-sql-procedures.md`), поиск по описаниям SMF/DFM,
  общее LSA-пространство «спеки+описания» (решение разведки: отдельные модели
  по причинам изоляции качества и жизненного цикла).
