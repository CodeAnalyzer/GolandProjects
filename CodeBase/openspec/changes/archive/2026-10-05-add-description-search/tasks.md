# Задачи: add-description-search

## 1. Парсер: извлечение описаний

- [x] 1.1 Состояние окна захвата: `awaitingDescription` на декларации (все три стиля: `procBeginRe`/`DCL_PROC_BEGIN`, `procDeclRe`/`create proc`, `procCreateRe`/`API_CREATE_PROC` — включая создание pending-состояния в ветке `procCreateRe`), закрытие на первой кодовой строке/`__BEGIN_PROCEDURE__`
- [x] 1.2 Аккумулятор блок-комментария в трекинге `inBlockComment` (строки 911-921): захват строк при открытом окне, завершение на `*/`
- [x] 1.3 Очистка: срез первой строки `^\w+\.(sql|h)$`, удаление декоративных линий из `-`/`=`, обрезка 8 КБ с хвоста
- [x] 1.4 Поле `Description string` в `model.SQLProcedure`; прокидывание в результат парсера
- [x] 1.5 Тесты парсера: типовой header после `as` (DCL_PROC_BEGIN), `create proc` без `__BEGIN_PROCEDURE__`, API-стиль без `as`, отсутствие описания, имя файла + декоративные линии, обрезка > 8 КБ, `--`-комментарий не описание, CP866-текст

## 2. Store: колонка, векторы, индексы

- [x] 2.1 Миграция `sql_procedures.description TEXT` (`ADD COLUMN IF NOT EXISTS`) в `db_schema.go`; запись в batch-insert (`db_insert_sql.go`)
- [x] 2.2 Выражения `search_vector` для `sql_procedures` и `api_contracts` (имя A / описание B, `'russian'`), GIN-индексы, trgm-GIN на `description`
- [x] 2.3 Идемпотентный бэкфилл векторов (по образцу `EnsureSpecSearchVectors`), вызов из init/update-пайплайна
- [x] 2.4 Тесты store: миграция на существующей БД, бэкфилл контрактов без переиндексации, заполнение description при перепарсинге (по образцу `schema_integration_test.go`)

## 3. Query: поисковый запрос

- [x] 3.1 `SearchDescriptions(ctx, text, kinds, limit)`: UNION `sql_procedures` + `api_contracts`, `ts_rank`, `ts_headline`-сниппет, фильтр kind (`procedure | service | event | callback_event | used_service`), `LIMIT` + метаданные усечения
- [x] 3.2 Дедуп «контракт вытесняет процедуру»: `NOT EXISTS` (implements_contract → контракт с непустым описанием) на стороне процедур
- [x] 3.3 Колонка `description` в выдаче `query procedure` (обратная совместимость)
- [x] 3.4 Тесты query: hit по описанию контракта (`API_CCred_BindClassifier`), hit по описанию процедуры (`ReturnCashFund_Insert`), имя ранжируется выше описания, фильтр kind (одиночный и повторяемый), дедуп пары, fallback при пустом описании контракта, пустой результат = `[]`

## 4. LSA-слой (desc-модель)

- [x] 4.1 Таблицы `desc_vocab`/`desc_embeddings` (generation-ключ, схема по образцу `spec_vocab`/`spec_embeddings`); публикация поколения транзакционно, удержание текущего+предыдущего (`db_lsa.go` по образцу `PublishSpecLSAGeneration`/`DeleteSpecLSAGenerationsExcept`)
- [x] 4.2 Конфиг: секция desc-LSA (`lsa_enabled`, `lsa_k`, `lsa_min_df`, `lsa_max_df`, `lsa_min_corpus`, `lsa_retrain_threshold`, `lsa_model_path`, `lsa_min_cosine`, `lsa_relative_cutoff`) с независимыми дефолтами
- [x] 4.3 Пост-обработка индексера: загрузка корпуса описаний (процедуры: имя×2 + description; контракты: имя×2 + short + full), машина переобучения по fingerprint (retrain / skip-idempotent / defer), публикация поколения, sidecar-пара `desc_lsa_model.bin` + `desc_lsa_state.json`
- [x] 4.4 Гибридный поиск: загрузка эмбеддингов поколения, трансформация запроса, semantic-хиты (cosine-ранг, MinCosine + relative cutoff), объединение с exact, метка источника хита, graceful exact-only без модели
- [x] 4.5 Тесты: первое обучение, идемпотентный пропуск, defer до порога, spec-модель не затронута, публикация/ротация поколений, semantic-хит по перефразировке (golden-набор), fallback без модели

## 5. CLI и MCP

- [x] 5.1 Команда `codebase query desc-search` (`--text`, повторяемый `--kind`, `--limit`), `--json`/`--summary`/`--ndjson` envelope, пустой результат = `[]`
- [x] 5.2 MCP-инструмент `codebase_query_desc_search` (`text`, `kind`, `limit`) в `internal/mcp/tools.go`, профиль query, штатная пагинация
- [x] 5.3 Тесты: JSON-envelope команды, регистрация MCP-инструмента, пагинация большого ответа

## 6. Верификация и документация

- [x] 6.1 `openspec validate --all` — дельты и спеки валидны
- [x] 6.2 Прогон на FA: полная пересборка (`update --modified=false`) — **выполняется вручную пользователем**; после прогона: заполнение `description` (~98% процедур), первое обучение desc-LSA, поисковые запросы из сценариев спеки (exact + semantic), сравнение качества semantic-выдачи
- [x] 6.3 Release notes: описания процедур заполняются после перепарсинга; контрактный поиск доступен сразу после бэкфилла; desc-LSA обучается при полной пересборке, до обучения — exact-only
