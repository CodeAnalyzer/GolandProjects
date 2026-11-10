# Change Review: add-table-exists-in-metadata

- **Дата**: 2026-11-10
- **Ревьюер**: opencode (L3/pro)
- **Вердикт**: ⚠️ **ГОТОВ К РЕАЛИЗАЦИИ ПОСЛЕ УСТРАНЕНИЯ F1/F2** (высокий/средний приоритет) и небольших доработок F3/F4 в design. Артефакты change структурно корректны, механика правила реализуема по существующим паттернам, пилотный дефект подтверждён живым индексом. F1 — единственный содержательный блокер: эмпирический уровень ложных срабатываний существенно выше заявленного в Risks.

## Что проверялось

1. Структура: `openspec validate add-table-exists-in-metadata --strict` — valid; delta распознаётся (1 ADDED-требование в `review/deploy-stoppers`, 7 сценариев); proposal/design/tasks внутренне согласованы (impact-файлы = задачи).
2. Реализуемость по коду: `types.go`/`catalog.go` (каталог = единый источник, `RuleListString()` сам протаскивает правило в CLI `--rules` и MCP-описание `codebase_review_sql`, registry.go:373 — ручных правок не нужно); паттерны `prewarm*`/`cachedLookup*` (runner.go, review_lookup.go); контексты `parsed.Tables` из sql_parser.go; индексы БД (db_schema.go:1077,1084).
3. Живой индекс (локальная БД localhost:5435/codebase, продукт fa-contracts; плюс MCP-сервер на полной копии): подтверждение пилотного дефекта, паттерна TmpTbl, масштабов источников и — главное — прямое измерение шума правила по запроектированному фильтру (D1+D6).

## Подтверждённые положительно

- **Пилотный дефект — истина.** `pAPI_Account_MassClose`: 6 строк `sql_tables` (delete+insert) из `ContractMassLoad.sql` в трёх копиях веток (fa-contracts/11016/11017); в `api_contract_tables`/`api_business_object_tables` — 0 объявлений; при этом `pAPI_Acc_MassClose` объявлена 8 раз в DSArchitect XML (fa-generalledger, fa-cards, fa-contracts-11017, fa-custody, fa-factoring). Правило поймает дефект с корректной строкой (insert, ~2567).
- **TmpTbl-паттерн даёт `context='create'`.** `pCons_ObjectID`: 3 строки create (по одной на ветку, из `pCons_ObjectID_TmpTbl.sql`, `create table pCons_ObjectID` в тексте файла). Сценарий 2 дельты механически верен.
- **Индексные факты D2 верны**: `idx_sql_tables_table_name_lower`, `idx_sql_column_definitions_lower_table_col` существуют; у api-таблиц lower-индекса нет (только contract_id/file_id); объёмы: sql_tables 1.58M строк (create 43 432), select_into-определений 183 таблицы, api_contract_tables 47 140, api_business_object_tables 12 112. Seq scan api-таблиц на файл приемлем (дизайн это фиксирует).
- **Схема БД не меняется** — bump `CurrentSchemaVersion` не нужен; откат удалением правила из каталога совместим.
- **Трассировка**: задачи 2.3 покрывают все 7 сценариев дельты; канареечная ручная проверка 4.3 хорошо поставлена.

## Findings

### F1 [High] — Ложные срабатывания: эмпирика опровергает оценку Risks; фильтров D1+D6 недостаточно

Прямой замер по запроектированному фильтру (DML-контексты, без `#`, источники создания = UNION из D2) на локальном индексе, продукт fa-contracts:

- **160 различных имён** таблиц в DML-контексте без источника создания;
- **662 из 35 691 SQL-файлов (1.9%)** получат ≥1 severity-1 finding;
- из 160 имён реальными дефектами являются единицы (`papi_account_massclose` и соседи по ContractMassLoad); остальное — систематический шум, НЕ зависящий от полноты индекса (т.е. не исчезнет на рабочем ПК):

| Класс шума | Примеры из замера | Покрывается фильтрами D1/D6? |
|---|---|---|
| Курсоры в DML-контексте (`where current of X`, fetch-паттерны) | `dateselectcursor` (**context='update'**, line 164), `arprebuildcursor`, `optemplate_fetch`, `cur_contract` | **Нет** — design D1 утверждает «имена курсоров попадают в unknown», эмпирически опровергнуто |
| Триггерные pseudo-таблицы | `inserted` (select x18), `deleted` (select x12) | Нет — их нет в `sqlKeywordsMap` |
| Системные каталоги | `sysobjects` (select x41), `syscolumns` (x30), `sysindexes`, `systypes`, `sysconstraints`, `master..` | Нет |
| Macro-placeholder-токены генераторов | `__table__`, `__api_table_in__`, `_tablename_`, `t##__table__##` | Нет |
| Префикс БД из cross-db ссылок (`from nt72db.dbo.tX`) | `nt72db`, `testdb12`, `testdb4` | Нет |
| Алиасы/derived-table имена | `contract`, `documents`, `items`, `master`, `balance`, `operations` | Нет |
| Прочий парсерный шум в DML-контексте | `a`, `d`, `i`, `s`, `open`, `fetch`, `goto`, `drop_create`, `isnull`, `raiserror` | Частично (`open`/`fetch`/`goto` нет в sqlKeywordsMap) |

Правило severity 1 (deploy stopper) с ~2% поражённых файлов и шумом на файле в среднем 1–3 finding'а будет систематически деградировать доверие к review.

**Требуемые исправления до реализации** (расширить D6, deltas/tasks синхронно):

1. Исключать имена, объявленные в анализируемом файле как курсоры — review-парсер уже извлекает декларации курсоров (`parseCursorDeclarations`, review_parser.go), механика бесплатна. Это закрывает `where current of`/`fetch from` для собственных курсоров файла.
2. Стоп-список: `inserted`, `deleted`, `master`, `sys*` (sysobjects/syscolumns/sysindexes/systypes/sysconstraints/systypes-подобные), `information_schema` — дополнение к `sqlKeywordsMap` (в фильтре правила, сам map расширять не обязательно).
3. Regexp-фильтр placeholder-токенов: `^_+.*_+$` / содержащие `##` / начинающиеся с `__`.
4. **Перепрогон пилотного замера с полным набором фильтров** до включения правила по умолчанию (задача в tasks.md, раздел Verification): целевой ориентир — «affected files» сходится к файлам с реальными дефектами; результат зафиксировать в design (Risks → Measurements).

Альтернатива, если шум подавить не удастся: осознанно сменить severity или default-off — но это противоречит proposal, поэтому сначала п.1–4.

### F2 [Med] — Сценарий «Курсор не считается таблицей» не защищает реальный паттерн

Сценарий 7 дельты оговаривает «объявляет и открывает курсор, но не использует его как таблицу в DML» — declare/open действительно не дают DML-строк. Но в реальном коде курсоры **читают** (`fetch next from X into ...` → context='select' через `tableNameRe` after FROM) и **обновляют** (`where current of X` → context='update', подтверждено датасетом: `dateselectcursor` с context='update'). Спека в текущем виде пройдёт тест, но не защитит FETCH/CURRENT OF. Вместе с F1.1: добавить в дельту сценарий «FETCH FROM курсора / WHERE CURRENT OF курсора не даёт finding» и покрыть его в tasks 2.3.

### F3 [Med] — SQL D2: фильтр `= ANY($1)` вне CTE — полагается на pushdown планировщика

`WHERE name = ANY($1)` стоит снаружи `creators`; проталкивание предиката в ветви UNION — на усмотрение планировщика. По прецедентам кодовой базы фильтр кладут **внутрь каждой ветви** (`BatchLookupTableProductIDs`, `findAPITableNames` в review_lookup.go) — тогда гарантируется использование `idx_sql_tables_table_name_lower` / `idx_sql_column_definitions_lower_table_col`. Рекомендуемая форма: `UNION ALL` (дедуп не нужен — map в Go сам схлопывает) с `WHERE LOWER(table_name) = ANY($1)` в каждой ветви; `SELECT DISTINCT` убрать. Дополнительно: включить в UNION ветвь `sql_tables.context='select_into'` (значение уже используется существующими lookup'ами; сейчас не пишется парсером, но это бесплатно и устойчиво к будущим изменениям индексации).

### F4 [Med] — Конкурентность `tableCreatorCache`: ленивый fallback не должен писать в общий map

Правила исполняются параллельно (`runRuleTasks`, до NumCPU воркеров). Превенторы (proc/tableProductID-кэши) безопасны потому, что заполняются до старта правил, а fallback при miss **не пишет** в кэш. Если реализация «одного ленивого batch-запроса» станет писать результат в `tableCreatorCache` из горутины правила — data race. Зафиксировать в D4: (а) prewarm заполняет кэш **до** правил, включая негативные записи (`false` для запрошенных и не найденных — тогда ленивый путь в проде не срабатывает вовсе); (б) ленивый fallback результат в общий map не пишет (паттерн `cachedLookupTableProductIDs`), либо мьютекс + флаг загруженности (паттерн `isMemoryTable`).

### F5 [Low] — D3 не покрывает same-file `SELECT ... INTO` при устаревшем индексе

Same-file CREATE TABLE покрыт через `context='create'` из parsed, а same-file SELECT INTO — только через БД (`definition_kind='select_into'` от предыдущей индексации). Дёшево закрыть: добавлять в множество созданных имена из `parsed.ColumnDefinitions` с `DefinitionKind='select_into'`. Design честно принимает остаточный риск — улучшение, не блокер. Замечание к механизму в целом: `select * into tX` (без проекции) не даёт column-определений → в индексе такой tX не найдётся вовсе; редкий в FA стиль, отметить в Risks.

### F6 [Low] — Рассинхрон спеки после харденинга F1 и устаревший счётчик в Purpose

Тело требования фиксирует фильтры (#/unknown), но не будущие стоп-листы (inserted/deleted/sys*/курсоры файла) — после принятия F1 обновить формулировку MUST-части и/или сценарии, иначе спека и имплементация расходятся. Отдельно: `## Purpose` main-спеки `review/deploy-stoppers` декларирует «25 правил», а требований там уже 26 (станет 27) — счётчик устарел ещё до этого change; обновить при архивации (или осознанно оставить, зафиксировав решение).

### F7 [Low] — tasks 4.2 не запускает integration-тесты

Store-тесты по конвенции живут за build-тегом `//go:build integration` (db_lookup_*_integration_test.go). `go test ./internal/review ./internal/store ./cmd` из 4.2 их не прогонит. В задаче 1.2 (или 4.2) явно указать `go test -tags integration ./internal/store -run TestBatchLookupTableCreators` и прогон integration review-тестов, если они появятся.

### F8 [Low] — Документационная неточность D1 про `dfm_embedded`/`pas_embedded`

В review `parsed.Tables` порождается только SQL-парсером, который эти контексты не эмитирует (они существуют лишь в индексе от DFM/PAS-парсеров). Фраза «dfm_embedded, pas_embedded игнорируются» относится не к фильтру правила, а к признакам индекса — переформулировать, чтобы не вводить в заблуждение при реализации.

### F9 [Info] — Верифицированные факты (для протокола)

- Автопропагация правила в CLI/MCP через `RuleListString()` — проверено (registry.go:373, cmd/review.go:245); новых флагов не требуется, заявление Impact корректно.
- `#`/`##`: `strings.HasPrefix(name, "#")` покрывает оба — ок; ключ кэша `normalizeIdentifier` достаточен (regex-имена парсера не содержат `.`/`[]`).
- Оценка шума воспроизводима запросом из приложения (DML-context set minus creators UNION) — переиспользовать в задаче-пилоте из F1.4.
- Контрольный файл для задачи 4.3: на `ContractMassLoad.sql` ожидать не только `pAPI_Account_MassClose`, но и `pAPI_Acc_MassCloseNTF`, `pAPI_Acc_MassInsertNTF`, `pAPI_Acc_MassInsProtocol` (кандидаты: возможно реальные необъявленные таблицы) и шумовой `contract` (алиас — уйдёт после F1); «контрольный файл без findings» собирать из таблиц с подтверждённым create-рядом (например `pCons_ObjectID`), не из `tConsRestructingDetail` (в доступных индексах create-ряд для него не подтверждён — DISTINCT ON в query table скрывает контексты, прямой SQL-замер по локальной БД строк не показал).

## Резюме

| # | Суть | Приоритет | Действие |
|---|---|---|---|
| F1 | Шум severity-1: 160 имён / 1.9% файлов; классы нефильтруемого шума | High | Расширить D6 (курсоры файла, стоп-список, placeholder-фильтр) + перепрогон пилота как задача |
| F2 | Сценарий курсора не покрывает FETCH/CURRENT OF | Med | Дополнить дельту сценарием + тест |
| F3 | `ANY($1)` вне CTE; UNION→UNION ALL | Med | Переписать SQL D2 |
| F4 | Гонка при ленивой записи в общий кэш | Med | Зафиксировать модель в D4 |
| F5 | Same-file SELECT INTO при stale index | Low | Расширить D3 на parsed.ColumnDefinitions |
| F6 | Синк формулировки требования; «25 правил» в Purpose | Low | Обновить дельту/Purpose |
| F7 | `-tags integration` в задачах | Low | Поправить tasks 4.2/1.2 |
| F8 | Неточность про dfm_embedded в D1 | Low | Переформулировать |

Change содержательно верный и хорошо подготовленный; после внесения F1–F4 в proposal/design/tasks/delta (F5–F8 по желанию) готов к реализации без повторного ревью — достаточно точечного чек-листа выше.
