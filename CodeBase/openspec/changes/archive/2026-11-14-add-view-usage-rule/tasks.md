# Tasks: add-view-usage-rule

## 1. Ленивый кэш имён представлений

- [x] 1.1 Добавить в `Runner` (internal/review/runner.go) поля `viewNameMu sync.Mutex`, `viewNameCache map[string]struct{}`, `viewNamesLoaded bool`; инициализировать в `NewRunner`, сбрасывать в `RunSQLFileCtx` рядом со сбросом memory-кэша. Проверить сборку `go build ./...`
- [x] 1.2 Реализовать `isViewTable(ctx, tableName)` в internal/review/review_lookup.go по образцу `isMemoryTable`: при первом обращении один запрос `SELECT DISTINCT (regexp_match(query_text, '(?i)create\s+(?:or\s+replace\s+)?view\s+([A-Za-z_#][A-Za-z0-9_#.]*)'))[1] FROM query_fragments WHERE component_type='sql_script' AND query_text ~* '(?i)create\s+(?:or\s+replace\s+)?view\s+'`; сырые имена читаются через `sql.NullString` (NULL → skip) и нормализуются в Go через `normalizeTableKey` (без нормализации в SQL); флаг загруженности ставить и при ошибке; nil-safe при `db == nil` (существующие тесты конструируют Runner без БД). Unit-тест без БД: кэш пуст → false, повторный вызов не штормит (флаг установлен)

## 2. Правило viewUsageInServerCode

- [x] 2.1 Добавить `RuleViewUsageInServerCode RuleID = "viewUsageInServerCode"` в internal/review/types.go и зарегистрировать в `ruleCatalog` (catalog.go) с `SeverityDeployStopper` и описанием «Использование представления (VIEW) в серверном коде». Проверить, что `DefaultEnabledRules()` включает правило (unit-тест обхода каталога)
- [x] 2.2 Реализовать `checkViewUsageInServerCode` (review_rules.go): для `parsed.Tables` с контекстами select/insert/update/delete/log — `isViewTable(TableName)` → finding severity 1 с именем, файлом и строкой (Line из table ref); дедупликация `name:line` и `CurrentProductID` по образцу `checkTableExistsInMetadata`. Переиспользовать фильтры исключений `tableExistsInMetadata` (#-таблицы, курсоры файла, pseudo-таблицы, placeholder, строковые литералы). Unit-тесты: SELECT из view → finding; UPDATE view → finding; обычная таблица → нет; #-таблица/курсор с именем view → нет
- [x] 2.3 Сценарий «Определение представления не является нарушением»: integration-тест на сниппете `create view Batch ... from tPropertyUsr, tUserResSel` — 0 findings `viewUsageInServerCode`. Отдельно view-поверх-view: прогон на `fa-administrator/Administrator/Server/Core/VIEW.SQL` — ровно 1 finding (`tImportFileGroup`, тело `ImportFileGroup`, строка ~350) и на `view_trash.sql` — 1 finding (`tFund`); сегодня этим обращениям соответствует `tableExistsInMetadata` (замер: VIEW.SQL → tImportFileGroup line 350)

## 3. Подавление tableExistsInMetadata

- [x] 3.1 В `checkTableExistsInMetadata` перед формированием finding добавить `isViewTable(name)` → skip. Unit-тест: имя-view без создателя-таблицы не даёт `tableExistsInMetadata`; integration-фикстура с фрагментом `create view X` в индексе — обращение к `X` даёт только `viewUsageInServerCode`
- [x] 3.2 Убедиться, что при ошибке БД (множество view не загрузилось) `tableExistsInMetadata` работает как раньше — fail-open нового правила не меняет поведение старого (unit-тест с db=nil / незагруженным кэшем)

## 4. Документация

- [x] 4.1 Добавить `viewUsageInServerCode` в ручной список правил README.md (секция «Deploy stoppers», строки ~1037-1067). Списки в CLI `--rules` (cmd/review.go:245) и MCP-описании `codebase_review_sql` (registry.go:373) генерируются из `review.RuleListString()` — проверить smoke-тестом, что правило появилось автоматически, правок кода не требуется

## 5. Версия и сборка

- [x] 5.1 Забампить `buildNumber` в cmd/root.go:29 с `"1694"` до `"1697"` и пересобрать `go build -o CodeBase.exe`; проверить `CodeBase.exe --version` — banner показывает build 1697, дальнейшие приёмочные запуски в терминале идут по новой версии

## 6. Приёмочные замеры на индексе FA

- [x] 6.1 Прогнать review на известных нарушителях (срез 2026-11-14: 49 обращений в 30 файлах, из них `tfund` — 27 в 9): `fa-cards/Cards/SERVER/SCRIPT_UTIL/USR_GPB_CreditCards2.sql` (10+ findings `tFund`), `fa-payments/Payments/SERVER/Patch/patch_dml7_2_1846.sql` (`titermgroup`), `fa-cards/Cards/SERVER/SCRIPT_UTIL/SU_TW_CRD_GenOpTpForTran.sql` (`batch`) — findings `viewUsageInServerCode` severity 1, `tableExistsInMetadata` на эти имена отсутствует. Сверить итоговые counts с 49/30 (raw) с учётом методики из proposal
- [x] 6.2 Замерить влияние на время review одного файла (до/после): дельта ≤ 0.2 с на файл; зафиксировать в design.md фактический `EXPLAIN (ANALYZE, BUFFERS)` запроса (baseline: Bitmap Index Scan по `idx_query_fragments_query_text_trgm`, 156 строк, 26.9 мс) для контроля деградации индекса
- [x] 6.3 Контрольный прогон на файле без view-обращений `D:\GITHUB\GolandProjects\FA\fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ContractMassLoad.sql` (файл в `Contracts/Server/Contract/` в индексе отсутствует — путь вымышленный): `success=true`, 113 findings всеми правилами, 0 `viewUsageInServerCode`, регресса других правил нет

## 7. Правки по code review (code-review-add-view-usage-rule.md)

- [x] 7.1 (C2) Пропуск локально созданных таблиц в `checkViewUsageInServerCode`: `created` (CREATE TABLE файла) + `select_into` из parsed.ColumnDefinitions; unit-тест «локальная таблица с именем view не даёт finding»
- [x] 7.2 (C1) Сузить тело требования спеки (префикс в обращении — при сохранении парсером; квалифицированные ссылки — FN) + пин-тест `from dbo.tfund` → 0 findings; ограничение зафиксировано в design Risks
- [x] 7.3 (C3) `regexp_matches(..., 'g')` в извлечении имён (2+ view в одном фрагменте) + assumption про комментарии в design D1 + integration-тест на двойное определение
- [x] 7.4 (C4) Проверка `rows.Err()` в `isViewTable`: частичный кэш отбрасывается, правило инертно; комментарий контракта в D1
- [x] 7.5 (C6) Integration-тест реальной ошибки запроса: закрытое соединение → кэш пуст + загружен, повторный вызов без шторма
- [x] 7.6 (C5) Методика приёмки 45/26 зафиксирована в design Migration Plan; `grant` в view_trash.sql описан как предсуществующий ложный finding (кандидат в отдельный change)
- [x] 7.7 (C7) Уточнение спеки: findings дедуплицируются по имени и строке
- [x] 7.8 Пересборка CodeBase.exe (build 1697) после правок, полный прогон unit + integration тестов и sweep 30 файлов (контроль неизменности 45/26)
