# Code Review: add-table-exists-in-metadata

- **Дата**: 2026-11-10
- **Ревьюер**: opencode (L3/pro/fast)
- **Предмет ревью**: реализация change `add-table-exists-in-metadata` (build 1684)
- **Вердикт**: ✅ **ОДОБРЕНО. Реализация соответствует proposal/design/tasks и дельте; все проверки зелёные. Найдены только замечания Low/Info, блокирующих нет — change готов к архивации.**
- **Код не изменялся** — выполнены только чтение, сборка и прогон тестов.

## Что проверялось

1. Соответствие реализации артефактам change: `proposal.md`, `design.md` (D1–D6, Measurements, Migration), `tasks.md` (1.1–4.5), дельта `specs/review/deploy-stoppers/spec.md`.
2. Устранение находок предыдущего ревью `change-review-add-table-exists-in-metadata.md` (F1–F9).
3. Изменённый код:
   - `internal/review/types.go`, `catalog.go` — регистрация правила;
   - `internal/review/review_rules.go` — `checkTableExistsInMetadata` + хелперы (`stripQuotedSegments`, `onlyQuotedOccurrence`, `isTableExistsNoiseName`);
   - `internal/review/review_lookup.go` — `prewarmTableCreators`, `cachedLookupTableCreator`;
   - `internal/review/runner.go` — поле кэша, сброс, вызов prewarm;
   - `internal/store/db_lookup_sql.go` — `BatchLookupTableCreators`;
   - тесты: `table_exists_metadata_test.go`, `review_lookup_integration_test.go`, `db_lookup_sql_integration_test.go`, `runner_test.go`;
   - `cmd/root.go` — bump build 1681 → 1684;
   - `openspec/specs/review/deploy-stoppers/spec.md` — Purpose.
4. Корректность относительно инфраструктуры: контексты/номера строк `parsed.Tables` (SQL-парсер), нормализация имён, модель конкурентности правил, дефолтное включение правила, индексы БД для предикатов.

## Выполненные проверки (фактические результаты)

| Проверка | Результат |
|---|---|
| `go build ./...` | EXIT 0 |
| `gofmt -l internal/review internal/store` | пусто (форматирование чистое) |
| `go vet ./internal/review ./internal/store` | EXIT 0 |
| `go vet -tags integration ./internal/store ./internal/review` | EXIT 0 |
| `go test ./internal/review ./internal/store ./cmd` | все `ok` |
| `go test -tags integration ./internal/store -run TestBatchLookupTableCreators` | PASS (9.36 s) |
| `go test -tags integration ./internal/review -run TestPrewarmTableCreators` | PASS (3.55 s) |
| `go test ./internal/review -run 'TestCheckTableExistsInMetadata|...'` | 11/11 PASS |
| `openspec validate add-table-exists-in-metadata --strict` | valid |
| `openspec validate review/deploy-stoppers --strict` | valid |

## Соответствие change-артефактам

- **tasks.md**: все пункты 1.1–4.5 закрыты и подтверждены кодом/прогонами. `gofmt`, build-bump, integration-теги (`-tags integration`) — выполнены.
- **D1 (контексты)**: `tableExistsDMLContexts` = select/insert/update/delete/log; `unknown` игнорируется. Учтена эмпирическая поправка про `FETCH`/`CURRENT OF` (отдельный фильтр курсоров).
- **D2 (SQL)**: `BatchLookupTableCreators` — `UNION ALL` из 4 ветвей, `LOWER(table_name) = ANY($1)` **в каждой ветви**, без `DISTINCT`, дедуп Go-мапой, ветвь `sql_tables.context='select_into'` присутствует. `pq.Array(normalized)` с предварительным lower+trim. Соответствует прецеденту `BatchLookupTableProductIDs`.
- **D3 (same-file)**: учитываются `context='create'` и `parsed.ColumnDefinitions` с `DefinitionKind='select_into'`.
- **D4 (конкурентность)**: `tableCreatorCache` заполняется в `prewarmTableCreators` до `runRuleTasks`; пишутся негативные записи `false`; ленивый `cachedLookupTableCreator` **не пишет** в общий map — гонки нет (map только читается во время параллельных правил). Соответствует паттерну `cachedLookupTableProductIDs`.
- **D5 (имя/severity)**: `RuleTableExistsInMetadata = "tableExistsInMetadata"`, severity 1 (DeployStopper), описание «Таблица отсутствует в метаданных». `DefaultEnabledRules()` включает правило автоматически (обход каталога).
- **D6 (фильтры)**: реализованы `#`/`##`, курсоры файла (`parseCursorDeclarations`, ключи нормализованы), стоп-набор `inserted/deleted/master/information_schema/sys*/open/fetch/goto`, `sqlKeywordsMap`, `m_*`, placeholder (`^_.+_$`, `##`, `__`-префикс), строковые литералы (`onlyQuotedOccurrence`). Общий `sqlKeywordsMap` не расширялся.
- **Дельта**: тело MUST-требования и 10 сценариев соответствуют коду; добавленные сценарии (FETCH/CURRENT OF, inserted/deleted/sys*, placeholder, строковый литерал) покрыты тестами.
- **F1–F9 предыдущего ревью**: все учтены (курсоры — F1.1/F2, стоп-набор — F1.2, placeholder — F1.3, перепрогон замера — F1.4/4.5, UNION ALL/ветвь select_into — F3, no-write fallback — F4, same-file SELECT INTO — F5, синк дельты и Purpose — F6, `-tags integration` — F7, уточнение D1 — F8).
- **Measurements**: замер 4.5 (100 имён / 239 файлов, 0.67%) и согласованное решение «принять как есть» зафиксированы в design; правило оставлено severity 1 и включено по умолчанию — совпадает с принятым решением.

## Подтверждено положительно

- Регистрация правила в каталоге и автопропагация в CLI `--rules` и MCP `codebase_review_sql` через существующий механизм каталога; новых флагов нет.
- Схема БД индекса не меняется — bump `CurrentSchemaVersion` не требуется (новых DDL нет).
- Запрос использует существующие индексы `idx_sql_tables_table_name_lower`, `idx_sql_column_definitions_lower_table_col`; предикат протолкнут в каждую ветвь.
- Дубли в выборке безопасно схлопываются в Go-мапе; `LOWER` в SQL и нормализация в Go согласованы по регистру.
- Номера строк берутся из expanded-контента (`r.exec.content`), того же, на котором парсился файл, и позже ремапятся в оригинал через `SourceMap` — `onlyQuotedOccurrence` работает по правильной строке.
- `parseCursorDeclarations` хранит ключи через `normalizeIdentifier`, фильтр сравнивает нормализованное имя — регистронезависимость корректна.
- Тесты содержат полезные guard'ы (курсор в DML-контекстах select+update), интеграционный prewarm проверяет и позитивную, и негативную (`false`) запись.

## Замечания (не блокируют)

### C1 [Low] — prewarm выполняется всегда, даже если правило отключено через `--rules`

`r.prewarmTableCreators(ctx, parsed)` вызывается безусловно в `RunSQLFileCtx` (`internal/review/runner.go:161`) до `enabledRuleSet` (`:163`), а этот батч-запрос нужен только правилу `tableExistsInMetadata`. При запуске с `--rules <другое>` review всё равно потратит один лишний запрос к БД на файл. Также в набор prewarm-имён включён контекст `create`, который правилу не нужен для `cachedLookupTableCreator` (создатели того же файла уже в `created`). Влияние низкое (правила по умолчанию все включены), но можно гейтить prewarm по `ruleSet[RuleTableExistsInMetadata]` (потребует вычислить `ruleSet` до prewarm) и/или исключить `create` из набора.

### C2 [Low] — ошибка БД в ленивом пути трактуется как «таблица не создана»

В `cachedLookupTableCreator` (`internal/review/review_lookup.go:495`) при `err != nil` возвращается `false`, что даёт severity-1 finding. В проде prewarm заполняет все DML-имена, поэтому ленивый путь не срабатывает; но при сбое prewarm/DB транзиентная ошибка может дать ложный deploy-stopper. Это согласуется с паттерном `cachedLookupTableProductIDs`, и для stopper'а «лучше перестраховаться» — но поведение стоит зафиксировать в комментарии/design (ошибка lookup = «не найдено»).

### C3 [Low] — `onlyQuotedOccurrence` — приближённая эвристика (подстрока)

Проверка `!strings.Contains(strippedLowerLine, lowerName)` (`internal/review/review_rules.go:1645`) сравнивает имя как подстроку, не как отдельный токен, и не учитывает `--`-комментарии и `[]`-идентификаторы. Возможны редкие ложные «не skip» на строке, где имя входит в состав другого unquoted-токена, при этом реальное вхождение — только внутри литерала. Смещение ошибки безопасное (склонность сообщить, а не скрыть), но для устойчивости можно проверять токен-границы. Не блокер.

### C4 [Low] — широкие префиксные фильтры `sys`/`m_`

`isTableExistsNoiseName` (`:1657`) отбрасывает любое имя с префиксом `sys` и `m_`. Это соответствует D6 и покрывает наблюдаемый шум, но теоретически может скрыть реальные таблицы `system*`/`m_*` (редко в FA). Приемлемо; можно сузить до явного списка при появлении таких таблиц.

### C5 [Low] — находки не дедуплицируются по имени таблицы

Ключ дедупа — `name:line` (`review_rules.go:1683+`), поэтому одна и та же отсутствующая таблица, использованная на нескольких строках, даст несколько findings. Для deploy-stopper'а, возможно, достаточно одного finding на таблицу в файле (с перечнем строк). Не дефект относительно спеки (finding = имя+файл+строка), но влияет на «шумность» вывода. Наблюдение на будущее.

### C6 [Info] — остаточный шум соответствует принятому решению

Остаточные классы замера (`nt72db`/`testdb4`/`testdb12`, `isnull`, `truncate`, `drop`, `TARGET`, `SEED_GEN`, однобуквенные алиасы, имена колонок) не фильтруются — это и есть измеренные 0.67%. Решение «принять как есть» зафиксировано в design ## Measurements; расхождения «код ↔ решение» нет. Точечные фильтры — отдельным change.

### C7 [Info] — design недоговаривает остаточный пробел same-file SELECT INTO

Design/D3 описывает остаток как «`select * into` без списка проекции». По механизму `extractSelectIntoColumnNames`/`parseColumnDefinition` пробел шире: `created` пополняется только из `sql_column_definitions definition_kind='select_into'`, которые не создаются, когда из проекции не извлекается валидное имя колонки (подтверждающие кейсы парсера: `""`, `"select"`, `"*"`, `@var`-проекции). Стоит уточнить формулировку Risks/design, чтобы не создавать ложного впечатления, что закрыт весь SELECT INTO кроме `select * into`. На поведение правила это не влияет (риск осознанно принят).

### C8 [Info] — косметический дефект комментария

В шапке `stripQuotedSegments` (`review_rules.go:1622`) употреблён символ `”` вместо описания `''`: «экранированный апостроф ” внутри литерала…». Чистая косметика.

### C9 [Info] — посторонние изменения в рабочем дереве

- `internal/store/db.go` и `internal/store/ds_info_match.go` — изменения **только по whitespace** (проверено `git diff -w` = пусто; это gofmt-выравнивание). Семантики не меняют, но засоряют diff — при коммите желательно отделить/подтвердить, что это осознанный gofmt.
- `CodeBase.exe` изменён (пересборка) — бинарник в коммит обычно не включают.
- Untracked LSA-артефакты (`spec_lsa_*`, `desc_lsa_*`) и `Modifications/DiasoftSystemInfo.csv` — не включать в коммит change (см. рабочие соглашения CodeBase).
- `openspec/specs/review/deploy-stoppers/spec.md` (Purpose «25 правил» → «правила каталога») отредактирован **напрямую в main spec** до архивации. Правка корректна и валидна, но обычно main spec меняется при `openspec archive`; изменение только Purpose-формулировки — допустимое исключение, зафиксировать осознанность.

### C10 [Info] — устойчивость тестов

- `TestCheckTableExistsInMetadata_StringLiteralNoise_Skipped` не содержит guard'а, что парсер реально породил псевдо-таблицу `contract` из `PROFILE_TIME_EX('update contract')`. Тест проверяет только «нет finding по contract», поэтому при изменении парсера может стать вакуумным. По аналогии с курсорным тестом полезно добавить assert, что `contract` присутствует в `parsed.Tables` в DML-контексте.
- Аналогично `insert pTmpTbl (...) select ...` одной строкой: известное ограничение парсера (однострочный `insert … select` может не извлекаться) делает соответствующую часть `CreatedTables_NoFinding` потенциально вакуумной (на результат не влияет, т.к. имя есть в кэше).

## Резюме

| # | Суть | Приоритет | Действие |
|---|---|---|---|
| C1 | prewarm безусловен (лишний запрос при `--rules`, лишний `create`) | Low | Опционально гейтить по ruleSet / исключить `create` |
| C2 | ошибка БД в lazy-пути = «не создана» | Low | Зафиксировать поведение в комментарии/design |
| C3 | `onlyQuotedOccurrence` — подстрочная эвристика | Low | Опционально проверка токен-границ |
| C4 | широкие префиксы `sys`/`m_` | Low | Опционально сузить до списка |
| C5 | несколько findings на одну таблицу по строкам | Low | Опционально дедуп по имени |
| C6 | остаточный шум = принятые 0.67% | Info | Отдельный change при необходимости |
| C7 | design занижает остаток SELECT INTO | Info | Уточнить формулировку Risks |
| C8 | косметика `”` в комментарии | Info | Поправить при случае |
| C9 | сопутствующие whitespace/бинарник/артефакты; правка main spec | Info | Не включать артефакты; учесть при коммите |
| C10 | вакуумные участки тестов | Info | Добавить guard'ы |

**Итог:** реализация полная, корректная и соответствует change-артефактам; все обязательные и интеграционные тесты, `go vet`/`gofmt` и `openspec validate --strict` проходят. Замечания носят характер улучшений/наблюдений и не препятствуют архивации change.

## Resolution (2026-11-10, разбор автором реализации)

Вердикт: C1–C3, C7, C8, C10 — **приняты и исправлены**; C4, C5 — **отклонены с обоснованием**; C6, C9 — **подтверждения без правок**.

### Исправлено

- **C1 [Low]** — `ruleSet` теперь вычисляется до prewarm'ов в `RunSQLFileCtx`; `prewarmTableCreators` вызывается только при `ruleSet[RuleTableExistsInMetadata]` (при отключении правила через `--rules` batch-запрос не выполняется). `create`-контекст исключён из набора prewarm-имён: same-file создатели учитываются в правиле напрямую из `parsed`, кэш для них не нужен.
- **C2 [Low]** — поведение зафиксировано в комментарии `cachedLookupTableCreator` и в design D4: ошибка БД в ленивом пути = fail-closed «не найдено» (транзиентный сбой даёт ложный finding, а не пропуск дефекта severity 1); в проде путь не срабатывает благодаря негативному prewarm.
- **C3 [Low]** — `onlyQuotedOccurrence` переведён с подстрочной проверки на word-boundary: добавлены `containsWord`/`isSQLWordChar` (word-символы — `[a-zA-Z0-9_#]`; `"contract"` не матчится в `"subcontracting"`, но матчится в `"[contract]"`). Тест `TestContainsWord_Boundaries`.
- **C7 [Info]** — формулировки D3/Risks уточнены: остаточный пробел same-file SELECT INTO шире, чем «`select * into` без списка проекции», — `created` не пополняется при любой проекции, из которой парсер не извлекает имена колонок (`*`, `@`-переменные без алиаса-имени, пустые псевдонимы).
- **C8 [Info]** — символ `”` в комментарии `stripQuotedSegments` заменён на `''`.
- **C10 [Info]** — добавлены guard'ы: `CreatedTables_NoFinding` теперь ассертит, что все четыре имени реально извлечены парсером в DML-контексте; `StringLiteralNoise_Skipped` — что `contract` действительно порождён из строкового литерала. Guard'ы сразу окупились: вскрыли хрупкость фикстур (см. ниже) — обе переписаны под гарантированно извлекаемые формы. Однострочный `insert … select` заменён на двухстрочную форму первым statement.

Дополнительно при работе guard'ов установлены факты о SQL-парсере (важно для будущих фикстур): `insert X (...) select …` извлекается (context=insert) только первым statement после `as` — после предшествующих select'ов не извлекается вовсе; `PROFILE_TIME_EX('update contract')` порождает pseudo-таблицу (context=select) только если строка идёт после select-блока.

### Отклонено с обоснованием

- **C4 [Low] (широкие префиксы `sys`/`m_`)** — префикс `sys` выбран осознанно: он покрывает весь класс системных каталогов SQL Server (sysobjects, syscolumns, sysindexes, systypes, sysconstraints, sysdatabases, … — десятки имён), явный список устареет при первом же новом sys-объекте. Реальных пользовательских таблиц с префиксами `sys*`/`m_*` в FA не существует — замер 4.5 (100 остаточных имён) не показал ни одной. Сужение до списка — при появлении таких таблиц (отдельным изменением).
- **C5 [Low] (дедуп findings по имени таблицы)** — дельта-спека требует «Finding MUST указывать имя таблицы, файл и строку обращения», т.е. finding привязан к конкретной строке; поведение паритетно существующему `execNotExistsProc` (finding на каждый вызов). Изменение семантики вывода — отдельный change при эксплуатационной необходимости.

### Подтверждено без правок

- **C6 [Info]** — остаточный шум = измеренные и принятые 0.67% (решение зафиксировано в design ## Measurements).
- **C9 [Info]** — осознанность зафиксирована: gofmt-выравнивание `db.go`/`ds_info_match.go` — требование задачи 4.1 («gofmt -l — пустой вывод»); `CodeBase.exe`, LSA-артефакты (`spec_lsa_*`, `desc_lsa_*`), `Modifications/DiasoftSystemInfo.csv` — в коммит change не включать; правка Purpose main spec до архивации — осознанное исключение (замена хрупкого числового счётчика).

### Верификация после правок

`gofmt -l` пусто; `go build ./...`, `go vet` ok; `go test ./internal/review ./internal/store ./cmd` PASS; integration `TestBatchLookupTableCreators`, `TestPrewarmTableCreators` PASS; `openspec validate add-table-exists-in-metadata --strict` valid; `openspec validate --all --strict` 37/37. `CodeBase.exe` пересобран без бампа build (1684 — ревью-фиксы не бампают по прецеденту).
