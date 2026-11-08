# Change review: memory-tables-flag

Дата ревью: 2026-11-08. Ревью выполнено без правок кода (только этот файл).

**Проверено:** `proposal.md`, `design.md`, `tasks.md`, 5 дельт `specs/` (7 требований),
`.openspec.yaml`; сверка с кодом `internal/indexer/runner.go`, `indexer.go`,
`source_registry.go`, `source_git.go`, `internal/fswalk/fswalk.go`,
`internal/config/config.go`, `internal/model/model.go`,
`internal/store/db_schema.go`, `db_reset.go`, `db_reset_integration_test.go`,
`db_stats.go`, `internal/query/query.go`, `query_sql.go`, `api_query.go`,
`internal/mcp/registry.go`, `internal/branchkey/branchkey.go`,
`internal/parser/dsxml`; сверка дельт с основными спеками (названия MODIFIED
совпадают, полный текст воспроизведён, прежние сценарии сохранены;
`indexing/lst-parsing` в основных спеках отсутствует — новая capability
заявлена правомерно). `openspec validate memory-tables-flag --strict` — valid;
`openspec validate --all --strict` — 36 passed, 0 failed.

**Живые проверки (без изменения кода/БД):**
- В дереве `D:/GITHUB/GolandProjects/FA` ровно 6 `.lst`-файлов: 1
  `ModuleTableInMemory.lst` и 5 `ld_AfterApiReg.lst` / `ld_BeforeApiReg.lst`.
  Формат `ld_*.lst` — списки путей (без `;`), то есть диспетчеризация по
  каноническому basename действительно обязательна.
- Файл `FA/fa-dbmaintenance-ext-tbl-in-memory/DBMaintenance_Ext/Server/ModuleTableInMemory.lst`:
  36 строк, 5 секций, 26 строк таблиц (`pAPI_AttrValue;API_Administrator` в
  строке 2, `pRuleDoc_QtyStack;GAAP` в строке 26).
- Каталог `fa-dbmaintenance-ext-tbl-in-memory` — checkout того же репозитория,
  что и `fa-dbmaintenance-ext`: remote `http://fgit.diasoft.ru/build/fa-dbmaintenance-ext.git`,
  ветка `prj/TblInMemory` (не `develop`); в `fa-dbmaintenance-ext` (develop)
  этого `.lst` нет.
- В активном индексе есть обе таблицы из сценариев: `pRuleDoc_QtyStack`
  (1 строка, `fa-gaap`/`develop`, 23 usages) и `pAPI_AttrValue`
  (8 строк `query table`, 24 строки `query api-table`, включая
  `fa-administrator`/business_object). Сценарии спеки опираются на реальные данные.

## Замечания

1. **[High] `*.lst` не попадёт в обход: не обновлён `GetSupportedExtensions`.**
   - `walkerPatterns` не использует `include_patterns` напрямую: он фильтрует
     конфиг по белому списку `fswalk.GetSupportedExtensions()`
     (`internal/indexer/runner.go:18-61`, отбрасывание неизвестного расширения —
     `runner.go:44-45`).
   - В `GetSupportedExtensions` `.lst` отсутствует
     (`internal/fswalk/fswalk.go:459-462`).
   - Следствие: даже после добавления `*.lst` в `codebase.toml` и в дефолты
     `config.go` паттерн молча выкинется, `Walker` не обойдёт файл, кейс `LST`
     в `processFileInTx` (`indexer.go:1686-1764`) никогда не сработает, а
     сценарий `indexing/file-walking` «`.lst`-файлы попадают в обход» будет
     ложным. `tasks.md:4` (1.2) про `GetSupportedExtensions` не упоминает.
   - Рекомендация: добавить `".lst"` в `GetSupportedExtensions` и в
     `TestGetSupportedExtensions` (`fswalk_test.go:111-116`); добавить тест
     `walkerPatterns` с `IncludePatterns: []string{"*.lst"}` (сейчас
     `indexer_helpers_test.go:140-147` проверяет только `config == nil`, поэтому
     пропуск конфиг-паттерна не ловится). В `tasks.md` перенести verify из
     «fswalk-тест» в «indexer/config-тест»: сам `fswalk` дефолты не применяет.
   - Там же: второй дефолтный список `config.CreateDefault`
     (`config.go:526-556`, паттерны — `:550-552`) тоже требует `*.lst`;
     `tasks.md` говорит «добавить в дефолтный include_patterns» в единственном
     числе, а списков два (`NormalizeConfig` — `config.go:399-403` и
     `CreateDefault` — `:550-552`).

2. **[High] `memory_tables` не внесена в `codebaseResetTables` — гарантированно
   падает сторож-тест схемы.**
   - `ResetCodebaseTables` усекает фиксированный список
     (`internal/store/db_reset.go:16-53`); список зеркалируется в тесте
     (`db_reset_integration_test.go:17-37`).
   - `TestResetCodebaseTables_GuardSchemaCoverage`
     (`db_reset_integration_test.go:160-199`) сверяет все таблицы `pg_tables`
     с объединением `resetTables+keptTables` и падает с сообщением «новая
     таблица схемы требует решения: усечение или сохранение», как только в
     схеме появится `memory_tables`. `TestResetCodebaseTables_ClearsCodebaseKeepsAnalyzers`
     дополнительно проверяет `n == len(resetTables)` (`:120-122`).
   - Рекомендация: добавить `memory_tables` в `codebaseResetTables` и в
     тестовый `resetTables`; при желании засеять одну строку в
     `seedResetFixture`. Дополнительно обновить `requiredTables`
     (`schema_integration_test.go:35-44`) — сейчас список неполный, но это
     хорошая точка фиксации. В `tasks.md` 1.1 и Impact добавить `db_reset.go`.

3. **[Medium] Единственный фактический `.lst` лежит в non-default ветке: «приоритет
   default-ветки» нельзя реализовать фильтром.**
   - `fa-dbmaintenance-ext-tbl-in-memory` — checkout `fa-dbmaintenance-ext`
     на ветке `prj/TblInMemory`, `ds_products_branch.is_default = false`
     (default — `develop`). В develop-копии файла нет.
   - `design.md:65-74` (D3) и `Risks` требуют «детерминированно первую по
     приоритету default-ветки». Если реализация отфильтрует
     `br.is_default = TRUE` (или будет искать только default-копию), признак
     будет отсутствовать на текущем дереве — единственная копия non-default.
     Сортировка `is_default DESC, file_id` без фильтра работает; фильтр — нет.
   - Рекомендация: зафиксировать в design/tasks точную SQL-форму выбора
     (`LEFT JOIN LATERAL ... ORDER BY COALESCE(br.is_default, FALSE) DESC,
     mt.id LIMIT 1`) и тест с двумя копиями: default + non-default (проверить,
     что берётся default при наличии и что non-default не теряется при
     отсутствии default). В `tasks.md` 3.1/3.2 сейчас только «детерминированный
     выбор», без формы.

4. **[Medium] D4 переоценивает «сопоставимость с продуктами кодовой базы»: секции
   `.lst` не всегда равны именам продуктов индекса.**
   - `branchkey.NormalizeProduct` — механическая нормализация
     (`internal/branchkey/branchkey.go:53-83`), а не сопоставление с каталогом.
   - Фактические секции: `[FA-Administrator]` → `fa-administrator` (совпадает),
     `[FA-GAAP]` → `fa-gaap` (совпадает), `[FA-GeneralLedger]` →
     `fa-generalledger` (совпадает), но `[FA-Payment]` → `fa-payment`, тогда
     как индексный продукт — `fa-payments` (это видно по результатам `query
     table` для `pAPI_AttrValue` и по remote `fa-payments.git`);
     `[FA-ContractTransfers]` → `fa-contracttransfers`, тогда как в дереве
     есть `fa-contracts`, а в манифесте dsinfo — `FA-ContractTransfers-Ext`
     (→ `fa-contracttransfers-ext`).
   - На функциональность это не влияет: D3 join-ит только по имени таблицы, а
     product/module — справочные поля декларации. Но формулировки
     `design.md:76-83` («чтобы признак сопоставлялся с продуктами кодовой базы»,
     «гарантирует сопоставимость») и спеки
     (`specs/indexing/lst-parsing/spec.md:31-34`) вводят в заблуждение; будущее
     review-правило, сравнивающее `memory_tables.product_name` с
     `ds_products.product_name`, получит ложные расхождения.
   - Рекомендация: переформулировать D4 и спеку: «product_name — продукт из
     декларации, нормализованный для отображения; сопоставление признака —
     по имени таблицы, product для join не используется». Явный alias-маппинг
     (если нужен) — вне scope.

5. **[Medium] Контракт парсера и краевые случаи не зафиксированы; «lenient» может
   обернуться откатом всей транзакции файла.**
   - `processFileCtx` выполняет весь разбор файла в одной транзакции
     (`indexer.go:1674-1684`). Если парсер вернёт ошибку, транзакция
     откатится, и все записи `.lst` будут потеряны — это противоречит
     требованию «мусорная строка не прерывает разбор»
     (`specs/indexing/lst-parsing/spec.md:33,47-52`, `design.md:84-90`).
   - Не определены: строка `Таблица;Модуль` до первой секции (при
     `product_name TEXT NOT NULL` без default); пустая секция `[]`; больше
     одного `;`; пустая таблица/модуль; BOM/UTF-16; регистр basename.
   - Рекомендация: зафиксировать `ParseResult` как «records + non-fatal
     diagnostics» (например `MalformedLines []int`), в `processFileInTx`
     писать диагностику в `errorLogger`, инкрементировать `stats.Errors` и
     возвращать `nil`; строку вне секции считать malformed (не создавать
     запись с `product_name=''`); делить по первому `;`, поля Trim; basename
     сравнивать `strings.EqualFold`; при необходимости декодировать
     WIN1251/UTF-16 (сейчас `Design D1` предполагает ASCII). Добавить
     unit-тесты на каждый случай.

6. **[Medium] `ScanStats.LSTFiles` (D7) описан неполно: нет `mergeScanStats`, нет
   вывода, а привязка к `scan_runs`/`--summary` неверна.**
   - `mergeScanStats` (`indexer.go:146-197`) не знает про `LSTFiles`. Разбор
     идёт в `localStats` (`indexer.go:237-246`, `271-279`), поэтому без явного
     `dst.LSTFiles += src.LSTFiles` счётчик всегда будет 0.
   - `cmd/init.go:163-177` и `cmd/update.go:97-115` печатают per-language
     счётчики (SQL/PAS/…/XML/MD/YAML); без новой строки счётчик не виден
     пользователю.
   - `scan_runs` хранит только `files_scanned/files_indexed/errors/encoding_refined/xml_skipped`
     (`db_schema.go:30-47`, `db_scan_runs.go:51-61`); per-language счётчиков
     там нет. `--summary` — флаг query-команд (`cmd/query_shared.go`), а не
     индексатора. Утверждение `design.md:101-105` «наблюдаемость ... в
     `--summary` и scan_runs без отдельной миграции» внутренне
     противоречиво: либо счётчик только в `ScanStats` + печать, либо нужна
     колонка `scan_runs.lst_files` (это DDL) и вывод в `stats`.
   - Рекомендация: выбрать поверхность наблюдаемости и зафиксировать в design;
     минимум — `ScanStats.LSTFiles` + `mergeScanStats` + строка в
     `cmd/init.go`/`cmd/update.go` + тест `TestMergeScanStatsAndCollector`;
     определить семантику (все `.lst` или только канонический).

7. **[Medium] При нескольких `memory_tables`-записях на таблицу обычный LEFT JOIN
   размножит строки результата; нужен LATERAL/DISTINCT ON.**
   - `SearchTable` возвращает по одной строке на `chosen`
     (`query_sql.go:74-90`); `SearchAPITable` — `SELECT * FROM (union) dedup
     ... LIMIT $2` (`api_query.go:228-237`). Если добавить
     `LEFT JOIN memory_tables mt ON LOWER(mt.table_name) = LOWER(ch.table_name)`
     без ограничения, две копии `.lst` (например, default + non-default)
     дадут дубли строк и «съедят» лимит.
   - D3/Tasks обещают «детерминированно первую», но SQL-форма не описана.
     Рекомендация: `LEFT JOIN LATERAL (SELECT ... ORDER BY ... LIMIT 1) m ON ...`
     в обеих функциях; тест с двумя копиями и с дублем строки внутри файла.
     На текущем дереве копия одна, поэтому баг не проявится без такого теста.

8. **[Low] Списки дефолтов и документация обновляются не полностью.**
   - Кроме `config.go:399-403` и `codebase.toml:29-42`, есть
     `CreateDefault` (`config.go:550-552`), `codebase.docker.toml:22-24`,
     `README.md:108` (пример `include_patterns`), `README.md:17,149-153`
     (перечень поддерживаемых форматов и инструкция по добавлению `.t01`).
     Сейчас tasks/Impact называют только `config.go` и `codebase.toml`.
   - Рекомендация: обновить все перечисленные места (строки ASCII, файлы —
     UTF-8) или явно записать, что Docker/README вне scope.

9. **[Low] Мелочи реализации, которые стоит зафиксировать заранее.**
   - Basename сравнивается точно (`design.md:43-50`); на Windows разумнее
     `strings.EqualFold`, чтобы регистр имени файла не ломал разбор
     (каноническое имя всё равно фиксировано).
   - `file.Content` — сырые байты; при UTF-16/BOM (`DetectBOMOrUTF16` в
     `fswalk.go:301-303`) парсер должен декодировать/срезать BOM, иначе в
     `table_name`/`module_name` попадут NUL-байты. Для текущего ASCII-файла
     не критично, но `sanitizeUTF8String` в `BatchInsertMemoryTables`
     желателен по образцу `db_insert_retcode.go:74-76`.
   - `line_number` считать по исходным строкам (1-based) с учётом CRLF.

10. **[Info] Данные/фикстуры спеки.**
    - Сценарий «повторная индексация заменяет записи» использует
      `pAPI_STP_ParamSet;Administrator` (`specs/indexing/lst-parsing/spec.md:27-29`),
      которой в актуальном `.lst` нет (есть, например,
      `pAPI_Property_Value;API_Administrator`). Это допустимая гипотетическая
      фикстура, но лучше либо использовать реальную строку, либо явно
      оговорить «гипотетическая фикстура».
    - `proposal.md:9` называет `fa-dbmaintenance-ext-tbl-in-memory`
      репозиторием, тогда как это checkout репозитория `fa-dbmaintenance-ext`
      (ветка `prj/TblInMemory`). Для понимания источника и деплой-заметки
      полезно уточнить.
    - `source_registry.match` корректно различает `fa-dbmaintenance-ext` и
      `fa-dbmaintenance-ext-tbl-in-memory` по границе пути и выбирает самый
      длинный префикс (`source_registry.go:41-76`), поэтому коллизии
      продуктов/веток здесь нет — это проверено.

11. **[Info] `codebase stats` не покажет LST, если не расширять `store.Stats`.**
    - `computeStats` считает per-extension поля из `files`
      (`db_stats.go:98-130`), `cmd/stats.go:126-132,243-249` их печатает.
    - Если «наблюдаемость формата» нужна и в `stats`, добавить `LSTFiles` в
      `store.Stats`, `computeStats` и вывод; если достаточно summary
      `init/update` — ограничиться пунктом 6.

## Вывод

Направление верное и хорошо сверено с реальными данными: источник признака
найден и стабилен (26 деклараций, 5 секций), формат `ld_*.lst` действительно
другой (диспетчеризация по basename обоснована), обе таблицы из сценариев есть
в живом индексе, `pAPI_AttrValue` реально встречается в чужих продуктах, а
`branchkey.NormalizeProduct` даёт ожидаемые `fa-administrator`/`fa-gaap`.
Хранение в отдельной file-keyed таблице с `ON DELETE CASCADE` согласуется с
существующими паттернами (`ds_return_codes`), инкрементальность через общий
update-механизм работает, дельты структурно корректны и проходят `--strict`.

Блокирующих дефектов два: **(1)** без `GetSupportedExtensions` фича не работает
вообще — `*.lst` молча отбрасывается `walkerPatterns`; **(2)** без
`memory_tables` в `codebaseResetTables` падает сторож-тест схемы. Перед
`/opsx-apply` рекомендуется устранить п.1–2 и уточнить п.3–7 (ветка non-default,
переформулировка D4, контракт парсера, счётчик/наблюдаемость, LATERAL-выбор при
дублях). Пункты 8–11 — уточнения формулировок/тестов, не блокирующие
реализацию.
