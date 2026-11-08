# Code review: memory-tables-flag

Дата ревью: 2026-11-08. Ревью выполнено без правок кода (только этот файл).
Ревьюится реализация в коммите `165125f` («парсинг ModuleTableInMemory.lst и
сохранение списка "таблиц в памяти"») относительно `proposal.md`, `design.md`,
`tasks.md` и пяти дельт `specs/`; pre-implementation review
(`change-review-memory-tables-flag.md`, п.1–11) учтён как чек-лист.

**Проверено:** `internal/parser/lst/lst.go` + `lst_test.go` (новый пакет),
`internal/indexer/indexer_lst.go` + `indexer_lst_integration_test.go` (новые),
`internal/indexer/indexer.go` (`processFileInTx` кейс `LST`, `mergeScanStats`),
`indexer_helpers_test.go`, `internal/model/model.go` (`MemoryTableRecord`,
`ScanStats.LSTFiles`), `internal/store/db_schema.go` (v7), `db_insert_memory_tables.go`,
`db_reset.go`, `db_reset_integration_test.go`, `schema_integration_test.go`,
`internal/query/query.go` (`TableResult`), `query_sql.go` (`SearchTable`),
`api_query.go` (`SearchAPITable` + `enrichInMemoryFlag`),
`query_table_in_memory_integration_test.go` (новый), `internal/fswalk/fswalk.go`
(+тест), `internal/config/config.go` (+тест), `cmd/init.go`, `cmd/update.go`,
`cmd/root.go`, `codebase.toml`, `codebase.docker.toml`, `README.md`, main-спеки
после синка (`openspec/specs/indexing/lst-parsing/…` и др.).

**Автоматическая проверка (выполнена):**
`go build ./...` — ok; `go vet ./internal/... ./cmd/...` — ok;
`gofmt -l` по изменённым файлам — чисто, кроме `cmd/root.go` (см. замечание 2);
unit-тесты `parser/lst`, `fswalk`, `config`, `model`, `store`, `query`, `indexer`,
`cmd` — PASS; интеграционные `-tags integration`:
`TestInitSchema_MemoryTablesV7`, `TestResetCodebaseTables_*`,
`TestInitSchema_IdempotentAndHasRequiredObjects` (store, 13.3s),
`TestLSTIndexing`, `TestLSTUpdateIncremental` (indexer),
`TestSearchTable_InMemoryFlag`, `TestSearchAPITable_InMemoryFlag` (query) — все
PASS; `openspec validate --all --strict` — 36 passed, 0 failed.

**Живые проверки (read-only, локальная БД `localhost:5435/codebase`, маркер v7):**
- `memory_tables` = 26 строк (совпадает с tasks 4.2 и числом деклараций в
  `ModuleTableInMemory.lst`); `files` c `language='LST'` = 6 (1 канонический +
  5 `ld_*.lst`); дублей по `LOWER(table_name)` нет; строк с пустым
  `product_name`/`table_name` нет.
- `codebase query table --name pRuleDoc_QtyStack --json` →
  `"in_memory": true, "in_memory_product": "fa-gaap", "in_memory_module": "GAAP"`;
  `codebase query api-table --name pAPI_AttrValue --json` → `"in_memory": true,
  "in_memory_product": "fa-administrator", "in_memory_module": "API_Administrator"`.
- MCP `registry.go` не объявляет output-схемы — новые поля `TableResult`/
  `APITableResult` сериализуются в ответы `codebase_query_table` /
  `codebase_query_api_table` автоматически, обратно совместимо (как обещано в
  proposal). Человекочитаемый режим CLI печатает структуры через `%+v`
  (`cmd/query_execution.go:92`) — признак виден и там.

## Выполнение pre-implementation review (change-review, п.1–11)

Все блокирующие и существенные замечания отработаны:

- **п.1 (white-list расширений)** — `.lst` в `GetSupportedExtensions`
  (`fswalk.go:465`), `lst`→`WIN1251/LST` (`fswalk.go:441-444`), тест
  `TestGetSupportedExtensions` и `TestWalkerPatterns_FiltersUnsupportedAndDedups`
  дополнены (паттерн `*.lst` больше не выбрасывается) — закрыто.
- **п.2 (reset/сторож)** — `memory_tables` в `codebaseResetTables`
  (`db_reset.go:41`), в тестовый `resetTables` (61 таблица) и в `requiredTables`
  (`schema_integration_test.go:38`); сторож-тесты зелёные — закрыто.
- **п.3+п.7 (non-default ветка, LATERAL без размножения)** — обе выборки
  используют `LEFT JOIN LATERAL … ORDER BY COALESCE(is_default, FALSE) DESC,
  mt.id LIMIT 1` — приоритет реализован СОРТИРОВКОЙ, не фильтром
  (`query_sql.go:89-102`, `api_query.go:318-328`); тест с двумя копиями
  (default+non-default) фиксирует отсутствие размножения строк и победу default
  (`query_table_in_memory_integration_test.go:143-156`) — закрыто (кроме
  оговорки в замечании 1 ниже).
- **п.4 (D4 справочное поле)** — `MemoryTableRecord` документирован как
  декларативный признак со справочными ProductName/ModuleName
  (`model.go:656-666`), join без product-scope — закрыто.
- **п.5 (контракт парсера)** — `lst.Result{Records, MalformedLines}`, парсер не
  возвращает ошибку; indexer пишет каждую строку в `errorLogger`, инкрементирует
  `stats.Errors`, транзакция файла фиксируется (`indexer_lst.go:32-35`);
  `EqualFold` для basename, BOM/`\r`-обрезка, деление по первой `;`, строка до
  секции → malformed, `[]` → malformed, пустой модуль допустим; unit-тесты на
  каждый случай (`lst_test.go`) — закрыто.
- **п.6 (наблюдаемость)** — `ScanStats.LSTFiles` (семантика «все .lst», как
  `SQLFiles`), `dst.LSTFiles += src.LSTFiles` в `mergeScanStats`
  (`indexer.go:168`), печать `LST files:` в `cmd/init.go:178` и
  `cmd/update.go:116`, тест merge — закрыто.
- **п.8 (дефолты/документация)** — оба списка `config.go` (`:401`, `:551`),
  `codebase.toml`, `codebase.docker.toml`, README (перечень форматов,
  `- Indexация файлов` и блок `.lst` с deploy-предупреждением) — закрыто.
- **п.9 (мелочи)** — `sanitizeUTF8String` в инсёрте
  (`db_insert_memory_tables.go:59-61`), `line_number` 1-based по исходным
  строкам — закрыто.
- **п.10–11 (Info)** — фикстуры используют реальные имена; `codebase stats`
  сознательно не расширялся (Non-Goal D7) — принято.

## Замечания

1. **[Medium] Сценарий «только non-default копия даёт признак» не покрыт
   тестом — фиксация пустого результата вместо признака.**
   - Дизайн (D3, Risks) и tasks 3.1 требуют тест: «ОДНА non-default копия даёт
     признак»; change-review п.3 просил тот же случай. В фикстуре
     `pOnlyInPrj` задекларирована только в non-default копии (`lstPrj`,
     `query_table_in_memory_integration_test.go:107`), но у неё нет вхождения
     ни в `sql_tables`, ни в `api_business_object_tables`.
   - Поэтому `SearchTable(ctx, "pOnlyInPrj", …)` закономерно возвращает 0 строк,
     и тест (`:168-177`) фиксирует только `len(res) == 0` — то есть проверяет,
     что LATERAL-обогащение НЕ создаёт фантомные строки (полезная проверка, но
     комментарий «единственная декларация в non-default ветке не теряется»
     вводит в заблуждение: теряться тут нечему — строки-носителя нет).
   - Сама реализация корректна: `ORDER BY COALESCE(mbr.is_default, FALSE) DESC`
     без `WHERE is_default = TRUE` возвращает lone non-default строку; но
     регрессия «кто-то заменит сортировку фильтром» этим тестом не ловится.
   - Рекомендация: добавить в фикстуру вхождение `pOnlyInPrj` в `sql_tables`
     (любой продукт) и ассерт `InMemory == true` с `InMemoryProduct ==
     "fa-contracttransfers"`; комментарий поправить на честную семантику
     (два отдельных сценария: «non-default не теряется» и «join не порождает
     фантомов»).

2. **[Low] BOM (U+FEFF) внесён в `cmd/root.go` и `README.md`.**
   - Диф коммита показывает `-package cmd` → `+<BOM>package cmd`
     (`cmd/root.go:1`) и аналогичное для заголовка `README.md:1`.
   - Компилятор BOM допускает (build/vet зелёные), но `gofmt -l` теперь
     флагает `cmd/root.go` (диф gofmt — ровно удаление BOM), то есть заявка
     tasks 4.1 «gofmt чист» выполняется только с оговоркой. Для README BOM
     безвреден, но это побочный артефакт редактора, а не содержание изменения.
   - Рекомендация: снять BOM с обоих файлов. Попутно: `gofmt -l` по репо
     флагает ещё ~14 файлов с тем же симптомом (например `cmd/review.go`,
     `internal/encoding/encoding.go`) — это pre-existing долг, не из этого
     change, но паттерн воспроизводится в каждом коммите; стоит почистить разом.

3. **[Low] Два разных механизма обогащения и англоязычный комментарий.**
   - `SearchTable` присоединяет признак inline-LATERAL в основном запросе
     (`query_sql.go:89-102`), `SearchAPITable` — пост-обработкой отдельным
     batch-запросом `enrichInMemoryFlag` (`api_query.go:295-365`). Оба подхода
     корректны (лимит не съедается, дедуп имён по точной строке с LOWER-сравнением
     на стороне SQL), но асимметрия не объяснена: `SearchTable` мог бы
     использовать тот же хелпер (один roundtrip вместо per-row подзапросов;
     на таблице из 26 строк это не измеримо — чисто консистентность).
   - Doc-комментарий `enrichInMemoryFlag` написан по-английски
     (`api_query.go:295-301`), тогда как конвенция репо — русские комментарии
     (AGENTS.md). Внутри уже есть русская цитата формулировки — довести до
     единого языка.
   - Рекомендация: не блокирует; при следующем касании — привести комментарий
     к русскому и рассмотреть переиспользование хелпера в `SearchTable`.

4. **[Low] Формат-шум в `db_schema.go`.**
   - Существующий DDL `ds_return_codes` де-индентирован (3 таба → 2,
     `db_schema.go:579-589`) без содержательной причины — лишний диф-шум в
     чужом блоке; новый DDL `memory_tables` (`:587-595`) сразу записан в
     нестандартном для файла стиле (колонки на 2 таба вместо 3, закрывающая
     скобка на 1 таб).
   - На исполнение SQL не влияет (строковые литералы), но ломает единообразие
     списка DDL. Рекомендация: выровнять оба блока к стилю файла при следующем
     касании (потребует bump версии схемы — только вместе с очередным DDL).

5. **[Low] В коммит смешаны несвязанные удаления LSA-артефактов.**
   - Помимо фичи коммит удаляет из отслеживания `desc_lsa_model.bin`,
     `desc_lsa_state.json`, `spec_lsa_model.bin`, `spec_lsa_state.json`
     (≈68 МБ бинарных артефактов прогона индексации). Функционально безвредно:
     sidecar-модель регенерируется при следующем `init`/`update`, в рабочей
     копии файлы остались как untracked. Но это отдельная работа по гигиене
     репо, а не часть memory-tables-flag — перемешивание затрудняет откат
     фичи отдельно от чистки.
   - Само включение обновлённого `CodeBase.exe` — практика репо (предыдущий
     коммит `1456ca7` тоже обновлял бинарник), отклонением не считается.
   - Рекомендация: на будущее — разделять такие чистки в отдельные коммиты.

6. **[Info] Семантика `stats.Errors` при lenient-разборе.**
   - Каждая мусорная строка инкрементирует `stats.Errors` (per-line,
     `indexer_lst.go:32-35`) и попадает в `scan_runs.errors` — файл при этом
     успешно проиндексирован. Это осознанный контракт D5, зафиксирован тестом
     (`TestLSTIndexing`: `Errors == 1` при успешной индексации); потребителям
     счётчика ошибок (алерты «errors > 0») стоит это учитывать.

7. **[Info] Краевые случаи парсера (поведение зафиксировано, не дефекты).**
   - `[a;b]` распознаётся как секция (внутренности нормализуются как имя
     продукта); `[X] текст` (текст после скобки) уходит в malformed как строка
     без `;`; строка-секция с числовыми токенами (`[FA-9-01]`) нормализуется
     до `fa` (механика `NormalizeProduct`). Реальный формат этого не содержит.
   - Файл декодируется по `files.encoding = WIN1251` (fswalk-приор): UTF-8
     кириллица в таблицах/модулях превратилась бы в mojibake. Реальный файл
     ASCII/CP1251 (проверено pre-review), интеграционная фикстура с UTF-8
     кириллицей проходит корректно только потому, что мусорная строка не
     содержит `;`. Приемлемо (D5 оговаривает), но при появлении UTF-8-варианта
     файла потребуется доработка детекции.

8. **[Info] Границы обогащения соблюдены.**
   - Признак добавлен только в `SearchTable`/`SearchAPITable`; `loadAPIContractTables`
     (таблицы внутри `query api-contract`), `SearchTableSchema`,
     `SearchSQLTableIndex` — не обогащаются, что соответствует дельтам
     (`query/entity-queries`, `query/api-relations-queries` — только про
     `query table`/`query api-table`). `codebase stats` без LST — Non-Goal D7.

## Вывод

Реализация качественная и заметно аккуратнее среднего: все 7 существенных и оба
блокирующих замечания pre-implementation review отработаны (три точки попадания
`.lst` в обход, сторож-тесты схемы, lenient-контракт без отката транзакции,
LATERAL-сортировка вместо фильтра, merge+печать счётчика, справочные поля D4).
Контракты: парсер — `Records + MalformedLines` без ошибок; indexer — per-file
транзакция с диагностированием в errorLogger; замена записей при
реиндексации/удалении работает через общий механизм update + CASCADE и
покрыта тестами (`TestLSTUpdateIncremental`). Инкрементальность, каскад,
reset/сторож и миграция v6→v7 проверены интеграционно — всё зелёное; живая БД
содержит ровно 26 корректных деклараций, CLI/MCP отдают признак.

Блокирующих дефектов нет. Перед следующим касанием кода рекомендуется
устранить п.1 (тест сценария «только non-default копия» — единственное
реальное расхождение между tasks/design и тестами) и п.2 (BOM); п.3–5 —
косметика на усмотрение.

## Резолюция (2026-11-08, после ревью)

- **п.1 [Medium] — ПРИМЕНЁН.** В фикстуру добавлено вхождение `pOnlyInPrj` в
  `sql_tables` (fa-gaap) и новая декларация `pMemoryOnly` (default-копия, без
  вхождения в sql_tables). Тест разведён на два честных сценария: lone
  non-default декларация даёт признак (`InMemory=true`,
  `fa-contracttransfers`/`CT` — регрессия «замена сортировки фильтром» теперь
  ловится) и декларация без вхождения в sql_tables не порождает фантомных
  строк (`pMemoryOnly` → 0 строк). Комментарии фикстуры/теста переписаны.
  `TestSearchTable_InMemoryFlag` + `TestSearchAPITable_InMemoryFlag` — PASS.
- **п.2 [Low] — ПРИМЕНЁН.** BOM снят с `cmd/root.go` и `README.md`;
  `gofmt -l` по `cmd/root.go` чист. Pre-existing BOM-долг (~14 файлов,
  `cmd/review.go`, `internal/encoding/encoding.go` и др.) осознанно отложен —
  чужие файлы, отдельная чистка отдельным коммитом.
- **п.3 [Low] — ПРИМЕНЁН частично.** Комментарий `enrichInMemoryFlag`
  переведён на русский (конвенция AGENTS.md). Унификация механизмов
  (`SearchTable` на batch-хелпере) отклонена: inline LATERAL — это не per-row
  roundtrip, а set-based подзапрос в том же statement (один roundtrip,
  LIMIT 1 — без размножения строк, колонки признака сканируются основной
  выборкой); batch-пост-обработка в `SearchAPITable` обусловлена структурой
  её основного запроса. На 26 строках memory_tables выигрыш неизмерим,
  переписывание зелёного пути — чистый churn. Вернуться при росте
  memory_tables или усложнении логики сопоставления.
- **п.4 [Low] — отложен** по рекомендации самого ревью: выравнивание DDL
  требует bump `CurrentSchemaVersion` — только вместе с очередным DDL.
- **п.5 [Low] — принято к сведению** (гигиена коммитов на будущее; код не
  меняется).
- **п.6–8 [Info] — принято к сведению**, действий не требует.

Верификация: `gofmt -l` чист, `go build ./...` ok, `go vet` ok; интеграционные
`TestSearchTable_InMemoryFlag`/`TestSearchAPITable_InMemoryFlag` PASS. Изменения
косметические + тестовые — production-поведение не меняется, rebuild exe и bump
build не требуются.
