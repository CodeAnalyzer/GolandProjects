# Design: Материализация owner_name в таблице symbols

## Context

Ключ дедупликации `SearchSymbol`/`SearchInContent` — `DISTINCT ON (name_key, symbol_type, owner_key, product_key)` (internal/query/query.go:466, :563) — включает `owner_key`, который сегодня вычисляется константой `symbolOwnerKeyExpr` (query.go:377-382): CASE с коррелированными подзапросами `pas_methods → pas_classes` (method) и `dfm_components → dfm_forms` (component), выполняемыми для каждой строки CTE `base` до дедупликации. Потребителей выражения три: `SearchSymbol` (query.go:423, через `symbolBaseBranch` — все ветви UNION ALL трёхполевого fallback report_form), `SearchInContent` (query.go:545) и тест-эталон `legacyReportFormLookup` (internal/query/query_symbol_types_integration_test.go:328) — третья точка обнаружена при ревизии предложения и учтена в задачах.

Замер bug-репорта B4: `--name DoAction --type method` → 4 278 строк × SubPlan (28 625 buffer hits), 13,2 с холодный / 259–408 мс прогретый. Отложить вычисление владельца после дедупа нельзя — `owner_key` часть ключа `DISTINCT ON` и JOIN-условия `chosen`; LATERAL-костыль отвергнут консенсусом bug-репорта.

Схемные паттерны, на которые опираемся: fast-path `InitSchemaCtx` по маркеру версии (db_schema.go:13, readonly-schema-policy — bump `CurrentSchemaVersion` обязателен), именованные миграции через `applyMigrationTx` (db_schema.go:1279), образец backfill `files_is_generated_backfill_v1` (db_schema.go:1249) и паттерн «ALTER после CREATE» (`scan_runs` v4-колонки, `files.is_generated`).

Заполнение владельца на индексации опирается на два единственных места создания таких символов (подтверждено grep): цикл после `FindPASMethodIDsByUnit` (internal/indexer/indexer_sql_pas.go:920-943, `method.ClassName` в руках) и цикл после `FindDFMComponentIDsByForm` (internal/indexer/indexer.go:819-845, родительская форма всегда в том же файле — `formIDByName` строится от `FindDFMFormIDsByFile(fileID)`; компоненты с нерезолвнутой формой символов не создают, indexer.go:793-795).

## Goals / Non-Goals

**Goals:**

- Убрать per-row коррелированные подзапросы владельца из планов `SearchSymbol`/`SearchInContent` (включая все ветви report_form fallback и `SearchInContent`).
- Значение владельца фиксируется один раз на индексации; инкрементальный update консистентен без reconcile.
- Backfill существующих БД — однократный, семантически эквивалентный прежнему runtime-выражению для резолвнутых строк.

**Non-Goals:**

- B4-остатки, не связанные с владельцем: холодные пики symbol-поиска по не-индексным условиям, `SearchInContent` ILIKE-план (seq/trgm scan сам по себе), symbol LIKE 19.6 с — отдельное направление.
- Материализация владельцев для прочих типов символов (procedure→модуль и т.п.).
- Изменение CLI/MCP-контрактов: `SymbolResult`, JSON envelope, NDJSON не меняются.

## Decisions

### D1. Колонка `owner_name` в самой строке symbols (не computed, не lookup на лету)

Альтернативы отвергнуты: join к `pas_methods`/`pas_classes` в CTE `base` (возвращает per-row lookup, только меняет форму), LATERAL после LIMIT (невозможно — ключ дедупа), generated column (зависит от других таблиц — PG не умеет). Массивная денормализация одного текстового поля — ~1 байт на строку для большинства типов (пустая varlena), реальные значения только у method/component.

### D2. Backfill: сужение по типам + два type-scoped set-based UPDATE

Миграция `symbols_owner_name_backfill_v1` (по образцу `files_is_generated_backfill_v1`) — два UPDATE по одному типу каждый, без self-join `symbols` и без CASE:

```sql
UPDATE symbols s SET owner_name = COALESCE(pc.class_name, '')
FROM pas_methods pm LEFT JOIN pas_classes pc ON pm.class_id = pc.id
WHERE s.symbol_type = 'method' AND pm.id = s.entity_id AND s.owner_name = '';

UPDATE symbols s SET owner_name = COALESCE(df.form_class, '')
FROM dfm_components dc JOIN dfm_forms df ON dc.form_id = df.id
WHERE s.symbol_type = 'component' AND dc.id = s.entity_id AND s.owner_name = '';
```

Семантика против исходного предложения («ровно с текущим коррелированным выражением»): обе формы дают для резолвнутых строк то же значение (`COALESCE(NULL→'')` тот же), для нерезолвнутых — `''`, а строки с dangling `entity_id` (INNER FROM-join их пропускает) сохраняют DEFAULT `''` — ровно как прежний runtime (подзапрос без строк → `''`). План — по одному проходу UPDATE по method/component-строкам `symbols` с hash/merge-соединением к таблицам владельцев, без миллионов per-row SubPlan коррелированной формы. Сужение по типам семантически прозрачно: для прочих типов прежнее runtime-выражение возвращает `''`, что уже содержит DEFAULT — эти строки не переписываются вовсе (UPDATE не создаёт новые версии строк и WAL). Оценка: вместо полного прохода по ~5,8 млн строк — только method/component (~1–2 млн), вне критического пути последующих прогонов. Предикат `owner_name = ''` на штатном переходе v4→v5 — no-op (все строки с DEFAULT), но защищает при нештатном повторном применении (потеря/ручной сброс маркера миграции): backfill пишет **регистр декларации** (`pas_classes.class_name`), а индексатор — **регистр ссылки** (`method.ClassName`, D4.2), и перезапись затёрла бы уже материализованные корректные reference-case значения плюс лишний WAL. Отсюда же ожидаемое переходное расхождение: легаси-строка после миграции и переиндексированная строка той же ветки с неконсистентным написанием класса получают разные `owner_key` и временно не схлопнутся — следствие D4.2, рассасывается по мере переиндексации. Альтернатива «literal-коррелированная форма» и обобщённый self-join вариант с CASE отвергнуты: дольше и сложнее без выгоды в корректности.

### D3. Источник значения в индексаторе: как записано в коде; санитизация — в store

- **method**: индексатор кладёт `OwnerName = method.ClassName` — ровно та строка, что парсер вытащил из декларации метода, без преобразований. TRIM не применяется: `pas_classes.class_name` пишется verbatim (`sanitizeUTF8String(class.ClassName)`, db_insert_pas.go:125), TRIM внёс бы расхождение с backfill-значениями на данных с краевыми пробелами.
- **component**: индексатор кладёт `OwnerName = formClassByID[component.FormID]` — `form.FormClass` родительской формы. Значение доступно без дополнительного lookup: в `parseDFMFile` цикл построения `formIDByName` (indexer.go:777-786) параллельно заполняет карту `formClassByID[formID] → form.FormClass` из уже распарсенного `formsBatch`; цикл создания component-символов берёт значение из карты. В исходном предложении допускался «один lookup по образцу componentIDsCache» — ревизия показала, что он не нужен.
- **санитизация**: выполняется в `insertSymbolsBatch` (db_insert_j.go) вместе с остальными текстовыми полями символа (`sanitizeUTF8String(symbol.SymbolName)` и т.д., строки 147-152) — существующий паттерн; `sanitizeUTF8String` не экспортирована из `store`, вызов из индексатора не компилируется. Local `sanitizeUTF8Lookup` индексатора не эквивалентна (не удаляет `0x00`).
- **прочие типы**: не заполнять (DEFAULT `''`).

### D4. Осознанные семантические отличия от runtime-выражения (два, оба принимаются)

1. **Нерезолвнутые классы (method)**: сегодня `class_id = 0` → runtime возвращает `''` → одноимённые методы разных нерезолвнутых классов схлопываются в одну группу дедупа (скрытый over-collapse). После материализации владелец = имя класса как записано → группы корректные. **Ограничение backfill**: `pas_methods` хранит только `class_id`, имя класса-ссылки нигде не сохранено — миграция не может восстановить его и оставляет легаси-нерезолвнутым строкам `''`; инкрементальный update не переиндексирует неизменённые файлы (pre-filter mtime+size), поэтому такие строки дедуплицируются по пустому ключу до переиндексации их файлов (для кросс-веточных копий over-collapse сохраняется переходно). Новые и переиндексируемые строки получают имя сразу. Для component недостижимо: компонент без резолвнутой формы не создаёт символа вовсе. **Факт, обнаруженный при реализации**: индексатор вообще не создаёт символов для методов с классом, отсутствующим в юните (`FindPASMethodIDsByUnit` ключует по резолвнутому `pas_classes.class_name`, а поиск идёт по `method.ClassName` — class_id=0 даёт пустой ключ и промах) — пре-существующее поведение, изменение его не расширяет; поэтому «нерезолвнутые» строки в живых БД — это легаси-backfill/dangling, а новые `owner_name` у method-символов всегда именованные.
2. **Регистр ссылки vs регистр декларации (method, pending-резолв)**: pending-резолв матчит класс case-insensitive (`ToLower(Trim(...))`, indexer_postprocess_pas.go:110), после чего runtime читает `pas_classes.class_name` — регистр декларации класса; материализуем регистр ссылки в коде метода. При расхождении написаний (метод пишет `tmyclass`, класс объявлен `TMyClass`) группы дедупа сегодня и после различаются (сегодня схлопнутся, после — нет). На практике Diasoft-код пишет имена классов консистентно; отличие задокументировано в дельте symbol-search и принимается как цена самодостаточности строки (значение фиксируется на момент создания символа и не зависит от судьбы class_id: переиндексация файла с классом, удаление класса, повторный резолв).

Эти же отличия — единственные допустимые расхождения в верификационном сравнении «до/после»; всё прочее обязано совпадать побитово.

### D5. Запросы и тест-эталон

- В `symbolBaseBranch` (SearchSymbol) и `SearchInContent`: `LOWER(s.symbol_name) AS name_key, COALESCE(s.owner_name, '') AS owner_key`; константа `symbolOwnerKeyExpr` удаляется. COALESCE защитный: колонка NOT NULL, но выражение устойчиво к будущим изменениям nullability.
- `legacyReportFormLookup` (query_symbol_types_integration_test.go:328) — единственный оставшийся потребитель — получает инлайн-копию прежнего SQL-выражения с комментарием-якорем «эталон до материализации owner_key». Роль теста (эквивалентность планов B3) сохраняется; для report_form-строк ветка владельца всегда `''`, поэтому эталон остаётся корректным.
- Клиентских контрактов (CLI --json, MCP tools) изменение не касается.

### D6. Схема и версионирование

- `CurrentSchemaVersion`: `codebase_schema_v4` → `codebase_schema_v5`.
- Колонка попадает и в `CREATE TABLE symbols` (свежие БД), и отдельным `ALTER TABLE ... ADD COLUMN IF NOT EXISTS owner_name TEXT NOT NULL DEFAULT ''` (существующие БД; идемпотентно на переходах версии).
- Порядок в транзакции `InitSchemaCtx`: DDL-список → `symbols_owner_name_backfill_v1` → маркер версии; сбой на любом шаге откатывает всё (идемпотентный повтор).
- `model.Symbol` получает поле `OwnerName`; `BatchInsertSymbols` включает колонку в `pq.CopyIn` (db_insert_j.go:138). Тестовые INSERT со явными списками колонок не затронуты (DEFAULT).

## Risks / Trade-offs

- [Тяжёлая одноразовая миграция на больших БД] → сужение по типам + set-based план (D2); транзакция атомарна, повтор при сбое идемпотентен; окно advisory-lock `codebase/schema-init` удлиняется на время UPDATE — конкурирующие `init`/`update` ждут, read-only команды не конкурируют (fast-path).
- [Read-only команды на непромигрированной БД падают «требуется обновление схемы»] → существующая readonly-schema-policy; диагностика уже ведёт к `codebase update`.
- [Регистровая дельта D4.2 меняет группировку на неконсистентно написанных классах] → документирована, эффект ограничен непатологическими данными; сравнение до/после на копии боевой БД зафиксирует реальные случаи.
- [Владелец фиксируется на момент создания символа; переименование класса без переиндексации файла метода не обновит owner_name] → runtime-выражение в этой ситуации тоже деградирует (dangling class_id → `''`, менее информативно); материализованное значение остаётся осмысленным. Полная переиндексация выравнивает.
- [Денормализация требует дисциплины при будущих источниках method/component-символов] → таких источников два и оба покрыты; новые символы прочих типов получают DEFAULT, деградации нет.

## Migration Plan

1. Деплой бинаря → первый `codebase update` на каждой БД применяет v5: ALTER + backfill + маркеры (одна транзакция).
2. Откат бинарем прежней версии совместим без отката схемы: маркеры в `schema_migrations` накапливаются (INSERT, без удаления), старый бинарь с `CurrentSchemaVersion = codebase_schema_v4` находит свой маркер v4 в `CheckSchemaVersion` и работает; колонку `owner_name` он не читает, а пишет только индексатор нового бинаря. Ручной откат (`ALTER TABLE symbols DROP COLUMN owner_name` + удаление маркеров) не предусмотрен автоматикой и не требуется.
3. Верификация на копии боевой БД: сравнение `query symbol` до/после по пачке имён (допустимые расхождения — только D4), пин backfill-семантики легаси-нерезолвнутых строк (`''` до переиндексации; см. задачу 4.1), `EXPLAIN (ANALYZE, BUFFERS)` без SubPlan, замер warm/cold `--name DoAction --type method`.
