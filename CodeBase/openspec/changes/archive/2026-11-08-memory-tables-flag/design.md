## Context

Источником признака служит deployment-extension «таблицы в памяти»: каталог
`fa-dbmaintenance-ext-tbl-in-memory` — checkout репозитория `fa-dbmaintenance-ext`
на ветке `prj/TblInMemory` (в develop-копии `.lst` нет); ds_products_branch такой
копии имеет `is_default = false` — единственная фактическая декларация лежит в
non-default линии. Содержательный файл один:
`DBMaintenance_Ext/Server/ModuleTableInMemory.lst` (36 строк, 5 секций, 26
деклараций). Формат: секции `[<Продукт>]` + строки `<Таблица>;<Модуль>`;
документация — `CodeBase/Modifications/Extension по таблицам в памяти.md`.
В FA есть другие `.lst` другого формата (`ld_AfterApiReg.lst`,
`ld_BeforeApiReg.lst`) — расширение не является маркером формата, маркер —
каноническое имя файла.

«Таблица» в CodeBase — виртуальная сущность: `SearchTable` агрегирует вхождения
`sql_tables` по ключу `(table_name, product, branch)` с dedup по веткам;
API-таблицы — строки `api_business_object_tables`. Единой физической строки
«таблица» нет, поэтому признак хранится отдельно и присоединяется на чтении.
Прецедент диспетчеризации «файл в обходе, сущности по условию» — XML вне
`/DSArchitectData/` (indexer.go: `case "XML"` с проверкой пути); прецедент
file-keyed справочника — `ds_return_codes`. Нормализация имён продуктов уже
существует (`branchkey.NormalizeProduct`), но она механическая и совпадение с
`ds_products.product_name` не гарантирует (см. D4).

Схема БД — v6 (`codebase_schema_v6`); любое DDL-изменение требует bump маркера
(readonly-schema-policy, fast-path по маркеру).

## Goals / Non-Goals

Goals:

- Признак «объявлена как таблица в памяти» в индексе, единый для API- и обычных
  p-таблиц, видимый в `query table` / `query api-table` (CLI и MCP).
- Инкрементальность «бесплатно»: изменение/удаление `.lst` обрабатывается общим
  механизмом update (per-file транзакция, каскад).
- Хранение, читаемое будущими review-правилами (memory-optimized ограничения).

Non-Goals:

- Review-правила по in-memory таблицам (только закладываем читаемость).
- Отражение runtime-истины `tTableInmemory` целевой БД (экспорт не существует).
- Отдельный list-запрос «все таблицы в памяти» / фильтр по признаку.
- Разбор прочих `.lst`-форматов (ld_*.lst) — только файл-строка без сущностей.
- Alias-маппинг продуктов декларации на каталог продуктов индекса
  (`fa-payment` → `fa-payments`) — product_name хранится как есть после
  механической нормализации.
- Колонка `scan_runs.lst_files` и вывод в `codebase stats` — наблюдаемость
  ограничена `ScanStats` + печатью init/update (см. D7).

## Decisions

### D1: Диспетчеризация по каноническому имени, не по расширению

Попадание `.lst` в обход требует ТРЁХ изменений, иначе паттерн молча не работает:
(1) `.lst` в `fswalk.GetSupportedExtensions()` — `walkerPatterns`
(`internal/indexer/runner.go`) фильтрует конфиг-паттерны по этому белому списку
и отбрасывает неизвестные расширения; (2) `lst`-кейс в
`getEncodingAndLanguage` → language `LST` (приор WIN1251: содержимое ASCII,
но legacy-окружение CP1251-безопасно); (3) `*.lst` в оба дефолтных списка
config (`NormalizeConfig` и `CreateDefault`) и в `codebase.toml` /
`codebase.docker.toml` / README. В `processFileInTx` кейс `LST` парсит файл
только при совпадении basename с `ModuleTableInMemory.lst` — сравнение
`strings.EqualFold` (Windows-регистр не должен ломать разбор); иначе — файл
проиндексирован, сущностей нет (прецедент: XML вне `/DSArchitectData/`).
Альтернатива «парсить все .lst по расширению» отвергнута: ld_*.lst имеют другой
формат (списки путей без `;`).

### D2: Отдельная таблица `memory_tables`, не generic-атрибуты и не колонки

Схема: `memory_tables(id BIGSERIAL PK, file_id BIGINT NOT NULL REFERENCES
files(id) ON DELETE CASCADE, product_name TEXT NOT NULL, module_name TEXT NOT
NULL DEFAULT '', table_name TEXT NOT NULL, line_number INTEGER NOT NULL
DEFAULT 0)` + `idx_memory_tables_table ON memory_tables(LOWER(table_name))` +
`idx_memory_tables_file_id`. Альтернативы: колонка в `sql_tables` (вхождения —
тысячи строк на таблицу, флаг размазался бы и сломал update-паттерн);
generic `table_attributes(attr_name, attr_value)` (YAGNI — сегодня один
признак); relation file→table (в relations нет entity-типа «файл», семантика —
атрибут, не ребро графа). Product_name хранится нормализованным (см. D4),
но join по нему не фильтрует.

### D3: Name-based join без product-scope, LATERAL без размножения строк

Обогащение `SearchTable`/`SearchAPITable` — без условия по продукту вхождения:
физическая таблица одна на БД, декларация `[FA-Administrator] pAPI_AttrValue`
действительна и для вхождений `pAPI_AttrValue` в коде `fa-cards`. Результат при
установленном признаке несёт продукт/модуль декларации (поля вида `in_memory`,
`in_memory_product`, `in_memory_module`).

SQL-форма — `LEFT JOIN LATERAL ... LIMIT 1`, НЕ голый LEFT JOIN: обе функции
возвращают по одной строке на сущность (`SearchTable` — dedup-набор `chosen`,
`SearchAPITable` — `SELECT * FROM (union) dedup LIMIT`), и обычный join по
`LOWER(table_name)` при двух записях (копия `.lst` в двух ветках, дубль внутри
файла) размножил бы строки и съел бы лимит:

```sql
LEFT JOIN LATERAL (
    SELECT mt.product_name, mt.module_name
    FROM memory_tables mt
    JOIN files f ON f.id = mt.file_id
    LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
    WHERE LOWER(mt.table_name) = LOWER(ch.table_name)
    ORDER BY COALESCE(br.is_default, FALSE) DESC, mt.id
    LIMIT 1
) m ON TRUE
```

Выбор записи — приоритет это СОРТИРОВКА, не фильтр: фильтр `is_default = TRUE`
опустошил бы признак на текущем дереве — единственная копия `.lst` лежит в
non-default ветке `prj/TblInMemory` (см. Context). Тест обязан покрывать оба
случая: default-копия предпочитается при наличии; non-default-копия не
теряется при отсутствии default.

### D4: Нормализация продукта секции — справочное поле, не сопоставление

`[FA-Administrator]` → `fa-administrator` — механическая нормализация
`branchkey.NormalizeProduct`, удобная для единообразного отображения. Она НЕ
гарантирует совпадения с каталогом продуктов индекса: `[FA-Payment]` →
`fa-payment` при продукте индекса `fa-payments`; `[FA-ContractTransfers]` →
`fa-contracttransfers` при `fa-contracts` в дереве. Поэтому `product_name` в
`memory_tables` — справочное поле декларации, в join не входит (D3 join-ит
только по имени таблицы); будущие review-правила не должны сравнивать его с
`ds_products.product_name` без alias-маппинга (вне scope). Модуль хранится
как есть (`API_Administrator`, `GAAP`).

### D5: Lenient-разбор — контракт «records + диагностика», без ошибки

Контракт парсера: `ParseResult{ Records []MemoryTableRecord; MalformedLines []int }`
— парсер НЕ возвращает ошибку на мусорной строке. Весь разбор файла идёт в
одной per-file транзакции (`processFileCtx`): вернуть `err` значило бы откатить
и корректные записи, поэтому `processFileInTx` пишет каждую строку из
`MalformedLines` в `errorLogger` (контекст: файл, номер строки), инкрементирует
`stats.Errors` и возвращает `nil` — файл проиндексирован с корректной частью.

Правила разбора: пустые строки/`\r`/BOM обрезаются; заголовок секции — `[...]`
без содержимого между скобками игнорируется как мусорная строка; корректная
строка — `Таблица;Модуль` с разделением по ПЕРВОЙ `;` и Trim обоих полей;
пустая таблица после трима → мусорная строка, пустой модуль допустим
(`''`); табличная строка до первого заголовка секции — мусорная (нет продукта,
запись с `product_name=''` не создаётся); `line_number` — 1-based по исходным
строкам. Вставка идёт через sanitize (по образцу `db_insert_retcode.go`
`sanitizeUTF8String`) — защита от NUL/невалидных байтов, если файл окажется
UTF-16/BOM-вариантом (fswalk `DetectBOMOrUTF16` отдаёт байты как есть).

### D6: Попадание в обход — дефолтный include_pattern + правка конфигов

`*.lst` добавляется в дефолтный набор `include_patterns` (config.go-фолбэк и
документированный дефолт file-walking) и в `codebase.toml` репозитория.
Известная асимметрия: код-фолбэк в config.go короче документированного дефолта
(без xml/md/yaml) — существующий разрыв, не углубляем: `*.lst` добавляется в оба
списка. Конфиги рабочих установок (deploy на C:\NT\FA#...) правятся вручную —
без `*.lst` файл не попадёт в обход (молча, как и любой непаттерн-файл).

### D7: Наблюдаемость формата — ScanStats + merge + печать init/update

`ScanStats.LSTFiles` считает ВСЕ обработанные `.lst`-файлы (канонические и
ld_*, по образцу SQLFiles, считающего все .sql). Поверхность: строка
`dst.LSTFiles += src.LSTFiles` в `mergeScanStats` (без неё счётчик всегда 0 —
разбор идёт в localStats), строка `LST files:` в `cmd/init.go` и `cmd/update.go`
рядом с XML/MD/YAML. `scan_runs` per-language колонок не имеет (новая колонка =
DDL, не оправдано), `--summary` — флаг query-команд; `codebase stats`
(`store.Stats`/`computeStats`) — вне scope. Тест: `TestMergeScanStats`-стиль —
коллектор + merge дают ожидаемое число.

## Risks / Trade-offs

- [Несколько копий `.lst` из разных веток → дубль записей] → LATERAL LIMIT 1 с
  сортировкой `is_default DESC, id` (D3): одна строка результата, лимит не
  съедается; дубли записей не фейлят чтение.
- [Единственная копия `.lst` в non-default ветке → фильтр по is_default
  опустошит признак] → Приоритет реализован сортировкой, не фильтром (D3);
  тест фиксирует сценарий «только non-default копия».
- [Забыть `memory_tables` в `codebaseResetTables` → сторож-тест схемы
  `TestResetCodebaseTables_GuardSchemaCoverage` упадёт при появлении таблицы] →
  Внести в список усечения и в тестовый `resetTables` в той же задаче, что и DDL
  (tasks 1.1); там же обновить `requiredTables` в `schema_integration_test.go`.
- [`.lst` отсутствует в конфиге деплоя или выброшен `walkerPatterns` → флаг
  «молча» пуст] → D1 фиксирует все три точки попадания в обход; проверка в
  tasks 4.2: после деплоя убедиться, что `ModuleTableInMemory.lst` виден в files.
- [Мусорная строка в `.lst` → возврат err откатит транзакцию файла целиком] →
  Контракт D5: парсер возвращает records + MalformedLines, indexer пишет в
  errorLogger/stats.Errors, транзакция фиксируется.
- [Имя таблицы с лишними `;`/пробелами в `.lst`] → Trim полей, деление по
  первой `;`; пустая таблица после трима — мусорная строка (D5).
- [Review-правила будущего потребуют продукт-scope или alias продуктов] →
  `product_name` хранится в записи; фильтр/маппинг добавляется точечно в
  правиле, миграции не нужно (D4).

## Migration Plan

1. Bump `CurrentSchemaVersion` → `codebase_schema_v7` с DDL `memory_tables`
   (CREATE TABLE IF NOT EXISTS + индексы) в списке `InitSchemaCtx`.
2. Существующие БД получают таблицу первым `update` после деплоя (fast-path
   по маркеру снимется; DDL — миллисекунды, данных нет).
3. `codebase.toml` деплоев: добавить `"*.lst"` в `include_patterns` ДО первого
   update, затем один прогон подхватит `.lst`.
4. Откат: удалить `*.lst` из include_patterns — файл перестаёт обходиться,
   существующая строка files удаляется при следующем update (каскад снесёт
   memory_tables-записи); таблицу можно оставить (пустая, безвредна).

## Open Questions

- Имена выходных полей CLI/MCP (`in_memory` vs `memory`): решается при
  реализации, на спеку не влияет (в спеке — «признак in-memory», без имён полей).
