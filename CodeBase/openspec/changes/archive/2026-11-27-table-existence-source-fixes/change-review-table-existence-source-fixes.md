# Change-review: `table-existence-source-fixes`

- Дата: 2026-11-27. Ревью артефактов change (proposal / design / tasks / 4 дельта-спеки)
  и кода, который планируется изменить (сборка `1724`, `cmd/root.go:29`).
- Проверено: `openspec validate table-existence-source-fixes --strict` — valid; все 5
  `MODIFIED`-требований существуют в основных спеках и сохраняют все прежние сценарии
  (только добавления: 12→17, 3→5, 21→25, 5→6, 4→5).
- Реальные данные: точечные замеры rg по `fa-payments` и `fa-contracts` (полный корпус FA
  не сканировался — 106 тыс. SQL-файлов); ниже приведены готовые psql-запросы для
  проверок, которые без индекса не сделать.

## Вердикт

Change к реализации готов после правки артефактов: **1 блокирующее** замечание (место
вставки `reg_table_name` названо неверно — без правки реестр в боевой БД не заполнится),
4 средних, 7 низких. Замеры на индексе FA (psql, запросы ниже): `sql_columns` с
`table_name='dbo'` — **1521**, `sql_index_definitions` — 12, `sql_procedures` — 1,
`__REG_TABLE_NAME__`-вхождений — 1186 (50 фрагментов), пересечение
«модификатор+префикс» — 0. Архитектурные решения D1–D5 корректны; регэкспы D1/D2,
схема, lookup-ветвь D4 и каскад проверены по коду — замечаний нет.

## Замечания

### Высокая серьёзность — блокирует

**H1. Неверно названо место вставки строк `reg_table_name` (design D3, tasks 2.2).**
- Где: design.md D3 «вызов в `processFileCtx` (indexer.go) рядом с `tablesBatch`»;
  tasks.md 2.2 — то же.
- Что: `processFileCtx` (`internal/indexer/indexer.go:1675`) `tablesBatch` не содержит —
  он лишь диспетчеризует: `.sql` → `parseSQLFile` (`indexer.go:1691`) →
  `parseSQLLikeFile` (`internal/indexer/indexer_sql_pas.go:35`; batch `:52`, вставка
  `BatchInsertSQLTables` `:168`). `tablesBatch` в `indexer.go:493/:576` — это ветка
  `.h`-файлов с маркерами процедур, `ProductTable.sql` через неё не идёт.
- Последствие: при буквальном следовании design реестр не вставится ни для одного
  `.sql`-файла; unit-тесты парсера зелёные, интеграционный тест 2.2 ловит дефект только
  если файл в фикстуре имеет расширение `.sql`.
- Рекомендация: в D3 и 2.2 указать `parseSQLLikeFile` (`indexer_sql_pas.go`, рядом с
  `:168`) — он покрывает и `.sql`, и `.t01` (`indexer.go:1625`); в интеграционном тесте
  явно использовать `ProductTable.sql`.

### Средняя серьёзность

**M1. Регэксп реестра не берёт имена в скобках (design D3, tasks 2.1, миграция D5.1).**
- Доказательство: `fa-payments/Pooling/SERVER/DB/ProductTable.sql` — 3 строки вида
  `__REG_TABLE_NAME__([tPoolAccountLimitTable])` (также `tPoolAccountLock`,
  `tPoolAccountSweepOrder`); запланированный `(?i)__REG_TABLE_NAME__\s*\(\s*([A-Za-z_#][A-Za-z0-9_#]*)\s*\)`
  их не матчит. В индексе FA: 50 REG-фрагментов, 1186 вхождений со скобочным вариантом
  регэкспа (с запланированным — 1183); в 32 `ProductTable.sql` rg даёт 1187 строк —
  расхождение 1 строка (не в фрагментах) и 3 скобочных имени.
- Последствие: 3 таблицы остаются без свидетельства; инвариант 6.3 «строк
  `reg_table_name` = числу вхождений» даёт расхождение на 3, в зависимости от способа подсчёта.
- Рекомендация: в обоих регэкспах (Go и SQL-миграция) допустить необязательные скобки —
  `\(\s*\[?([A-Za-z_#][A-Za-z0-9_#]*)\]?\s*\)`; unit-тест + сценарий в дельте
  `sql-parsing`.

**M2. Миграция создателей, форма с модификатором (D5.2) — неверное утверждение о DISTINCT.**
- Что: форма 1 не содержит группы schema-префикса, поэтому `create unlogged table dbo.x`
  даёт имя `dbo`, форма 2 — `x`; `DISTINCT` разные имена не схлопывает. Утверждение
  «пересечение схлопывается DISTINCT» неверно.
- Последствие: в индексе FA пересечение = 0 (замер), дефект латентный; при появлении —
  вставился бы фиктивный создатель `dbo` (и symbol), который удалит только миграция 3
  благодаря порядку — скрытая связность.
- Рекомендация: в форму 1 добавить `(?:[A-Za-z_#][A-Za-z0-9_#]*(?:\s*\.\s*)+)*` перед
  захватом (т.е. полный паттерн D1 с `+` на модификаторах) — обе формы дают
  одинаковое имя, `DISTINCT` работает как заявлено.

**M3. Неполный охват D1 и инвентаря мусора — подтверждено замером.**
- design D1: «все регэкспы, извлекающие имя таблицы» — но список не содержит `createIndexRe` /
  `createIndexOnRe` (`on <table>`), табличную группу `alterTableAddRe`, `procDeclRe`,
  `updateSetTargetRe` / `insertColumnsRe`. В `sql_columns` колонки `insert into dbo.t (a,b)`
  и `update dbo.t set` попадают с `table_name='dbo'` — таблицы `sql_columns` нет ни в
  инвентаре (Context), ни в очистке (D5.3).
- Доказательство (индекс FA): `sql_columns` = **1521**, `sql_index_definitions`
  (`'dbo'`/`'middle'`) = 12; в fa-contracts Test
  (`teg05_InnerBalance.sql:28,47,50,53`) — `create index … on MIDDLE..dcSubcontoConnect`.
  `alter table dbo.` и `create procedure dbo.` в fa-payments/fa-contracts — 0.
- Последствие: после миграции в БД останется 1521 мусорная строка `sql_columns` и 12
  `sql_index_definitions` — инвентарь design и текст дельты `database-schema` неполны.
- Рекомендация: `sql_columns` и `sql_index_definitions` — в инвентарь, очистку и спеку
  (обязательно); индексные регэкспы и табличную группу `alterTableAddRe` — либо в D1,
  либо в Non-Goals с числами.

**M4. Очистка удаляет symbol процедуры `dbo`, но оставляет её `sql_procedures`-строку (D5.3).**
- Что: `symbols` 137 = 136 table + 1 procedure (design Context); DELETE по
  `symbol_type IN ('table','procedure')` оставляет `sql_procedures.proc_name='dbo'`
  (в индексе FA — ровно 1 такая строка, замер) и её рёбра — «висячая» процедура без символа.
- Рекомендация: решить явно — либо дополнить миграцию удалением
  `sql_procedures WHERE LOWER(proc_name)='dbo'` (и рёбер на неё) перед symbols, либо
  исключить `'procedure'` из DELETE.

### Низкая серьёзность

**L1. Расхождение чисел proposal ↔ design.** Proposal Why: «121 вхождение … в 86 файлах
(8 имён)»; design Context: «116 вхождений / 92 файла / 10 distinct», `sql_tables` 136 =
116 + 20. Привести к одним цифрам (актуальные — design).

**L2. Неверный факт в Non-Goals.** «Скобочные формы `[dbo].[t]` (0 вхождений в FA)» —
`fa-payments/Payments/SERVER/Patch/patch_dml8_3_394.sql:221`: `from [dbo].[tKey] k M_NOLOCK`
(+2 в строковых литералах). Для `tableExistsInMetadata` не FP, а FN (строка не
извлекается вовсе) — исправить число, FN отметить.

**L3. Мёртвый регэксп в перечне D1 / tasks 1.3.** `selectIntoTableRe` определён
(`sql_parser.go:29`), нигде не используется; SELECT INTO идёт через
`findTopLevelIntoClause`. Не править; упоминание убрать (или удалить регэмп в рамках change).

**L4. Имя типа.** design/tasks: `SQLParseResult.RegTables`; фактический тип —
`sqlparser.ParseResult` (`sql_parser.go:488`).

**L5. Техника интеграционного теста миграций (tasks 3.2) не оговорена.** `testutil.Open`
уже выполняет InitSchema (маркеры записаны, fast-path активен) — тест должен: засеять
легаси-данные → `DELETE FROM schema_migrations WHERE version IN (3 маркера)` +
`testutil.ResetSchemaVersion` → `InitSchemaCtx` (прецедент
`db_schema_owner_name_integration_test.go:102`). Иначе тест будет no-op. Заодно поправить
текст ошибки `schema_integration_test.go:698` («v8 migration» → v9).

**L6. `ALTER TABLE t ADD COLUMN x int` даст `column_name='COLUMN'`** (`parseColumnDefinition`
`sql_parser.go:2187`, `COLUMN` не в `isKeyword`). В FA (T-SQL/Sybase) не встречается —
не блокирует; при желании добавить `COLUMN` в семью пропуска D2.

**L7. Backfill-регэкспы идут по сырому `query_text` вместе с `--`-комментариями (D5.1/D5.2).**
- Что: `appendStatementLine` (`sql_parser.go:1040`) выполняется до пропуска комментариев
  (`:1043`), поэтому закомментированные `-- __REG_TABLE_NAME__(tOld)` и
  `-- create table dbo.tOld` остаются в тексте фрагмента; миграция сделает из них
  свидетельство/создателя, тогда как парсер (построчный, после пропуска комментариев) — нет.
- Последствие: ложное свидетельство для выведенной из реестра таблицы (FN) до
  переиндексации файла; расхождение backfill и парсера. В текущем индексе FA — 0
  таких вхождений (замер), дефект латентный.
- Рекомендация (не блокирует): во всех backfill'ах матчить по
  `regexp_replace(query_text, '--[^\n]*', '', 'g')` — либо зафиксировать как известное
  ограничение в Risks.

## Проверено — замечаний нет

- D1 `createTableRe`: непрозрачный префикс `(?:ident(?:\s*\.\s*)+)*` корректно съедает
  `dbo.` и `MIDDLE..`, модификаторы допущены, захват остаётся группой 1 —
  `FindStringSubmatchIndex` (`sql_parser.go:1462`) без изменений, `column_number`
  становится точнее. Тот же приём для `tableNameRe`/`tableAliasRe`/`insertTableRe`/
  `deleteTableRe`/`selectIntoRe`/`continuedTableRe` согласован с их использованием
  (`:1632–1750`).
- D2: локальная проверка префикса в обработчике `alterTableAddRe` (`:1368`) безопаснее
  расширения `isKeyword` (`:2216`, используется и для фильтрации таблиц/алиасов);
  `parseColumnDefinition` обрезает `[]` — скобочные имена колонок не пострадают.
- D3: каскад подтверждён — `DeleteFilesByPathsExcept` (`runner.go:406`) удаляет старую
  строку `files`, FK `ON DELETE CASCADE` работает как у `memory_tables`; DDL и
  `BatchInsertRegTables` по образцу `db_insert_memory_tables.go` уместны.
- D4: третья ветвь `UNION ALL` с `JOIN files f` + `tableSourceAreaFilter`
  (`db_lookup_sql.go:726`, `:849–893`) согласована с существующими; индекс
  `LOWER(table_name)` используется; `prewarmTableEvidence`/`cachedLookupTableEvidence`
  меняются автоматически, гейт `indexCheckVerifiable` не затронут.
- D5: структура `applyMigrationTx` и место перед маркером версии (`db_schema.go:1318`)
  верны; синтаксис data-modifying CTE `RETURNING` корректен; PG ARE поддерживает
  `(?:…)`, `(?i)`, `\s`, `regexp_matches(…,'g')`.
- Схема: bump v8→v9 обязателен и запланирован (`db_schema.go:13`); `CheckSchemaVersion`
  после апгрейда корректно потребует `codebase update`.
- Сообщение/документация: `review_rules.go:2055`, ожидание в
  `table_exists_metadata_test.go:442`, `README.md:1073` — покрыты задачами 4.1/5.1;
  `buildNumber` `1724`→`1727` соответствует `cmd/root.go:29`.

## Контрольные запросы (psql, индекс FA) — результаты на 2026-11-27

```sql
-- 1. Мусор префиксной экстракции вне инвентаря design (M3).
--    Результат: sql_columns 1521, sql_index_definitions 12, sql_procedures 1.
SELECT 'sql_columns' AS src, COUNT(*) FROM sql_columns WHERE LOWER(table_name) = 'dbo'
UNION ALL SELECT 'sql_index_definitions', COUNT(*) FROM sql_index_definitions
  WHERE LOWER(table_name) IN ('dbo', 'middle')
UNION ALL SELECT 'sql_procedures', COUNT(*) FROM sql_procedures WHERE LOWER(proc_name) = 'dbo';
-- Поля индексов (sql_index_definition_fields) имени таблицы не несут: FK
-- table_index_id → sql_index_definitions ON DELETE CASCADE — удалятся вместе с индексом.

-- 2. Эталон для инварианта 6.3 с учётом скобочных имён (M1). Результат: 50 / 1186
--    (с запланированным регэкспом без скобок — 1183). Миграция схлопывает повторы
--    DISTINCT (file_id, name, line_number) — контроль вести в тех же терминах.
--    \s в обычном литерале требует standard_conforming_strings=on (default).
SELECT COUNT(*) FILTER (WHERE query_text ILIKE '%__REG_TABLE_NAME__%') AS reg_fragments,
       (SELECT COUNT(*) FROM query_fragments qf,
          regexp_matches(qf.query_text,
            '__REG_TABLE_NAME__\s*\(\s*\[?([A-Za-z_#][A-Za-z0-9_#]*)\]?\s*\)', 'gi') m) AS reg_names
FROM query_fragments;

-- 3. Пересечение «модификатор + префикс» на всём FA (M2). Результат: 0 — дефект
--    латентный, правка формы 1 остаётся рекомендуемой.
SELECT COUNT(*) FROM query_fragments qf,
  regexp_matches(qf.query_text,
    'create\s+(?:(?:global\s+)?temp(?:orary)?\s+|unlogged\s+)+table\s+(?:[A-Za-z_#][A-Za-z0-9_#]*(?:\s*\.\s*)+)+[A-Za-z_#]',
    'gi') m;

-- 4. Закомментированные регистрации и create-with-prefix в фрагментах (L7).
--    Результат: 0 / 0 — дефект латентный, L7 переведён в «не блокирует».
SELECT COUNT(*) FILTER (WHERE m.n[1] ILIKE '%__REG_TABLE_NAME__%') AS commented_reg,
       COUNT(*) FILTER (WHERE m.n[1] ~* '^\s*create\s+') AS commented_create
FROM query_fragments qf,
  regexp_matches(qf.query_text, '--([^\n]*(?:__REG_TABLE_NAME__|create\s+(?:(?:global\s+)?temp(?:orary)?\s+|unlogged\s+)*table\s+[A-Za-z_]+\s*\.))', 'gi') AS m(n);
```

## Итог

Перед реализацией внести в артефакты: H1 (место вставки — `parseSQLLikeFile`), M1
(скобочные имена реестра), M2 (префикс в форме 1 миграции), M3 (очистка `sql_columns`
1521 и `sql_index_definitions` 12 — обязательно; прочие регэкспы — D1 или Non-Goals),
M4 (процедура `dbo`); L1–L7 — по усмотрению (L7: вырезание комментариев в backfill —
0 вхождений в индексе, достаточно записи в Risks). После правок повторить
`openspec validate table-existence-source-fixes --strict`.
