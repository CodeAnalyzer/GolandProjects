## Context

См. `proposal.md` — Why. Текущее состояние (design `table-existence-evidence`,
D1/D2): источники существования таблиц дают `BatchLookupTableCreators`
(`sql_tables.context IN ('create','select_into')`, `select_into`-определения,
api-таблицы) и `BatchLookupTableExistenceEvidence` (`alter_add`,
`macro_add_field`, `create_index`, `macro_create_index`); prewarm обоих —
`tableCreatorCache`/`tableEvidenceCache`.

Ключевые регэкспы (`internal/parser/sql/sql_parser.go`):
`createTableRe` (стр. 21) захватывает первый идентификатор после
`create table` — для `create table dbo.tReplTables` это `dbo`;
`alterTableAddRe` (стр. 22→1368) кладёт любой `ADD <definition>` как колонку;
ссылочные (`tableNameRe`, `insertTableRe`, `deleteTableRe`, `selectIntoRe`,
`tableAliasRe`, `continuedTableRe`) захватывают идентификатор сразу после
DML-ключевого слова — для `from/join dbo.X` это тоже `dbo`. Тот же захват
первого идентификатора: `updateSetTargetRe` (:62→1054, таблица
UPDATE-SET-аккумулятора, попадает в `sql_columns` как `table_name`),
`createIndexRe` (:23→1403, группа 3 — таблица после `ON`), `createIndexOnRe`
(:25→758/:1387), группа имени таблицы `alterTableAddRe` и группа имени
процедуры `procDeclRe` (:19→1309). `selectIntoTableRe` (:29),
`insertColumnsRe` (:39), `updateColumnsRe` (:40) — мёртвые (определены,
нигде не вызываются). `CREATE UNLOGGED TABLE` не матчится вовсе
(`create\s+table` без допуска модификаторов). Имена в квадратных скобках
(`[dbo].[tKey]`) не матчатся ни одним регэкспом — FN (сегодня строка
вообще не извлекается).

Замеры по текущему индексу FA (206 606 файлов), на которые опирается дизайн:

| Факт | Значение |
|---|---|
| `create table <prefix>.<t>` во фрагментах | 116 вхождений / 92 файла / 10 distinct текстов; имена: tSeed, pSeed, tReplTables + 5 p-таблиц; форма `MIDDLE..t` — `dcSubcontoConnect`/`dcSubcontoSubst` (fa-contracts Test) |
| Покрытие этих вхождений фрагментами | 116/116 — все create-with-prefix, включая тела процедур, есть в `query_fragments` |
| `CREATE UNLOGGED TABLE` во фрагментах | 4 (`pAPI_ExpProperty_Value` ×2, `pAPI_ExpProperty_Type` ×2, PG bloating); `CREATE TEMP TABLE` реальных — 0 (350 матчей — литералы `PROFILE_TIME('Create temp table.')`); `select into dbo.` — 0 |
| Мусор префиксной экстракции | `sql_tables` 658 (`dbo` 136: 116 create + 20 select; `MIDDLE` 522: create 6 / select 276 / insert 123 / delete 84 / update 33), `sql_columns` 2097 (1521 `dbo` + 576 `MIDDLE`), `sql_column_definitions` 273 (240 `dbo` + 33 `MIDDLE`), `sql_index_definitions` 12 (все `MIDDLE`), `sql_procedures` 1 (`dbo`, procDeclRe) с одним входящим `calls_procedure`-ребром, `symbols` 659 (658 table + 1 procedure; 6 define `MIDDLE` и 1 column_definition `Middle` — легитимные сущности, не мусор), `relations` 950 (949 рёбер `references_table` на фиктивные `sql_tables`-строки `dbo`/`MIDDLE` + 1 ребро `calls_procedure` на процедуру `dbo`) |
| Кривые `alter_add` | ровно 2: `pAPI_ExpProperty_Type` (PG bloating, `ADD CONSTRAINT … UNIQUE … DEFERRABLE`) — и это её единственная запись существования: `CREATE UNLOGGED TABLE` не индексирован |
| Реестр `__REG_TABLE_NAME__` | 32 файла `DB/ProductTable.sql` по продуктам, 50 фрагментов, 1186 вхождений (3 — имя в квадратных скобках: `__REG_TABLE_NAME__([tPoolAccountLimitTable])`, fa-payments/Pooling); все 32 файла дают REG-фрагменты (покрытие 32/32); 21 имя fa-payments без иных свидетельств (tKey — 623 finding / 147 файлов) |

Ограничения: DDL выполняют только `init`/`update` (readonly-schema-policy,
bump `CurrentSchemaVersion` v8→v9 обязателен); существующий механизм
миграций — только SQL-выражения в `applyMigrationTx` с маркером в
`schema_migrations` (прецеденты `files_is_generated_backfill_v1`,
`symbols_owner_name_backfill_v1`); Go-колбэков нет; update переиндексирует
только изменённые по mtime/hash файлы (принудительного reparse нет).

## Goals / Non-Goals

**Goals:**

- Реальные имена таблиц из `CREATE TABLE` с schema/db-префиксом
  (`dbo.t`, `MIDDLE..t`) и модификаторами (`UNLOGGED`, `TEMP`, `TEMPORARY`,
  `GLOBAL TEMPORARY`) — как создатели в `sql_tables`/`symbols`.
- Ссылочная экстракция после DML-ключевых слов даёт реальное имя, не префикс.
- `ALTER TABLE … ADD <constraint-семья>` не попадает в `sql_column_definitions`.
- `reg_table_name` — извлечение `__REG_TABLE_NAME__`, вставка при индексации,
  каскад при переиндексации, backfill и реестр как свидетельство в review.
- Миграции: backfill создателей/реестра из `query_fragments` + очистка мусора
  — чистым SQL, без реиндексации корпуса.
- Устранение FP `tableExistsInMetadata`: tReplTables (fa-contracts), tKey и
  остальные 20 имён реестра (fa-payments).

**Non-Goals:**

- Гейт `indexExistsInDB` не расширяется на реестр: реестр — свидетельство, а
  не создатель (без DDL проверка индексов неверифицируема — цена, заявленная
  в design `table-existence-evidence`).
- Query/MCP-инструменты не показывают реестр (`query table` и symbol-поиск
  не расширяются).
- Backfill ссылочных контекстов (`from/join dbo.x`): мусорные строки
  `dbo` удаляются, корректные ссылки появляются при переиндексации файлов
  (forward-only); `tables_referenced` JSONB существующих фрагментов не
  переписывается.
- Скобочные формы `[dbo].[tKey]` — не поддерживаются (в FA есть 1 DML-вхождение
  `from [dbo].[tKey]`, fa-payments `Patch/patch_dml8_3_394.sql:221`, и 2 в
  строковых литералах; сегодня это FN — имя не извлекается вовсе, останется
  FN), `CREATE UNLOGGED SEQUENCE`, `select into dbo.x` (0 вхождений) — не
  поддерживаются.
- Мёртвые регэкспы `selectIntoTableRe`/`insertColumnsRe`/`updateColumnsRe`
  не удаляются (вне скоупа; упоминание `selectIntoTableRe` в ранней версии
  D1 убрано — change-review L3).
- Принудительная реиндексация/`reparse`-механизм — не вводится.

## Decisions

### D1. Нормализация имён: непрозрачный префикс в самих регэкспах

Все регэкспы, извлекающие имя таблицы, получают общий приём: перед
захватывающей группой — непрозрачная (non-capturing) группа
«идентификатор + одна или более точек», повторяемая ноль и более раз;
захватывает только последний сегмент. Для `createTableRe`:

```
(?i)\bcreate\s+(?:(?:global\s+)?temp(?:orary)?\s+|unlogged\s+)*table\s+(?:[A-Za-z_#][A-Za-z0-9_#]*(?:\s*\.\s*)+)*([A-Za-z_#][A-Za-z0-9_#]*)
```

- `dbo.tSeed` → префиксная группа съедает `dbo.`, захват `tSeed`;
- `MIDDLE..dcSubcontoConnect` → `MIDDLE..` (квантификатор `+` на точечной
  части допускает несколько точек подряд), захват `dcSubcontoConnect`;
- модификаторы `UNLOGGED`/`TEMP(ORARY)`/`GLOBAL TEMPORARY` — непрозрачная
  группа перед `table`;
- захватывающая группа остаётся первой (нумерация групп не меняется —
  `FindStringSubmatchIndex` для `column_number` продолжает работать).

Тот же приём применяется ко всем живым регэкспам, извлекающим имя
таблицы/процедуры после ключевого слова: `selectIntoRe`,
`insertTableRe`, `deleteTableRe`, `tableNameRe` (FROM/JOIN/INTO/UPDATE),
`tableAliasRe`, `continuedTableRe` (в последнем точечный префикс означает
schema-квалификацию в контексте продолжения списка FROM; неоднозначность
«alias.column» отсутствует в реальных FROM-продолжениях FA —
контролируется замером шума в ## Measurements), `updateSetTargetRe`
(таблица UPDATE-SET-аккумулятора — закрывает мусор `sql_columns`),
`createIndexRe`/`createIndexOnRe` (таблица после `ON` — закрывают мусор
`sql_index_definitions`), группа таблицы `alterTableAddRe` и группа имени
`procDeclRe` (закрывает `sql_procedures`-строку `dbo`). Мёртвые
`selectIntoTableRe`/`insertColumnsRe`/`updateColumnsRe` не трогаются.

Два уточнения, добавленные по результату первого прогона замера (## Measurements,
регресс 499 findings против baseline 215):

- **Системные схемы**: ссылка, квалифицированная `sys.` или
  `information_schema.` (например `from sys.indexes ind`), — системный каталог
  SQL Server, а не таблица продукта. Прежде захватывался квалификатор `sys`
  и гасился системным префиксом правила; после среза префикса голое имя
  (`indexes`, `tables`, `columns`, `schemas`, `index_columns`) теряло
  исключение → 287 FP на 27 файлах. Ссылочная экстракция (`selectIntoRe`,
  `insertTableRe`, `deleteTableRe`, `tableNameRe`, `continuedTableRe`) такие
  ссылки пропускает целиком (хелпер `isSystemSchemaQualified` по слову перед
  последней точкой; `dbo.`/`MIDDLE..` остаются ссылками). Известное
  ограничение (code-review C3): пропуск срабатывает до регистрации алиасов,
  поэтому для `from sys.objects o` пары `o.object_id` пишутся в
  `sql_columns` с `table_name='o'` (прежде — `'sys'`) — эквивалентный по
  качеству мусор, на findings не влияет (замер новых имён не дал). В индексе
  унаследованные 1 580 строк `sql_tables`='sys' остаются до переиндексации
  файлов (инертны: имя 'sys' никто не запрашивает).
- **Дисамбигуация dotted-продолжений по реестру алиасов**: строка-продолжение,
  начинающаяся с пары `x.y`, — это `alias.column`, если `x` зарегистрирован
  как алиас таблицы (`tableAliases`): продолжение условия ON/WHERE, ссылка не
  извлекается; иначе — `schema.table`, извлекается `y`. Ранее (замер)
  `c.InterfaceObjectID not in (...)` из тел макросов `M_TAG_ERROR` давала
  таблицу `InterfaceObjectID` (33 FP), а до изменения — мусорные имена `c`/`cc`
  (33 FP baseline). Остаточный случай: неалиасный квалификатор подзапроса
  (`q.Col` без регистрации `q`) извлечётся как `Col` — редкость, гасится
  переиндексацией с полным набором алиасов.

Побочный эффект (спек-уровень, выявлен при прогоне тестов): `tableNameRe`
закрывает известный FN `viewUsageInServerCode` — квалифицированное обращение
`from dbo.tfund` теперь даёт ссылку `tfund` и finding по view (прежде парсер
срезал до `dbo`, спека фиксировала ограничение). Требование «Запрет
представлений в серверном коде» обновлено MODIFIED-дельтой в
`specs/review/deploy-stoppers` (+ сценарий «Квалифицированное обращение к
view»); unit-тест переписан с guard'а ограничения на новое поведение.

Альтернатива «захват полного dotted-токена + разрезание в Go» отклонена:
меньшая локальность изменения, двойная правка точек использования
(`FindStringSubmatchIndex`), риск sloppy-захвата хвоста. Альтернатива
«исправлять только `createTableRe`» отклонена решением по скоупу:
ссылочная экстракция — в скоупе (14 фрагментов `from/join dbo.`, 20 мусорных
строк — «dbo» вместо реальной таблицы в `sql_tables` и `FindLatestSQLTableIDsByNames`).

`isIgnoredTableName`/`isKeyword`/`##`-фильтры применяются к захваченному
последнему сегменту без изменений; `isTemporaryTableName` продолжает видеть
`#`-префикс.

Второй источник мусора `sql_columns` с `TableName='dbo'` (найден при
реализации, сверх инвентаря M3): pending-колонки `columnNameRe` — регэксп
пары «квалификатор.колонка» (`ident.ident`), у которой квалификатором
оказывается schema-префикс (`from dbo.tConfigParam` → пара
`dbo.tConfigParam`, записанная как колонка `tConfigParam` таблицы `dbo`).
Правка: каждая извлечённая таблица строки регистрируется хелпером
`markSeenTable` одновременно в построчном `lineSeenTables` и во множестве
таблиц стейтмента `stmtSeenTables` (сброс в `resetStatementState`; цель
`alterTableAddRe` пишется туда же). В блоке `columnNameRe` пара
пропускается, если правый сегмент — таблица строки ИЛИ стейтмента
(межстрочный случай code-review C1: `and dbo.t.id = 1` на строке
продолжения условия, где `t` уже не видна построчному множеству) и левый
сегмент при этом — не зарегистрированный алиас. Алиас-предохранитель
(code-review C4) сохраняет легитимную колонку, одноимённую таблице:
`select a.tKey from tKey a` — пара `(a, tKey)` не отбрасывается и
разрешается в реальную таблицу. Легитимные пары `t1.a` не затронуты
(правый сегмент — колонка).

### D2. Constraint-семья пропускается в `alterTableAddRe`

В обработчике `alterTableAddRe` (sql_parser.go:1368) после извлечения
`definition` проверяется префикс (без учёта регистра) на семью
`CONSTRAINT` / `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` / `CHECK` /
`DEFAULT` (регэксп с границей слова); при совпадении
`appendColumnDefinition` не вызывается. Реальные `ADD <колонка> <тип>`
продолжают записываться как `alter_add`. Ведущее слово `COLUMN`
(PostgreSQL-форма `ADD COLUMN x int`, в FA — 0 вхождений) в семью пропуска
не входит, а срезается как noise-word перед разбором: сейчас такая строка
даёт мусорную `column_name='COLUMN'` (change-review L6), но это реальная
колонка, и её нужно сохранить, а не пропустить.

Альтернатива «записывать с новым kind `alter_add_constraint`» отклонена: сущность без
потребителя; свидетельство существования таблицы для двух реальных записей
(`pAPI_ExpProperty_Type`) замещается создателем из D1 (`CREATE UNLOGGED
TABLE` этой же правкой + backfill), регрессFP отсутствует — проверяется
замером (кейс PG bloating, ## Measurements).

### D3. `reg_table_name`: схема, модель, вставка, каскад

DDL (в общем списке `db_schema.go`, bump `CurrentSchemaVersion` → v9):

```sql
CREATE TABLE IF NOT EXISTS reg_table_name (
    id BIGSERIAL PRIMARY KEY,
    file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    line_number INTEGER NOT NULL DEFAULT 0
)
```

Индексы по образцу `sql_tables`: `idx_reg_table_name_lower
ON reg_table_name (LOWER(table_name))` (ветвь lookup'а D4),
`idx_reg_table_name_file_id ON reg_table_name (file_id)`.

Модель: `model.SQLRegTable{Name, LineNumber, FileID}` и
`sqlparser.ParseResult.RegTables` — отдельный срез, НЕ `result.Tables`:
регистрации не участвуют в контекстах DML и не попадают во
`fragment.TablesReferenced` (indexer.go строит его только из
`result.Tables`). Парсер: `regTableNameRe =
(?i)__REG_TABLE_NAME__\s*\(\s*\[?([A-Za-z_#][A-Za-z0-9_#]*)\]?\s*\)` —
построчно в основном цикле; имя MAY быть заключено в квадратные скобки
(`__REG_TABLE_NAME__([tPoolAccountLimitTable])` — 3 строки
fa-payments/Pooling, change-review M1); маркер `__MASS_DELETE_TABLE_NAME__`
без аргумента не матчится. Вставка: `store.BatchInsertRegTables` по образцу
`BatchInsertSQLTables` (db_insert_sql.go); вызов — в `parseSQLLikeFile`
(`internal/indexer/indexer_sql_pas.go`, рядом с batch-срезами :52 и
вставкой `BatchInsertSQLTables` :168): именно он парсит и вставляет
SQL-сущности для `.sql` и `.t01`. `processFileCtx` (indexer.go:1675) —
диспетчер по расширению без batch-срезов, а `tablesBatch` в indexer.go
:493/:576 — ветка `.h`; указание их как места вставки в ранней версии
design оставило бы реестр пустым (change-review H1). Ветка `.h` также
вставляет reg-строки: гейт SQL-парсинга `.h` (indexer.go:482, прежде —
только маркеры процедур) расширен маркером `__REG_TABLE_NAME__`, вставка
`BatchInsertRegTables` рядом с `tablesBatch` (code-review C2 — иначе
переиндексация .h-файла с реестром молча теряла бы строки, внесённые
backfill'ом из фрагментов; в FA реестр в `.h` не встречается — латентная
симметрия). Очистка при
переиндексации — каскад `files` (update удаляет запись файла и
переиндексирует его), прецедент — `memory_tables`; отдельных
DELETE-запросов не требуется. `reg_table_name` входит в список усечения
`ResetCodebaseTables` (db_reset.go: таблица кодовой базы, как
`memory_tables`; guard-тест покрытия схемы потребовал явного решения —
дельта MODIFIED «Сброс таблиц кодовой базы»).

### D4. Реестр — третья ветвь `BatchLookupTableExistenceEvidence`

```sql
SELECT LOWER(rt.table_name) FROM reg_table_name rt
  JOIN files f ON f.id = rt.file_id
  WHERE LOWER(rt.table_name) = ANY($1)`+areaFilter+`
```

(глобальный вариант — без JOIN, как у существующих ветвей). Свидетельство, не
создатель: `BatchLookupTableCreators` и гейт `indexCheckVerifiable` не
меняются — реестр не открывает проверку индексов для таблиц без DDL. Prewarm
не меняется: реестр входит в тот же `tableEvidenceCache` через существующий
`prewarmTableEvidence`. Сообщение правила:
«Таблица %s отсутствует в метаданных: не найден создающий DDL, изменение
структуры патчем, реестр таблиц продукта, SELECT INTO или объявление
DSArchitect». Областная привязка — тот же `tableSourceAreaFilter` по
файлу-источнику реестра: реестр чужого продукта вне разрешённой области
существование не подтверждает (симметрично свидетельствам патчей, D5
исходного design).

### D5. Миграции: SQL-only backfill и очистка (вариант A)

Три именованные миграции в `InitSchemaCtx` после существующих, до маркера
`CurrentSchemaVersion` (v9); каждая — `applyMigrationTx(ctx, tx, <имя>,
[]string{...})` с маркером в `schema_migrations` (идемпотентность по
маркеру, прецедент — `symbols_owner_name_backfill_v1`).

1. `reg_table_name_backfill_v1`:

```sql
INSERT INTO reg_table_name (file_id, table_name, line_number)
SELECT DISTINCT qf.file_id, (m.n)[1], qf.line_number
FROM query_fragments qf
CROSS JOIN LATERAL regexp_matches(
  regexp_replace(qf.query_text, '(?m)^[ \t]*--[^\n]*', '', 'g'),
  '(?i)__REG_TABLE_NAME__\s*\(\s*\[?([A-Za-z_#][A-Za-z0-9_#]*)\]?\s*\)', 'g') AS m(n)
WHERE qf.query_text ILIKE '%__REG_TABLE_NAME__%'
```

Регэксп допускает квадратные скобки вокруг имени (M1: 3 имени
`__REG_TABLE_NAME__([tPool…])`; 1186 против 1183 вхождений без допуска).
`regexp_replace` вырезает полные `--`-строки, зеркаля `commentRe` парсера
(`appendStatementLine` выполняется до пропуска комментариев, :1040, —
фрагменты содержат комментарии; сегодня таких вхождений 0, change-review
L7): закомментированная регистрация не должна становиться свидетельством
до переиндексации файла.

2. `sql_table_creators_backfill_v1` — вставка создателей (sql_tables +
symbols через data-modifying CTE `RETURNING`) из фрагментов. Паттерн — копия
`createTableRe` D1, у которой обязательна ровно одна из отличительных
частей: модификатор перед `table` либо schema-префикс после него. Обязательность
нужна, чтобы не дублировать корректно проиндексированные plain-create
(строка оператора внутри многострочного фрагмента не совпадает со стартом
фрагмента, и `NOT EXISTS` по точной строке их не отсеял бы). Обе формы
несут полную непрозрачную префикс-группу (в форме с обязательным
модификатором — с квантификатором `*`): иначе `create unlogged table
dbo.x` дал бы от формы 1 имя `dbo`, и `DISTINCT` разные имена не схлопнул бы
(change-review M2). Пересечение форм (модификатор + префикс, сегодня 0)
обеими формами даёт одно и то же имя и схлопывается `DISTINCT`;
`regexp_replace` вырезает `--`-комментарии (L7):

```sql
WITH ins AS (
  INSERT INTO sql_tables (file_id, table_name, context, is_temporary, line_number, column_number)
  SELECT DISTINCT file_id, table_name, 'create', false, line_number, 0
  FROM (
    SELECT qf.file_id, (m.n)[1] AS table_name, qf.line_number
    FROM query_fragments qf
    CROSS JOIN LATERAL regexp_matches(
      regexp_replace(qf.query_text, '(?m)^[ \t]*--[^\n]*', '', 'g'),
      '(?i)create\s+(?:(?:global\s+)?temp(?:orary)?\s+|unlogged\s+)+table\s+(?:[A-Za-z_#][A-Za-z0-9_#]*(?:\s*\.\s*)+)*([A-Za-z_#][A-Za-z0-9_#]*)',
      'g') AS m(n)  -- форма с обязательным модификатором
    UNION ALL
    SELECT qf.file_id, (m.n)[1], qf.line_number
    FROM query_fragments qf
    CROSS JOIN LATERAL regexp_matches(
      regexp_replace(qf.query_text, '(?m)^[ \t]*--[^\n]*', '', 'g'),
      '(?i)create\s+(?:(?:global\s+)?temp(?:orary)?\s+|unlogged\s+)*table\s+(?:[A-Za-z_#][A-Za-z0-9_#]*(?:\s*\.\s*)+)+([A-Za-z_#][A-Za-z0-9_#]*)',
      'g') AS m(n)  -- форма с обязательным schema-префиксом
  ) matches
  WHERE NOT EXISTS (SELECT 1 FROM sql_tables t
                    WHERE t.file_id = matches.file_id AND t.context = 'create'
                      AND LOWER(t.table_name) = LOWER(matches.table_name)
                      AND t.line_number = matches.line_number)
  RETURNING id, file_id, table_name, line_number
)
INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number, signature)
SELECT file_id, table_name, 'table', 'sql', id, line_number, table_name FROM ins;
```

`symbols`-строки: `symbol_type='table'`, `entity_type='sql'`, `entity_id`
из `RETURNING id`, `signature=table_name` — зеркало того, что делает
indexer.go для `result.Tables`.

3. `sql_tables_prefix_garbage_cleanup_v1` (порядок важен — сначала
зависимые; поля индексов `sql_index_definition_fields` удаляются каскадно
по FK `table_index_id`):

```sql
DELETE FROM relations WHERE
  (target_type = 'sql_procedure'
   AND target_id IN (SELECT id FROM sql_procedures WHERE LOWER(proc_name) = 'dbo'))
  OR (target_type = 'sql_table'
   AND target_id IN (SELECT id FROM sql_tables WHERE LOWER(table_name) IN ('dbo','middle')));
DELETE FROM sql_procedures WHERE LOWER(proc_name) = 'dbo';
DELETE FROM symbols WHERE symbol_type IN ('table','procedure') AND LOWER(symbol_name) IN ('dbo','middle');
DELETE FROM sql_columns WHERE LOWER(table_name) IN ('dbo','middle');
DELETE FROM sql_column_definitions WHERE LOWER(table_name) IN ('dbo','middle');
DELETE FROM sql_index_definitions WHERE LOWER(table_name) IN ('dbo','middle');
DELETE FROM sql_tables WHERE LOWER(table_name) IN ('dbo','middle');
DELETE FROM sql_column_definitions WHERE definition_kind = 'alter_add'
  AND UPPER(column_name) IN ('CONSTRAINT','PRIMARY','FOREIGN','UNIQUE','CHECK','DEFAULT','COLUMN');
```

Охват шире ранней версии design (change-review M3/M4): `sql_columns`
(1521 строка UPDATE-SET с `table_name='dbo'`), `sql_index_definitions`
(12 строк `MIDDLE`), строка `sql_procedures` `dbo` (procDeclRe; у неё одно
входящее `calls_procedure`-ребро — удаляется первым, чтобы не оставить
висячий конец) и легаси-мусор `column_name='COLUMN'` (0 строк сегодня —
noise-word D2 предотвратит новые). При реализации замер выявил мусор
префиксной экстракции `MIDDLE` (форма `db..table`) во всех тех же таблицах
(`sql_tables` 522, `sql_columns` 576, `sql_column_definitions` 33,
`symbols` 522 table, 822 рёбра) — предикаты всех DELETE расширены до
`IN ('dbo','middle')` (решение пользователя; те же класс мусора и
первопричина); `symbols`-предикат ограничен типами table/procedure, поэтому
легитимные 6 define `MIDDLE` и 1 column_definition `Middle` не затрагиваются.
Корректные строки появятся при переиндексации файлов (forward).

Альтернативы отклонены: (B) Go-колбэки в миграциях — новый механизм ради
точных номеров строк, которые никто не потребляет; (C) удаление files-строк
затронутых файлов с переиндексацией силами update — ломает рёбра
`references_table` других файлов до reconcile и затрагивает больше данных,
чем нужно; полная реиндексация корпуса — дорога и не нужна: покрытие
фрагментами 116/116 и 32/32 делает backfill эквивалентным по эффекту для
lookup'ов (lookup'и именные, не пофайловые: одной строки создателя в
разрешённой области достаточно, чтобы погасить FP по имени везде).

## Risks / Trade-offs

- [`continuedTableRe` с точечным префиксом: неоднозначность «alias.column» в
  продолжении списка FROM] → замер выявил её на практике (тела `M_TAG_ERROR`:
  `c.InterfaceObjectID not in (…)` → 33 FP + 287 FP `sys.*`-каталогов);
  закрыто дисамбигуацией по реестру алиасов и скипом системных схем (D1);
  остаточный риск — неалиасный квалификатор подзапроса, редкость.
- [line_number backfill-строк = старт фрагмента, для многострочных операторов
  приблизительный] → информационное поле: ни creators-, ни evidence-lookup
  не читают строку; выравнивается при первой переиндексации файла
  (parser D1 пишет точную строку).
- [Регэксп миграции может разойтись с регэкспом парсера при будущих правках]
  → паттерн — символьная копия `createTableRe`; расхождение ловится
  контрольным сравнением в ## Measurements (счётчики backfill против
  независимого psql-запроса).
- [Backfill вырезает только полные `--`-строки; закомментированные блоки
  `/* … */` в тексте фрагмента миграция видит, парсер — нет] → сегодня
  вхождений с маркерами внутри `--`-комментариев 0, внутри блочных —
  замерить при реализации; расхождение самоустранимо при переиндексации
  файла (change-review L7).
- [Предикат очистки `IN ('dbo','middle')` во всех DELETE, не только
  индексах] → реальные таблицы с именами `dbo`/`middle` в FA отсутствуют;
  `MIDDLE` — имя БД из формы `db..table`, не таблица; symbols-предикат
  ограничен типами table/procedure, легитимные 6 define `MIDDLE` и
  1 column_definition `Middle` не удаляются.
- [Вырожденный ввод `create table dbo.` без имени: бэктрек даст имя
  `dbo`] → в корпусе отсутствует; принимается как edge (прежнее поведение
  для такого ввода было тем же `dbo`).
- [Удаление 950 рёбер `references_table`/`calls_procedure` на фиктивные
  таблицы `dbo`/`MIDDLE` и процедуру `dbo`] →
  рёбра вели к несуществующей сущности (мусор); корректные рёбра на реальные
  таблицы появятся при переиндексации файлов-источников (forward-only);
  инвариант графа «без мёртвых концов» не нарушается (удаление до
  `sql_tables`).
- [Реестр чужого продукта вне разрешённой области не подтверждает
  существование: ссылки на tKey из чужих продуктов продолжат шуметь] →
  симметрично свидетельствам патчей (областная модель); фиксируется как
  известное ограничение, не регресс.
- [`tables_referenced` существующих фрагментов содержит 'dbo'] → косметика
  (display в query-инструментах); нового мусора парсер не порождает,
  старый уходит при переиндексации файлов.
- [Миграция INSERT без UNIQUE-констрейнта на reg_table_name] → маркер
  миграции гарантирует однократность; повторная вставка невозможна; у
  `sql_tables` тот же паттерн (никаких UNIQUE).

## Migration Plan

- Схема: DDL `reg_table_name` + 2 индекса + bump `CurrentSchemaVersion`
  v8→v9; три именованные SQL-миграции — в `InitSchemaCtx` (применяются
  `init`/`update`). Никакой реиндексации.
- Свежая БД: таблицы создаются, маркеры backfill'ов записываются без строк
  (query_fragments пуст).
- Откат: revert кода; DDL и backfill-данные безвредны (реестр и создатели
  консистентны с фрагментами; мусор уже удалён и не восстановится).

## Measurements

Протокол — design `add-table-exists-in-metadata` / `table-existence-evidence`
(## Measurements): baseline до правок на текущей сборке и «после» на том же
снимке индекса (между прогонами — только `codebase update` с миграцией схемы
v9, без переиндексации исходников). Корпусы и ожидания:

- fa-contracts Consumer (3 783 файла, `--rules tableExistsInMetadata`):
  `tReplTables` (22 finding в `DB/eis_5nt.sql`) → 0; новых имён нет; имён и
  файлов не больше baseline.
- fa-payments, 275 файлов со ссылками на 29 древних таблиц: все 21
  непокрытое имя получают реестр; `tKey` (623 finding / 147 файлов) → 0;
  итог и остаток фиксируются здесь.
- Кейсы: `ParticipantSB_Delete.sql` — 0; контрольная опечатка
  `ContractMassLoad.sql` (`pAPI_Account_MassClose`) — finding остаётся;
  `api_pAPI_ExpProperty_Type_nonpart_tmptbl.sql` — после удаления 2 кривых
  `alter_add` finding не появляется (создатель из UNLOGGED-backfill).
- Инварианты миграции (psql, после `codebase update`): маркеры
  `reg_table_name_backfill_v1`, `sql_table_creators_backfill_v1`,
  `sql_tables_prefix_garbage_cleanup_v1`, `codebase_schema_v9` в
  `schema_migrations`; мусор = 0: `sql_tables`, `sql_column_definitions`,
  `sql_columns`, `sql_procedures`, `symbols` (table/procedure) с именем
  `dbo`/`middle`; `sql_index_definitions` с `dbo`/`middle` (легитимные
  6 define `MIDDLE` и 1 column_definition `Middle` в symbols остаются);
  рёбра на 'dbo'/'middle'-строки `sql_tables`/`sql_procedures` = 0;
  `reg_table_name` строк = числу
  `__REG_TABLE_NAME__`-вхождений во фрагментах с допуском скобок
  (`DISTINCT (file_id, name, line_number)` — эталон 1186/50 фрагментов);
  создатели присутствуют для всех имён из таблицы фактов (8 +
  `pAPI_ExpProperty_Value`/`pAPI_ExpProperty_Type`); арифметика
  `sql_tables`: до − 658 + вставленные = после.
- Деградация ссылочной экстракции: дельта уникальных имён
  `tableExistsInMetadata`-шума не содержит новых имён, похожих на
  column/alias-фрагменты (контроль `continuedTableRe`).

Результаты (сборка 1727, миграция v9 применена `codebase update` на живом
индексе FA — 206 606 файлов, переиндексировано 0; между baseline и «после»
исходники не менялись; скрипты/результаты — `Tests/Temp/regsrc_*`):

- **fa-contracts Consumer, 3 783 файла**: baseline — archived
  `table-existence-evidence` (сборка 1719, 2026-11-24, тот же индекс):
  215 findings / 47 имён / 37 файлов. После: **179 / 44 / 34**. Ушедшие
  имена разобраны: `c` (16) + `cc` (17) — мусор alias.column-продолжений
  (существовал в baseline ДО изменения; закрыт дисамбигуацией D1 — побочное
  улучшение), `tPayInstruct` (3) — регистрация в реестре
  `fa-payments/PayInstruct/SERVER/DB/ProductTable.sql` (чужой продукт,
  default-ветка — в разрешённой области). Арифметика точная:
  215 − 16 − 17 − 3 = 179; новых имён нет; счётчики остальных имён
  идентичны baseline построчно.
- **Промежуточный прогон до уточнения D1** (первый «после»): 499 / 50 / 61 —
  регресс: 287 FP системных каталогов `sys.*` (`indexes`/`tables`/`columns`/
  `schemas`/`index_columns` на 27 файлах — префикс срезан, имя потеряло
  системное исключение `sys*`) + 33 FP `InterfaceObjectID` (пара
  `c.InterfaceObjectID` из тел `M_TAG_ERROR` трактована как schema.table).
  Оба дефекта закрыты в том же change (D1, тесты
  `TestParseContent_ReferenceSysCatalogSkip` /
  `…_ContinuationAliasColumn` / `…_ContinuationSchemaQualified`), после
  фикса — 179/44/34.
- **fa-payments, 275 файлов** (список tee_62_files.txt): baseline 1719 —
  1216 findings / 174 файла; после: **154 / 28 имён / 29 файлов**.
  Ушедшие 8 имён (1062 finding) — все зарегистрированы в реестре:
  `tKey` (623), `tPayInstruct`, `tPayInstructObject`, `tPayInstructRelation`,
  `hDocument`, `tRequisitParticipantTELEX`, `tWebUserCertificate`,
  `tResourceDisposer`; арифметика точная 1216 − 1062 = 154. Новых имён нет
  (все 28 остаточных присутствовали и в 1719 — колонки INSERT-полей
  `DateMarkBankPlat`/`QtyAccept`/… как «таблицы» строк-продолжений поля
  в list-режиме и `pPayInstruct`-семейство; унаследованный шум
  `continuedTableRe`, существующий вне этого изменения, кандидат на
  отдельную доработку; archived design неточно утверждал «все — на 5
  таблицах», фактически в 1719 было 36 имён).
- **Кейсы 5.3** (после update с миграцией): `eis_5nt.sql` — все 5 копий
  fa-administrator: tReplTables = 0 findings (до — 22);
  `ParticipantSB_Delete.sql` — 0; контрольный `ContractMassLoad.sql` —
  `pAPI_Account_MassClose` (L2567/2568) + 3 соседних имени по-прежнему
  дают findings, сообщение содержит «реестр таблиц продукта»;
  `api_pAPI_ExpProperty_Type_nonpart_tmptbl.sql` — 0 (создатель из
  UNLOGGED-backfill заместо удалённых 2 кривых `alter_add`).
- **Инварианты миграции** (psql): маркеры `reg_table_name_backfill_v1`,
  `sql_table_creators_backfill_v1`, `sql_tables_prefix_garbage_cleanup_v1`,
  `codebase_schema_v9` — все 4 в `schema_migrations`; мусор `dbo`/`middle`
  = 0 в `sql_tables`, `sql_column_definitions`, `sql_columns`,
  `sql_index_definitions`, `sql_procedures`, `symbols` (table/procedure),
  рёбра на 'dbo'/'middle'-строки = 0 (легитимные 6 define `MIDDLE` и 1
  column_definition `Middle` остались); `reg_table_name` 1186 строк /
  851 имя = эталону DISTINCT (file_id, name, line) по фрагментам 1186;
  создатели для всех 12 имён таблицы фактов присутствуют
  (tReplTables 24, tSeed 61, pSeed 27, 5 p-таблиц, dcSubcontoConnect 6,
  dcSubcontoSubst 6, pAPI_ExpProperty_Type 2, pAPI_ExpProperty_Value 2);
  эквивалентность backfill: двухформенный паттерн по текущим фрагментам
  даёт 436 DISTINCT (file_id, name, line) — 436/436 закрыты creator-строками
  (116 create-with-prefix вхождений дают >436 строковых пар из-за повторов
  в телах процедур; полнота полная).
- Тесты после уточнения D1: `go build`/`go vet`/`gofmt` чисты; unit
  (parser/sql, store, review, indexer) и integration (review
  TableExistsInMetadata+RunSQLFileCtx, indexer RegTable, store
  RegTableNameV9+Backfills+LookupEvidence) — PASS.

## Open Questions

Нет.
