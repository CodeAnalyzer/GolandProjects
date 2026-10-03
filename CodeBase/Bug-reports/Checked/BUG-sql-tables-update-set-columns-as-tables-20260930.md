# Bug: SQL-парсер пишет столбцы из `UPDATE ... SET` в таблицу `sql_tables` как имена таблиц

**Дата:** 2026-09-30
**Статус:** Fixed (2026-10-02, change `openspec/changes/fix-sql-tables-update-pollution`)
**Приоритет:** Medium (искажение индекса, не блокирует deploy)
**Компонент:** SQL-индексатор (`internal/parser/sql/sql_parser.go`), таблица `sql_tables`
**Версия CodeBase:** 0.9.2 build 1539

## Суть

При разборе многострочного `UPDATE ... SET` имена столбцов левой части присваивания (`TemplateSysName`, `TenderDate`, `TermCalendarID`, ...) сохраняются в `sql_tables` как **имена таблиц** с `context='update'`. В результате `sql_tables` содержит мусорные записи, которые далее трактуются как код-сущности (`sql_table`) всеми потребителями индекса, включая запрос покрытия спеками.

## Шаги воспроизведения

Источник: `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\Cons_GetDocToProcess.sql`

```sql
813  update pCreditDocument
814       set OperationType   = f.Condition,
815           TemplateSysName = f.UserTag
816      from pCreditDocument       p M_UPDLOCK_INDEX(XIE0pCreditDocument)
817     inner join pAPI_FO_Template f M_NOLOCK_INDEX(XPKpAPI_FO_Template)
818             on f.Spid = @@Spid
819            and f.TemplateID = p.OperTemplateID
820     where p.Spid = @@Spid
821    M_FORCEORDER
```

Запрос к индексу:

```sql
select * from sql_tables t where t.table_name = 'TemplateSysName';
```

## Фактический результат

Возвращается строка:

```
id=318418  file_id=46140  table_name=TemplateSysName  context=update  is_temporary=false  line_number=815  column_number=10
```

`file_id=46140` → `path=C:/NT/FA#/7.2GIT/fa-contracts/Consumer/SERVER/Consumer/Cons_GetDocToProcess.sql`, `line_number=815` — строка `TemplateSysName = f.UserTag` (позиция 10 — начало идентификатора).

Аналогично в `sql_tables` попадают имена столбцов из других `UPDATE ... SET`, например: `TemplateSchemePeriodID`, `TenderDate`, `TermCalendarID`, `TermContCalendarID`, `TermDate`, `TemplateType`.

## Ожидаемый результат

`TemplateSysName` и прочие LHS-идентификаторы `SET` **не должны** попадать в `sql_tables`. В `sql_tables` должны быть только реальные имена таблиц:

```sql
update pCreditDocument   -- pCreditDocument  (context=update)
from pCreditDocument     -- pCreditDocument  (уже был)
inner join pAPI_FO_Template -- pAPI_FO_Template
```

## Технический анализ (корневая причина)

### 1) `tableNameRe` включает ключевое слово `UPDATE`

`internal/parser/sql/sql_parser.go:34`:

```go
tableNameRe = regexp.MustCompile(`(?i)\b(?:FROM|JOIN|INTO|UPDATE)\s+([A-Za-z_#][A-Za-z0-9_#]*)`)
```

На строке 813 (`update pCreditDocument`) срабатывает эта ветка (`:1341`) и в её конце **безусловно** включается режим «списка таблиц»:

```go
// :1422
inFromTableList = true
```

### 2) Стоп-лист не содержит `SET`, а `continuedTableRe` матчит любой идентификатор

Обработка последующих строк идёт в ветке `else if inFromTableList` (`:1423`). Стоп-слова (`:1443-1455`) — `insert`/`select`/`update`/`delete`/`if`/`case`/`else`/`end`/`exec`/`where`/`group`/`order`/`union` — **`set` отсутствует**.

Далее линия анализируется как продолжение списка таблиц через `continuedTableRe` (`:35`):

```go
continuedTableRe = regexp.MustCompile(`^\s*,?\s*([A-Za-z_#][A-Za-z0-9_#]*)\b`)
```

### 3) Пошаговая трассировка примера

| Строка | Содержимое | Действие парсера | Результат |
|---|---|---|---|
| 813 | `update pCreditDocument` | `tableNameRe` матч → `inFromTableList = true` (`:1422`) | таблица `pCreditDocument`, `context=update` |
| 814 | `set OperationType = f.Condition,` | `tableNameRe` не матч; `inFromTableList=true`; `set` не в стоп-листе; `continuedTableRe` захватил `set` → `isKeyword("set")==true` → пропуск, но флаг **не сброшен** | — |
| 815 | `TemplateSysName = f.UserTag` | `continuedTableRe` захватил `TemplateSysName`; не ключевое слово, не `TBL` → **добавление в `result.Tables`** (`:1461-1488`) | `sql_table TemplateSysName`, `context=update`, `col=10` |

### 4) Сопутствующий дефект

Столбцы `UPDATE ... SET` при этом **не попадают** в `sql_columns`: `updateColumnsRe` (`:39`) заякорен и требует `update ... set ...` в одной строке, а здесь они на разных. То есть теряется и полезная информация, и добавляется мусорная.

## Область поражения

Баг проявляется для любой конструкции, где:
1. строка начинается с `UPDATE <table>` (включая `update t set a = 1` — тогда следующая строка `col = ...`), либо `FROM/JOIN/INTO` в контексте, где далее идут не-табличные строки, начинающиеся с идентификатора;
2. продолжение начинается с идентификатора, не являющегося ключевым словом.

`FROM`-ветка `UPDATE ... FROM` обычно не страдает: строки `inner join ...`/`on ...`/`and ...` начинаются с ключевых слов (`JOIN`, `ON`, `AND`) либо матчатся своим `JOIN`. Основной источник мусора — `SET`-присваивания.

## Влияние

- `sql_tables` загрязняется ложными именами таблиц — нарушается достоверность индекса таблиц.
- Любые потребители `sql_tables`/`symbols` видят несуществующие «таблицы»:
  - запросы покрытия спеками (ветка `sql_table` в `specsvc.go`) дают ложные «непокрытые» сущности;
  - проверки использования чужих таблиц (`foreignTablesUsing`/`foreignPTablesUsing`), полноскановые и т.п. могут получать недостоверные данные.
- Регистровые дубли (`tEntattrValue` / `tEntAttrValue`) усугубляют шум: имя хранится как в тексте без нормализации.

## Предлагаемое исправление

### Основное: не включать режим списка таблиц после ключевого слова `UPDATE`

После `UPDATE <t>` идёт `SET`, а не список таблиц через запятую, поэтому `inFromTableList` для этого случая включать не нужно (список таблиц в `UPDATE` идёт за `FROM`, и та строка матчится отдельно своим `FROM`-ключевым словом):

```go
// :1422, было:
inFromTableList = true

// стало:
if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(line)), "UPDATE ") {
    inFromTableList = true
}
```

### Дополнительно (защита): расширить стоп-лист

```go
// :1443-1455 — добавить:
strings.HasPrefix(trimmedLower, "set ") ||
strings.HasPrefix(trimmedLower, "values ") ||
```

> Важно: вариант «только добавить `set` в стоп-лист» не покрывает случай `update t set a = 1,` (UPDATE и SET в одной строке) — поэтому основной фикс обязателен.

### Исправить сопутствующий дефект

Сделать извлечение столбцов `UPDATE ... SET` независимым от одной строки (накапливать `set`-часть как multi-statement), чтобы LHS-идентификаторы попадали в `sql_columns`, а не в `sql_tables`.

### Тесты

- `UPDATE t\n SET a = 1,\n b = 2\n FROM ...` → в `sql_tables` только `t` и таблицы `FROM`/`JOIN`.
- `update t set a = 1,\n b = 2` → то же.
- `FROM t1,\n t2,\n t3` → все три таблицы по-прежнему извлекаются (регресс на валидный список).
- `INSERT INTO t (a, b)\n VALUES (...)` → без ложных таблиц.

## Обходной путь до фикса

В запросе покрытия ограничивать `sql_table` только именами, у которых есть DDL:

```sql
AND t.table_name IN (SELECT DISTINCT table_name FROM sql_tables WHERE context = 'create')
```

Не является полноценным решением (p-таблицы без проиндексированного DDL выпадут); правильный путь — фикс парсера + `codebase update`.

## Связанные файлы

- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\parser\sql\sql_parser.go`
  - `tableNameRe` :34, `continuedTableRe` :35, `updateColumnsRe` :39
  - ветка `tableNameRe` :1341-1422 (`inFromTableList = true` :1422)
  - ветка `else if inFromTableList` :1423-1490, стоп-лист :1443-1455, добавление таблицы :1461-1488
  - `isKeyword` :1860, `isIgnoredTableName` :1802, `isTemporaryTableName` :1702
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\store\db_schema.go` (`sql_tables` :76-84)
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\store\db_insert_sql.go` (`insertSQLTablesBatch` :102-131, COPY :108)
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\specsvc\specsvc.go` (ветка `sql_table` покрытия :799-808)
- `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\Cons_GetDocToProcess.sql` (строки 813-821)

## Способ обнаружения

- Анализ запроса «код fa-contracts, не покрытый спеками»: в выдаче среди `sql_table` оказались имена столбцов (`TemplateSysName`, `TenderDate`, ...).
- Проверка строкой индекса: `select * from sql_tables t where t.table_name = 'TemplateSysName';` → `context=update`, `line_number=815`, `column_number=10`.
- Верификация по исходнику `Cons_GetDocToProcess.sql:815` и по коду парсера `sql_parser.go` (`inFromTableList`/`continuedTableRe`).

## Решение (2026-10-02)

Реализовано в change `fix-sql-tables-update-pollution` (см. `openspec/changes/fix-sql-tables-update-pollution/`):

1. **Основной фикс**: list-режим таблиц не включается после `UPDATE`-строки (`inFromTableList = !p.updateRe.MatchString(line)`); стоп-лист дополнен `set `/`values ` (и `where` без пробела — случай `where--`).
2. **Хинт-макросы**: фильтр `^#?M_[A-Z0-9_]+$` (case-sensitive — lowercase `m_table` легитимна) в `isIgnoredTableName`, применён во всех ветках извлечения таблиц SQL-парсера; уничтожил ~9 000+ мусорных строк (`M_ISOLAT` select 2 476, `M_FORCEORDER`/`M_KEEPPLAN` и др.).
3. **Макро-плейсхолдеры**: имена с префиксом `##` не извлекаются как таблицы (~210 строк).
4. **Сопутствующий дефект исправлен**: колонки многострочного `UPDATE ... SET` аккумулируются и сохраняются в `sql_columns` (+228к колонок в индексе FA, ранее терявшихся).

Верификация на FA (пересборка `codebase init`, build 1549): целевые имена-мусор = 0; only-update имена 8 011 → 102; `##` → 22; хинты → ~38 в экзотических путях (`dfm_embedded`/underscore-токены — вне скоупа, кандидаты в отдельный репорт). Воронки: `Tests/SQL-пак для DBeaver.txt`.

Сопутствующая доработка: фикс семантики `--modified` (`runner.go` — pre-filter только при `onlyModified`); полная пересборка через `update --modified=false` непрактична (per-file каскадные DELETE) — предложение в `Modifications/update-full-rebuild-truncate-c41d7f.md`.
