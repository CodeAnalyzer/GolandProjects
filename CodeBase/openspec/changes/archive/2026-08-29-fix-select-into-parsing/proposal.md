## Why

SQL-парсер некорректно обрабатывает строки, открывающие блочный комментарий `/*`: строка `/* create table tConsRuleAccSync` устанавливает `inCreateTableDefinition = true`, но не пропускается (отсутствует `continue`). В результате `inCreateTableDefinition` остаётся активным, и строки SELECT-проекции из последующего `SELECT ... INTO` парсятся как определения колонок CREATE TABLE — с неверными именами (`lat.Brief` вместо `TypeRule`), неверными типами (`as` вместо реальных), и мусорными колонками из CASE-выражений (`else`, `trim(rn.Brief)),`). Дополнительно `splitSQLByTopLevelComma` не фильтрует `--` комментарии, ломая извлечение имён колонок для `select_into`.

## What Changes

- **Баг #1**: Добавить `continue` после установки `inBlockComment = true` в основном цикле парсера — строка, открывающая блочный комментарий, не должна обрабатываться дальше (включая матчинг `createTableRe`, `selectIntoRe` и т.д.)
- **Баг #2**: Добавить `ELSE` в карту ключевых слов `isKeyword` — чтобы `else` внутри CASE-выражений не парсился как columnName
- **Баг #3**: Фильтровать `--` комментарии в `splitSQLByTopLevelComma` перед разделением по запятым — чтобы запятые внутри комментариев не создавали лишние сегменты
- Регрессионные тесты для всех трёх багов на основе файла `patch7_2_1056.sql`

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/sql-parsing`: исправление требований по обработке блочных комментариев и SELECT INTO проекций

## Impact

- `internal/parser/sql/sql_parser.go` — три точечных фикса (строки ~766, ~1848, ~157)
- `internal/parser/sql/sql_parser_test.go` — новые регрессионные тесты
- Затронутый SQL-файл (тестовый): `fa-contracts/Consumer/SERVER/Patch/patch7_2_1056.sql`
- Результат `codebase_query_table_schema` для `tConsRuleAccSync` изменится: вместо ~18 мусорных колонок `create_table` с типами `as`/`""`  будут корректные колонки `select_into` с `DSUNKNOWN` (с последующим enrich через `enrichSelectIntoDataTypes`)
