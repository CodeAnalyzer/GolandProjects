# Code-review: `table-existence-source-fixes` (реализация)

- Дата: 2026-11-27. Ревью рабочего дерева CodeBase (19 изменённых + 2 новых
  integration-теста; сборка `0.9.5` build `1727`, `cmd/root.go:29`) на соответствие
  артефактам change (обновлённым по итогам change-review) и на отсутствие регресса.
- Проверено исполнением: `gofmt -l` пусто; `go vet ./...`, `go build ./...` — чисто;
  `go test` parser/sql, store, review, indexer, model — ok; `go test -tags integration`
  (Postgres доступен, skip'ов нет) store/review/indexer — все PASS, включая новые
  `TestInitSchema_RegTableNameV9`, `TestTableExistenceSourceBackfills`,
  `TestRegTableIndexing`, `TestRegTableUpdateIncremental`,
  `TestCheckTableExistsInMetadataRegistryScope`; `openspec validate --all --strict` —
  41/41; tasks.md — 17/17 отмечено.

## Вердикт

Реализация соответствует design D1–D5 и дельта-спекам, все замечания change-review
(H1, M1–M4, L1–L7) закрыты, регрессов не найдено. Код замечаний: 1 среднее (латентное
на корпусе FA — рекомендация или запись в Risks), 3 низких. К архивированию готово.

## Независимая верификация на живом индексе FA

| Инвариант | Результат |
|---|---|
| Маркеры `reg_table_name_backfill_v1`, `sql_table_creators_backfill_v1`, `sql_tables_prefix_garbage_cleanup_v1`, `codebase_schema_v9` | 4/4 |
| Мусор `dbo`/`middle`: `sql_tables`, `sql_columns`, `sql_column_definitions`, `symbols` (table/procedure), `sql_index_definitions`, `sql_procedures` | 0 во всех |
| Рёбра: dangling target `sql_table` / `sql_procedure` | 0 / 0 |
| Кривые `alter_add` (CONSTRAINT…DEFAULT, COLUMN) | 0 |
| `reg_table_name` | 1186 строк = эталон; 851 distinct; 3 скобочных имени (`tPoolAccount*`) присутствуют; все — из `.sql` |
| Создатели tReplTables / pAPI_ExpProperty_Type / dcSubcontoConnect / pAPI_ExpProperty_Value | 4/4 |
| Backfill-эквивалентность (независимый двухформенный regexp по фрагментам ↔ строки `create`) | 436/436 |
| Ручные кейсы 5.3 (`CodeBase.exe review --rules tableExistsInMetadata`) | `eis_5nt.sql` ×5 → 0; `ParticipantSB_Delete.sql` → 0; `api_pAPI_ExpProperty_Type_nonpart_tmptbl.sql` → 0; контроль `ContractMassLoad.sql` → finding `pAPI_Account_MassClose` сохранён, текст сообщения с «реестр таблиц продукта» |
| Прогон парсера на 20 сниппетах (префиксы `dbo.`/`MIDDLE..`, UNLOGGED/TEMP, constraint-семья, ADD COLUMN, реестр со скобками и `--`, `sys.`-пропуск, продолжения FROM, регресс-сценарии спеки) | ожидаемые таблицы/колонки/определения; регресс-сценарии («FROM через запятую», «INSERT INTO + VALUES на следующей строке», plain `create table`) без изменений |

## Замечания к коду

### Средняя серьёзность

**C1. Остаточный мусор `sql_columns` для межстрочных квалифицированных колонок.**
- Где: `internal/parser/sql/sql_parser.go:1932-1940` — предохранитель опирается на
  построчный `lineSeenTables` и `alterLineTarget` текущей строки.
- Что: `select a.x from t a` + следующая строка `  and dbo.t.id = 1` → на строке
  продолжения `t` не «видна», пара `(dbo, t)` уходит в `pendingColumns` →
  `sql_columns(table_name='dbo', column_name='t')`. Воспроизведено прогоном парсера;
  однострочная форма (`where dbo.t.id = 1` в строке FROM) закрывается корректно.
- Масштаб: в fa-payments/fa-contracts трёхсегментных ссылок `dbo.t.col` — 0 (rg),
  очистка миграции — одноразовый снимок; дефект латентный, но claim D1 «закрывает мусор
  в `sql_columns`» верен только для той же строки.
- Рекомендация: множество таблиц уровня оператора (сброс в `resetStatementState`/
  `flushStatement`) вместо построчного; в pending-пути пропускать пару, когда
  квалификатор — не алиас и не таблица оператора, а правый сегмент — таблица оператора.
  Либо зафиксировать в Risks как известное ограничение. Тест-кейс — сниппет выше.

### Низкая серьёзность

**C2. Ветка `.h`-файлов с SQL-маркерами теряет `RegTables`.**
- `internal/indexer/indexer.go:480-590` не вызывает `BatchInsertRegTables`, тогда как
  backfill из фрагментов покрывает любой тип файла — асимметрия. В FA реестр только в
  `.sql` (0 не-`.sql` файлов с маркером) — латентно. Добавить вставку рядом с
  `tablesBatch` (`:575`) либо зафиксировать допущение тестом.

**C3. `sys.`-пропуск не регистрирует алиас.**
- `sql_parser.go:1686-1690`: `continue` перед блоком регистрации алиасов → для
  `from sys.objects o` пары `o.object_id` пишутся как `sql_columns(table_name='o')`
  вместо прежнего `'sys'`. Эквивалентный по качеству мусор, на findings не влияет
  (замер design: новых имён нет) — только задокументировать в D1.

**C4. FN-край: колонка, имя которой совпадает с таблицей той же строки.**
- `select a.tKey from tKey a` → пара `(a, tKey)` отбрасывается предохранителем `:1937`
  (правый сегмент — «виденная» таблица) — теряется легитимная колонка `tKey`. Редкий
  случай, принять как edge и упомянуть в комментарии.

## Наблюдения вне скоупа (не вызваны доработкой)

- 14 рёбер `executes_query` с «висячим» `source_id` (одна процедура, id 224817) —
  до-миграционное состояние, к удалённой процедуре `dbo` отношения не имеет.
- 1159 table-symbols без строки `sql_tables` и 5052 `create`-строк без symbol —
  асимметрия прочих путей вставки (PAS/JS-фрагменты), не затронута.
- 427 строк `sql_tables` с именами ≤ 2 символов (прочий алиас-шум) — как было.

## Закрытие замечаний change-review

| # | Статус | Где |
|---|---|---|
| H1 место вставки | закрыто | `indexer_sql_pas.go:59,154-160,314-321`; тесты используют `ProductTable.sql` |
| M1 скобочные имена | закрыто | `regTableNameRe` (`sql_parser.go:74`), миграция; 3 строки в индексе |
| M2 префикс в форме 1 | закрыто | `db_schema.go`, `sql_table_creators_backfill_v1` — полная префикс-группа |
| M3 инвентарь мусора | закрыто | cleanup: `sql_columns`, `sql_index_definitions`, `MIDDLE` (`db_schema.go:1398-1410`); в индексе 0 |
| M4 процедура `dbo` | закрыто | рёбра target + `sql_procedures` удалены; dangling 0 |
| L1 числа proposal/design | закрыто | proposal Why — 116 |
| L3 `selectIntoTableRe` | не тронут | мёртвый регэксп оставлен |
| L4 имя типа | закрыто | поле `RegTables` в `ParseResult` (`sql_parser.go:504`) |
| L5 техника теста миграций | закрыто | `db_schema_reg_backfill_integration_test.go:236` — сброс 3 маркеров + `ResetSchemaVersion` |
| L6 ADD COLUMN | закрыто | `alterAddColumnWordRe` (`sql_parser.go:78`), cleanup включает `COLUMN` |
| L7 комментарии в backfill | закрыто | `regexp_replace(…'(?m)^[ \t]*--[^\n]*'…)` в обеих backfill-миграциях |
| L2 факт о `[dbo].[t]` | закрыто | Non-Goals/design исправлены (выборочная проверка) |

## Итог

Код соответствует запланированному, все проверки зелёные, инварианты миграции
воспроизведены независимо. Перед архивированием: C1 — либо правка (множество таблиц
уровня оператора + тест), либо запись в Risks; C2–C4 — по усмотрению (C2 — две строки
кода). Временные пробы в `Tests/Temp` удалены.
