# Design: add-view-usage-rule

## Context

Review-движок (`internal/review`) исполняет правила параллельно из
`ruleCatalog`; на каждый файл `RunSQLFileCtx` сбрасывает кэши Runner.
Существуют два паттерна доступа к справочным множествам БД:

- prewarm (eager): batch-запрос до старта правил (`prewarmTableCreators` для
  `tableExistsInMetadata`, гейтится `ruleSet`);
- ленивый: `isMemoryTable` (review_lookup.go) — один `SELECT DISTINCT` под
  мьютексом при первом обращении, флаг загруженности различает «не запрошено»
  и «запрошено и пусто», при ошибке множество остаётся пустым до конца
  запуска.

Представления в индексе не имеют собственной сущности: SQL-парсер знает
только `createTableRe`, `create view` попадает лишь в `query_fragments`
(component_type='sql_script') как текст фрагмента. Замер индекса FA:
156 фрагментов `create [or replace] view`, 105 уникальных имён, 21 файл,
6 продуктов; 0 коллизий имён view с созданными таблицами; 49 обращений к
view в 30 SQL-файлах (47 select + 2 update; после свёртки параллельных ветвей
— 33 уникальных `(путь, строка, имя)`). В файле-определениях есть
view-поверх-view: `VIEW.SQL:347` (`ImportFileGroup → tImportFileGroup`),
`view_trash.sql:333` (`→ tFund`).

Производительность источника (замер `EXPLAIN (ANALYZE, BUFFERS)` на индексе
2.4М фрагментов): regex как index cond по GIN trigram
`idx_query_fragments_query_text_trgm` — Bitmap Index Scan, 156 строк,
**~27 мс**. (Попутно зафиксировано: `psql \timing` в русской локали печатает
десятичную запятую — «47,361 ms» есть 47.4 мс.)

Сегодня все 49 обращений к view дают ложные severity-1
`tableExistsInMetadata` — создателя-таблицы для этих имён нет и не будет
(подтверждено прогоном review: `USR_GPB_CreditCards2.sql` — 10 findings по
`tFund`, `VIEW.SQL` — 1 finding по `tImportFileGroup`).

## Goals / Non-Goals

**Goals:**

- Правило `viewUsageInServerCode` (severity 1) на всех `.sql`-файлах.
- Подавление `tableExistsInMetadata` для имён-view (атрибуция одному правилу).
- Без изменений схемы БД и без переиндексации.

**Non-Goals:**

- Видимость view в query-слое (`query table`/`symbol`/`inspect`) и связи
  view→таблицы — отдельная будущая работа (потребует парсерной сущности или
  материализации).
- Проверка клиентского кода (PAS/JS/DFM) — view там легальны.
- Детект bank-specific view, отсутствующих в репозитории (правило видит
  только индекс; неизвестное имя — FN, не FP).

## Decisions

### D1. Источник имён — review-time деривация из query_fragments (ленивый кэш)

Множество имён view строится одним SQL-запросом при первом обращении правила
по образцу `isMemoryTable`:

```sql
SELECT DISTINCT (regexp_match(query_text, '(?i)create\s+(?:or\s+replace\s+)?view\s+([A-Za-z_#][A-Za-z0-9_#.]*)'))[1]
FROM query_fragments
WHERE component_type = 'sql_script'
  AND query_text ~* '(?i)create\s+(?:or\s+replace\s+)?view\s+'
```

- SQL возвращает **сырые** имена как есть (без срезания схемы и lower):
  нормализация выполняется в Go единственной точкой — `normalizeTableKey`
  (срезает всё до последней точки в любом регистре, снимает `[]`/`""`,
  lower), ключи кэша = нормализованные имена. Дублирование логики
  нормализации в SQL — скрытая мина (lowercase-only `public.`) и устраняется;
- `regexp_matches(..., 'g')` извлекает все определения фрагмента (2+
  `create view` в одном query_text); комментарии внутри query_text не
  вырезаются — assumption: `create view` в line/block-комментарии перед
  реальным определением в корпусе не встречается (замер: 0 фрагментов);
  `regexp_match` может вернуть NULL (квитированные/скобочные идентификаторы):
  сканирование через `sql.NullString`, NULL-строки пропускаются (0 на корпусе);
- regex вместо пары ILIKE: устойчив к whitespace (`create\tview`) и покрывает
  ASE `create view` и PG `create or replace view` одной веткой; препроцессорный
  префикс `#else` (7 фрагментов) не мешает — regex неякорный; trigram GIN
  проталкивает regex в index cond (~27 мс, замер в Context);
- Runner: `viewNameCache map[string]struct{}` + `viewNamesLoaded bool` +
  mutex; сброс в `RunSQLFileCtx` рядом со сбросом memory-кэша; при ошибке
  запроса ИЛИ ошибке итерации (`rows.Err()`) частичный кэш отбрасывается,
  флаг загруженности ставится — правило инертно до конца запуска (fail-open,
  без шторма запросов); nil-safe при `db == nil` (unit-тесты конструируют
  Runner без БД — паттерн `isMemoryTable`).

Альтернативы отвергнуты: материализация в пост-обработке индексации
(требует bump схемы и миграции ради 105 имён), парсерная сущность `createViewRe`
(hash pre-filter `internal/indexer/runner.go:319-324` не перепарсит
неизменённые файлы → потребовала бы полного re-init ~2 ч). Обе остаются
доступными как будущая миграция без смены потребительского контракта
(`isViewTable`).

### D2. Fail-open вместо fail-closed

`tableExistsInMetadata` при ошибке lookup возвращает «не найдено» (fail-closed:
ложный finding лучше пропуска дефекта). Новое правило — бан-правило: при сбое
БД оно обязано молчать (FN приемлем, FP нет). Правило и подавление (D4)
инертны при незагруженном множестве — `tableExistsInMetadata` на время сбоя
ведёт себя как сегодня, деградации нет.

### D3. Регистрация и контексты проверки

- `RuleViewUsageInServerCode RuleID = "viewUsageInServerCode"`, severity
  `SeverityDeployStopper`, описание «Использование представления (VIEW) в
  серверном коде»; `DefaultEnabledRules()` подхватывает обходом каталога.
- Проверяются `parsed.Tables` с контекстами `select/insert/update/delete/log`
  (симметрично `tableExistsInMetadata`, включая его работу на `.h`-файлах);
  его фильтры исключений (#-таблицы, курсоры, pseudo-таблицы, placeholder,
  литералы) переиспользуются как есть — единая точка фильтрации. Findings
  дедуплицируются по `name:line` и заполняют `CurrentProductID` по образцу
  `checkTableExistsInMetadata`.
- View-on-view (обращение тела одного view к другому) флагается — область
  «все SQL-файлы review (.sql/.h)» выбрана осознанно; имя точно view, FP
  исключён. Известные случаи в корпусе: `VIEW.SQL:347`
  (`ImportFileGroup → tImportFileGroup`), `view_trash.sql:333` (`→ tFund`).
  Сценарий «Определение представления не является нарушением» относится к
  телам view над обычными таблицами (сниппет `Batch`), а не ко всему файлу
  определений.
- Кэш не гейтится prewarm'ом: правило само лениво грузит множество (одна
  точка обращения, нет дублирования логики гейтинга).

### D4. Подавление tableExistsInMetadata

В `checkTableExistsInMetadata` перед формированием finding добавляется
проверка `isViewTable(name)`: если имя — view, finding не формируется
(атрибуция `viewUsageInServerCode`). Порядок неважен: правила параллельны и
независимы, каждый читает ленивый кэш (мьютекс), гонки нет. Подавление
работает и при запуске `--rules tableExistsInMetadata` (правило лениво
загружает множество само); при ошибке загрузки имя «не распознано» и
`tableExistsInMetadata` ведёт себя как сегодня.

## Risks / Trade-offs

- [FN: qualified-обращение к view (`from dbo.tView`)] → известное
  ограничение парсера: `tableNameRe` (internal/parser/sql/sql_parser.go:35)
  срезает qualified-имя до первого токена (таблицей становится `dbo`) —
  ограничение общее для всех правил, не только view; на корпусе 0
  квалифицированных ссылок на view-имена; поведение запинено тестом
  `TestCheckViewUsageInServerCode_QualifiedReferenceParserLimitation`.
  Возможный фикс — расширение класса символов `tableNameRe` точкой —
  отдельным change (потребует ревизии всех правил, читающих parsed.Tables).
- [Ложное срабатывание при шэдоуинге: файл создаёт локальную таблицу с
  именем view из другой ветки] → правило пропускает имена из `created`
  (CREATE TABLE в файле) и `select_into` из parsed.ColumnDefinitions —
  симметрия с `checkTableExistsInMetadata`; generic-имена view (Account,
  Balance, Branch, ...) делают защиту обязательной, тест
  `TestCheckViewUsageInServerCode_LocalTableShadowsView`.
- [FN: view определён в банке, но не в репозитории] → принято: правило
  документирует стандарт для кода поставки; неизвестные имена не флагаются.
- [FN: экзотический whitespace в `create view`] → regex `\s+` покрывает
  табы/переводы строк; на текущем корпусе 156/156 фрагментов матчатся
  (совпадение с ILIKE-подсчётом).
- [Branch-scope: глобальный набор view-имён vs branch-scoped
  `tableExistsInMetadata`] → принято как FN: если имя определено как view в
  чужой ветке, а в проверяемой ветке это отсутствующая таблица, подавление
  скроет `tableExistsInMetadata`, а новое правило даст `viewUsageInServerCode`
  — дефект не пройдёт молча, но атрибутируется иначе. Коллизий view↔таблица
  на корпусе 0 (включая API- и memory-таблицы); 9 имён view определены в
  двух параллельных ветках одного продукта — это не конфликт. Область
  множества не сужается до области файла: view — объекты общего namespace БД
  (паттерн `memory_tables`), а бан-семантика правила банк-глобальна.
- [Выкатка: severity-1 по умолчанию подсветит существующий долг — 49
  обращений в 30 файлах] → принято осознанно: это реальные нарушения
  стандарта, видимость долга и есть цель правила. CI/пакетные прогоны могут
  исключать правило через `--rules` без него или `--min-severity`;
  baseline/waiver-механика в change не входит (Non-Goal). Топ-долг
  концентрирован: `tfund` — 27 из 49 ссылок.
- [Зависимость от планировщика PG: деградация trigram GIN] → fallback-поведение
  при таймауте/ошибке — fail-open (правило инертно). Фактический baseline
  (build 1697, индекс FA 2026-11-14), `EXPLAIN (ANALYZE, BUFFERS)` запроса
  из D1: `Bitmap Heap Scan on query_fragments (actual rows=156) → Bitmap
  Index Scan on idx_query_fragments_query_text_trgm, Execution Time:
  26.865 ms`; изолированная стоимость правила на тяжёлом файле
  (`USR_GPB_CreditCards2.sql`, `--rules viewUsageInServerCode` vs
  `--rules shouldBeCP866`): +25–55 мс на файл (порог приёмки ≤ 0.2 с);
  при систематической деградации миграция на материализацию (D1).
- [Ложное подавление `tableExistsInMetadata`, если имя одновременно view в
  одной ветке и таблица в другой] → на корпусе 0 коллизий; namespace БД
  общий, конфликт определений невозможен в одной БД; риск принят.
- [~30 мс на файл (холодный кэш Runner)] → пренебрежимо на фоне полного
  review файла; кэш один на файл, не на правило.

## Migration Plan

Изменение чисто кодовое: без DDL, без bump `CurrentSchemaVersion`, без
переиндексации. Выкатка — пересборка бинарника; откат — revert. После
реализации прогнать review на известных файлах-нарушителях и убедиться,
что ложные `tableExistsInMetadata` на view-имена исчезли (замер до/после:
`USR_GPB_CreditCards2.sql` — 10 findings `tFund`, `VIEW.SQL` — 1 finding
`tImportFileGroup`). Существующий долг (49 ссылок) остаётся видимым до
исправления командами-владельцами.

Методика приёмки 45/26 (воспроизводимость): набор файлов — SQL по индексу
(`sql_tables` DML-контексты join с именами view из `query_fragments`,
без `#`-имён — 30 путей), на каждом `codebase review <path> --json`
(build 1697), счёт — `viewUsageInServerCode` findings после дедупликации
правила `name:line`. Итог: 45 находок в 26 файлах из 30 (4 файла без
находок — ссылки в них отфильтрованы шумо-фильтрами правила), 0 остаточных
`tableExistsInMetadata` на view-имена. Попутно зафиксирован предсуществующий
ложный `tableExistsInMetadata(object=grant)` в `view_trash.sql:52`
(`GRANT ... TO public` парсится как таблица `grant`) — не регрессия данного
change, устранение (стоп-имена `grant`/`revoke` в `isTableExistsNoiseName`
или фикс парсера) — кандидат в отдельный change.

## Open Questions

(нет)
