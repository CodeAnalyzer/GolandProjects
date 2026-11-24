## Context

См. `proposal.md` — Why. Текущая реализация (design `add-table-exists-in-metadata`,
D2/D4): единственный источник «таблица существует» —
`store.BatchLookupTableCreators` (`sql_tables.context IN ('create','select_into')`,
`sql_column_definitions.definition_kind='select_into'`, `api_contract_tables`,
`api_business_object_tables`), prewarm в `tableCreatorCache`, правило
`checkTableExistsInMetadata` читает кэш через `cachedLookupTableCreator`.
`checkIndexExistsInDB` (`analyzeStatementForIndexExists`, `checkDeletePtableMacros`)
про существование таблицы не знает вовсе: `lookupIndexExists` по пустому списку
кандидатов даёт finding.

Уже проиндексированные, но не используемые как свидетельства данные
(парсер `internal/parser/sql/sql_parser.go`):

| Свидетельство | Регэксп парсера | Таблица индекса |
|---|---|---|
| `ALTER TABLE t ADD …` | `alterTableAddRe` | `sql_column_definitions`, kind `alter_add` |
| `M_ADD_FIELD('t', …)` | `mAddFieldRe` | `sql_column_definitions`, kind `macro_add_field` |
| `CREATE [UNIQUE] INDEX … ON t(…)` | `createIndexRe` | `sql_index_definitions`, kind `create_index` |
| `M_CRT_INDEX(…, 'ix', 't', …)` | `mCreateIndexRe` | `sql_index_definitions`, kind `macro_create_index` |

Кейс: `ParticipantSB_Delete.sql` — `tRequisitParticipantSB` без `CREATE TABLE`
везде в FA; в индексе есть kind `macro_add_field` (patch7_2_271) и
`macro_create_index` (patch7_2_273), `XPK…` отсутствует.

Индексы БД: `idx_sql_column_definitions_lower_table_col (LOWER(table_name),
LOWER(column_name))` — годится; для `sql_index_definitions` есть только
`idx_sql_index_definitions_table_name (table_name)` без `LOWER()`.

## Goals / Non-Goals

**Goals:**

- Свидетельства изменения структуры (добавление колонки, создание индекса)
  подтверждают существование таблицы в `tableExistsInMetadata` в той же
  областной привязке, что и создающие источники.
- `indexExistsInDB` молчит для таблиц без создающего источника (обе ветки:
  index-хинты и `M_DELETE_PTABLE*`), без изменения поведения для таблиц с DDL
  или DSArchitect-декларацией.
- Ноль миграций данных и реиндексации: только чтение уже проиндексированных
  записей.

**Non-Goals:**

- Реестр `__REG_TABLE_NAME__` в `DB/ProductTable.sql`, `GRANT/REVOKE … ON`,
  `ALTER TABLE … ALTER COLUMN`, `M_MODIFY_FIELD`/`M_DROP_FIELD`,
  `M_LOCKSCHEME` — не индексируются как свидетельства (кандидат на отдельное
  изменение, см. Risks).
- Изменение набора контекстов DML и фильтров шума D6 — нет.
- Информационный finding «DDL таблицы отсутствует» — нет (c2 отклонён).

## Decisions

### D1. Два lookup'а вместо одного: создающие источники и свидетельства

`BatchLookupTableCreators` не меняется. Добавляется
`store.BatchLookupTableExistenceEvidence(ctx, names, pairs) (map[string]bool, error)`:
`UNION ALL` двух ветвей с предикатом `LOWER(table_name) = ANY($1)` в каждой
(прецедент — D2 исходного design), с `JOIN files f` и тем же `areaFilter`
(`unnest` product/branch), два варианта — с непустыми `pairs` и глобальный:

```sql
SELECT LOWER(scd.table_name) FROM sql_column_definitions scd JOIN files f ON f.id = scd.file_id
 WHERE scd.definition_kind IN ('alter_add','macro_add_field') AND LOWER(scd.table_name) = ANY($1) <areaFilter>
UNION ALL
SELECT LOWER(i.table_name) FROM sql_index_definitions i JOIN files f ON f.id = i.file_id
 WHERE i.definition_kind IN ('create_index','macro_create_index') AND LOWER(i.table_name) = ANY($1) <areaFilter>
```

Фильтр по kind в обеих ветвях перечисляет все существующие значения
(`sql_index_definitions.definition_kind` — только `create_index`/
`macro_create_index`): явное соответствие видам свидетельств в спеке; новый
kind парсера потребует расширения списка.

Альтернативы: расширить существующий lookup — отклонено, гейт
`indexExistsInDB` (D4) нуждается именно в предикате «создающий источник»;
возвращать из одного запроса пары (имя, вид) — отклонено, ломает сигнатуру и
prewarm существующего lookup'а ради экономии одного round-trip на файл.

Индекс `idx_sql_index_definitions_table_name_lower ON sql_index_definitions
(LOWER(table_name))` добавляется в DDL-список `db_schema.go` с bump
`CurrentSchemaVersion` (readonly-schema-policy): без него ветвь по индексам —
seq scan на каждый файл. Применение индекса планировщиком проверяется
`EXPLAIN` ветви, а миграция — интеграционным тестом по образцу
`TestInitSchema_MemoryTablesV7` (`testutil.ResetSchemaVersion` →
`InitSchemaCtx` → `pg_indexes.indexdef` содержит `LOWER(table_name)`), а не
только ручным `\d`. Альтернатива «сопоставление по `table_name` без LOWER в
исходном регистре обращения» отклонена: нарушает регистронезависимость
требования.

### D2. Порядок проверки в `checkTableExistsInMetadata`

После `cachedLookupTableCreator == false` выполняется
`cachedLookupTableEvidence`; `true` — пропуск (до `isViewTable`, как создатель).
Свидетельство берётся из снимка индекса, а не из `parsed` проверяемого файла.
Следствие, которое фиксируется тестом: проверяемый файл сам проиндексирован
(`getIndexedFile`), поэтому его `M_ADD_FIELD`/`CREATE INDEX` при актуальном
индексе попадают в свидетельства и finding подавляют — в том числе для
опечатки в самом файле. «Индекс, а не parsed» даёт только одно наблюдаемое
свойство: при устаревшем индексе свидетельство, добавленное в файл после
последнего `codebase update`, не учитывается (finding остаётся до обновления
индекса).

Полное исключение same-file-свидетельств (сравнение `file_id` источника с
`files.id` проверяемого файла) отклонено: lookup возвращает только имена
(нужно менять сигнатуру и prewarm), а файл, легитимно добавляющий колонку или
индекс таблице, которую сам же использует, получил бы ложный finding.
Самоподтверждение опечатки остаётся осознанным остаточным риском
(Risks): патч с опечаткой падает на установке, а ушедшие имена разбираются в
D6.

Сообщение finding: «Таблица %s отсутствует в метаданных: не найден создающий DDL,
изменение структуры патчем, SELECT INTO или объявление DSArchitect».

### D3. Кэш и prewarm свидетельств

`Runner.tableEvidenceCache map[string]bool`, сброс в `runSQLFileCtx`,
`prewarmTableEvidence(ctx, parsed)` по образцу `prewarmTableCreators`
(негативные записи `false`, до старта параллельных правил; при ошибке кэш `nil`
и ленивый путь), `cachedLookupTableEvidence` — чтение кэша, при промахе
одиночный batch-запрос без записи в общий map (паттерн
`cachedLookupTableProductIDs`). Набор имён DML-контекстов вынести в общий
хелпер `dmlTableNames(parsed)`, используемый обоими prewarm'ами. Условие prewarm:
`ruleSet[RuleTableExistsInMetadata]`. Ошибка БД в ленивом пути → `false`
(fail-closed: даёт finding, а не пропуск дефекта).

### D4. Гейт `indexExistsInDB` по создающему источнику

Хелпер `indexCheckVerifiable(ctx, tableName) bool` — предикат создающего
источника без изменений (`create`/`select_into`/api-таблицы), со своим
порядком разрешения (отличается от `cachedLookupTableCreator`, где `db == nil`
→ `false`):

1. в непустом `tableCreatorCache` есть запись для имени (включая `false`) →
   вернуть её;
2. иначе `r.db == nil` → `true` (тестовый seam: гейт инертен, проверка
   выполняется — иначе unit-тесты `indexExistsInDB` без БД потеряли бы
   находки);
3. иначе `cachedLookupTableCreator` (ленивый batch-запрос; имена p-таблиц из
   `M_DELETE_PTABLE*` в DML-наборе prewarm'а отсутствуют и идут этим путём).

Единственная непротиворечивая комбинация для тестов: unit-тест «кэш `false` +
хинт → нет finding» проходит по шагу 1, существующие тесты с пустым кэшем и
`db == nil` (`review_block_comment_test.go`, `review_rule_coverage_test.go`)
остаются инертными по шагу 2.

Вызовы: `analyzeStatementForIndexExists` — после дедупликации и числового
фильтра, до `lookupIndexExists` (экономит загрузку кандидатов);
`checkDeletePtableMacros` — перед `lookupIndexExists`. DSArchitect-декларация
считается создающим источником (`BatchLookupTableCreators` скоупит API-таблицы
по `files` через `areaFilter`) — для API-таблиц разрешённой области проверка
остаётся активной; это доказывается интеграционным тестом.

Ошибки БД: ленивый lookup → `false` → гейт подавляет (fail-open);
транзиентный сбой даёт ложное молчание только по индексам, table-level finding
остаётся fail-closed (асимметрия осознанная).

Prewarm создателей: условие в `runSQLFileCtx` становится
`ruleSet[RuleTableExistsInMetadata] || ruleSet[RuleIndexExistsInDB]`.

Тестирование гейта: при `db == nil` все unit-тесты идут мимо шага 3, поэтому
подавление доказывается интеграционными тестами через `RunSQLFileCtx`
(реальная БД, фикстура `seedReviewScopeFixture`): (а) нет создателя + хинт →
нет finding; (б) создатель есть + отсутствующий индекс → finding;
(в) `M_DELETE_PTABLE_INDEX` на таблице без создателя → нет finding;
(г) API-таблица в разрешённой области → гейт пройден, finding по отсутствующему
индексу. Фикстуры существующих интеграционных тестов, ожидающих finding
`indexExistsInDB`, должны содержать создателя таблицы (`insertTableCreate`),
иначе гейт их погасит.

### D5. Область видимости

Свидетельства подчиняются режиму existence `review/review-scope` через тот же
`areaFilter`, что и создающие источники. В дельте `review/review-scope`
уточнена формулировка требования «Область видимости EXT-продуктов»
(свидетельство патча — тоже подтверждение существования в объединённой области
`develop ∪ prj`, ветка вне области не подтверждает и свидетельством) и добавлен
сценарий prj-файла с таблицей, изменённой патчем из ext-develop; сценарий
веточной привязки своего продукта — в дельте `deploy-stoppers`.

### D6. Замер шума до/после

Протокол — design `add-table-exists-in-metadata`, ## Measurements: грубый
SQL-фильтр кандидатов по индексу fa-contracts → end-to-end `codebase review
--rules tableExistsInMetadata --json` по кандидатам → уникальные имена и файлы
с ≥1 finding. Критерий приёмки — сравнение двух прогонов на **одном снимке
индекса и соседних сборках**: baseline снимается до правок на текущей сборке
(`buildNumber 1718`), «после» — на сборке с изменением. Исторический контроль
100 имён / 239 файла (build 1684) — только справка: между 1684 и 1718 правило
менялось (атрибуция view → `viewUsageInServerCode`), поэтому он не годится как
критерий. Ожидание: «после» не больше baseline; список ушедших имён разбирается
вручную — каждое должно объясняться свидетельством (`alter_add`/
`macro_add_field`/индекс), имя, похожее на опечатку (самоподтверждение), —
эскалация. Скрипты и оба результата — в `Tests/Temp`, итог — в этом design
(секция Measurements).

## Risks / Trade-offs

- [Самоподтверждение опечатки: `M_ADD_FIELD('tRequisitParticipntSB', …)` в
  проиндексированном патче делает опечатку «существующей»; same-file
  свидетельство при актуальном индексе тоже подавляет finding (D2)] →
  осознанный остаточный риск: разбор ушедших имён в D6; патч с опечаткой падает
  на установке.
- [Гейт намеренно игнорирует свидетельства: у старой таблицы, существующей по
  патчу, реально отсутствующий или опечатанный индекс (в т.ч. `XPK…`) найден не
  будет] → заявленная цена устранения FP; закрывается только индексацией DDL
  таких таблиц (кандидат — реестр `DB/ProductTable.sql`).
- [Fail-open гейта индексов при ошибке БД] → table-level finding остаётся
  fail-closed; в проде ленивый путь не срабатывает благодаря prewarm.
- [Подавление гейтом недоказуемо unit-тестами (`db == nil` — гейт инертен)] →
  интеграционные тесты через `RunSQLFileCtx` (D4), включая API-таблицу в
  разрешённой области.
- [Непокрытые виды свидетельств (`__REG_TABLE_NAME__`, GRANT/REVOKE,
  `ALTER COLUMN`, `M_MODIFY_FIELD`)] → FP остаются для таблиц, у которых нет
  ни одного покрытого свидетельства; кандидат-последователь — индексация
  реестра `DB/ProductTable.sql` (в fa-payments/Payments покрывает все 29
  древних таблиц по построению).
- [Seq scan `sql_index_definitions` без lower-индекса] → D1 добавляет индекс;
  schema bump обязателен, иначе fast-path не применит DDL.
- [Устаревший индекс: новый патч-свидетельство не в индексе] → `codebase update`
  перед review, как и для создателей.
- [Парсер: `ALTER TABLE … ADD CONSTRAINT` попадает в `sql_column_definitions`
  как `alter_add` с именем колонки `CONSTRAINT` (в индексе FA это единственные
  2 записи `alter_add`)] → для свидетельства это всё равно подтверждение
  существования таблицы; точность извлечения колонки — вне области изменения.
- [Гейт не учитывает same-file `CREATE TABLE`/`SELECT INTO` из `parsed`
  (code-review F1): при устаревшем индексе файл, сам создающий таблицу и
  использующий на ней index-хинт, не получит finding за отсутствующий индекс] →
  остаточный риск вне контракта «`codebase update` перед review»: в FA это
  1 521 файл из 35 083 с хинтами (в 1 389 из них индекс тоже создан в файле —
  раньше там был FP; 126 — с регулярной таблицей). Полное закрытие — same-file
  учёт таблицы и индекса в гейте, кандидат на отдельное изменение.
- [Ленивый creator-запрос гейта для макро-таблиц `M_DELETE_PTABLE*` вне
  DML-набора prewarm'а — по запросу на пару `table|index`, без мемоизации
  (code-review F3)] → известное ограничение: запрос точечный (по индексу), а
  для таблиц без создателя гейт экономит загрузку кандидатов индексов;
  мемоизация по таблице в точке вызова — кандидат follow-up.

## Measurements

Корпус — все 3 783 `.sql` файла `FA\fa-contracts\Consumer\SERVER` (полный
прогон правила, без SQL-грубого фильтра кандидатов: он нужен был только для
сокращения прогона на 35 691 файле). Протокол — design
`add-table-exists-in-metadata` (## Measurements): `codebase review --rules
tableExistsInMetadata --json` по списку файлов → уникальные имена и файлы
с ≥1 finding. Скрипт и оба прогона — `Tests/Temp/tee_measure.ps1`,
`tee_baseline_*` / `tee_after_*`. Оба прогона — на одном снимке индекса FA
(206 606 файлов, `codebase update` между ними не переиндексировал ни одного
файла; применена только миграция схемы v8).

| Прогон | Сборка | Findings | Имена | Файлы | Время (чанк 2000 файлов) |
|---|---|---|---|---|---|
| baseline | 1718 | 246 | 49 | 40 | 892 с |
| после | 1719 | 215 | 47 | 37 | 879 с |

Исторический контроль (build 1684, весь fa-contracts, 35 691 файлов):
100 имён / 239 файла (0.67%) — только справка, корпус другой.

Имён и файлов не больше (49 → 47, 40 → 37), новых имён нет. Ушедшие имена
разобраны по индексу, каждое объясняется свидетельством:

- `tReplTables` (22 finding в `DB/eis_5nt.sql`) — `create unique index
  XPKtReplTables on tReplTables` в самом `eis_5nt.sql` (develop; те же
  копии ещё в 20+ модулях FA). Первопричина FP иная, чем «DDL вне исходников»:
  таблица создаётся там же строкой `create table dbo.tReplTables`, но
  `createTableRe` парсера (`create\s+table\s+([A-Za-z_#]…)`) захватывает
   `dbo` как имя таблицы — создатель с schema-префиксом не индексируется.
   Кандидат на отдельное изменение (парсер: `dbo.`-префикс в CREATE TABLE);
   масштаб: 121 вхождение `create table dbo.<имя>` в 86 файлах FA, 8 имён
   (`tseed` 60, `pseed` 27, `tReplTables` 25, ещё 5 по одному).
- `tOperTemplateDoc` (9 finding в `SCRIPT_UTILS/OperTemlateConverter*.sql`) —
  `M_ADD_FIELD('tOperTemplateDoc', …)` в патчах чужих продуктов:
  `fa-administrator/Administrator/Server/Patch/patch6_7_32.sql`,
  `fa-gaap/GAAP/SERVER/Patch/patch6_8_288.sql`, `patch6_8_333.sql` (develop =
  default-ветки, разрешённая область для файла fa-contracts). Базовая таблица
  ядра, опечатки нет.

Имен, похожих на опечатку (самоподтверждение), среди ушедших нет. Затраты на
prewarm свидетельств в пределах шума времени прогона.

Виды свидетельств в индексе FA (справка): `sql_column_definitions` —
`macro_add_field` 3 546, `alter_add` 2 (оба `ADD CONSTRAINT`); `sql_index_definitions` —
`create_index` 30 217, `macro_create_index` 23 610. Реально свидетельства дают
`M_ADD_FIELD` и индексы; `ALTER TABLE … ADD <колонка>` в FA практически не
встречается.

**Кейс**: `ParticipantSB_Delete.sql` — до: 4 finding (`indexExistsInDB` ×2,
`tableExistsInMetadata` ×2 на `tRequisitParticipantSB`), после: 0.
Контроль `ContractMassLoad.sql`: `pAPI_Account_MassClose` (строки 2567–2568)
и `pAPI_Acc_Mass*` — по-прежнему finding `tableExistsInMetadata`.

**fa-payments, древние таблицы** (реестр `Payments/SERVER/DB/ProductTable.sql`,
`__REG_TABLE_NAME__`): 161 таблица, у 29 нет создающего источника нигде в FA.
Свидетельства есть у 8 (`hDocRelation`, `hIRTAccountService`, `hKey`, `hSwift`,
`hSwiftLine`, `tAccSystemCash`, `tRequisitParticipantSB`, `tWebUser`), у 21 —
нет: `hDocAccount`, `hDocAnalytic`, `hDocument`, `hDocumentCurrTranCode`,
`hDocumentSwift`, `hDocumentTag*` (7), `hDocValueSet`, `tClientContract`,
`tClientService`, `tKey`, `tNovosibNumbers`, `tRequisitParticipantTelex`,
`tResourceDisposer`, `tWebUserCertificate`. Review 275 файлов fa-payments,
ссылающихся на эти 29 имён (build 1719): 1 216 finding в 174 файлах, все — на
5 непокрытых таблицах: `tKey` (623 finding / 147 файлов — системная таблица
ключей, обращения `update tKey`/`join tKey` в теле макросов), `tRequisitParticipantTelex`
(5/5), `hDocument`, `tResourceDisposer`, `tWebUserCertificate` (по 1); остальные
16 непокрытых имён находок не дают (вхождения не в DML-контексте таблицы). Все
21 имя — кандидаты на индексацию реестра `__REG_TABLE_NAME__`; после этого
`tKey` остаётся единственным массовым источником шума правила в fa-payments.

Наблюдение (вне области): review с одним правилом `tableExistsInMetadata`
идёт ~0.45 с/файл (чанк 2000 файлов ≈ 15 мин) — стоимость prewarm'ов правила
не меняется данным изменением, но общая скорость review — кандидат на
отдельный анализ.

## Migration Plan

- Схема: один дополнительный индекс + bump `CurrentSchemaVersion`; применяется
  `init`/`update`, без миграции данных и реиндексации.
- Откат: revert кода; индекс в БД безвреден.

## Open Questions

Нет.
