# Change review: memory-unique-index-update

Дата ревью: 2026-11-09. Ревью выполнено без правок кода (только этот файл).

**Статус изменения:** на момент ревью в коде реализации нет — `rg` по
`memoryUniqueIndexUpdate`/`isMemoryTable`/`memoryTableCache` вне `openspec/`
ничего не находит, папка `openspec/changes/memory-unique-index-update/`
untracked, все задачи `tasks.md` не отмечены. Это **pre-implementation**
ревью плана против текущего кода и текущего индекса FA.

**Проверено:**

- Артефакты: `proposal.md`, `design.md`, `tasks.md`, дельта
  `specs/review/deploy-stoppers/spec.md`, `.openspec.yaml`.
- Код-цели: `internal/review/types.go` (`tableIndexCandidate`,
  `RuleID`, `Finding`), `internal/review/catalog.go` (`ruleCatalog`,
  `DefaultEnabledRules`), `internal/review/runner.go` (кэши, `RunSQLFileCtx`,
  `enabledRuleSet`), `internal/review/review_rules.go`
  (`checkDeferredUpdate`), `internal/review/review_lookup.go`
  (`batchLoadIndexCandidates`, `lookupTableIndexCandidates`,
  `extractUpdateTargetTable`, `extractTablesFromFromClause`),
  `internal/review/review_parser.go` (`splitStatementsWithOffsets`,
  `extractSetColumns`), `internal/review/review_helpers.go`
  (`normalizeIdentifier`, `sameTableReference`, `enabledRuleSet`).
- Источники данных: `internal/store/db_schema.go` (v7: `memory_tables`,
  `sql_index_definitions`, `api_business_object_table_indexes`),
  `internal/parser/sql/sql_parser.go` (извлечение индексов, `UNIQUE`,
  `M_CRT_INDEX`), `internal/parser/dsxml/parser.go` (ветки API/PTable,
  `IndexType`), `internal/query/query_sql.go`, `internal/query/api_query.go`
  (прецедент name-based memory join).
- Спеки: `openspec/specs/review/deploy-stoppers/spec.md`,
  `openspec/specs/indexing/lst-parsing/spec.md`.
- Документация: `README.md` (списки правил), `cmd/root.go`
  (`version`/`buildNumber`).

**Автоматическая проверка (выполнена):** `openspec validate
memory-unique-index-update --strict` — valid; `openspec validate --all
--strict` — 37 passed, 0 failed. Текущий бинарник: `CodeBase 0.9.5 build 1679`
(`cmd/root.go:28-29`).

## Живые проверки (read-only)

БД: `localhost:5435/codebase` (psql, только SELECT). Дерево: `D:/GITHUB/GolandProjects/FA`.

- `memory_tables`: 26 строк, 26 различных `LOWER(table_name)` — совпадает с
  `ModuleTableInMemory.lst` (26 деклараций).
- **Покрытие предложенным критерием** (`sql_index_definitions.is_unique = true`
  ИЛИ `api_business_object_table_indexes.index_type = 2`): **2 таблицы из 26**:
  - `pRuleDoc_QtyStack` — `XPKpRuleDoc_QtyStack`, `is_unique = true`
    (`M_CRT_INDEX('UNIQUE', ...)`), поля `SPID, DealProtocolID, Brief`;
  - `pAPI_LstTpl` — `XPKpAPI_LstTpl`, `index_type = 2`, поля
    `SPID, TemplateID, ActionServiceOrder, FinOperServiceID`.
  - Остальные 24 таблицы не имеют ни `is_unique=true`, ни `index_type=2`:
    22 API-таблицы имеют `XPK` с `index_type` 0/3 (реже только `XIE` с 0),
    `pObjectForRight` имеет `XPK` с `is_unique=false`
    (`M_CRT_INDEX('', ...)`), `pCTT_FO_TemplateExtraAttr` вообще без индексов
    в индексе.
- SQL-источник для memory-таблиц: единственный `is_unique=true` —
  `pRuleDoc_QtyStack`; `pObjectForRight` (`XPK` и `XIE1`) — `false`;
  `pAPI_Partc_Code` (`XPK`, `create_index`) — `false`.
- API-источник для memory-таблиц: единственный `index_type=2` — `pAPI_LstTpl`;
  у всех прочих `index_type` 0 или 3 (включая `pAPI_AttrValue`,
  `pAPI_AutoNum_ExtraAttr`, `pAPI_Property_Value`, `pAPI_RuleAcc_TypeID`).
- Распределение `index_type` по всей БД (не только memory):
  `0`: XPK 3088, XAK 40, XIE 2222; `1`: XPK 750; `2`: XPK 437, XAK 222,
  XIE 9, XKP 1; `3`: XPK 5829. Кросс-сверка API↔SQL по одинаковым именам
  индексов: при `api_type=2` у `XPK` встречается 1 SQL-строка с
  `is_unique=false`; при `api_type=3` — 162 SQL-строки с `is_unique=true`;
  при `api_type=0` — 20 SQL-строк с `is_unique=true`. То есть
  `index_type` **не является** надёжным признаком уникальности.
- `sql_index_definitions` с `index_type='_UNIQUE_'` и
  `is_unique=false`: **987 строк** (654 SQL-файла FA). Пример:
  `XAK1tWarPropertyUsr` объявлен как `M_CRT_INDEX('_UNIQUE_', ...)` в
  `FA/fa-warranty/Warranty/SERVER/Patch/patch11_1_644.sql:104`, спека
  `fa-warranty` называет его уникальным, а в индексе он `is_unique=false`.
- Данные сценариев дельты:
  - `pRuleDoc_QtyStack` (`FA/fa-gaap/GAAP/SERVER/TmpTbl/pRuleDoc_QtyStack_TmpTbl.sql:14-29`):
    колонки `SPID, Brief, DealProtocolID, Qty, FundID, ID, Date`;
    колонки `QtyStack` и `Comment` **не существуют**; уникальный `XPK`
    содержит `SPID, DealProtocolID, Brief`; колонка `ID` в PK не входит.
  - `pAPI_AttrValue` (`FA/fa-administrator/API_Administrator/DSArchitectData/BObject/ExtraAttrib/Table/pAPI_AttrValue.xml:3-54`):
    колонки `Name, Date, PKey, Value, Brief`; колонки `AttrValue`
    **не существует**; единственный индекс — `XPKpAPI_AttrValue`
    (`IndexType=0`) по `SPID, Name`.
  - `pAPI_LstTpl` (`.../API_Administrator_PTbl/Table/pAPI_LstTpl.xml:112-114`):
    колонка `TemplateID` существует; `XPKpAPI_LstTpl` (`IndexType=2`) по
    `SPID, TemplateID, ActionServiceOrder, FinOperServiceID`.
  - `tContract`: единственный уникальный индекс — `XPKtContract(ContractID)`;
    уникального индекса по `Name` нет; `tContract` в `memory_tables` нет.
- Реальных `UPDATE`, затрагивающих уникальные колонки покрытых таблиц, нет:
  `pRuleDoc_QtyStack` обновляется только по `ID` и `Qty`
  (`CreateDocument.sql:946-947, 1651-1652, 2531-2532, 3305-3306`);
  `update pAPI_LstTpl` в дереве отсутствует. То есть живой true positive
  для задачи 4.2 из текущего FA не получить — понадобится синтетическая
  фикстура.

## Замечания

### 1. [High] Предложенный критерий уникальности покрывает 2 таблицы из 26 — правило практически неактивно

- Требование и `design.md` (D4, Non-Goals) сознательно исключают PK/XPK без
  «признака уникальности». Но в текущем `memory_tables` подавляющее
  большинство таблиц имеет именно `XPK`:
  `pAPI_AttrValue`, `pAPI_Calc_ExprOperand`, `pAPI_ConfSet_ValueList`,
  `pAPI_FinOper_Action`, `pAPI_FinOper_ExtrAttr`, `pAPI_FO_ActionState`,
  `pAPI_Oper_ExtrAttr`, `pAPI_Partc_Code`, `pAPI_Partc_Contact`,
  `pAPI_Property_Value`, `pAPI_RuleAcc_TypeID` и др. — `index_type` 0/3;
  `pObjectForRight` — `XPK` с `is_unique=false`; часть GAAP-таблиц
  (`pAPI_RuleDoc_*`) имеет только неуникальные `XIE`.
- Итог: под критерий попадают только `pRuleDoc_QtyStack` (SQL `UNIQUE`) и
  `pAPI_LstTpl` (`index_type=2`). Для memory-optimized таблиц PK — это тоже
  уникальный индекс, немедленное enforcement'ное поведение которого
  ломает swap-приёмы; исключение PK оставляет 24 из 26 деклараций без
  обнаружения.
- Рекомендация: до реализации зафиксировать решение.
  - Если цель — реально ловить класс ошибок: учитывать PK/alternate keys
    (как минимум `XPK`/`XKP`/`XAK`, см. замечание 2) и переформулировать
    Non-Goals/сценарий «Первичный ключ без признака уникальности».
  - Если решение «только явно помеченные уникальные» сохраняется — честно
    записать в proposal/design, что правило покрывает 2/26 таблиц и
    является «точечным», иначе заявленная цель («review видит этот класс
    ошибок») не достигается.

### 2. [High] `api_business_object_table_indexes.index_type = 2` — ненадёжный признак уникальности

- `dsxml`-парсер в ветке API-таблиц сохраняет «сырой» `IndexType`
  (`internal/parser/dsxml/parser.go:338-383`), а в ветке internal PTable
  трактует `2` как `unique nonclustered`
  (`parser.go:410-430`). Перенос этой трактовки на API-ветку ничем не
  подтверждён.
- Живые данные: `index_type=2` встречается у `XPK` (437), `XAK` (222),
  `XKP` (1) и **`XIE` (9)**; `XPK` также массово встречается с `0` (3088),
  `1` (750), `3` (5829). Кросс-сверка по одинаковым именам: `api_type=2`
  даёт 1 SQL-строку с `is_unique=false`; `api_type=3` даёт 162 SQL-строки
  с `is_unique=true`. Стало быть, `index_type=2` даст и false positives
  (неуникальные индексы), и массовые false negatives.
- Пример из FA: `XAK1pADM_SM_ParamObject` — `IndexType=0`
  (`.../API_Administrator_PTbl/Table/pADM_SM_ParamObject.xml`), при этом
  `XAK` по конвенции FA — уникальный alternate key
  (`fa-warranty/openspec/specs/dictionaries/war-property-usr/spec.md:16-25`
  прямо называет `XPK`/`XAK` уникальными). Обратный пример —
  `XIE1pAPI_Cls_DelObjectLinkNTF` с `IndexType=2`.
- Рекомендация: не выводить уникальность API-таблиц из одного
  `index_type`. Варианты:
  - определять уникальность по конвенции имён (`XPK%`/`XKP%`/`XAK%`), а
    `XIE%` считать уникальным только при наличии явного признака
    (`index_type=2`/`is_unique`); прецедент использования конвенции —
    `shouldKeepChosenIndexForPKJoin` (`review_rules_test.go:642-655`);
  - либо добавить в модель/парсер `APIBusinessObjectTableIndex.IsUnique`
    и материализовать его в индексе (потребует DDL + bump
    `CurrentSchemaVersion` — выйти за рамки «схема не меняется», зато
    честно).
  - В любом случае — расширить задачу 3.2: контрольный запрос должен
    проверять не только наличие `index_type=2`, но и полноту распознавания
    `XPK`/`XAK`/`_UNIQUE_`.

### 3. [High] Все 7 сценариев дельты не соответствуют данным индекса и не могут быть проверены

Сценарии — acceptance criteria; сейчас ни один не воспроизводится на живых
данных:

- `spec.md:9` — `UPDATE pRuleDoc_QtyStack SET QtyStack = ...`: колонки
  `QtyStack` нет (есть `Qty`); в уникальный `XPK` входят
  `SPID, DealProtocolID, Brief`.
- `spec.md:15` — `UPDATE pAPI_AttrValue SET AttrValue = ...`: колонки
  `AttrValue` нет (есть `Value`); единственный индекс — `XPK` с
  `index_type=0`, поэтому предложенная реализация finding не сформирует.
- `spec.md:21` — `SET Comment` у `pRuleDoc_QtyStack`: колонки `Comment` нет.
- `spec.md:27` — `tContract SET Name`, «`Name` входит в уникальный индекс»:
  у `tContract` единственный уникальный индекс — `XPKtContract(ContractID)`,
  по `Name` уникального индекса нет.
- `spec.md:33` — `SET ID`, «`ID` входит только в первичный ключ, не
  отмеченный уникальностью»: `ID` вообще не входит в `XPK`
  (`SPID, DealProtocolID, Brief`), а сам `XPK` помечен `UNIQUE`.
- `spec.md:39` — alias-сценарий снова использует несуществующую колонку
  `QtyStack`.
- `spec.md:45` — `fa-cards` + `pAPI_AttrValue SET AttrValue`: нет ни
  колонки `AttrValue`, ни распознанного уникального индекса; в основной
  ветке `fa-cards` нет и `UPDATE pAPI_AttrValue` (update'ы есть в
  `fa-warranty`/`fa-contracts`).

Рекомендованные валидные фикстуры (по живым данным):

- positive p-table: `UPDATE pRuleDoc_QtyStack SET Brief = @Brief ...`
  (`Brief` в `XPK`, `is_unique=true`);
- positive pAPI: `UPDATE pAPI_LstTpl SET TemplateID = @TemplateID ...`
  (`XPKpAPI_LstTpl`, `index_type=2`, колонка `TemplateID` существует);
- negative non-unique: `UPDATE pRuleDoc_QtyStack SET Qty = @Qty ...`;
- negative not-in-memory: `UPDATE tContract SET ContractID = @ID ...`
  (уникальный `XPKtContract`, `tContract` не в памяти);
- negative PK: только если решение из замечания 1 сохраняет исключение PK —
  `UPDATE pObjectForRight SET ObjectID = @ID ...` (`XPK` объявлен
  `M_CRT_INDEX('', ...)` → `is_unique=false`); при включении PK сценарий
  нужно убрать/переформулировать;
- cross-product: `pAPI_LstTpl` объявлен в `.lst` (файл
  `fa-dbmaintenance-ext`), а `UPDATE pAPI_LstTpl SET TemplateID` — в файле
  любого другого продукта.

### 4. [Medium] Список источников уникальности в proposal неполон и частично неверен

- «inline `UNIQUE` в `CREATE TABLE`» **не** попадает в индекс:
  `inCreateTableDefinition` явно пропускает строки, начинающиеся с
  `PRIMARY KEY`/`UNIQUE` (`internal/parser/sql/sql_parser.go:1356-1362`),
  и не вызывает `appendIndexDefinition`; тестов на inline-constraint нет
  (в `sql_parser_test.go:371` — standalone `create unique index`).
- Вариант `M_CRT_INDEX('_UNIQUE_', ...)` не распознаётся: парсер сравнивает
  `strings.EqualFold(indexType, "UNIQUE")`
  (`sql_parser.go:1428-1433`), а `_UNIQUE_` даёт `is_unique=false`.
  В живой БД — 987 таких строк; `XAK1tWarPropertyUsr` — реальный пример.
- `M_CRT_INDEX('', 'XPK...')` (напр. `pObjectForRight_TmpTbl.sql:26`)
  даёт `is_unique=false`, хотя `XPK` — первичный ключ.
- Следствие: «признак уникальности в индексе» сейчас неполон не только для
  API, но и для SQL-источника; риск из `design.md` («парсер не распознаёт
  способ объявления → молчаливый false negative») **подтверждён**, а не
  гипотетичен.
- Рекомендация: либо расширить парсер (`_UNIQUE_`, inline `UNIQUE`/PK,
  `M_CRT_INDEX('')` для `XPK`), с тестами и переиндексацией, либо явно
  ограничить требование и записать исключения в спеку. В любом случае
  задачу 3.2 нужно переписать под проверку `_UNIQUE_`/XPK/XAK.

### 5. [Medium] Task 4.1 противоречит фактическому README

- `README.md:1070` помещает `deferredUpdate` в секцию **PostgreReq**
  (severity=2), тогда как `catalog.go:243-244` и main-спека
  (`deploy-stoppers/spec.md:246-254`) дают ему severity 1 (deploy stopper).
  Формулировка «добавить в Deploy stoppers … рядом с `deferredUpdate`»
  буквально приведёт к размещению нового severity-1 правила в
  severity-2-секции.
- Там же `README.md:1100` утверждает «по умолчанию все deploy stoppers»,
  тогда как `DefaultEnabledRules` (`catalog.go:430-435`) включает **все**
  правила каталога, а `minSeverity` по умолчанию — `SeverityFineCode`
  (`cmd/review.go:106-108`).
- Рекомендация: в задаче 4.1 явно указать «в секцию Deploy stoppers» и, если
  допустимо расширить scope, поправить позиционирование `deferredUpdate`
  (и, возможно, `inSubQuery`/`varcharSize`/`columnInsert`, которые также
  имеют severity 1 в `catalog.go:250-269`, но лежат в README в PostgreReq).

### 6. [Medium] Нет интеграционного покрытия нового пути данных

- В `internal/review` нет DB-интеграционных тестов (`*_integration_test.go`
  отсутствуют; тесты — только `runner_test.go`, `review_*_test.go`).
  План (task 3.1) ограничивается юнит-тестом «хелпера сопоставления».
- Новая логика — это как раз данные: агрегация `IsUnique` по двум
  источникам, ленивая загрузка `memory_tables`, резолв алиаса, пустой набор,
  дубли индексов. Юнит-тест чистого хелпера не поймает, например,
  `index_type=2` у неуникального `XIE`, `_UNIQUE_`, пустой `memory_tables`
  (повторные запросы), дубли определений.
- Рекомендация: добавить (а) store-интеграционный тест
  `batchLoadIndexCandidates` с `IsUnique` на фикстурах SQL+API и
  (б) интеграционный тест правила на синтетическом файле + временно
  проиндексированном `ModuleTableInMemory.lst` (по образцу
  `indexer_lst_integration_test.go`). Task 3.2 дополнить фиксацией
  результата «2/26» (этот отчёт).

### 7. [Low] Ленивый кэш `memory_tables` требует флага «загружено»

- `design.md` (D3) и task 1.2 говорят «при пустом кэше один SELECT».
  Если `memory_tables` пуста (именно риск «`*.lst` не проиндексирован»),
  проверка `len(cache)==0` будет истинной на каждом `UPDATE`, и правило
  будет повторять SELECT на каждый оператор/файл.
- Рекомендация: хранить `memoryTablesLoaded bool` (сбрасывать вместе с
  кэшем в `RunSQLFileCtx` рядом с `indexCandCache`, `runner.go:77-80`);
  для ошибки БД — тоже решить, кэшировать ли «не загружено/ошибку», чтобы
  не устраивать шторм запросов.

### 8. [Low] Нормализация цели и порядок проверки памяти не зафиксированы

- Task 2.2 допускает чтение как «сначала `isMemoryTable(target)`, потом
  резолв алиаса». Для alias-сценария
  (`UPDATE x ... FROM pRuleDoc_QtyStack x`) проверять память нужно **после**
  резолва через `extractTablesFromFromClause`/`sameTableReference`
  (`review_lookup.go:1051`, `review_helpers.go:927`).
- `isMemoryTable` должен применять ту же нормализацию, что и
  `sameTableReference`/`normalizeIdentifier`: `LOWER`, снятие `[]`/`""` и
  схемы (`dbo.`). В текущем FA схемных префиксов нет, но это дешёвая
  страховка.
- Рекомендация: явно записать порядок и нормализацию в design/task.

### 9. [Low] Дедупликация finding'ов по нормализованному имени индекса

- Кандидаты группируются по `(tableLower, indexID, normName)`
  (`review_lookup.go:451-457`), поэтому одно и то же имя индекса из разных
  файлов/веток даёт разные кандидаты. Если оба уникальны, правило по
  формулировке «один finding на (колонка, индекс)» может выдать дубль.
  В живой БД уже есть дубли имени индекса (напр. `XIE1pAPI_RuleDoc_ExtrAttr`),
  сейчас неуникальный; для уникальных — потенциальные дубли.
- Рекомендация: дедуп finding'ов по `(column, LOWER(index_name))`.

### 10. [Low] Нет задачи на bump build/rebuild; Impact неполон

- Предыдущие изменения бампали `buildNumber` в `cmd/root.go`
  (сейчас `0.9.5`, `1679`) и пересобирали `CodeBase.exe`
  (см. archive `2026-11-08-memory-tables-flag/tasks.md:22`).
  В `tasks.md` этого change такой задачи нет.
- `proposal.md` (Impact) перечисляет `types.go`, `catalog.go`,
  `review_rules.go`, `review_lookup.go`, но не `runner.go`, хотя новый кэш
  и его сброс живут там (task 1.2 это подразумевает).
- Рекомендация: добавить задачу «bump `buildNumber` + `go build -o
  CodeBase.exe` + smoke-прогон review», дополнить Impact `runner.go`
  (и, при желании, `runner_test.go` — по прецеденту по одному
  `TestEnabledRuleSet_*` на правило).

### 11. [Info] Кросс-веточная union-семантика

- `batchLoadIndexCandidates` не branch/product-scoped
  (`review_lookup.go:492-513`): `is_unique=true` из определения в любой
  ветке сделает индекс уникальным глобально. Это наследуемый прецедент
  (аналогично `lookupIndexExists`), но для нового правила риск ложных
  срабатываний на ветке, где индекс неуникален, стоит зафиксировать в
  `Risks / Trade-offs`.

### 12. [Info] Ложные срабатывания на легальных UPDATE

- Правило флагует **любое** изменение колонки уникального индекса, а не
  только «swap значений между строками». Одиночный UPDATE, меняющий
  уникальный ключ на неконфликтующее значение, на memory-optimized
  таблице легален, но будет помечен severity 1 (блокировка деплоя).
- `deferredUpdate` устроен так же консервативно, но у него триггер уже
  (только хинтованный индекс). Для нового правила стоит явно записать этот
  trade-off в `Risks`, а формулировку сообщения — привязать к «изменению
  полей уникального индекса» (как в требовании), а не утверждать, что
  конкретно swap.

## Вывод

Направление (отдельное review-правило для `UPDATE` полей уникального
индекса in-memory таблицы, name-based признак из `memory_tables`, переиспользование
`checkDeferredUpdate`/`batchLoadIndexCandidates`) согласуется с архитектурой
review и с прецедентами (`deferredUpdate`, `memory-tables-flag`). Дельта
структурно корректна и проходит `--strict`.

Но **центральная предпосылка изменения не подтверждается данными**:

- предложенный критерий уникальности покрывает **2 таблицы из 26**
  memory-деклараций; PK/XPK (доминирующий тип в `memory_tables`) исключён;
- `index_type=2` не является надёжным признаком уникальности API-индексов
  (смешанные `XPK`/`XAK`/`XIE`, ложные ± по живой БД);
- SQL-признак `is_unique` неполон: `_UNIQUE_` (987 строк), inline `UNIQUE`,
  `M_CRT_INDEX('')`-PK не распознаются;
- **все 7 сценариев дельты** используют несуществующие колонки/неверные
  факты и не могут пройти на живом индексе;
- реальных true-positive `UPDATE` в текущем FA нет — task 4.2 потребует
  синтетической фикстуры.

Блокирующие для `/opsx-apply`: п.1–4 (критерий уникальности и покрытие,
данные сценариев, полнота источников). П.5–10 — обязательные уточнения
плана/тестов/документации; п.11–12 — зафиксировать как trade-off.

---

## Post-review: разбор замечаний и правки артефактов (2026-11-09)

Замечания разобраны; п.2–3, 5–12 приняты, п.1 и п.4 — частично (см. ниже);
все правки внесены в proposal/design/specs/tasks, `validate --all --strict`
37/37. Обоснования:

- **п.1**: конвенция `XPK*`/`XAK*` отклонена данными — `pObjectForRight` (`XPK`,
  `M_CRT_INDEX('', ...)`, неуникален) дала бы severity-1 false positive.
  Зафиксировано flag-based-решение + честные цифры покрытия.
- **п.2**: исправлено на `index_type IN (2, 3)` — подтверждено ручной проверкой
  по рабочей БД (2/3 — уникальные).
- **п.4**: «inline UNIQUE» убран из proposal; `_UNIQUE_`-фикс вынесен в
  Non-Goals (на покрытие memory-таблиц не влияет — см. контроль ниже).

## Контроль полноты критерия по рабочему индексу FA (task 3.4, 2026-11-09)

Критерий «`sql_index_definitions.is_unique=TRUE` ИЛИ
`api_business_object_table_indexes.index_type IN (2,3)`»:

- **(а) Покрытие: 7 из 26** memory-таблиц — `pRuleDoc_QtyStack` (SQL `UNIQUE`)
  и 6 API-таблиц: `pAPI_AutoNum_ExtraAttr`, `pAPI_ExtraAttrib_SaveNTF`,
  `pAPI_FO_TemplateExtraAttr`, `pAPI_LstTpl`, `pAPI_Property_Value`,
  `pAPI_RuleAcc_TypeID` (XPK вида 2/3). Совпадает с прогнозом ревью.
- **(б) Непокрытые 19**: у всех индексы без признака уникальности — API XPK/XIE
  вида 0 либо SQL-декларации с `is_unique=false` (`pAPI_Partc_Code`,
  `pObjectForRight`); противоречащих признаков (SQL-стороны `is_unique=true`)
  нет ни у одной. **Открытый пункт ручной сверки**: XPK вида 0/1, фактически
  уникальный в целевой MSSQL БД, индексатором не отличим — сверка по sys.indexes
  рабочей БД выполняется вручную (как сделано для видов 2/3); при обнаружении
  таких случаев критерий расширяется отдельным решением.
- **(в) `_UNIQUE_` у memory-таблиц: 0** (при 987 строках `_UNIQUE_` с
  `is_unique=false` по всей БД — цифра ревью подтверждена). Парсер-фикс
  `_UNIQUE_` на покрытие правила не влияет, остаётся отдельным change.
