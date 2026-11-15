## Why

Препроцессор SQL в review не реализует token-paste `##` для **параметров**
макроса: при раскрытии `M_INSERT_FUNDS_CATEGORY2(_VAL_, mx)` тело
`ch##mx##.LinkID` превращается в `ch##1##.LinkID` вместо `ch1.LinkID` —
`##` схлопывается только перед *именем макроса*, но не вокруг подставленного
аргумента. Из-за этого алиасы таблиц в раскрытом тексте выглядят как
`ch##1##`, а регулярка квалифицированных ссылок `(\w+)\.(\w+)` не может
связать `ch##1##.SPID` с условием `WHERE`, и правило `tableFullScan` даёт
ложный finding. На реальном файле
`FA/fa-contracts/API_Credit/Server/Callback/API_CON_RuleDoc_MassGetLimitAcc.sql:2092`
это ложное срабатывание по `pcon_cover_checkpaystrategy`, хотя каждая ветка
`union all` фильтруется `chN.SPID = @@spid` и `ON`-условиями.

Прямой механизм FP (`alias→WHERE/ON` через `extractColumnRefsFromWhere`/
`extractColumnRefsFromOn` + `isTableFiltered`) сейчас использует только
`tableFullScan`. Однако искажённый алиас `ch##1##` возвращает общий
`parseTableWithAlias`/`extractTablesFromFromClause`, которым пользуются и
другие правила (`indexWrong`, `useEqColumn`, `deferredUpdate`,
`diffTypesComparison` и т.п.). Их поведение при `##`-алиасах нужно проверить
отдельно (baseline-диф), а не считать заранее сломанным.

## What Changes

- Реализовать в препроцессоре review token-paste `##` для **объявленных
  параметров** макроса: подстановка аргумента в `X##param##Y` MUST давать
  `X<arg>Y` (например `ch##mx##.LinkID` при `mx=1` → `ch1.LinkID`,
  `sb##mx##.SPID` → `sb1.SPID`). Paste-обёртка распознаётся шаблоном
  `(?:##)?\b<param>\b(?:##)?` (с `regexp.QuoteMeta` и literal-replacement),
  поэтому обычный `mx` без `##` по-прежнему подставляется, а подстроки
  (`LinkID` при параметре `id`) не портятся. Пустой аргумент (`__InterestValue__()`)
  MUST поглощать оба `##`.
- Схлопывать `##` **только** примыкающий к имени раскрываемого параметра.
  `##`, не примыкающий к параметру (prefix global temp `##GlobalRest`,
  placeholder файла `##CURRENT_MODULEID##`, встроенный маркер `##PPVARCNT##`,
  объектный макрос `##_SHARED_SEED_##`), MUST NOT изменяться.
- Раскрытие ограничено `#define` текущего файла: `#include` не резолвится
  (существующее поведение препроцессора review).
- Добавить регрессионный тест на реальном кейсе: `INSERT…SELECT` из
  12 `union all` через параметризованный `#define` с `##mx##` не порождает
  ложный `tableFullScan`, а алиасы раскрываются в `ch1…ch12`,
  `sb1…sb12`, `dk1…dk12`.
- Добавить baseline-диф на репрезентативной выборке FA до/после правки
  (включая `fa-contracts/Underwriting/USR_GET_*.sql` с `i##__N__##` и
  `fa-warranty/*WarrantyInterest*.sql` с `##PREFVAR##`): новые findings не
  появляются, ожидаемые ложные — исчезают.
- Зафиксировать контракт препроцессора review (раскрытие `#define`,
  многострочные `\`-продолжения, подстановка параметров, token-paste,
  сегментация операторов) отдельной спецификацией, чтобы дальнейшие правки
  раскрытия макросов опирались на источник истины.

Не входит в scope: устранение FN по веткам `union all` №2–12
(`extractTablesFromFromClause`/`extractColumnRefsFromWhere` смотрят только
первый `FROM`/`WHERE`) — отдельный кандидат-последователь.

## Capabilities

### New Capabilities

- `review/sql-preprocessing`: раскрытие `#define`-макросов и сегментация
  SQL-операторов в review перед статическим анализом — параметризованные
  макросы, многострочные продолжения, token-paste `##` и сохранение
  исходных номеров строк.

### Modified Capabilities

(нет)

## Impact

- Код: фактическая правка — `replaceIdentifierToken` в
  `internal/review/review_parser.go` (вызывается из `applyMacroArguments`);
  paste перед именем макроса в `findMacroCall`/`expandFirstMacroCall` уже
  существовал и не менялся. Регрессионные тесты
  `internal/review/review_parser_test.go` и
  `internal/review/review_rules_test.go`.
- Поведение: у `tableFullScan` исчезает ложный finding по `##`-алиасам;
  раскрытые алиасы становятся цельными (`ch1`) для всех потребителей
  `extractTablesFromFromClause`. Для остальных правил (`indexWrong`,
  `useEqColumn`, `deferredUpdate`, `diffTypesComparison`) это **требует
  проверки** baseline-дифом, а не гарантируется заранее.
- Сужение формулировки: «синтаксически корректным» относится только к
  paste-алиасам; placeholder'ы, не примыкающие к параметру
  (`##CURRENT_MODULEID##`, `##PPVARCNT##`), остаются не-SQL и вне scope.
- Затронутые семейства файлов: `fa-contracts/Underwriting/USR_GET_*.sql`
  (`i##__N__##`), `fa-warranty/*WarrantyInterest*.sql` (`##PREFVAR##`,
  включая пустой аргумент), `seed.h` (`##_SHARED_SEED_##`).
- Риск: неверное схлопывание не-параметрических `##` — закрывается
  param-bound семантикой и тестами (`##GlobalRest`, `##CURRENT_MODULEID##`,
  `##PPVARCNT##`, `##_SHARED_SEED_##`).
- Зависимости/БД/API: не затрагиваются.
