# SQL Preprocessing

## Purpose

Раскрытие `#define`-макросов и сегментация SQL-операторов в review перед
статическим анализом: параметризованные макросы, многострочные продолжения,
token-paste `##` и сохранение исходных номеров строк для якорей finding'ов.
Раскрываются только `#define` текущего файла; `#include` не резолвится.

## Requirements

### Requirement: Раскрытие параметров макроса с token-paste

Система SHALL раскрывать параметризованные `#define`-макросы до запуска
правил review, подставляя аргументы вызова в тело макроса. Раскрытие MUST
использовать только `#define`, объявленные в текущем файле; макросы из
`#include`-заголовков не резолвятся. Если в теле параметр примыкает к
оператору token-paste `##` (например `ch##mx##`, `sb##mx##`, `dk##mx##`),
подстановка MUST выполнять конкатенацию: при `mx = 1` результат MUST быть
`ch1`, `sb1`, `dk1` — без остаточных символов `##`. Поглощение `##`
применяется только к имени объявленного параметра; подстрока, совпадающая с
именем параметра внутри другого идентификатора (`LinkID` при параметре
`id`), MUST NOT изменяться. Пустой аргумент (`__InterestValue__()` → `ObjectID`)
MUST поглощать оба `##`. Для paste-алиасов раскрытый текст MUST позволять
распознавать квалифицированные ссылки `<alias>.<column>`, чтобы правила
сопоставляли алиас таблицы с условиями `WHERE`/`ON`. token-paste MUST
поддерживаться как для параметров, так и для имени макроса
(`sp.##M_LOGINTIME` → `sp.loggedindatetime`).

#### Scenario: Параметр в paste-идиоме раскрывается в цельный алиас

- **GIVEN** макрос `#define M_INSERT(_VAL_, mx) select @@spid from T ch##mx## where ch##mx##.SPID = @@spid and ch##mx##.ExcludeReason = 0`
- **AND** вызов `M_INSERT('X', 1)`
- **WHEN** выполняется review файла
- **THEN** раскрытый текст содержит алиас `ch1` и условие `ch1.SPID = @@spid`
- **AND** остаточных вхождений `##` в раскрытом алиасе нет

#### Scenario: Обычный параметр без paste подставляется

- **GIVEN** макрос с параметром `mx`, в теле которого встречается обычное `mx` без `##`
- **AND** вызов с `mx = 1`
- **WHEN** выполняется review файла
- **THEN** `mx` заменён на `1`
- **AND** наличие paste-логики не отменяет базовую подстановку

#### Scenario: Подстрока с именем параметра не портится

- **GIVEN** макрос с параметром `id`, в теле которого есть идентификатор `LinkID`
- **WHEN** выполняется подстановка `id = X`
- **THEN** `LinkID` остаётся `LinkID` (не превращается в `LinkX`)

#### Scenario: Пустой аргумент поглощает оба ##

- **GIVEN** макрос `#define __InterestValue__(PREFVAR) ... ##PREFVAR##ObjectID`
- **AND** вызов `__InterestValue__()` (пустой аргумент)
- **WHEN** выполняется review файла
- **THEN** раскрытый текст содержит `ObjectID`
- **AND** не содержит `##ObjectID` или `ObjectID##`

#### Scenario: INSERT…SELECT с 12 union all не даёт ложный tableFullScan

- **GIVEN** `INSERT…SELECT` из 12 веток `union all`, каждая через `#define` с параметром `mx`, где таблица в `FROM` использует алиас `ch##mx##` и фильтруется `ch##mx##.SPID = @@spid` и `ON`-условиями
- **WHEN** выполняется review с правилом `tableFullScan`
- **THEN** finding по таблице `pCON_COver_CheckPayStrategy` не формируется
- **AND** алиасы раскрыты в `ch1…ch12`, `sb1…sb12`, `dk1…dk12`

#### Scenario: Paste перед именем макроса сохраняется

- **GIVEN** `#define M_LOGINTIME loggedindatetime` и выражение `select sp.##M_LOGINTIME from t`
- **WHEN** выполняется review файла
- **THEN** раскрытый текст содержит `sp.loggedindatetime`
- **AND** остаточных `##` нет

### Requirement: Сохранение ##, не примыкающих к объявленному параметру

Система SHALL схлопывать `##` только тогда, когда он непосредственно
примыкает к имени **объявленного параметра** раскрываемого макроса. `##`,
не примыкающий к параметру, MUST NOT изменяться: prefix глобальной временной
таблицы (`##GlobalRest`, `##OUTTABLE`), placeholder уровня файла
(`##CURRENT_MODULEID##`), встроенный маркер (`##PPVARCNT##`,
`@val##PPVARCNT##PPLINE`) и объектные макросы (`##_SHARED_SEED_##`) MUST
сохраняться без изменений. Глобальное правило «схлопывать `##` между
частями идентификатора» MUST NOT применяться. Если `_TABLENAME_` объявлен
параметром конкретного макроса, он схлопывается по требованию
«Раскрытие параметров макроса с token-paste»; если не объявлен — остаётся
как есть.

#### Scenario: Глобальная временная таблица не искажается

- **GIVEN** SQL-файл содержит `select ID from ##GlobalRest`
- **WHEN** выполняется review файла
- **THEN** раскрытый текст по-прежнему содержит `##GlobalRest`
- **AND** `GlobalRest` (без `##`) не появляется

#### Scenario: ##-плейсхолдер в имени таблицы сохраняется

- **GIVEN** SQL-файл содержит `insert into ##OUTTABLE (a) values (1)`
- **WHEN** выполняется review файла
- **THEN** раскрытый текст по-прежнему содержит `##OUTTABLE`

#### Scenario: Placeholder уровня файла не схлопывается

- **GIVEN** SQL-файл `FCD_##CURRENT_MODULEID##_HM_Proc.sql` не объявляет `CURRENT_MODULEID` параметром какого-либо макроса
- **WHEN** выполняется review файла
- **THEN** текст `##CURRENT_MODULEID##` сохраняется без изменений

#### Scenario: Встроенный маркер в окружении слова не схлопывается

- **GIVEN** тело макроса содержит `@val##PPVARCNT##PPLINE`, и `PPVARCNT` не является объявленным параметром этого макроса
- **WHEN** выполняется подстановка параметров
- **THEN** фрагмент `val##PPVARCNT##PPLINE` сохраняется без изменений
- **AND** глобальное схлопывание `\w##\w` не применяется

#### Scenario: Объектный макрос сохраняется

- **GIVEN** SQL-файл содержит `insert into dbo.##_SHARED_SEED_##tTable`, и `_SHARED_SEED_` не объявлен параметром какого-либо макроса
- **WHEN** выполняется review файла
- **THEN** префикс `##_SHARED_SEED_##` сохраняется без изменений
- **AND** глобальное схлопывание `##` не применяется

### Requirement: Сегментация операторов и исходные номера строк

Система SHALL сегментировать раскрытое содержимое на SQL-операторы и
сопоставлять finding'и исходным строкам файла через `SourceMap` раскрытия.
Для `INSERT…SELECT` оператор MUST начинаться со строки `insert` (а не со
строки макроса или `select`), и якорь finding'а MUST указывать на строку
начала оператора. Многострочные `#define` с продолжением `\` MUST
потребляться целиком и не порождать самостоятельных операторов или таблиц.

#### Scenario: Якорь INSERT…SELECT

- **GIVEN** оператор `INSERT…SELECT`, начинающийся на строке 2092, с телом `select` ниже
- **WHEN** правило формирует finding внутри SELECT-части
- **THEN** `line` finding'а равен 2092

#### Scenario: Многострочный define не даёт операторов

- **GIVEN** `#define M_X(...)` с continuation-строками `\`, содержащими `select … from …`
- **AND** вызов `M_X(...)` ниже
- **WHEN** выполняется review
- **THEN** строки определения макроса не анализируются как отдельный оператор
- **AND** таблицы из тела макроса учитываются только в точках вызова

## Related code

- `internal/review/review_parser.go` — `collectMacroDefinitions`,
  `expandMacrosInLines`, `expandFirstMacroCall`, `findMacroCall`,
  `applyMacroArguments`, `replaceIdentifierToken`.
- `internal/review/runner.go` — `fileProcessedContent`, `mapExpandedLineToOriginal`.

## Notes

- Раскрытие ограничено `#define` текущего файла: `#include` не резолвится.
- Подстановка параметров не различает строковые литералы — известное
  pre-existing ограничение, вне scope текущей capability.
