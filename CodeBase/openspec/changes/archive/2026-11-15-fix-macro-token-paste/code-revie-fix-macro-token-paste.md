# Ревью кода: `fix-macro-token-paste`

- **Дата:** 2026-11-15
- **Change:** `CodeBase/openspec/changes/fix-macro-token-paste`
- **Реализованные файлы:** `internal/review/review_parser.go`,
  `internal/review/review_parser_test.go`, `internal/review/review_rules_test.go`,
  `cmd/root.go` (build number), `CodeBase.exe` (пересобран)
- **Валидация change:** `openspec validate fix-macro-token-paste --strict` → **valid**

## Итоговый вердикт

**Реализация корректна, соответствует запланированным изменениям, регресса не
обнаружено.** Код fixing'а минимален и точечен (`replaceIdentifierToken`),
поведение подтверждено тестами и независимым baseline-диффом на реальных
файлах FA. Блокирующих и существенных (MAJOR) замечаний по коду нет.

Есть только MINOR-замечания: одно к формулировке сценария в дельте спеки
(противоречит param-bound семантике на реальном `seed.h`), одно к полноте
теста `checkIndexWrong` и несколько редакционных. Они не мешают архивации
change после (желательной) правки сценария спеки.

## Что проверено

### Изменения в коде

Diff затронул ровно ожидаемую область:

- `internal/review/review_parser.go` (+7/−2) — только
  `replaceIdentifierToken`;
- `internal/review/review_parser_test.go` (+47) — два новых теста;
- `internal/review/review_rules_test.go` (+90) — три новых теста;
- `cmd/root.go` — `buildNumber` `1697` → `1699`;
- `CodeBase.exe` — пересобран.

Не тронуты (и это правильно): `applyMacroArguments`, `expandFirstMacroCall`,
`findMacroCall`, `reLookupQualifiedRef`, `extractColumnRefs*`, SQL-парсер
индексатора.

### Финальный код фикса

```go
re := cachedRegexp(`(?i)(?:##)?\b` + regexp.QuoteMeta(trimmedToken) + `\b(?:##)?`)
return re.ReplaceAllLiteralString(text, replacement)
```

Ключевые свойства, подтверждённые матрицей тестов и ручной проверкой:

- `\b` стоит вокруг **имени параметра**, поэтому подстроки не портятся
  (`LinkID` при параметре `id` не меняется);
- `(?:##)?` с обеих сторон поглощает paste-обёртку при наличии/одной стороне/
  пустом аргументе (`##PREFVAR##ObjectID` + `""` → `ObjectID`);
- `@##OBJECT##SeedID` работает (граница слова вокруг `OBJECT`, а не вокруг `##`);
- `ReplaceAllLiteralString` не трактует `$` в аргументе как группу замены;
- обычный `mx` без `##` по-прежнему подставляется.

### Команды и результаты

| Проверка | Результат |
|---|---|
| `go test ./internal/review/...` | `ok` |
| `go test ./cmd/...` | `ok` |
| `go vet ./internal/review/...` | без замечаний |
| `go build ./...` | `BUILD_ALL_OK` |
| `openspec validate --strict` | `valid` |
| Smoke `review …API_CON_RuleDoc_MassGetLimitAcc.sql --rules tableFullScan` | `count: 0` (было 1) |

### Red→green (независимая проверка)

Собран бинарник из `HEAD`-версии `review_parser.go` (старый код) и на нём
прогнаны новые тесты:

- `TestReplaceMacros_ParamTokenPaste` — **FAIL**: `ch##1##` вместо `ch1`;
- `TestReplaceIdentifierToken_TokenPasteMatrix` — **FAIL** в подкейсах
  `paste_both_sides`, `paste_qualified`, `one_sided_paste`, `empty_argument`,
  `paste_after_at_sign`, `dollar_in_replacement_is_literal`;
- `TestCheckTableFullScan_MacroParamTokenPaste_NoFinding` — **FAIL**:
  ложный `tableFullScan` по `psrc_base`.

На текущем коде все они проходят. Задача 2.8 (red→green) выполнена фактически.

### Baseline-диф old vs new (реальные файлы FA)

Собран отдельный бинарник из `HEAD`-версии препроцессора, прогнан `review` на
репрезентативной выборке; сравнение по `(rule, line, object)`:

| Файл | old | new | Диф |
|---|---|---|---|
| `API_CON_RuleDoc_MassGetLimitAcc.sql` (`--rules tableFullScan`) | 1 | 0 | удалён `tableFullScan|2092|pcon_cover_checkpaystrategy` |
| `USR_GET_PAYMENT_COUNT.sql` | 7 | 7 | нет |
| `USR_GET_MAX_OVER.sql` | 6 | 6 | нет |
| `USR_GET_MAX_OVER_AMOUNT.sql` | 8 | 8 | нет |
| `USR_GET_INSTITUTION_OVER.sql` | 12 | 12 | нет |
| `USR_GET_CUR_DEBT_TOTAL.sql` | 6 | 6 | нет |
| `WarrantyInterest_Insert.sql` | 3 | 3 | нет |
| `WarrantyInterest_Update.sql` | 40 | 40 | нет |
| `WarrantyInterest_Delete.sql` | 0 | 0 | нет |
| `WarrantyInterest.sql` | 0 | 0 | нет |
| `FCD_##CURRENT_MODULEID##_HM_Sensor_Reg.sql` | 1 | 1 | нет |

Вывод: на файлах с `i##__N__##` и `##PREFVAR##` (включая пустой аргумент) новых
findings не появилось; файл с file-level placeholder `##CURRENT_MODULEID##`
не изменился вовсе. Регресса нет.

## Соответствие запланированным изменениям

| Пункт change | Статус |
|---|---|
| token-paste для объявленных параметров (`ch##mx##`→`ch1`) | выполнено |
| схлопывание только примыкающего к параметру `##` | выполнено (глобального прохода по `##` нет; regex применяется только к объявленным параметрам) |
| обычный параметр без `##` не регрессировал | выполнено + тест |
| подстрочный guard (`LinkID`/`id`) | выполнено + тест |
| пустой аргумент поглощает оба `##` | выполнено + тест |
| non-param `##` (`##GlobalRest`, `##CURRENT_MODULEID##`, `##PPVARCNT##`, `##_SHARED_SEED_##`) | не затрагиваются + тесты + e2e-диф |
| область раскрытия — только `#define` текущего файла | поведение не менялось, зафиксировано в spec |
| регрессия 12×`union all` | тест `TestCheckTableFullScan_MacroParamTokenPaste_NoFinding` |
| якорь через `Runner`/`SourceMap` | тест `…_Anchor` + ручная e2e-проверка |
| baseline-диф (Underwriting/Warranty) | воспроизведён независимо — регресса нет |
| Non-Goals соблюдены (`reLookupQualifiedRef`, union-ветки, SQL-парсер) | да |

## Замечания

### MINOR-1. Сценарий спеки «Объектный макрос сохраняется» неточен

`specs/review/sql-preprocessing/spec.md:112-116`:

> **GIVEN** SQL-файл содержит `##_SHARED_SEED_##t##OBJECT##Seed`
> **THEN** все `##`-маркеры объектного макроса сохраняются без изменений

На реальном `FA/fa-contracts/Underwriting/SERVER/Include/seed.h:85`
`##_SHARED_SEED_##t##OBJECT##Seed` находится внутри
`#define _MULTISEED_SEEDID_MTABLE(OBJECT)`, и этот макрос вызывается с
`OBJ`/`Contract` (`seed.h:196`, `seed.h:247`). То есть `OBJECT` — **объявленный
параметр**, и по требованию «Раскрытие параметров макроса с token-paste» он
**должен** схлопнуться; неизменным остаётся только `##_SHARED_SEED_##`
(объектный макрос). Тело требования (строки 72-84) корректно, а сценарий —
нет и противоречит телу.

Рекомендация: сузить сценарий до `dbo.##_SHARED_SEED_##tTable` (без
параметрического `OBJECT`) либо явно указать, что `OBJECT` здесь не объявлен
параметром. Код менять не нужно, это правка дельты спеки до архивации.

### MINOR-2. Тест `checkIndexWrong` — unit-level, не end-to-end

`TestIndexWrongInputs_MacroParamTokenPasteAlias` вызывает
`replaceIdentifierToken` напрямую и затем `extractTablesFromFromClause` +
`extractConditionColumnsForIndexWrong`. Это проверяет alias→columns, но не
прогоняет `checkIndexWrong` целиком. Task 3.3 формально закрыт, но улучшение
поведения самого правила не утверждается — только отсутствие регресса (что
подтверждено baseline-диффом). Приемлемо; при желании — добавить фикстуру с
`insert…select` и проверить, что у `indexWrong` нет новых findings/alias
`ch1` распознан. Не блокирует.

### MINOR-3. Impact в proposal ссылается на неизменённые функции

`proposal.md:71-74` перечисляет `applyMacroArguments`, `expandFirstMacroCall`,
`findMacroCall` как область правки, но фактически изменён только
`replaceIdentifierToken` (в котором и вызывается из `applyMacroArguments`).
Функционально это верно, но для точности Impact стоит указать фактическое
место правки.

### MINOR-4. Build number `1697` → `1699` (пропуск 1698)

`cmd/root.go` поднят на две единицы. Вероятно, 1698 израсходован промежуточной
сборкой; на работу не влияет. Отметить в PR/коммите, чтобы не выглядело
ошибкой.

### MINOR-5. Бинарник `CodeBase.exe` изменён

Если бинарник трекается репозиторием (в diff он есть), его надо закоммитить
вместе с исходниками и новым build number; иначе — исключить из коммита.
Это организационное, не дефект кода.

### Наблюдения (не требуют действий)

- Задача 2.8 «red→green» не зафиксирована текстом в тесте, но фактически
  подтверждена (см. раздел выше). Можно добавить комментарий в тест.
- Тест `TokenPasteMatrix` проверяет `##CURRENT_MODULEID##`/`##PPVARCNT##` с
  токеном `mx` (то есть тривиально «не совпало»). Это нормально для guard'а на
  отсутствие глобального схлопывания; end-to-end гарантию даёт baseline-диф
  (`FCD_##CURRENT_MODULEID##…` — без изменений).
- Pre-existing ограничения (подстановка внутри строковых литералов, отсутствие
  резолва `#include`) корректно зафиксированы в design/spec как вне scope.

## Рекомендации перед архивацией

1. Поправить сценарий MINOR-1 в
   `specs/review/sql-preprocessing/spec.md` (обязательно: спека — источник
   истины).
2. По желанию: уточнить Impact (MINOR-3) и добавить end-to-end кейс для
   `checkIndexWrong` (MINOR-2).
3. Убедиться, что `CodeBase.exe` включён/исключён из коммита осознанно
   (MINOR-5), и что build number соответствует принятой нумерации.

После этого change готов к архивации; код-регрессий не выявлено.
