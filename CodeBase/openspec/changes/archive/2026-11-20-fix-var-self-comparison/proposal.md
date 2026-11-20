## Why

В `fa-contracts/Consumer/SERVER/Consumer/BPConsConditionInsert.sql:1723` в `DELETE` вместо `and BankProductID = @BankProductID` записано `and @BankProductID = @BankProductID`. Условие заведомо истинно, поэтому удаляются строки p-таблицы по всем банковским продуктам текущего SPID, а не только по текущему — несохранённые правки других продуктов теряются. `review` эту опечатку не видит: правило `useEqColumn` по спецификации игнорирует любой операнд с `@`.

Дополнительно та же строка порождает ложное срабатывание `indexWrong` на `BPConsConditionInsert.sql:1720`: эвристика `extractUnqualifiedConditionColumns` считает идентификатор внутри `@BankProductID` колонкой `BankProductID`. В результате условие выглядит как фильтр по `BankProductID`, и правило предлагает `XIE1pBPConsCoolingOff (SPID, BankProductID, AmountMin)` вместо корректного `XPKpBPConsCoolingOff (SPID, AutoID)`. Класс системный: любой `@Столбец = литерал` в `WHERE` может подменить реальный набор колонок условия.

## What Changes

- Расширить `useEqColumn`: в условиях `WHERE`/`ON`/`HAVING` обнаруживать самосравнение переменной (`@X = @X`, регистронезависимо) и формировать finding severity 1. Сообщение для этого случая — «Заведомо истинное сравнение: @X = @X».
- Сохранить текущее поведение для прочих случаев: `@A = @B` с разными именами, `@var = column`, сравнения с литералами и битовые операции (`&`, `|`, `^`) не считаются самосравнением; named-args в `EXEC`/`SET`/`SELECT`-алиасах по-прежнему не анализируются.
- Исправить `extractUnqualifiedConditionColumns`: идентификатор, непосредственно предшествуемый `@` или `@@`, не должен попадать в набор колонок условия. Как следствие, `indexWrong` перестаёт рекомендовать индекс по несуществующему предикату.
- Зафиксировать регрессионные кейсы: `BPConsConditionInsert.sql:1720/1723` (Contracts) — 0 `indexWrong` и 1 `useEqColumn`; `BPConsCoolingOff_Transfer.sql:40-43` — корректная форма без находок.

## Capabilities

### New Capabilities
(нет)

### Modified Capabilities
- `review/deploy-stoppers`: требование «Сравнение столбца с самим собой» расширяется на самосравнение переменных в условиях; требование «Неправильное использование индекса для p-таблиц» уточняется — в колонки условия попадают только реальные ссылки на колонки, но не идентификаторы внутри `@`/`@@`-переменных.

## Impact

- Код: `internal/review/review_rules.go` (`checkUseEqColumn`/`analyzeConditionForEqColumn`), `internal/review/review_helpers.go` (`extractUnqualifiedConditionColumns`), `internal/review/catalog.go` (описание/сообщение правила при необходимости), тесты `review_rules_test.go`/`review_helpers_test.go`.
- Поведение: новые findings `useEqColumn` (severity 1) на самосравнение переменных в предикатах; исчезают ложные `indexWrong`, вызванные `@`-префиксом.
- Данные: проверка на FA — в предикатах FA найдено 3 живых самосравнения (Contracts 1, GeneralLedger 2), в `EXEC`-аргументах шаблон массовый (проверено `rg`: 42 045 вхождений в 4 415 файлах fa-contracts), поэтому анализ остаётся ограничен условиями.
- Вне scope: при ревью обнаружен независимый дефект сканеров `review` — закрывающая строка шапки `------*/` начинается с `--` и пропускается раньше закрытия блочного комментария, из-за чего правило не видит файл после шапки (`checkIndexWrong` и ~10 других правил). Выносится отдельным change-последователем; на приёмку этого change не влияет.
- Совместимость: формат JSON-ответа review не меняется; меняется только состав findings.
