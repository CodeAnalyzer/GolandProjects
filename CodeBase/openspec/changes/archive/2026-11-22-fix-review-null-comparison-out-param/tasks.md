# Tasks: fix-review-null-comparison-out-param

## 1. Реализация

- [x] 1.1 В `internal/review/types.go` заменить группу направления выходного параметра в `nullParamDefaultRe` с `(?:output\s*)?` на `(?:out(?:put)?\s*)?`; обновить комментарий над регуляркой: оба синонима `out`/`output` (в согласии с разбором направлений в `review_helpers.go:2142`) и ограничение линейного якоря `$` — пропуск действует, только когда объявление завершает строку (хвост `,`/пробелы/`--`-комментарий; варианты `= null out)` или несколько параметров в одной строке не покрываются — в корпусе FA таких нет). Проверка: `go build ./...` и `go test ./internal/review/ -run TestCheckNullComparison` без ошибок.
- [x] 1.2 В `internal/review/review_rules_test.go` в таблицу теста `TestCheckNullComparison_NoFalsePositive` (рядом с кейсами `null output` около строки 3347) добавить кейсы: `@Message DSCOMMENT = null out,`, `@CheckClient DSINT_KEY = null out`, `@ProtocolID DSIDENTIFIER = NULL OUT,`, `@x DSMONEY = null Out ,`, `@Message DSCOMMENT = null out, -- passthrough`. Проверка: `go test ./internal/review/ -run TestCheckNullComparison -v` — все кейсы NoFalsePositive проходят, Detects не сломаны.
- [x] 1.3 Поднять buildNumber в `cmd/root.go` (сейчас `1713`) до `1714`. Проверка: `go build ./...` и запуск `codebase review --help` показывает build 1714.

## 2. Приёмка на реальных данных

- [x] 2.1 Прогнать `codebase review D:\GITHUB\GolandProjects\FA\fa-cards\Cards\SERVER\Card\Card_OffB_CheckClient.sql --rules nullComparison` — findings 0 (сейчас 2 на строках 9–10).
- [x] 2.2 Прогнать review `--rules nullComparison` на образцах из корпуса с короткой формой: `FA\fa-custody\Custody\Server\SecSwift\SETPRTY_Generate.sql`, `SecSwift_LongLine.sql`, `ValueByBlockStr.sql`, `FA\fa-payments\Payments\SERVER\UFEBS\CBRF_Maket_Unload.sql` — findings nullComparison на строках объявлений `= null out` отсутствуют (до фикса: 1/1/3/1).
- [x] 2.3 Доказать сохранение true-positive: synthetic-файл под `root_path` FA (временное размещение и удаление — см. acceptance-2.md; `Tests\Temp` не подходит, т.к. вне индекса) с объявлением `@Message DSCOMMENT = null out,` и настоящим сравнением `WHERE Col = NULL` в теле — review формирует ровно 1 finding на строке сравнения и 0 на строке объявления; unit-кейсы `TestCheckNullComparison_Detects` (review_rules_test.go:3288–3298) проходят без изменений.
- [x] 2.4 Зафиксировать результаты приёмки (счётчики до/после) в `openspec/changes/fix-review-null-comparison-out-param/acceptance-2.md`.
