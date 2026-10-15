# Tasks: tableFullScan — derived tables и INSERT…SELECT

## 1. Derived tables в FROM-парсере

- [x] 1.1 В `internal/review/review_lookup.go` добавить строко-aware поиск парной закрывающей скобки (переиспользовать логику `findTopLevelFromClauseBounds`: `'…'` с `''`-эскейпом). Юнит-тест: парность для `(select … (…) …) t`, литералы со скобками и эскейпом
- [x] 1.2 В `parseTablesInFromClause` различать части-derived tables: часть начинается с `(` → извлечь внутренности + алиас после закрывающей скобки; рекурсивно прогнать внутренности через `findTopLevelFromClauseBounds` + `parseTablesInFromClause` (лимит глубины ~10, при исчерпании — пропуск уровня). Таблицы подзапроса попадают в общий список с их собственными алиасами. Юнит-тест: `from (select … from pSrc k where k.SPID=@@spid …) t` → таблица `pSrc`, alias `k`, без записи `select`
- [x] 1.3 WHERE/ON для внутренних таблиц брать из текста подзапроса: в `analyzeStatementForFullScan` для derived-вложенных таблиц использовать refs из внутреннего WHERE/ON, а для обычных таблиц — из внешнего. Юнит-тест сценария спеки «Derived table с фильтром внутри — нет finding»
- [x] 1.4 Guard в `parseTableWithAlias`: `TableName` из набора SQL-ключевых слов (`select`, `from`, `where`, `on`, `as`, `and`, `or`, `group`, `order`, `having`, `join`, `union`, …) → запись отбрасывается. Юнит-тест: `parseTableWithAlias("(select x from t)")` не возвращает таблицу `select`

## 2. INSERT…SELECT

- [x] 2.1 Добавить `insert` в `findStatementStart` (review_lookup.go) по образцу `findStatementStartForTableHintExists`; проверить, что `checkTableFullScan` теперь ловит оператор со строки `insert`. Юнит-тест: buffer/statement start для `insert into … select …`
- [x] 2.2 В `analyzeStatementForFullScan` для `stmtType == "insert"`: цель INSERT не включается в анализируемые таблицы (только SELECT-часть), `Line` = строка начала оператора. Юнит-тесты: INSERT…SELECT без FROM-таблиц → нет finding; INSERT…SELECT с нарушением в SELECT-части → line = строка `insert`

## 3. Тесты правил и регресс

- [x] 3.1 Новые кейсы в `internal/review/review_rules_test.go` (таблица-кейсы `TestAnalyzeStatementForFullScan_*`): минимальный репро из BUG-отчёта (derived + left join + union) → нет finding; derived без фильтра внутри → finding `object=psrc`; INSERT…SELECT → line/object корректны
- [x] 3.2 Прогнать весь пакет: `go build ./... && go vet ./... && go test ./internal/review/...` — существующие тесты `tableFullScan`/`tableHintExists`/`indexWrong` и др. не сломаны (изменения общего парсера FROM)
- [x] 3.3 Контрольный прогон на FA: `codebase review` по `CreditTurnoverLink_FastProcess.sql` → 0 findings; diff findings до/после на выборке fa-contracts содержит только исчезающие `object=select`
- [x] 3.4 Обновить BUG-отчёт `Bug-reports/BUG-tableFullScan-derived-table-false-positive-20261015.md`: статус «Решено», ссылка на change (по образцу BUG-rti-prune-client-events-fk-20261015.md)

## 4. Финализация

- [x] 4.1 `openspec validate tablefullscan-derived-table --strict` — valid; `go build/vet/test ./...` — ok; пересобрать `CodeBase.exe` (по стандартному циклу сборки, как в прошлых change)
