## 1. Исправление дедупликации

- [x] 1.1 В `FindMethodsByTable` (`internal/query/query_sql.go`) заменить `SELECT` на `SELECT DISTINCT` для устранения fan-out по query-фрагментам; проверить `go build ./...`
- [x] 1.2 Убедиться, что `DISTINCT` совместим с `ORDER BY pu.unit_name, pm.method_name, pm.line_number` (все выражения присутствуют в select-list) — проверяется успешным выполнением запроса в интеграционном тесте 2.1

## 2. Тесты

- [x] 2.1 Добавить интеграционный тест (`//go:build integration`) в `internal/query/`, вставляющий PAS-метод с двумя query-фрагментами, ссылающимися на одну таблицу, и проверяющий, что `FindMethodsByTable` возвращает одну строку (`len == 1`); проверить падение до фикса и прохождение после
- [x] 2.2 Прогнать `go test -tags integration ./internal/query/...` против тестовой БД (`store/testutil`) и убедиться, что новый тест зелёный

## 3. Актуализация ожидания TC-056

- [x] 3.1 В `Tests/TEST_CASES.md` изменить ожидание TC-056 с `count=3` на `count=2` (уникальные методы `GetFilterQueryText` в `E_ObjClsRelation_Grid_Aim` и `E_ObjClsRelation_Grid_LoanAim`); проверить, что формулировка совпадает с фактическим результатом

## 4. Итоговая проверка

- [x] 4.1 `go build ./... && go test ./...` — зелёные; `go vet ./internal/query/...` без замечаний
- [x] 4.2 `codebase query methods --table tAim --json` (против FA) → `count=2`, без дублей; ранее красный TC-056 становится PASS
- [x] 4.3 Регресс соседних кейсов: TC-057 (`pConsDocAmount` → 0) и TC-058 (`tConsSalePortfolio` → 0) не изменились
