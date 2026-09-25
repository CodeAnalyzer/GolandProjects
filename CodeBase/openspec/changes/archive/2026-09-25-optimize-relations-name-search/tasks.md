## 1. Унификация резолва имён

- [x] 1.1 Обобщить первый проход: `relationEntityMatchQueryParts`/`findRelationEntityMatches` должны принимать явный `entityType` на сторону и возвращать exact-first набор пар `(entity_type, entity_id)`; проверить unit-тестом для точного совпадения и подстрочного фолбэка
- [x] 1.2 Реализовать второй проход с двумя наборами `VALUES` (source и target) в одном запросе: `JOIN (VALUES …) ON r.source_type/…` и, при наличии, `JOIN (VALUES …) ON r.target_type/…`, с `ORDER BY r.id DESC LIMIT`; проверить unit-тестом на пересечение
- [x] 1.3 Пробросить явные `source_type`/`target_type` и `relation_type` как ограничения второго прохода; проверить unit-тестом типизированного фильтра имени без полного сканирования (см. 4.3)

## 2. Диспетчеризация и удаление мёртвого кода

- [x] 2.1 Переписать `SearchRelations` (`internal/query/query_relations.go:220`) на единый двухпроходный путь: убрать ветки `searchRelationsByNameMatches` vs общая и удалить проверку «одно имя и другая сторона без типа»; проверить, что поведение всех комбинаций фильтров сохраняется существующими тестами
- [x] 2.2 Удалить `buildRelationAnyNameExistsCondition` (:185) и `buildRelationNameExistsCondition` (:135) и все их вызовы; проверить сборкой `go build ./...` и отсутствием ссылок (`rg`)

## 3. Тесты семантики

- [x] 3.1 Unit-тест «точное совпадение приоритетно над подстрокой»: `MyProc` и `MyProcExtended` — при `--source-name MyProc` возвращаются связи только `MyProc`
- [x] 3.2 Unit-тест «подстрочный фолбэк при отсутствии точного совпадения»: имя `yPro` находит сущности, содержащие `yPro`
- [x] 3.3 Unit-тест «пересечение при двух именах»: source и target заданы одновременно, возвращаются только связи пересечения
- [x] 3.4 Unit-тест «имя без типа ищется по всем типам сущностей»: `pPortObject` как таблица находится без явного `--target-type`
- [x] 3.5 Прогнать `go test ./internal/query/...` — все тесты зелёные

## 4. Верификация на реальном индексе

- [x] 4.1 `go test ./...` и `go vet ./...` — без ошибок
- [x] 4.2 `EXPLAIN` запроса выборки id из `relations` (unit-тест или ручной прогон на рабочей БД) не содержит `Seq Scan on relations`
- [x] 4.3 Прогон `codebase_query_relations` по нескольким вызовам из `Logs/WORK-LOG` до и после: замерить латентность и зафиксировать снижение; сравнить наборы id (регресс недопустим)
- [x] 4.4 `openspec validate optimize-relations-name-search --strict` — без ошибок
