## 1. Структура и SQL-запрос

- [x] 1.1 Добавить поле `File string` с JSON-тегом `json:"file"` в структуру `SpecByCodeHit` в `internal/specsvc/specsvc.go` (после поля `Product`, по аналогии с другими query-результатами). Проверить компиляцию: `go build ./internal/specsvc/`
- [x] 1.2 Добавить `JOIN files f ON f.id = m.file_id` в SQL-запрос `ExecuteSpecByCode` и выбрать `COALESCE(f.rel_path, '')` в списке SELECT. Добавить `&hit.File` в `rows.Scan` в соответствующей позиции. Проверить компиляцию: `go build ./internal/specsvc/`

## 2. Тесты

- [x] 2.1 Написать интеграционный тест `TestExecuteSpecByCode_WithFilePath` в `internal/specsvc/specsvc_integration_test.go`: индексировать spec-файл с упоминанием код-сущности в Related code, вызвать `ExecuteSpecByCode`, проверить что `hit.File` содержит непустой путь к spec-файлу. Проверить: `go test ./internal/specsvc/ -run TestExecuteSpecByCode_WithFilePath -v`
- [x] 2.2 Написать интеграционный тест `TestExecuteSpecByCode_UsecaseMention` в `internal/specsvc/specsvc_integration_test.go`: индексировать usecase-файл с упоминанием код-сущности, вызвать `ExecuteSpecByCode`, проверить что `hit.File` содержит путь к usecase-файлу (не к spec.md capability). Проверить: `go test ./internal/specsvc/ -run TestExecuteSpecByCode_UsecaseMention -v`
- [x] 2.3 Написать интеграционный тест `TestExecuteSpecByCode_NotFound` в `internal/specsvc/specsvc_integration_test.go`: вызвать `ExecuteSpecByCode` с именем несуществующей код-сущности, проверить что результат пустой (`Resolved = false`, `len(Hits) = 0`), без ошибки. Проверить: `go test ./internal/specsvc/ -run TestExecuteSpecByCode_NotFound -v`

## 3. Валидация и сборка

- [x] 3.1 Запустить полную сборку проекта: `go build ./...` — убедиться, что нет ошибок компиляции
- [x] 3.2 Запустить все интеграционные тесты specsvc: `go test ./internal/specsvc/ -v` — убедиться, что все тесты проходят, включая новые
- [x] 3.3 Обновить замечание в `spec-tools-analysis.md` (строка 287): заменить «Описание инструмента обещает file path, но фактическая структура результата его не содержит» на описание того, что file path теперь возвращается в поле `file`
