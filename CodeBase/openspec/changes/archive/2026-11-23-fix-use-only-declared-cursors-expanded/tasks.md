## 1. Источник текста правила

- [x] 1.1 В `internal/review/review_rules.go` в `checkUseOnlyDeclaredCursors` заменить `os.ReadFile(file.Path)` на `r.fileContent(ctx, file.Path)`; проверить `go build ./...`
- [x] 1.2 Удалить прямой вызов `os.ReadFile` в `checkUseOnlyDeclaredCursors` (импорт `os` остаётся — он нужен `checkShouldBeCP866`); проверить `go vet ./internal/review/`

## 2. Юнит-тесты: миграция существующих и новые регрессионные

- [x] 2.1 Перевести 9 существующих тестов `TestCheckUseOnlyDeclaredCursors_*` (`review_rules_test.go:4947-5262`) с temp-файла и `&Runner{}` на exec-паттерн (`r.exec` с expanded-контентом, `parsed` из `macroResult.Content`, `macroResult`), вынести общий хелпер (например `newCursorRuleRunner(t, content)`); временные файлы не нужны — `go test ./internal/review/ -run TestCheckUseOnlyDeclaredCursors_` — все проходят
- [x] 2.2 Добавить тест: файл с многострочным `#define` перед процедурой и корректным `__DECLARE_CURSOR__` + `OPEN/FETCH/CLOSE` даёт 0 findings — `go test ./internal/review/ -run TestCheckUseOnlyDeclaredCursors_`
- [x] 2.3 Добавить тест: при многострочном `#define` реально необъявленный курсор даёт finding; тест сам применяет `mapExpandedLineToOriginal(findings[0].Line, mr.SourceMap)` и сверяет результат со строкой использования в исходном файле — `go test ./internal/review/ -run TestCheckUseOnlyDeclaredCursors_`
- [x] 2.4 Добавить тест per-procedure области: курсор объявлен в первой процедуре и используется без объявления во второй — finding формируется во второй процедуре — `go test ./internal/review/ -run TestCheckUseOnlyDeclaredCursors_`

## 3. Приёмка на реальном файле

- [x] 3.1 В `CodeBase\cmd\root.go` поднять `buildNumber` с `"1714"` до `"1715"`, затем пересобрать `CodeBase.exe` (баннер должен показывать build 1715) и прогнать `codebase review D:\GITHUB\GolandProjects\FA\fa-generalledger\GeneralLedger\SERVER\SCRIPT_UTIL\CleaningData\Trunc_GL.sql --rules useOnlyDeclaredCursors` — ожидается 0 findings (было 4 FP)
- [x] 3.2 Временно добавить в копию того же файла использование необъявленного курсора в процедуре с многострочным `#define` выше — убедиться, что finding появляется ровно на строке использования в исходном файле; удалить копию

## 4. Финальные проверки

- [x] 4.1 `go test ./internal/review/...` и `gofmt -l internal/review` — без ошибок и без неотформатированных файлов
- [x] 4.2 `openspec validate fix-use-only-declared-cursors-expanded --strict` — valid
