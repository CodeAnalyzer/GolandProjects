# Tasks: fix-spec-doc-drift-batch

Номера строк — ориентиры на 2026-10-10; правки искать по содержимому (строки смещаются).

## 1. Правка кода: словарь направлений spec deps

- [x] 1.1 В `cmd/query_spec.go:70` заменить Use-string `deps --name <capability> [--direction outgoing|incoming] [--max-depth N]` на `deps --name <capability> [--direction depends_on|depended_by] [--max-depth N]` (флаг-описание `:156` «depends_on or depended_by» уже корректно; нормализация алиасов в `internal/specsvc/specsvc.go:481–490` не трогается). Проверка: `go build ./...` и `go test ./cmd/...` зелёные; grep `outgoing|incoming` по `cmd/*.go` не находит Use-string.
- [x] 1.2 Убедиться, что вывод help показывает новый словарь: `go run . query spec deps --help` выводит `[--direction depends_on|depended_by]`, а вызов с `--direction outgoing` по-прежнему работает (алиас нормализуется).

## 2. Прямые правки основных спек (Related code / Notes — вне дельта-механизма)

- [x] 2.1 `openspec/specs/rti-analysis/rti-parsing/spec.md`: в Related code (строка ~151) убрать «(94 записи)» из «`moduleIDMap` (94 записи), `ModuleNameByID`»; в Notes (~163) убрать «(94 записи)» из «Справочник `moduleIDMap` (94 записи) зашит в коде». Проверка: grep «94 записи» по всем спекам — пусто.
- [x] 2.2 `openspec/specs/mcp-server/mcp-pagination/spec.md`: в Related code добавить bullet «`cmd/root.go` — вызов `SetPaginationTTL` при загрузке конфигурации (bootstrap, до входа в `RunStdio`)»; в Notes (~133) заменить хвост «`SetPaginationTTL` меняет TTL для уже созданного store» на «`SetPaginationTTL` вызывается bootstrap CLI (`cmd/root.go`) при загрузке конфига — до создания store в `RunStdio`». Проверка: grep «уже созданного store» — пусто.
- [x] 2.3 `openspec/specs/trc-analysis/trc-storage/spec.md`: в Related code (~105) заменить «`ParseToDB`» на «`ParseFileToDB`». Проверка: grep «ParseToDB» (без File) по спекам — пусто.
- [x] 2.4 `openspec/specs/indexing/api-macros-t01/spec.md`: в Notes добавить фразу «Значения `macro_type` в БД хранятся в lowercase (`create_proc`/`init_event`/`exec_contract`); имена `API_CREATE_PROC` и т.п. — текст вызова макроса в исходнике, а не значение столбца». Проверка: фраза присутствует в Notes.

## 3. Валидация change и сборка

- [x] 3.1 `openspec validate fix-spec-doc-drift-batch` — без ошибок и warnings (дельты соответствуют существующим требованиям: rti-parsing «Справочник Module ID → Product Name», mcp-transport-tools «Query инструменты»/«Профильная регистрация инструментов»/«Spec инструменты», mcp-pagination «Динамическое применение TTL из конфига», spec-queries «Спеки по коду (spec_by_code)»/«Зависимости capability (spec_deps)», api-macros-t01 «Relations из API-макросов (implements_contract, publishes_event, executes_contract)»).
- [x] 3.2 `go build ./...` и `go test ./...` — зелёные (правка 1.1 — единственная кодовая, тестов на Use-string нет, но проверка дешёвая).

## 4. Архивирование и контроль пост-фактум

- [x] 4.1 Заархивировать change (`openspec archive fix-spec-doc-drift-batch`) — дельты применяются к основным спекам. Проверка: `openspec validate --specs` — без ошибок.
- [x] 4.2 Grep-контроль основных спек после архива: «55 инструментов», «(94 записи», «~17», «~30», «~14», «~5)» в mcp-transport-tools, «результат группируется по capability» в spec-queries, «`SetPaginationTTL`» рядом с «`RunStdio`» в mcp-pagination (в одной фразе о вызове) — всё пусто; `codebase_query_desc_search` и `codebase_query_spec_config` присутствуют в списках mcp-transport-tools.
