## 1. Схема БД

- [x] 1.1 Добавить `session_name TEXT NOT NULL DEFAULT ''` и идемпотентный backfill из `file_path` для `rti_sessions` и `trc_sessions` в `internal/store/db_schema.go`; проверить `TestInitSchema_IdempotentAndHasRequiredObjects` и повторный `InitSchemaCtx`.
- [x] 1.2 Добавить интеграционный тест: legacy-строка с пустым `session_name` получает имя файла после `InitSchemaCtx`, повторный запуск не меняет заполненное имя.

## 2. Доменные модели и store RTI

- [x] 2.1 Добавить `SessionName` в `RTISession` (`internal/rti/model.go`) и включить `session_name` в SELECT/list/get; проверить тестом `ListSessions`/`GetSession`.
- [x] 2.2 Изменить `rti.SaveSession` на приём `sessionName` и запись в INSERT; обновить вызов в `rtisvc` и проверить интеграционным тестом сохранения с именем.
- [x] 2.3 Добавить `SetSessionName` и использовать его в reuse-ветке при явно заданном имени; проверить интеграционным тестом «reuse переименовывает» и «reuse без имени не меняет».
- [x] 2.4 Добавить `FindSessionIDsByName` и вынести общий `deleteSessionIDs` из `DeleteSession`/`PruneSessions`; проверить существующие тесты удаления, correlated client events и legacy FK.
- [x] 2.5 Добавить `PruneSessionsOlderThan(cutoff)` с выборкой по `parsed_at < cutoff` через общий helper; проверить тестом: старые удалены, свежие нетронуты.

## 3. Доменные модели и store TRC

- [x] 3.1 Добавить `SessionName` в `TRCSession` (`internal/trc/model.go`) и включить `session_name` в SELECT/list/get; проверить тестом `ListSessions`/`GetSession`.
- [x] 3.2 Изменить `trc.ParseFileToDB` и `trc.SaveSession` на приём `sessionName` и запись в INSERT; обновить вызовы и проверить интеграционным тестом сохранения с именем.
- [x] 3.3 Добавить `SetSessionName` и использовать его в reuse-ветке; проверить тестами reuse с переименованием и без.
- [x] 3.4 Добавить `FindSessionIDsByName` и вынести общий `deleteSessionIDs` из `DeleteSession`/`PruneSessions`; проверить существующие интеграционные тесты удаления.
- [x] 3.5 Добавить `PruneSessionsOlderThan(cutoff)` через общий helper; проверить тестом «keep-hours удаляет только сессии старше cutoff».

## 4. Сервисный слой

- [x] 4.1 Ввести `ParseOptions{Name}`, `DeleteParams{SessionID, Name}`, `PruneParams{KeepLast *int, KeepHours *int}` в `internal/rtisvc/types.go` и `internal/trcsvc/types.go`; проверить компиляцию пакетов.
- [x] 4.2 Обновить `ExecuteParse` (default/имя/reuse-rename) для RTI и TRC; проверить интеграционными тестами обоих пакетов.
- [x] 4.3 Обновить `ExecuteDelete` (ровно один селектор, все совпадения по имени, `DeletedCount`/`SessionIDs`, ошибка при отсутствии) для RTI и TRC; проверить тестами одиночного/множественного удаления и конфликта параметров.
- [x] 4.4 Обновить `ExecutePrune` (взаимоисключение, `keep_hours > 0`, `nil,nil` → keep_last 0, `KeptHours`/`Cutoff`) для RTI и TRC; проверить тестами режимов и валидации.

## 5. CLI

- [x] 5.1 Добавить `--name` в `rti parse`/`trc parse` и передавать его в сервис; проверить `go test ./cmd` и ручным запуском с именем.
- [x] 5.2 Добавить `--name` в `rti delete`/`trc delete` с взаимоисключением `--session`; вывести `deleted_count`/ID; проверить тестами парсинга флагов и ручным запуском.
- [x] 5.3 Добавить `--keep-hours` в `rti prune`/`trc prune`, взаимоисключение с `--keep-last`, валидацию `> 0` и вывод `kept_hours`/`cutoff`; проверить тестами и ручным запуском.
- [x] 5.4 Выводить имя сессии в `rti list`/`trc list`; проверить `--json` и текстовый вывод.

## 6. MCP

- [x] 6.1 Добавить optional `name` в схемы и обработчики `codebase_rti_parse`/`codebase_trc_parse`; проверить тестом tool args и вызовом через stdio.
- [x] 6.2 Добавить `name` и взаимоисключение с `session_id` в `codebase_rti_delete`/`codebase_trc_delete`; проверить тестами конфликта и удаления по имени.
- [x] 6.3 Добавить `keep_hours` и взаимоисключение с `keep_last` в `codebase_rti_prune`/`codebase_trc_prune`; проверить тестами валидации и режима по часам.
- [x] 6.4 Обновить descriptions инструментов и проверить, что `codebase_rti_list`/`codebase_trc_list` отдают `session_name`.

## 7. Документация и финализация

- [x] 7.1 Обновить `README.md` (секции RTI/TRC): `--name`, delete-by-name, `--keep-hours`, поле имени в list.
- [x] 7.2 Прогнать `go test ./...` (unit + integration) и `go vet ./...`; устранить компиляционные ripple от изменённых сигнатур.
- [x] 7.3 Прогнать `openspec validate --all` и убедиться, что change и основные спеки валидны.
- [x] 7.4 Поднять `buildNumber` в `cmd/root.go` до `1618`, пересобрать `CodeBase.exe` в корне проекта (`D:\GITHUB\GolandProjects\CodeBase\CodeBase.exe`), применить схему через `codebase health` и выполнить smoke: parse с `--name`, reuse-rename, delete-by-name, prune `--keep-hours`.
