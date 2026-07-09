# Обязательный параметр `chunk` для `codebase_read_more`

Сделать `chunk` обязательным параметром инструмента `codebase_read_more`, убрав молчаливый fallback на значение `2`, чтобы агент не застревал в цикле повторного чтения одного чанка.

## Проблема

В `@d:\GITHUB\GolandProjects\CodeBase\internal\mcp\registry.go:82-88` хэндлер `codebase_read_more` использует `optionalInt(args, "chunk")` и при отсутствии/невалидном значении подставляет `chunk = 2`. При вызове без `chunk` агент бесконечно получает чанк 2 вместо ошибки.

Валидация диапазона (`chunkIdx < 2 || chunkIdx > len(entry.chunks)`) уже присутствует в `pageStore.readChunk` (`@d:\GITHUB\GolandProjects\CodeBase\internal\mcp\pagination.go:75-78`) — дублировать её в хэндлере не нужно (решение пользователя: без явной доп.проверки).

## Изменения

### 1. `internal/mcp/registry.go`

- Добавить функцию `requiredInt(args map[string]interface{}, key string) (int, error)` рядом с существующей `optionalInt` (после строки 59): возвращает ошибку `"missing required argument: %s"`, если ключ отсутствует/nil; поддерживает `int` и `float64` (как JSON-числа), иначе ошибка `"argument %s must be integer"`.
- В хэндлере `codebase_read_more` (строки 77-90) заменить `optionalInt` на `requiredInt`, убрать блок `if chunk < 2 { chunk = 2 }`.
- В `InputSchema` для `codebase_read_more` добавить `"required": []string{"continuation_id", "chunk"}` в саму схему (аналогично тому, как `querySchema` добавляет `schema["required"] = []string{requiredField}` на строке 924) — **не трогая** структуру `toolDefinition` (поля `Required` в ней нет и не нужно, т.к. `objectSchema` возвращает `map[string]interface{}`, куда можно добавить ключ напрямую).

Итоговый вид схемы:
```go
InputSchema: func() map[string]interface{} {
    s := objectSchema(map[string]interface{}{
        "continuation_id": stringProp("Continuation ID from the paginated response header"),
        "chunk":           intProp("Chunk number to read (as shown in the 👉 hint)"),
    })
    s["required"] = []string{"continuation_id", "chunk"}
    return s
}(),
```
(либо эквивалентно — присвоить переменной перед `toolDefinition{}` и передать её).

### 2. `internal/mcp/server_test.go`

Добавить тесты рядом с существующими `TestReadMoreHandler*` (после строки 214):

- `TestReadMoreHandlerRequiresChunk` — вызов с только `continuation_id`, ожидается ошибка.
- `TestReadMoreHandlerChunkWrongType` — вызов с `chunk: "abc"` (строка вместо числа), ожидается ошибка.

Существующие тесты `TestReadMoreHandlerRequiresContinuationID` и `TestReadMoreHandlerUnknownID` (используют `chunk: float64(2)`) не потребуют изменений — они уже передают корректный `chunk`.

Тест на `chunk < 2` не нужен на уровне хэндлера (по решению пользователя) — эта проверка остаётся в `readChunk` и уже покрыта существующими тестами в `pagination_test.go`.

## Проверка

```powershell
go build ./...
go test ./internal/mcp/...
```

## Затрагиваемые файлы

| Файл | Действие |
|---|---|
| `internal/mcp/registry.go` | добавить `requiredInt`; хэндлер `codebase_read_more` — `requiredInt` вместо `optionalInt`, убрать fallback; добавить `required` в схему |
| `internal/mcp/server_test.go` | 2 новых теста |
