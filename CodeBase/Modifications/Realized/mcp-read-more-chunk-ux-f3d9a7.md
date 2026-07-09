# UX-улучшение: `codebase_read_more` — обязательный `chunk` или авто-прогресс

## Проблема

Инструмент `codebase_read_more` предназначен для чтения последующих чанков пагинированного MCP-ответа. Если вызовать его без параметра `chunk`, хэндлер в `registry.go` молча заменяет отсутствующее значение на `2`:

```go
chunk, err := optionalInt(args, "chunk")
if chunk < 2 {
    chunk = 2
}
return globalPages.readChunk(id, chunk)
```

Получается, что при каждом вызове с одним только `continuation_id` возвращается чанк `2` снова и снова. Пользователь не получает ошибки, но и не продвигается по ответу. Это приводит к:

- повторному чтению одного и того же чанка;
- непониманию, что пора передать `chunk=3`, `chunk=4` и т.д.;
- лишним вызовам инструмента и трате контекста.

## Предлагаемое решение

**Вариант A (рекомендуемый):** сделать `chunk` обязательным параметром `codebase_read_more`. При отсутствии `chunk` инструмент должен возвращать ошибку, а не default `2`.

**Вариант B (альтернативный):** в `pageEntry` хранить `lastReadChunk` и при вызове без `chunk` автоматически возвращать следующий чанк.

В предлагаемом плане реализуется **Вариант A**, как наиболее простой, явный и не требующий изменения состояния `pageStore`.

## Затрагиваемые файлы

| Файл | Действие |
|---|---|
| `internal/mcp/registry.go` | `codebase_read_more`: сделать `chunk` обязательным, убрать fallback на `2` |
| `internal/mcp/server_test.go` | обновить/добавить тесты валидации `chunk` |

## Шаг 1 — `internal/mcp/registry.go`

Заменить хэндлер `codebase_read_more`:

```go
"codebase_read_more": {
    Definition: toolDefinition{
        Name: "codebase_read_more",
        Description: "Read the next chunk of a paginated MCP response. " +
            "Use this when a previous tool response starts with '⚠️ PAGINATED RESPONSE'. " +
            "Copy the continuation_id and chunk number from the '👉 Call' hint. " +
            "Repeat until you see '✅ FINAL CHUNK'.",
        InputSchema: objectSchema(map[string]interface{}{
            "continuation_id": stringProp("Continuation ID from the paginated response header"),
            "chunk":           intProp("Chunk number to read (as shown in the 👉 hint)"),
        },
        // chunk становится обязательным
        Required: []string{"continuation_id", "chunk"},
        ),
    },
    Handler: func(args map[string]interface{}) (interface{}, error) {
        id, err := requiredString(args, "continuation_id")
        if err != nil {
            return nil, err
        }
        chunk, err := requiredInt(args, "chunk")
        if err != nil {
            return nil, err
        }
        if chunk < 2 {
            return nil, fmt.Errorf("chunk must be >= 2")
        }
        return globalPages.readChunk(id, chunk)
    },
},
```

Добавить вспомогательную функцию `requiredInt` рядом с `requiredString`:

```go
func requiredInt(args map[string]interface{}, key string) (int, error) {
    value, ok := args[key]
    if !ok || value == nil {
        return 0, fmt.Errorf("missing required argument: %s", key)
    }
    switch v := value.(type) {
    case int:
        return v, nil
    case float64:
        return int(v), nil
    default:
        return 0, fmt.Errorf("argument %s must be integer", key)
    }
}
```

## Шаг 2 — `internal/mcp/server_test.go`

Добавить тесты:

```go
func TestReadMoreHandlerRequiresChunk(t *testing.T) {
    tool, ok := toolRegistry["codebase_read_more"]
    if !ok {
        t.Fatal("codebase_read_more not found in registry")
    }

    _, err := tool.Handler(map[string]interface{}{
        "continuation_id": "abc123",
    })
    if err == nil {
        t.Fatal("expected error when chunk is missing")
    }
}

func TestReadMoreHandlerChunkLessThanTwo(t *testing.T) {
    tool := toolRegistry["codebase_read_more"]

    _, err := tool.Handler(map[string]interface{}{
        "continuation_id": "abc123",
        "chunk":           1,
    })
    if err == nil {
        t.Fatal("expected error for chunk < 2")
    }
}
```

## Проверка

```bash
go build ./...
go test ./internal/mcp/...
```

## Результат

- Вызов `codebase_read_more` без `chunk` сразу возвращает понятную ошибку.
- Исключается молчаливое зацикливание на чанке 2.
- Поведение соответствует подсказке в пагинированном ответе (`chunk=2`, `chunk=3` ...).
