# Универсальная пагинация MCP-ответов

Добавить прозрачный механизм разбивки на чанки для всех MCP-инструментов: если ответ превышает лимит (задаётся в `codebase.toml`), он сохраняется в памяти и возвращается по частям через новый инструмент `codebase_read_more`.

---

## Затрагиваемые файлы

| Файл | Действие |
|---|---|
| `internal/config/config.go` | добавить `MCPConfig` + поле `MCP` в `Config` |
| `internal/mcp/pagination.go` | **новый** — `pageStore`, `rawMCPText`, `maybePaginate`, `readChunk` |
| `internal/mcp/pagination_test.go` | **новый** — unit-тесты пагинатора |
| `internal/mcp/server.go` | добавить `sdkToolPagedResult`, заменить вызов, инициализация из конфига |
| `internal/mcp/registry.go` | добавить инструмент `codebase_read_more` |
| `internal/mcp/server_test.go` | добавить тесты для `codebase_read_more` в реестре |

---

## Шаг 1 — Конфиг: `MCPConfig`

В `internal/config/config.go`:
- Добавить структуру:
  ```go
  type MCPConfig struct {
      PaginationChunkSize int `toml:"pagination_chunk_size"`
  }
  ```
- Добавить поле `MCP MCPConfig` в `Config`
- В `Load()` — дефолт: `cfg.MCP.PaginationChunkSize = 12_000` если 0
- В `CreateDefault` — `MCP: MCPConfig{PaginationChunkSize: 12_000}`

---

## Шаг 2 — Ядро пагинации: `internal/mcp/pagination.go`

**Типы и константы:**
```go
const defaultChunkSize = 12_000
const paginationTTL    = 15 * time.Minute

type rawMCPText string   // sentinel: verbatim, без JSON-маршалинга и без re-пагинации

type pageEntry struct {
    chunks    []string
    createdAt time.Time
}
type pageStore struct {
    chunkSize int
    mu        sync.Mutex
    entries   map[string]*pageEntry
}
var globalPages = newPageStore(defaultChunkSize)
```

**Методы `pageStore`:**
- `maybePaginate(text string) string`
  - Если `len(text) <= chunkSize` → возвращает без изменений
  - Иначе: `splitChunks` → сохраняет в `entries` с UUID → возвращает заголовок + `chunks[0]`:
    ```
    ⚠️ PAGINATED RESPONSE: chunk 1/N | continuation_id="abc123"
    👉 Call codebase_read_more(continuation_id="abc123", chunk=2) for next part.

    <первые 12KB JSON>
    ```
- `readChunk(id string, chunkIdx int) (rawMCPText, error)`
  - Находит entry, проверяет диапазон 2..N
  - Последний чанк удаляет entry из map, возвращает `✅ FINAL CHUNK`
  - Промежуточный — возвращает заголовок с подсказкой для следующего вызова
- `splitChunks(text string, size int) []string` — нарезка по байтам
- `gc()` — удаляет записи старше TTL (вызывается при каждом `maybePaginate`)
- `newEntryID() string` — `crypto/rand`, 8 байт hex

---

## Шаг 3 — `internal/mcp/server.go`

**Новая функция** (рядом с `sdkToolJSONResult`):
```go
func sdkToolPagedResult(value interface{}) (*mcpsdk.CallToolResult, error) {
    if raw, ok := value.(rawMCPText); ok {
        return &mcpsdk.CallToolResult{
            Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: string(raw)}},
        }, nil
    }
    text, err := toJSONText(value)
    if err != nil {
        return nil, err
    }
    text = globalPages.maybePaginate(text)
    return &mcpsdk.CallToolResult{
        Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: text}},
    }, nil
}
```

**Изменения в `RunStdio`** — сразу после `cfg := config.Get()`:
```go
if cfg.MCP.PaginationChunkSize > 0 {
    globalPages = newPageStore(cfg.MCP.PaginationChunkSize)
}
```

**Строка 68** в `registerSDKCoreTools`:
```go
// было:
return sdkToolJSONResult(result)
// стало:
return sdkToolPagedResult(result)
```

---

## Шаг 4 — `internal/mcp/registry.go`: инструмент `codebase_read_more`

Добавить запись в `buildToolRegistry`:
```go
"codebase_read_more": {
    Definition: toolDefinition{
        Name: "codebase_read_more",
        Description: "Read the next chunk of a paginated MCP response. "+
            "Use this when a previous tool response starts with '⚠️ PAGINATED RESPONSE'. "+
            "Copy continuation_id and chunk number from the '👉 Call' hint. "+
            "Repeat until you see '✅ FINAL CHUNK'.",
        InputSchema: objectSchema(map[string]interface{}{
            "continuation_id": stringProp("Continuation ID from the paginated response header"),
            "chunk":           intProp("Chunk number to read (as shown in the 👉 hint)"),
        }),
    },
    Handler: func(args map[string]interface{}) (interface{}, error) {
        id, err := requiredString(args, "continuation_id")
        if err != nil {
            return nil, err
        }
        chunk, _ := optionalInt(args, "chunk")
        if chunk < 2 {
            chunk = 2
        }
        return globalPages.readChunk(id, chunk)
    },
},
```

---

## Шаг 5 — `internal/mcp/pagination_test.go` (unit-тесты)

| Тест | Что проверяет |
|---|---|
| `TestSplitChunks_Even` | ровное деление без остатка |
| `TestSplitChunks_WithRemainder` | последний чанк меньше размера |
| `TestMaybePaginate_SmallResponse` | ответ ≤ chunkSize → без изменений |
| `TestMaybePaginate_LargeResponse` | создаёт entry, возвращает заголовок + chunk[0] |
| `TestReadChunk_Sequential` | чтение chunk 2, затем 3 (финальный) |
| `TestReadChunk_LastChunkDeletesEntry` | финальный чанк удаляет entry из store |
| `TestReadChunk_OutOfRange` | chunk < 2 или > N → ошибка |
| `TestReadChunk_UnknownID` | несуществующий ID → ошибка |
| `TestReadChunk_RawTextBypassesPagination` | `rawMCPText` не проходит через `maybePaginate` |
| `TestPageStore_GC` | истёкшие записи удаляются при следующем `maybePaginate` |

---

## Шаг 6 — `internal/mcp/server_test.go`

Добавить:
- `TestReadMoreToolInRegistry` — `codebase_read_more` присутствует в `toolRegistry`
- `TestReadMoreHandlerRequiresContinuationID` — пустые args → ошибка
- `TestReadMoreHandlerUnknownID` — неизвестный `continuation_id` → ошибка

---

## Проверка

```
go build ./...
go test ./internal/mcp/... ./internal/config/...
```
