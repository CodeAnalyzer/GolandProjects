# `rti_errors` и `rti_tree` не обогащают серверные вызовы данными из CodeBase (source_file, ret_val_meaning, error_constant)

**Дата:** 2026-07-06
**Файл:** `internal/mcp/registry.go`, обработчики `codebase_rti_errors` (строка ~633) и `codebase_rti_tree` (строка ~617)
**Версия CodeBase:** 0.8.3 build 1164
**Статус:** Не исправлено

## Описание

Команды `codebase_rti_errors` и `codebase_rti_tree` возвращают серверные вызовы без enrichment-данных из индекса CodeBase, несмотря на заявленное в описаниях:

- `rti_errors`: *"Returns server errors (procedure name, line number, return value, error context, elapsed time, nest level, module info, **error code description, source file**)"*
- `rti_tree`: *"The tree shows nested procedure calls with elapsed time, return values, module info, and **source file locations (enriched from CodeBase index)**"*

Поля `SourceFile`, `RetValMeaning`, `ErrorConstant` в структуре `RTICall` (model.go:28–31) **никогда не заполняются** — ни в `parseContent`, ни в `loadRTIFromArgs`, ни в обработчиках MCP.

## Воспроизведение

1. Вызвать `codebase_rti_errors` с `session_id` или `file_path` для лога с ошибками (RetVal ≠ 0).
2. В результате `server_errors` каждый вызов содержит `source_file: ""`, `ret_val_meaning: ""`, `error_constant: ""`.

3. Вызвать `codebase_rti_tree` с `session_id` или `file_path`.
4. В результате узлы дерева не содержат `source_file` (поле пустое).

### Альтернативный путь — работает

`codebase_rti_details` (registry.go:735–738) **корректно** обогащает вызовы через `rti.EnrichProcedure(q, procName)`, возвращая `enrichment` с `source_file`, `line_start`, `line_end`, `params`. Но это отдельный объект enrichment, а не заполнение полей в `RTICall`.

`codebase_rti_errors` и `codebase_rti_tree` вообще не вызывают `EnrichCalls` или `EnrichProcedure` для серверных вызовов.

## Анализ причины

### `rti_errors` — отсутствие enrichment

**Файл:** `internal/mcp/registry.go`, строки 645–668

```go
var serverErrors []callSlim
for _, c := range result.Calls {
    if c.RetVal != nil && *c.RetVal != 0 {
        serverErrors = append(serverErrors, callSlim{RTICall: c})
    }
}
// ← Нет вызова EnrichCalls или lookup return codes
// ← clientErrors обогащаются через EnrichClientEvents, serverErrors — нет
```

Обработчик фильтрует вызовы с ошибками, но:
1. Не вызывает `rti.EnrichCalls(q, errorCalls)` для получения `source_file`.
2. Не вызывает `q.LookupRetCode(retVal)` для получения `ret_val_meaning` (описания кода возврата).
3. Не заполняет `ErrorConstant` (имя константы ошибки).

При этом **клиентские** ошибки обогащаются: `clientEnrich = rti.EnrichClientEvents(q, clientErrors)` (строка 660). Асимметрия: клиентская часть обогащается, серверная — нет.

### `rti_tree` — отсутствие enrichment

**Файл:** `internal/mcp/registry.go`, строки 619–631

```go
tree := rti.BuildTree(result.Calls, procName, maxDepth)
if tree == nil {
    return nil, fmt.Errorf("procedure %q not found in RTI log", procName)
}
return tree, nil  // ← Нет enrichment
```

Дерево строится и возвращается как есть. `EnrichCalls` не вызывается. Узлы дерева (`RTITreeNode`) содержат `*RTICall` с пустыми `SourceFile`, `RetValMeaning`, `ErrorConstant`.

Функция `FormatTreeEnriched` (tree.go:95–99) существует и умеет форматировать дерево с enrichment, но в MCP-обработчике используется только `BuildTree` (возвращает структуру), а не форматирование с enrichment.

### `RetValMeaning` и `ErrorConstant` — никогда не заполняются

**Файл:** `internal/rti/model.go`, строки 27–31

```go
// Enriched fields (заполняются из CodeBase)
SourceFile    string `json:"source_file,omitempty"`
ModuleName    string `json:"module_name,omitempty"`
RetValMeaning string `json:"ret_val_meaning,omitempty"`
ErrorConstant string `json:"error_constant,omitempty"`
```

Комментарий гласит "заполняются из CodeBase", но поиск по кодовой базе показывает: **ни одно присваивание** этим полям не существует. `ModuleName` заполняется при парсинге (parser.go:189), остальные три поля — всегда пустые.

### `EnrichCalls` существует, но не вызывается из обработчиков

**Файл:** `internal/rti/enrich.go`, строки 28–46

```go
func EnrichCalls(q ProcedureLookup, calls []*RTICall) map[string]*ProcedureEnrichment {
    result := make(map[string]*ProcedureEnrichment)
    for _, c := range calls {
        if _, ok := result[c.Procedure]; ok { continue }
        enrich, err := EnrichProcedure(q, c.Procedure)
        // ...
        result[c.Procedure] = enrich
    }
    return result
}
```

Функция реализована, протестирована (`enrich_test.go`), но **не вызывается** ни из `rti_errors`, ни из `rti_tree`, ни из `rti_slow` для серверных вызовов. Возвращает map (procedure → enrichment), а не модифицирует `RTICall` напрямую.

## Влияние

- **`rti_errors`** — серверные ошибки не содержат `source_file` (непонятно, в каком файле процедура), `ret_val_meaning` (непонятно, что значит код возврата), `error_constant` (непонятно, какая константа ошибки). Пользователь вынужден отдельно вызывать `query_retcode` и `query_procedure` для каждой ошибки.
- **`rti_tree`** — узлы дерева не содержат `source_file`. Пользователь не может перейти к исходному коду процедуры из дерева вызовов.
- **`rti_slow`** — та же проблема: медленные вызовы не обогащаются `source_file`.
- Описание инструментов вводит в заблуждение, обещая enrichment, которого нет.

## Предлагаемое исправление

### Исправление `rti_errors`

**Файл:** `internal/mcp/registry.go`, обработчик `codebase_rti_errors`

```go
var serverErrors []callSlim
for _, c := range result.Calls {
    if c.RetVal != nil && *c.RetVal != 0 {
        serverErrors = append(serverErrors, callSlim{RTICall: c})
    }
}

// Enrich server errors with source files
var serverEnrich map[string]*rti.ProcedureEnrichment
if db != nil && len(serverErrors) > 0 {
    q := query.New(db)
    errorCalls := make([]*rti.RTICall, 0, len(serverErrors))
    for _, s := range serverErrors {
        errorCalls = append(errorCalls, s.RTICall)
    }
    serverEnrich = rti.EnrichCalls(q, errorCalls)

    // Lookup return code meanings
    for _, s := range serverErrors {
        if s.RetVal != nil {
            retCode, err := q.LookupRetCode(int64(*s.RetVal))
            if err == nil && retCode != nil {
                s.RetValMeaning = retCode.Message
                s.ErrorConstant = retCode.ProcName // или другое поле
            }
        }
    }
}

return map[string]interface{}{
    "server_errors":      serverErrors,
    "server_error_count": len(serverErrors),
    "server_enrichment":  serverEnrich,  // ← ДОБАВИТЬ
    "client_errors":      clientErrors,
    "client_error_count": len(clientErrors),
    "client_enrichment":  clientEnrich,
}, nil
```

### Исправление `rti_tree`

**Файл:** `internal/mcp/registry.go`, обработчик `codebase_rti_tree`

```go
tree := rti.BuildTree(result.Calls, procName, maxDepth)
if tree == nil {
    return nil, fmt.Errorf("procedure %q not found in RTI log", procName)
}

var enrichMap map[string]*rti.ProcedureEnrichment
if db != nil {
    q := query.New(db)
    enrichMap = rti.EnrichCalls(q, result.Calls)
}

return map[string]interface{}{
    "tree":       tree,
    "enrichment": enrichMap,  // ← ДОБАВИТЬ
}, nil
```

### Исправление `rti_slow`

Аналогично: добавить `serverEnrich = rti.EnrichCalls(q, slowCalls)` при `db != nil`.

### Тесты

- `TestRTIErrors_ServerEnrichment` — вызвать с `file_path` для лога с ошибками → `server_enrichment` не пустой, `server_errors[].source_file` (через enrichment) заполнен.
- `TestRTITree_Enrichment` — вызвать с `file_path` → `enrichment` не пустой, содержит `source_file` для процедур из дерева.
- `TestRTIErrors_RetValMeaning` — вызвать для лога с известным RetVal → `ret_val_meaning` совпадает с `query_retcode` для того же кода.

## Файлы для изменения

1. **`internal/mcp/registry.go`** — `codebase_rti_errors` (строки 645–668): добавить `EnrichCalls` и `LookupRetCode` для серверных ошибок.
2. **`internal/mcp/registry.go`** — `codebase_rti_tree` (строки 619–631): добавить `EnrichCalls` и вернуть `enrichment` в ответе.
3. **`internal/mcp/registry.go`** — `codebase_rti_slow` (строки 687–700): добавить `EnrichCalls` для медленных серверных вызовов.
4. **Тесты** — `internal/mcp/registry_test.go` или `internal/rti/enrich_test.go`.
