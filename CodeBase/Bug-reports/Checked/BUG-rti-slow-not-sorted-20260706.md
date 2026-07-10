# `rti_slow` не сортирует серверные вызовы по убыванию elapsed time

**Дата:** 2026-07-06
**Файл:** `internal/mcp/registry.go`, обработчик `codebase_rti_slow` (строка ~687)
**Версия CodeBase:** 0.8.3 build 1164
**Статус:** Не исправлено

## Описание

Команда `codebase_rti_slow` возвращает серверные вызовы с `elapsed_ms >= threshold`, но **не сортирует** их по убыванию `elapsed_ms`, как заявлено в описании: *"Returns server calls sorted by elapsed time descending"*. Вызовы возвращаются в порядке их следования в RTI-логе (или порядке ID из БД), что делает результат неудобным для анализа — самые медленные вызовы не находятся в начале списка.

## Воспроизведение

1. Вызвать `codebase_rti_slow` с `session_id` или `file_path` для RTI-лога, содержащего несколько вызовов с `elapsed_ms >= 100`.
2. Получить результат: `server_calls` содержит вызовы в порядке их появления в логе, а не отсортированные по `elapsed_ms` descending.

### Пример

```
rti_slow(session_id=18, threshold_ms=100)
→ server_calls: [
    {procedure: "API_LnExt_GetLstForCredHistReq", elapsed_ms: 8327, enter_line: 291},
    {procedure: "API_Acc_GetListRestTurnByDate", elapsed_ms: 101, enter_line: 7995},
    {procedure: "MassAccrual_Step", elapsed_ms: 5234, enter_line: 100},
    ...
  ]
```

Ожидаемый порядок: 8327, 5234, ..., 101 (по убыванию).
Фактический порядок: 8327, 101, 5234 (в порядке строк в логе).

## Анализ причины

**Файл:** `internal/mcp/registry.go`, строки 687–692

```go
var slow []callSlim
for _, c := range result.Calls {
    if c.ElapsedMs >= threshold {
        slow = append(slow, callSlim{RTICall: c})
    }
}
```

Вызовы фильтруются по порогу, но **не сортируются**. `result.Calls` из `LoadCalls` (store.go:644) загружаются `ORDER BY id` (порядок вставки = порядок строк в логе). Из `ParseFile` (parser.go) вызовы накапливаются в `allCalls` в порядке строк файла. Ни один из путей не обеспечивает сортировку по `elapsed_ms`.

Для сравнения, клиентские SQL-блоки в том же обработчике сортируются через `topSlowClientSQLEvents` (parser_client.go:349–358), которая вызывает `sortClientEventsByDuration`. Серверная сторона не имеет аналогичной сортировки.

## Влияние

- Результат `rti_slow` неудобен для анализа: самые медленные вызовы могут оказаться в любом месте списка.
- При большом количестве медленных вызовов (например, 134 в тестовом логе) пользователь вынужден вручную искать самые медленные.
- Описание инструмента вводит в заблуждение: заявлена сортировка, которой нет.

## Предлагаемое исправление

**Файл:** `internal/mcp/registry.go`, после строки 692

Добавить сортировку:

```go
var slow []callSlim
for _, c := range result.Calls {
    if c.ElapsedMs >= threshold {
        slow = append(slow, callSlim{RTICall: c})
    }
}
// Сортировать по elapsed_ms убыванию
sort.Slice(slow, func(i, j int) bool {
    return slow[i].ElapsedMs > slow[j].ElapsedMs
})
```

Необходимо добавить `"sort"` в импорты, если его нет.

### Тесты

- `TestRTISlow_SortedByElapsedDesc` — вызвать с `file_path` для лога с вызовами 200мс, 500мс, 100мс → результат `[500, 200, 100]`.

## Файлы для изменения

1. **`internal/mcp/registry.go`** — обработчик `codebase_rti_slow`, строки 687–692: добавить `sort.Slice` по `ElapsedMs` descending.
