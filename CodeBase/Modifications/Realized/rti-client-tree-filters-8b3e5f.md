# План: фильтры и краткий формат для `codebase_rti_client_tree`

Добавить в MCP-инструмент `codebase_rti_client_tree` опциональные фильтры (время, класс/метод) и краткий формат вывода (`format=short`), по аналогии с уже реализованным `codebase_rti_timeline`, чтобы сократить объём JSON-ответа для больших клиентских сессий.

## Контекст

- Проблема (из `@D:/GITHUB/GolandProjects/CodeBase/Modifications/rti-client-tree-filters.md`): для сессии из 1328 событий/5 PID ответ разбивается на 110 чанков пагинации — непригодно для интерактивного анализа.
- Текущий хэндлер: `@D:/GITHUB/GolandProjects/CodeBase/internal/mcp/registry.go:886-905`. Принимает только `session_id`/`file_path`/`pid`. Строит дерево через `rti.BuildClientTree(events, pid)`, обогащает все события целиком через `EnrichClientEvents`.
- `rti.BuildClientTree(events []*RTIClientEvent, pid int) []*RTIClientTreeNode` — группирует события по PID, сортирует по времени; при `pid > 0` возвращает только эту группу (`@D:/GITHUB/GolandProjects/CodeBase/internal/rti/tree.go:140-168`).
- `RTIClientTreeNode{PID int, Events []*RTIClientEvent}` (`tree.go:140-143`).
- Уже существует переиспользуемая инфраструктура из предыдущей доработки `codebase_rti_timeline` (`@D:/GITHUB/GolandProjects/CodeBase/internal/rti/timeline.go`): `TimelineFilter`, `ApplyTimelineFilter`, `RTIClientEventShort`, `ToShortEvent`. Эта доработка **переиспользует** их, а не дублирует логику.

## Решения по открытым вопросам (согласовано с пользователем)

1. **Именование счётчиков** — для единообразия со стилем `codebase_rti_timeline` использовать `total_events_count`/`filtered_events_count` (НЕ `total_count`/`filtered_count`, как в исходном proposal).
2. **Устранить дублирование PID-фильтрации** — после `FilterClientEvents` (фильтрация по `filter.PID`) события уже принадлежат нужному PID (если он указан). Поэтому `BuildClientTree` вызывается с `pid=0` (просто группировка отфильтрованных событий), а не с повторной передачей `pidVal`.

## Шаги реализации

### 1. `internal/rti/timeline.go` — новые функции/типы (дополнение к существующему файлу)

- `FilterClientEvents(events []*RTIClientEvent, f TimelineFilter) []*RTIClientEvent` — тонкая обёртка над `ApplyTimelineFilter(nil, events, f)`, возвращает только `events`-часть.
- `RTIClientTreeNodeShort struct { PID int; Events []RTIClientEventShort }`.
- `ToShortClientTreeNode(node *RTIClientTreeNode) RTIClientTreeNodeShort` — маппер, использующий уже существующий `ToShortEvent`.

### 2. `internal/mcp/registry.go` — схема и хэндлер `codebase_rti_client_tree`

**Схема** (добавить в `InputSchema`, аналогично `codebase_rti_timeline`):
- `time_from`, `time_to` (string, RFC3339)
- `class_name`, `method_name` (string, case-insensitive)
- `format` (string: `full`/`short`)
- `pid` — остаётся как есть

Обновить `Description` инструмента с упоминанием новых фильтров.

**Хэндлер** — переписать по образцу хэндлера `codebase_rti_timeline`:
1. `loadRTIFromArgs`.
2. Собрать `rti.TimelineFilter` из `class_name`, `method_name`, `format`, `time_from`, `time_to`, `pid` — **с обязательной обработкой ошибок** каждого `optionalString`/`optionalInt` (не игнорировать `err` через `_`, как сейчас в строке `pid, _ := optionalInt(args, "pid")`).
3. Парсинг `time_from`/`time_to` через `time.Parse(time.RFC3339, ...)` — при ошибке возвращать явную ошибку хэндлера.
4. `filteredEvents := rti.FilterClientEvents(result.ClientEvents, filter)`.
5. `nodes := rti.BuildClientTree(filteredEvents, 0)` — **pid=0**, так как PID-фильтрация уже применена на шаге 4 (решение по п.2 выше).
6. Обогащение **после** фильтрации: `clientEnrich = rti.EnrichClientEvents(q, filteredEvents)` (только если `db != nil && len(filteredEvents) > 0`).
7. Если `format == "short"` (`EqualFold`) — конвертировать `nodes` в `[]rti.RTIClientTreeNodeShort` через `ToShortClientTreeNode`.
8. Ответ:
   ```go
   map[string]interface{}{
       "nodes":                 respNodes,
       "enrichment":            clientEnrich,
       "total_events_count":    len(result.ClientEvents),
       "filtered_events_count": len(filteredEvents),
   }
   ```

### 3. Тесты

- `internal/rti/timeline_test.go` (дополнить существующий файл):
  - `TestFilterClientEvents_ByTimeAndClass` — базовая проверка обёртки над `ApplyTimelineFilter`.
  - `TestToShortClientTreeNode_DropsHeavyFields` — проверка, что узел корректно конвертируется, события внутри урезаны.
- Регресс существующих тестов `internal/mcp` (уже покрывающих `codebase_rti_client_tree`, если такие есть — проверить `@D:/GITHUB/GolandProjects/CodeBase/internal/mcp` на наличие тестов для этого инструмента при реализации).

### 4. Проверка

```powershell
go build ./...
go test ./internal/rti/... ./internal/mcp/...
```

Ручная проверка сценария из proposal: `pid=1344` + `format=short` → объём ответа должен сократиться с ~110 чанков до нескольких.

## Затрагиваемые файлы

| Файл | Действие |
|---|---|
| `internal/rti/timeline.go` | дополнить: `FilterClientEvents`, `RTIClientTreeNodeShort`, `ToShortClientTreeNode` |
| `internal/rti/timeline_test.go` | дополнить: 2 новых теста |
| `internal/mcp/registry.go` | схема + хэндлер `codebase_rti_client_tree`: фильтры, short-формат, счётчики, устранение дублирования PID-фильтрации |

## Риски / нюансы

- Текущая строка `pid, _ := optionalInt(args, "pid")` (`registry.go:893`) игнорирует ошибку — при рефакторинге обязательно обработать её явно.
- `BuildClientTree(filteredEvents, 0)` — убедиться, что при пустом `filteredEvents` функция возвращает пустой (не nil-с-паникой) слайс узлов; текущая реализация уже создаёт `nodes := make([]*RTIClientTreeNode, 0, len(order))`, что безопасно.
- `FilterClientEvents` вызывает `ApplyTimelineFilter(nil, events, f)` — убедиться, что `range` по `nil calls` в `ApplyTimelineFilter` не паникует (в Go `range nil` безопасен, возвращает 0 итераций) — уже подтверждено в текущей реализации `timeline.go`.
- Обновить `Description` инструмента консистентно с `codebase_rti_timeline` (единый стиль формулировок фильтров).
