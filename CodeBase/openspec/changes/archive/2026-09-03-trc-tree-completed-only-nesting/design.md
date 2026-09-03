## Context

Текущее построение дерева TRC использует стек Starting/Completed пар. Когда Starting-события отсутствуют (частая конфигурация Profiler), стек всегда пуст → все Completed-события становятся корневыми узлами с `ParentID = -1`, `Depth = 0`. Временные интервалы событий при этом игнорируются, хотя они явно указывают на вложенность (см. `trc-tree-flat.txt` — 6 плоских «пеньков» вместо 3 пар).

Парсинг выполняется через `IncrementalParentTracker` (`parent_tracker.go`), который стримит события в БД. `parent_id` и `depth` сохраняются в `trc_events`. При загрузке из БД `LoadEventsForTree` (`store.go`) использует recursive CTE по `parent_id` — CTE не нужно менять, если `parent_id` корректен.

## Goals / Non-Goals

**Goals:**
- Восстановить вложенность для Completed-only трейсов, используя временное вложение интервалов и иерархию EventClass
- Применить fallback в `IncrementalParentTracker` (стриминг), `ComputeParentIDs` (память) — `LoadEventsForTree` CTE не требует изменений
- Сохранить существующий алгоритм Starting/Completed для трейсов с Starting-событиями

**Non-Goals:**
- Не заменять Starting/Completed алгоритм — только fallback
- Не менять схему БД — `parent_id`/`depth` те же колонки
- Не менять CLI/MCP API — те же флаги, тот же формат вывода
- Не обрабатывать XEL-события (wait_completed и др.) — только RPC/SP/SP:Stmt/SQL:Batch/SQL:Stmt Completed

## Decisions

### 1. EventClass иерархия для fallback

Иерархия: `RPC:Completed` (11) > `SP:Completed` (43) > `SP:StmtCompleted` (45) > `SQL:BatchCompleted` (12) > `SQL:StmtCompleted` (41).

**Рationale**: EventClass численно отражает уровень вызова в SQL Server. RPC — внешний вызов, SP — хранимая процедура, SP:Stmt — оператор внутри SP, SQL:Batch — пакет, SQL:Stmt — оператор в пакете. Числовые значения EventClass не упорядочены по уровню (11 < 43 < 45, но 12 < 41), поэтому требуется явная карта порядка.

**Альтернатива**: Использовать NestLevel (column 29) — но колонка часто отсутствует в трейсах. EventClass + interval containment — более универсально.

### 2. Условие активации fallback

Fallback активируется per-SPID, когда для данного SPID не найдено ни одного события с `EventName` заканчивающимся на `Starting`. Это детектится:

- **`IncrementalParentTracker`**: флаг `completedOnly` per-SPID. Поскольку стриминг идёт последовательно, флаг устанавливается при первом `Starting` и сбрасывается. Но при стриминге мы не знаем заранее, есть ли Starting — поэтому используется «ленивый» подход: пока нет Starting, накапливаем Completed-события в буфер; при первом Starting — сбрасываем буфер и переключаемся на стековый режим. Если до конца SPID Starting не появился — обрабатываем буфер интервальным алгоритмом.

- **`ComputeParentIDs`**: двухпроходный — первый проход детектит `hasStarting` per-SPID, второй — строит дерево (стек или интервалы).

**Альтернатива**: Однопроходный с переключением — но при стриминге нельзя «вернуться назад» и переприсвоить ParentID уже записанным в БД событиям. Поэтому для `IncrementalParentTracker` нужен буфер.

### 3. Интервальный стек для fallback

Алгоритм (после сортировки по start time):
```
sort events by start_time
stack = []
for each event:
    // pop events whose interval ended before this event's start
    while stack not empty AND stack.top.end < event.start:
        stack.pop()
    // find parent: topmost stack entry with higher EventClass rank
    parent = nil
    for i := len(stack)-1 downto 0:
        if eventClassRank(stack[i]) > eventClassRank(event):
            parent = stack[i]
            break
    if parent != nil:
        event.ParentID = parent.EventIndex
        event.Depth = parent.Depth + 1
    else:
        event.ParentID = -1
        event.Depth = 0
    stack.push(event)
```

**Rationale**: Сортировка по start time обеспечивает хронологический порядок. Pop по end time очищает закрытые интервалы. Поиск parent по EventClass rank обеспечивает корректную иерархию (SP:StmtCompleted внутри SP:Completed, а не наоборот).

### 4. Буферизация в IncrementalParentTracker

При стриминг-парсинге `IncrementalParentTracker` не может отложить присвоение ParentID — события сразу записываются в БД. Поэтому:

- Для каждого SPID поддерживается `completedOnlyMode bool` и `pendingCompleted []TRCEvent` буфер
- Пока не встречен `Starting` для данного SPID: Completed-события накапливаются в буфере (без ParentID/Depth)
- При первом `Starting`: буфер сбрасывается (события получают ParentID через стековый режим), SPID переключается в normal mode
- При `flushBatch`: если буфер накопил достаточно событий и SPID всё ещё в completedOnlyMode — применить интервальный алгоритм к буферу, присвоить ParentID/Depth, очистить буфер

**Ограничение буфера**: не более `trcBatchSize` событий (5000 по умолчанию). Если буфер переполнен — flush с interval-nesting, продолжить в completedOnlyMode.

### 5. Серверный режим (LoadEventsForTree)

CTE не требует изменений — `parent_id` уже корректно вычислен при парсинге через `IncrementalParentTracker` (с fallback). Recursive CTE строит дерево по `parent_id` как обычно.

Единственное изменение: anchor CTE `e.parent_id IS NULL` уже корректно выбирает корневые Completed-события (их `parent_id = -1 → NULL` в БД).

## Risks / Trade-offs

- **[Неточность интервального вложения]** → Временные интервалы могут дать ложную вложенность, если два вызова одного EventClass частично перекрываются. Mitigation: строгое условие `start_B >= start_A AND end_B <= end_A` (полное вложение, не частичное); одинаковый EventClass → siblings, не parent-child.

- **[Буфер в IncrementalParentTracker может задержать запись в БД]** → Если SPID в completedOnlyMode и буфер растёт, события не записываются до flush. Mitigation: буфер flush'ится по достижении `trcBatchSize` или при завершении SPID (смена SPID / конец парсинга).

- **[EventClass rank неточен для нестандартных событий]** → Если в трейсе есть события вне иерархии (например, Audit Login), они получат rank = -1 и всегда будут leaves. Mitigation: такие события и сейчас не становятся корнями (existing behavior для default-ветки).

- **[Оверхед буферизации]** → Для трейсов со Starting-событиями буфер не используется (нулевая задержка). Для Completed-only трейсов — задержка на один flush-цикл, что пренебрежимо.
