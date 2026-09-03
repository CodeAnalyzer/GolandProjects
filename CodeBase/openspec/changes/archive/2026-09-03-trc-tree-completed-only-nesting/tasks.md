## 1. EventClass rank map и утилиты

- [x] 1.1 Создать функцию `eventClassRank(eventClass int) int` в `internal/trc/tree.go` — возвращает уровень иерархии: RPC:Completed(11)=4, SP:Completed(43)=3, SP:StmtCompleted(45)=2, SQL:BatchCompleted(12)=1, SQL:StmtCompleted(41)=0, прочие=-1. Verify: unit-тест `TestEventClassRank` проверяет все 5 классов + unknown.
- [x] 1.2 Добавить функцию `hasStartingEvents(events []TRCEvent) bool` — возвращает true если среди событий есть хотя бы одно с `EventName` заканчивающимся на `Starting`. Verify: unit-тест с событиями только Completed (→false) и со смесью (→true).

## 2. Interval nesting для ComputeParentIDs (файловый режим)

- [x] 2.1 Реализовать `computeParentIDsInterval(events []TRCEvent)` в `internal/trc/tree.go` — интервальный стек: сортировка по start time, pop по end time, parent ищется по `eventClassRank`. Присваивает `ParentID` и `Depth`. Verify: unit-тест `TestComputeParentIDs_IntervalNesting` — 3 события (RPC:Completed → SP:Completed → SP:StmtCompleted) с вложенными интервалами, проверка ParentID и Depth.
- [x] 2.2 Интегрировать fallback в `ComputeParentIDs`: для каждого SPID проверить `hasStartingEvents`; если false — вызвать `computeParentIDsInterval` вместо стекового алгоритма. Verify: unit-тест `TestComputeParentIDs_CompletedOnlySPID` — SPID с только Completed-событиями получает корректные ParentID/Depth.
- [x] 2.3 Тест: перекрывающиеся, но не вложенные интервалы → siblings (оба root). Verify: `TestComputeParentIDs_OverlappingNotNested` — два SP:Completed с частичным перекрытием, оба ParentID=-1.
- [x] 2.4 Тест: одинаковый EventClass с одинаковым интервалом → siblings. Verify: `TestComputeParentIDs_SameEventClassSameInterval` — два SP:Completed, оба root.

## 3. Interval nesting для buildSPIDTree (in-memory tree)

- [x] 3.1 Реализовать `buildSPIDTreeInterval(events []*TRCEvent) []*TRCTreeNode` в `internal/trc/tree.go` — строит дерево по интервальному вложению с использованием `eventClassRank`. Verify: unit-тест `TestBuildSPIDTree_IntervalNesting` — SP:Completed + SP:StmtCompleted с вложенным интервалом → SP:StmtCompleted ребёнок SP:Completed.
- [x] 3.2 Интегрировать fallback в `buildSPIDTree`: проверить `hasStartingEvents`; если false — вызвать `buildSPIDTreeInterval`. Verify: unit-тест `TestBuildTrees_CompletedOnlyNesting` — 3 события (RPC → SP → SP:Stmt) дают 3-уровневое дерево.
- [x] 3.3 Тест: fallback не активируется при наличии Starting. Verify: `TestBuildTrees_MixedStartingCompleted_NoFallback` — Starting+Completed события используют стековый алгоритм, не интервальный.

## 4. Interval nesting для IncrementalParentTracker (стриминг-парсинг)

- [x] 4.1 Добавить поля в `IncrementalParentTracker` (`parent_tracker.go`): `completedOnly map[int]bool` (per-SPID), `pendingCompleted map[int][]*TRCEvent` (буфер per-SPID), `seenStarting map[int]bool`. Verify: `go build` проходит без ошибок.
- [x] 4.2 Модифицировать `processWithSPID`: для SPID без Starting — накапливать Completed-события в `pendingCompleted` буфере. При первом Starting — сбросить буфер (присвоить ParentID через интервальный алгоритм), переключить SPID в normal mode. Флаг `seenStarting` предотвращает повторный вход в completed-only. Verify: unit-тест `TestIncrementalParentTracker_CompletedOnlyFallback` — 3 Completed-события накапливаются, затем обрабатываются интервальным алгоритмом.
- [x] 4.3 Реализовать `flushPendingCompleted(spid int)` — применяет интервальный алгоритм к буферу pendingCompleted[spid], присваивает ParentID/Depth, очищает буфер. Вызывается при: переполнении буфера (>= 5000), первом Starting, завершении парсинга. Verify: unit-тест `TestIncrementalParentTracker_CompletedOnly_MatchesComputeParentIDs` — сравнение с ComputeParentIDs.
- [x] 4.4 Интегрировать flush в `Process` и добавить метод `Flush()` для вызова извне при завершении парсинга. Verify: `TestIncrementalParentTracker_CompletedOnlyFallback` — после Flush все pendingCompleted пусты, ParentID/Depth присвоены.
- [x] 4.5 Добавить вызов `tracker.Flush()` в `ParseFileToDB` (`parse_to_db.go`) перед финальным `flushBatch`. Verify: `go build` проходит.

## 5. Интеграционные тесты

- [x] 5.1 Тест `TestExecuteTree_CompletedOnlyFileMode` в `internal/trcsvc/runtime_test.go` — .trc файл с только Completed-событиями, проверка что дерево имеет вложенную структуру (не плоский список). Verify: `go test ./internal/trcsvc/... -run TestExecuteTree_CompletedOnly -v`.
- [x] 5.2 Тест `TestBuildTrees_CompletedOnly_RealWorldPattern` — воспроизвести паттерн из `trc-tree-flat.txt` (SP:Completed + SP:StmtCompleted с совпадающими интервалами) и проверить что SP:StmtCompleted — ребёнок SP:Completed. Verify: `go test ./internal/trc/... -run TestBuildTrees_CompletedOnly_RealWorld -v`.
- [x] 5.3 Регрессионный тест: существующие тесты с Starting/Completed не сломаны. Verify: `go test ./internal/trc/... -count=1` — все PASS.

## 6. Финальная проверка

- [x] 6.1 Полная сборка и тесты: `go build ./...` и `go vet ./...` — чисто. `go test ./internal/trc/... ./internal/trcsvc/... -count=1` — все PASS. Verify: команды выполняются без ошибок.
- [x] 6.2 Проверка на реальном файле: `codebase trc tree Tests/SPCompletedOnly.trc` — дерево показывает вложенность вместо плоского списка. SP:Completed с детьми SP:StmtCompleted, SP:Recompile, SQL:StmtRecompile. Verify: визуальная проверка вывода — вложенность подтверждена.
