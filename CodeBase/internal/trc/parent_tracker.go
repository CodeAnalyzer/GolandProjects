package trc

import (
	"sort"
	"strings"
	"time"
)

// trackerFrame — фрейм стека вызовов для IncrementalParentTracker:
// family (префикс имени события без суффикса Starting/Completed), индекс
// события Starting в общем потоке и его глубина.
type trackerFrame struct {
	family string
	idx    int
	depth  int
}

// IncrementalParentTracker вычисляет ParentID и Depth инкрементально во
// время стриминга событий, вместо post-factum ComputeParentIDs по всему
// массиву. Алгоритм идентичен ComputeParentIDs (tree.go): группировка по
// SPID, стек Starting/Completed пар, ParentID = top of stack (или -1).
//
// Для SPID без Starting-событий (Completed-only) используется fallback:
// события накапливаются в буфере pendingCompleted и обрабатываются
// интервальным алгоритмом при flush (переполнение буфера, смена SPID,
// завершение парсинга).
type IncrementalParentTracker struct {
	stacks           map[int][]trackerFrame       // spid -> stack (normal mode)
	idx              int                          // счётчик событий (0-based)
	completedOnly    map[int]bool                 // spid -> true если в completed-only режиме
	pendingCompleted map[int][]*TRCEvent          // spid -> буфер Completed-событий
	seenStarting     map[int]bool                 // spid -> хотя бы один Starting был обработан
}

// NewIncrementalParentTracker создаёт новый трекер.
func NewIncrementalParentTracker() *IncrementalParentTracker {
	return &IncrementalParentTracker{
		stacks:           make(map[int][]trackerFrame),
		completedOnly:    make(map[int]bool),
		pendingCompleted: make(map[int][]*TRCEvent),
		seenStarting:     make(map[int]bool),
	}
}

// Process обрабатывает одно событие: заполняет ParentID, Depth и EventIndex.
// Должна вызываться последовательно для каждого события в порядке потока.
func (t *IncrementalParentTracker) Process(ev *TRCEvent) {
	ev.EventIndex = t.idx
	ev.ParentID = -1
	ev.Depth = 0

	spid, ok := ev.Columns[12].(int32)
	if ok {
		t.processWithSPID(ev, int(spid))
	}
	t.idx++
}

// processWithSPID обрабатывает событие с известным SPID, используя стек
// этого SPID. Логика идентична ComputeParentIDs для одного события.
func (t *IncrementalParentTracker) processWithSPID(ev *TRCEvent, spid int) {
	name := ev.EventName

	// Если для этого SPID уже был Starting — всегда normal mode.
	if t.seenStarting[spid] {
		t.processNormal(ev, spid)
		return
	}

	// Starting ещё не было для этого SPID.
	if strings.HasSuffix(name, "Starting") {
		// Первый Starting — сбрасываем буфер, переключаемся в normal mode.
		if t.completedOnly[spid] {
			t.flushPendingCompleted(spid)
			t.completedOnly[spid] = false
		}
		t.seenStarting[spid] = true
		t.processNormal(ev, spid)
		return
	}

	// Не Starting, Starting ещё не было — накапливаем в буфере.
	t.completedOnly[spid] = true
	t.pendingCompleted[spid] = append(t.pendingCompleted[spid], ev)

	// Если буфер переполнен — flush.
	const pendingFlushThreshold = 5000
	if len(t.pendingCompleted[spid]) >= pendingFlushThreshold {
		t.flushPendingCompleted(spid)
	}
}

// processNormal обрабатывает событие в normal (стековом) режиме.
func (t *IncrementalParentTracker) processNormal(ev *TRCEvent, spid int) {
	stack := t.stacks[spid]
	name := ev.EventName

	switch {
	case strings.HasSuffix(name, "Starting"):
		if len(stack) > 0 {
			top := stack[len(stack)-1]
			ev.ParentID = top.idx
			ev.Depth = top.depth + 1
		}
		stack = append(stack, trackerFrame{
			family: strings.TrimSuffix(name, "Starting"),
			idx:    t.idx,
			depth:  ev.Depth,
		})
		t.stacks[spid] = stack

	case strings.HasSuffix(name, "Completed"):
		family := strings.TrimSuffix(name, "Completed")
		if len(stack) > 0 && stack[len(stack)-1].family == family {
			stack = stack[:len(stack)-1]
			t.stacks[spid] = stack
		}
		if len(stack) > 0 {
			top := stack[len(stack)-1]
			ev.ParentID = top.idx
			ev.Depth = top.depth + 1
		}

	default:
		if len(stack) > 0 {
			top := stack[len(stack)-1]
			ev.ParentID = top.idx
			ev.Depth = top.depth + 1
		}
	}
}

// flushPendingCompleted обрабатывает накопленный буфер Completed-событий
// интервальным алгоритмом, присваивая ParentID и Depth, затем очищает буфер.
func (t *IncrementalParentTracker) flushPendingCompleted(spid int) {
	pending := t.pendingCompleted[spid]
	if len(pending) == 0 {
		return
	}

	type entry struct {
		ev    *TRCEvent
		start time.Time
		end   time.Time
		rank  int
	}

	var entries []entry
	for _, ev := range pending {
		start, ok := eventStartTime(ev)
		if !ok {
			continue
		}
		end, _ := eventEndTime(ev)
		entries = append(entries, entry{
			ev:    ev,
			start: start,
			end:   end,
			rank:  eventClassRank(ev.EventClass),
		})
	}

	// Сортируем по start time.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].start.Before(entries[j].start)
	})

	type stackFrame struct {
		idx   int
		depth int
		rank  int
		end   time.Time
	}
	var stack []stackFrame

	for _, e := range entries {
		// Pop закрытых интервалов.
		for len(stack) > 0 {
			top := stack[len(stack)-1]
			if !top.end.Before(e.start) {
				break
			}
			stack = stack[:len(stack)-1]
		}

		// Ищем родителя.
		var parentIdx int = -1
		var parentDepth int
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i].rank > e.rank {
				parentIdx = stack[i].idx
				parentDepth = stack[i].depth
				break
			}
		}

		if parentIdx >= 0 {
			e.ev.ParentID = parentIdx
			e.ev.Depth = parentDepth + 1
		} else {
			e.ev.ParentID = -1
			e.ev.Depth = 0
		}

		stack = append(stack, stackFrame{
			idx:   e.ev.EventIndex,
			depth: e.ev.Depth,
			rank:  e.rank,
			end:   e.end,
		})
	}

	// Очищаем буфер.
	t.pendingCompleted[spid] = nil
}

// Flush обрабатывает все накопленные буферы pendingCompleted для всех SPID.
// Должна вызываться после завершения стриминга всех событий.
func (t *IncrementalParentTracker) Flush() {
	for spid := range t.pendingCompleted {
		t.flushPendingCompleted(spid)
	}
}
