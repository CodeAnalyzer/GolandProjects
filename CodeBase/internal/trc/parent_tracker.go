package trc

import (
	"strings"
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
type IncrementalParentTracker struct {
	stacks map[int][]trackerFrame // spid -> stack
	idx    int                    // счётчик событий (0-based)
}

// NewIncrementalParentTracker создаёт новый трекер.
func NewIncrementalParentTracker() *IncrementalParentTracker {
	return &IncrementalParentTracker{
		stacks: make(map[int][]trackerFrame),
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
