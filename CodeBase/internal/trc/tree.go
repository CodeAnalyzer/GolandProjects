package trc

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// TRCTreeNode — узел дерева вызовов, восстановленного по SPID из
// Starting/Completed пар событий. Start — открывающее событие (либо любое
// одиночное событие без пары), End — соответствующее закрывающее событие
// (nil, если пара не встретилась до конца лога).
type TRCTreeNode struct {
	Start    *TRCEvent
	End      *TRCEvent
	Children []*TRCTreeNode
}

// BuildTrees восстанавливает деревья вызовов, сгруппированные по SPID.
// Внутри каждой группы строится стек по правилу: событие с именем класса,
// заканчивающимся на "Starting" (RPC:Starting, SQL:BatchStarting,
// SP:Starting, SQL:StmtStarting, SP:StmtStarting), открывает узел и
// вкладывает последующие события как детей до парного "Completed" события
// с тем же префиксом (совпадение определяется отбрасыванием суффикса
// Starting/Completed — общий префикс = "family"). Несовпавшие Completed
// (нет открытого Starting того же family) трактуются как отдельный
// одиночный узел на текущем уровне вложенности.
func BuildTrees(events []TRCEvent) map[int][]*TRCTreeNode {
	return BuildTreesWithDepth(events, 0)
}

// BuildTreesWithDepth строит деревья вызовов с ограничением глубины.
// maxDepth: 0 = без ограничений, 1 = только корневые узлы, и т.д.
func BuildTreesWithDepth(events []TRCEvent, maxDepth int) map[int][]*TRCTreeNode {
	bySPID := make(map[int][]*TRCEvent)
	var spidOrder []int
	for i := range events {
		spid, ok := events[i].Columns[12].(int32)
		if !ok {
			continue
		}
		s := int(spid)
		if _, seen := bySPID[s]; !seen {
			spidOrder = append(spidOrder, s)
		}
		bySPID[s] = append(bySPID[s], &events[i])
	}

	result := make(map[int][]*TRCTreeNode, len(bySPID))
	for _, spid := range spidOrder {
		result[spid] = buildSPIDTree(bySPID[spid])
	}
	if maxDepth > 0 {
		for spid, roots := range result {
			for _, root := range roots {
				pruneTreeDepth(root, 1, maxDepth)
			}
			result[spid] = roots
		}
	}
	return result
}

// pruneTreeDepth обрезает детей узла, если достигнута максимальная глубина.
func pruneTreeDepth(node *TRCTreeNode, depth, maxDepth int) {
	if depth >= maxDepth {
		node.Children = nil
		return
	}
	for _, child := range node.Children {
		pruneTreeDepth(child, depth+1, maxDepth)
	}
}

// LimitTrees ограничивает количество узлов в каждом SPID-дереве.
// Если limit > 0, оставляет только первые limit корневых узлов и
// обрезает детей каждого узла до limit.
func LimitTrees(trees map[int][]*TRCTreeNode, limit int) {
	if limit <= 0 {
		return
	}
	for spid, roots := range trees {
		if len(roots) > limit {
			roots = roots[:limit]
			trees[spid] = roots
		}
		for _, root := range roots {
			limitTreeNodeChildren(root, limit)
		}
	}
}

func limitTreeNodeChildren(node *TRCTreeNode, limit int) {
	if len(node.Children) > limit {
		node.Children = node.Children[:limit]
	}
	for _, child := range node.Children {
		limitTreeNodeChildren(child, limit)
	}
}

// eventClassRank возвращает уровень иерархии EventClass для fallback-
// алгоритма интервального вложения. Более высокое значение = более
// внешний (родительский) уровень.
// RPC:Completed(11)=4 > SP:Completed(43)=3 > SP:StmtCompleted(45)=2 >
// SQL:BatchCompleted(12)=1 > SQL:StmtCompleted(41)=0. Прочие = -1.
func eventClassRank(eventClass int) int {
	switch eventClass {
	case 11: // RPC:Completed
		return 4
	case 43: // SP:Completed
		return 3
	case 45: // SP:StmtCompleted
		return 2
	case 12: // SQL:BatchCompleted
		return 1
	case 41: // SQL:StmtCompleted
		return 0
	default:
		return -1
	}
}

// hasStartingEvents возвращает true, если среди событий есть хотя бы одно
// с именем, заканчивающимся на "Starting". Используется для определения,
// нужно ли применять fallback-алгоритм интервального вложения.
func hasStartingEvents(events []*TRCEvent) bool {
	for _, ev := range events {
		if strings.HasSuffix(ev.EventName, "Starting") {
			return true
		}
	}
	return false
}

// eventStartTime извлекает время начала события из колонки 14 (StartTime).
// Возвращает ok=false, если колонка отсутствует или невалидна.
func eventStartTime(ev *TRCEvent) (time.Time, bool) {
	st, ok := ev.Columns[14].(SystemTime)
	if !ok {
		return time.Time{}, false
	}
	return st.ToTime()
}

// eventEndTime извлекает время завершения события из колонки 15 (EndTime).
// Если EndTime отсутствует, вычисляет как StartTime + DurationMs.
// Возвращает ok=false, если StartTime также отсутствует.
func eventEndTime(ev *TRCEvent) (time.Time, bool) {
	if et, ok := ev.Columns[15].(SystemTime); ok {
		if t, ok2 := et.ToTime(); ok2 {
			return t, true
		}
	}
	st, ok := eventStartTime(ev)
	if !ok {
		return time.Time{}, false
	}
	return st.Add(time.Duration(ev.DurationMs) * time.Millisecond), true
}

type openFrame struct {
	family string
	node   *TRCTreeNode
}

// ComputeParentIDs вычисляет ParentID и Depth для каждого события в срезе
// events, используя тот же алгоритм Starting/Completed пар, что и buildSPIDTree,
// но заполняя поля ParentID (индекс родительского события в срезе) и Depth.
// ParentID = -1 для корневых событий. EventIndex заполняется порядковым номером.
// Вызывается после парсинга, перед сохранением в БД.
func ComputeParentIDs(events []TRCEvent) {
	for i := range events {
		events[i].EventIndex = i
		events[i].ParentID = -1
		events[i].Depth = 0
	}

	// Группируем по SPID, сохраняя порядок событий.
	type spidFrame struct {
		family string
		idx    int // индекс события Starting в срезе events
	}

	bySPID := make(map[int][]int) // spid -> indices
	for i := range events {
		spid, ok := events[i].Columns[12].(int32)
		if !ok {
			continue
		}
		s := int(spid)
		bySPID[s] = append(bySPID[s], i)
	}

	for _, indices := range bySPID {
		// Проверяем, есть ли Starting-события для этого SPID.
		hasStarting := false
		for _, idx := range indices {
			if strings.HasSuffix(events[idx].EventName, "Starting") {
				hasStarting = true
				break
			}
		}

		if !hasStarting {
			// Fallback: интервальный алгоритм для Completed-only SPID.
			computeParentIDsInterval(events, indices)
			continue
		}

		// Основной алгоритм: стек Starting/Completed пар.
		var stack []spidFrame
		for _, idx := range indices {
			ev := &events[idx]
			name := ev.EventName
			switch {
			case strings.HasSuffix(name, "Starting"):
				if len(stack) > 0 {
					top := stack[len(stack)-1]
					ev.ParentID = top.idx
					ev.Depth = events[top.idx].Depth + 1
				}
				stack = append(stack, spidFrame{
					family: strings.TrimSuffix(name, "Starting"),
					idx:    idx,
				})
			case strings.HasSuffix(name, "Completed"):
				family := strings.TrimSuffix(name, "Completed")
				if len(stack) > 0 && stack[len(stack)-1].family == family {
					stack = stack[:len(stack)-1]
				}
				// Completed событие — ребёнок текущего верхнего фрейма (если есть).
				if len(stack) > 0 {
					top := stack[len(stack)-1]
					ev.ParentID = top.idx
					ev.Depth = events[top.idx].Depth + 1
				}
			default:
				if len(stack) > 0 {
					top := stack[len(stack)-1]
					ev.ParentID = top.idx
					ev.Depth = events[top.idx].Depth + 1
				}
			}
		}
	}
}

// computeParentIDsInterval вычисляет ParentID и Depth для событий одного SPID
// с использованием интервального вложения (fallback для Completed-only трейсов).
// indices — индексы событий данного SPID в срезе events (в исходном порядке).
func computeParentIDsInterval(events []TRCEvent, indices []int) {
	type intervalEntry struct {
		idx   int // индекс в events
		start time.Time
		end   time.Time
		rank  int
	}

	var entries []intervalEntry
	for _, idx := range indices {
		ev := &events[idx]
		start, ok := eventStartTime(ev)
		if !ok {
			continue
		}
		end, _ := eventEndTime(ev)
		entries = append(entries, intervalEntry{
			idx:   idx,
			start: start,
			end:   end,
			rank:  eventClassRank(ev.EventClass),
		})
	}

	// Сортируем по start time, сохраняя исходный порядок для равных временных меткам.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].start.Before(entries[j].start)
	})

	type stackFrame struct {
		idx   int // индекс в events
		depth int
		rank  int
		end   time.Time
	}
	var stack []stackFrame

	for _, e := range entries {
		// Pop событий, чей интервал уже закончился до start текущего.
		for len(stack) > 0 {
			top := stack[len(stack)-1]
			if !top.end.Before(e.start) {
				break
			}
			stack = stack[:len(stack)-1]
		}

		// Ищем родителя: ближайший открытый интервал с более высоким rank.
		var parentIdx int = -1
		var parentDepth int
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i].rank > e.rank {
				parentIdx = stack[i].idx
				parentDepth = stack[i].depth
				break
			}
		}

		ev := &events[e.idx]
		if parentIdx >= 0 {
			ev.ParentID = parentIdx
			ev.Depth = parentDepth + 1
		} else {
			ev.ParentID = -1
			ev.Depth = 0
		}

		stack = append(stack, stackFrame{
			idx:   e.idx,
			depth: ev.Depth,
			rank:  e.rank,
			end:   e.end,
		})
	}
}

func buildSPIDTree(events []*TRCEvent) []*TRCTreeNode {
	if !hasStartingEvents(events) {
		return buildSPIDTreeInterval(events)
	}
	return buildSPIDTreeStack(events)
}

// buildSPIDTreeStack строит дерево вызовов по Starting/Completed парам (существующий алгоритм).
func buildSPIDTreeStack(events []*TRCEvent) []*TRCTreeNode {
	var roots []*TRCTreeNode
	var stack []openFrame

	attach := func(node *TRCTreeNode) {
		if len(stack) > 0 {
			top := stack[len(stack)-1].node
			top.Children = append(top.Children, node)
		} else {
			roots = append(roots, node)
		}
	}

	for _, ev := range events {
		name := ev.EventName
		switch {
		case strings.HasSuffix(name, "Starting"):
			node := &TRCTreeNode{Start: ev}
			attach(node)
			stack = append(stack, openFrame{family: strings.TrimSuffix(name, "Starting"), node: node})
		case strings.HasSuffix(name, "Completed"):
			family := strings.TrimSuffix(name, "Completed")
			if len(stack) > 0 && stack[len(stack)-1].family == family {
				stack[len(stack)-1].node.End = ev
				stack = stack[:len(stack)-1]
			} else {
				attach(&TRCTreeNode{Start: ev})
			}
		default:
			if len(stack) == 0 {
				continue
			}
			attach(&TRCTreeNode{Start: ev})
		}
	}
	return roots
}

// intervalFrame — элемент интервального стека для fallback-дерева.
type intervalFrame struct {
	node *TRCTreeNode
	end  time.Time
	rank int
}

// buildSPIDTreeInterval строит дерево вызовов по временному вложению интервалов
// для Completed-only трейсов. События сортируются по start time, затем для
// каждого события ищется родитель: ближайший открытый интервал с более высоким
// eventClassRank. События без родителя становятся корневыми узлами.
func buildSPIDTreeInterval(events []*TRCEvent) []*TRCTreeNode {
	var roots []*TRCTreeNode

	// Фильтруем события с валидным временем начала.
	type sortedEvent struct {
		ev    *TRCEvent
		start time.Time
		end   time.Time
	}
	var sorted []sortedEvent
	for _, ev := range events {
		start, ok := eventStartTime(ev)
		if !ok {
			// События без времени: Completed → корневой узел, диагностические → пропускаем.
			if strings.HasSuffix(ev.EventName, "Completed") {
				roots = append(roots, &TRCTreeNode{Start: ev})
			}
			continue
		}
		end, _ := eventEndTime(ev)
		sorted = append(sorted, sortedEvent{ev: ev, start: start, end: end})
	}

	// Сортируем по start time.
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].start.Before(sorted[j].start)
	})

	var stack []intervalFrame

	attach := func(node *TRCTreeNode, parentRank int) {
		// Ищем родителя: ближайший открытый интервал с более высоким rank.
		var parent *TRCTreeNode
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i].rank > parentRank {
				parent = stack[i].node
				break
			}
		}
		if parent != nil {
			parent.Children = append(parent.Children, node)
		} else {
			roots = append(roots, node)
		}
	}

	for _, se := range sorted {
		// Pop событий, чей интервал уже закончился до start текущего.
		for len(stack) > 0 {
			top := stack[len(stack)-1]
			if !top.end.Before(se.start) {
				break
			}
			stack = stack[:len(stack)-1]
		}

		rank := eventClassRank(se.ev.EventClass)
		node := &TRCTreeNode{Start: se.ev}
		attach(node, rank)
		stack = append(stack, intervalFrame{node: node, end: se.end, rank: rank})
	}

	return roots
}

// FormatTrees форматирует деревья (по SPID) в компактный текстовый вид.
func FormatTrees(trees map[int][]*TRCTreeNode) string {
	var sb strings.Builder
	spids := make([]int, 0, len(trees))
	for spid := range trees {
		spids = append(spids, spid)
	}
	sort.Ints(spids)
	for _, spid := range spids {
		fmt.Fprintf(&sb, "SPID %d:\n", spid)
		for _, root := range trees[spid] {
			formatTreeNode(&sb, root, 1)
		}
	}
	return sb.String()
}

func formatTreeNode(sb *strings.Builder, node *TRCTreeNode, depth int) {
	indent := strings.Repeat("  ", depth)
	ev := node.Start
	label := ev.EventName
	if label == "" {
		label = fmt.Sprintf("EventClass(%d)", ev.EventClass)
	}
	if ev.Procedure != "" {
		label += " exec " + ev.Procedure
	}
	durationPart := ""
	if node.End != nil && node.End.DurationMs > 0 {
		durationPart = fmt.Sprintf(" [%dms]", node.End.DurationMs)
	} else if ev.DurationMs > 0 {
		durationPart = fmt.Sprintf(" [%dms]", ev.DurationMs)
	}
	childCount := ""
	if len(node.Children) > 0 {
		childCount = fmt.Sprintf(" (%d children)", len(node.Children))
	}
	fmt.Fprintf(sb, "%s%s%s%s\n", indent, label, durationPart, childCount)
	for _, child := range node.Children {
		formatTreeNode(sb, child, depth+1)
	}
}

// FilterTreesByProcedure фильтрует деревья по имени процедуры: находит все
// узлы, где Start.Procedure совпадает с procedure, и возвращает новые деревья
// с этими узлами как корнями (со всеми их детьми). Если procedure пустой,
// возвращает trees без изменений.
func FilterTreesByProcedure(trees map[int][]*TRCTreeNode, procedure string) map[int][]*TRCTreeNode {
	if procedure == "" {
		return trees
	}
	result := make(map[int][]*TRCTreeNode)
	for spid, roots := range trees {
		var filtered []*TRCTreeNode
		for _, root := range roots {
			filtered = append(filtered, findNodesByProcedure(root, procedure)...)
		}
		if len(filtered) > 0 {
			result[spid] = filtered
		}
	}
	return result
}

// findNodesByProcedure рекурсивно обходит дерево и возвращает все узлы,
// где Start.Procedure совпадает с procedure (вместе с их поддеревьями).
func findNodesByProcedure(node *TRCTreeNode, procedure string) []*TRCTreeNode {
	var result []*TRCTreeNode
	if node.Start != nil && node.Start.Procedure == procedure {
		result = append(result, node)
		return result
	}
	for _, child := range node.Children {
		result = append(result, findNodesByProcedure(child, procedure)...)
	}
	return result
}
