package trc

import (
	"fmt"
	"sort"
	"strings"
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

type openFrame struct {
	family string
	node   *TRCTreeNode
}

func buildSPIDTree(events []*TRCEvent) []*TRCTreeNode {
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
			attach(&TRCTreeNode{Start: ev})
		}
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
