package rti

import (
	"fmt"
	"sort"
	"strings"
)

// RTITreeNode — узел дерева вызовов
type RTITreeNode struct {
	Call     *RTICall
	Children []*RTITreeNode
}

// BuildTree строит дерево вызовов от корневой процедуры.
// Если rootProcedure пустой, ищет NestLevel=1 вызов с наибольшим числом потомков.
// maxDepth: 0 = без ограничений, 1 = только корень, и т.д.
func BuildTree(calls []*RTICall, rootProcedure string, maxDepth int) *RTITreeNode {
	callByID := make(map[int64]*RTICall)
	for _, c := range calls {
		callByID[c.ID] = c
	}

	// Находим корневой call
	var root *RTICall
	if rootProcedure != "" {
		// Ищем вызов с наибольшим числом потомков среди совпадающих по имени
		bestCount := -1
		for _, c := range calls {
			if c.Procedure == rootProcedure {
				cnt := countDescendants(c, callByID, make(map[int64]bool))
				if cnt > bestCount {
					bestCount = cnt
					root = c
				}
			}
		}
	} else {
		// Автовыбор: NestLevel=1 с наибольшим числом потомков
		bestCount := -1
		for _, c := range calls {
			if c.NestLevel != 1 {
				continue
			}
			cnt := countDescendants(c, callByID, make(map[int64]bool))
			if cnt > bestCount {
				bestCount = cnt
				root = c
			}
		}
	}
	if root == nil {
		return nil
	}

	// Рекурсивно строим дерево
	return buildTreeNode(root, callByID, 0, maxDepth)
}

func countDescendants(call *RTICall, callByID map[int64]*RTICall, visited map[int64]bool) int {
	count := len(call.Children)
	for _, childID := range call.Children {
		if visited[childID] {
			continue
		}
		visited[childID] = true
		if child, ok := callByID[childID]; ok {
			count += countDescendants(child, callByID, visited)
		}
	}
	return count
}

func buildTreeNode(call *RTICall, callByID map[int64]*RTICall, depth, maxDepth int) *RTITreeNode {
	node := &RTITreeNode{Call: call}
	if maxDepth > 0 && depth >= maxDepth {
		return node
	}
	for _, childID := range call.Children {
		if child, ok := callByID[childID]; ok {
			node.Children = append(node.Children, buildTreeNode(child, callByID, depth+1, maxDepth))
		}
	}
	return node
}

// FormatTree форматирует дерево в компактный текстовый вид.
func FormatTree(node *RTITreeNode) string {
	var sb strings.Builder
	formatTreeNode(&sb, node, 0, "")
	return sb.String()
}

// FormatTreeEnriched форматирует дерево с enrichment-данными (source file).
func FormatTreeEnriched(node *RTITreeNode, enrichMap map[string]*ProcedureEnrichment) string {
	var sb strings.Builder
	formatTreeNodeEnriched(&sb, node, 0, enrichMap)
	return sb.String()
}

func formatTreeNodeEnriched(sb *strings.Builder, node *RTITreeNode, depth int, enrichMap map[string]*ProcedureEnrichment) {
	call := node.Call
	indent := strings.Repeat("  ", depth)

	retVal := ""
	if call.RetVal != nil {
		retVal = fmt.Sprintf(", RetVal=%d", *call.RetVal)
	}
	elapsed := ""
	if call.ElapsedMs > 0 {
		elapsed = fmt.Sprintf(" [%dms%s]", call.ElapsedMs, retVal)
	} else if retVal != "" {
		elapsed = fmt.Sprintf(" [%s]", strings.TrimPrefix(retVal, ", "))
	}

	module := call.ModuleName
	if module == "" {
		module = "Unknown"
	}

	fmt.Fprintf(sb, "%s%s%s ← %s (%d)", indent, call.Procedure, elapsed, module, call.ModuleID)

	if enrichMap != nil {
		if enrich, ok := enrichMap[call.Procedure]; ok && enrich != nil && enrich.Found {
			fmt.Fprintf(sb, "  → %s:%d", enrich.SourceFile, enrich.LineStart)
		}
	}
	fmt.Fprintln(sb)

	for _, child := range node.Children {
		formatTreeNodeEnriched(sb, child, depth+1, enrichMap)
	}
}

// RTIClientTreeNode — узел дерева клиентских событий, сгруппированных по PID.
// Примечание: полноценной вложенности вызовов (parent/child) для клиентских
// событий в v1 нет — реальная структура клиентского лога плоская (блоки не
// вкладываются друг в друга, как серверные Enter/Exit), поэтому дерево строится
// как группировка "PID → события этого PID, отсортированные по времени".
type RTIClientTreeNode struct {
	PID    int
	Events []*RTIClientEvent
}

// BuildClientTree группирует клиентские события по PID и сортирует каждую
// группу по времени. Если pid > 0, возвращается единственная группа для этого PID.
func BuildClientTree(events []*RTIClientEvent, pid int) []*RTIClientTreeNode {
	groups := make(map[int][]*RTIClientEvent)
	var order []int
	for _, ev := range events {
		if pid > 0 && ev.PID != pid {
			continue
		}
		if _, ok := groups[ev.PID]; !ok {
			order = append(order, ev.PID)
		}
		groups[ev.PID] = append(groups[ev.PID], ev)
	}
	sort.Ints(order)

	nodes := make([]*RTIClientTreeNode, 0, len(order))
	for _, p := range order {
		evs := groups[p]
		sort.Slice(evs, func(i, j int) bool { return evs[i].Timestamp.Before(evs[j].Timestamp) })
		nodes = append(nodes, &RTIClientTreeNode{PID: p, Events: evs})
	}
	return nodes
}

// FormatClientTree форматирует дерево клиентских событий в текстовый вид.
func FormatClientTree(nodes []*RTIClientTreeNode) string {
	var sb strings.Builder
	for _, node := range nodes {
		fmt.Fprintf(&sb, "PID %d (%d event(s)):\n", node.PID, len(node.Events))
		for _, ev := range node.Events {
			formatClientEventLine(&sb, ev, 1, nil)
		}
	}
	return sb.String()
}

// FormatClientTreeEnriched форматирует дерево клиентских событий с данными
// обогащения из CodeBase (PAS-метод, DFM-форма, query_fragment).
func FormatClientTreeEnriched(nodes []*RTIClientTreeNode, enrichMap map[string]*ClientEnrichment) string {
	var sb strings.Builder
	for _, node := range nodes {
		fmt.Fprintf(&sb, "PID %d (%d event(s)):\n", node.PID, len(node.Events))
		for _, ev := range node.Events {
			formatClientEventLine(&sb, ev, 1, enrichMap)
		}
	}
	return sb.String()
}

func formatClientEventLine(sb *strings.Builder, ev *RTIClientEvent, depth int, enrichMap map[string]*ClientEnrichment) {
	indent := strings.Repeat("  ", depth)
	desc := clientEventDescription(ev)
	fmt.Fprintf(sb, "%s[%s] %s.%s (%s)%s\n",
		indent, ev.Timestamp.Format("15:04:05.000"), ev.ClassName, ev.MethodName, ev.Kind, desc)
	if ev.ServerCallID != nil {
		fmt.Fprintf(sb, "%s  → server call #%d\n", indent, *ev.ServerCallID)
	}
	if enrichMap != nil {
		key := ev.ClassName + "." + ev.MethodName
		if enrich, ok := enrichMap[key]; ok && enrich != nil {
			if enrich.Found {
				fmt.Fprintf(sb, "%s  → %s:%d", indent, enrich.SourceFile, enrich.LineNumber)
				if enrich.Unit != "" {
					fmt.Fprintf(sb, " [%s]", enrich.Unit)
				}
				fmt.Fprintln(sb)
			}
			if enrich.DFMFormName != "" {
				fmt.Fprintf(sb, "%s  → DFM: %s", indent, enrich.DFMFormName)
				if enrich.DFMCaption != "" {
					fmt.Fprintf(sb, " (%s)", enrich.DFMCaption)
				}
				fmt.Fprintln(sb)
			}
			if enrich.QueryFragmentFile != "" {
				fmt.Fprintf(sb, "%s  → SQL origin: %s:%d", indent, enrich.QueryFragmentFile, enrich.QueryFragmentLine)
				if enrich.OriginMethod != "" {
					fmt.Fprintf(sb, " [%s]", enrich.OriginMethod)
				}
				fmt.Fprintln(sb)
			}
		}
	}
}

func clientEventDescription(ev *RTIClientEvent) string {
	switch ev.Kind {
	case "sql_block":
		if ev.SQL != nil {
			if ev.SQL.ExecProcedure != "" {
				return fmt.Sprintf(" exec %s", ev.SQL.ExecProcedure)
			}
			if ev.SQL.DurationSec > 0 {
				return fmt.Sprintf(" duration=%.3fs", ev.SQL.DurationSec)
			}
		}
	case "error":
		if ev.ErrorText != "" {
			return fmt.Sprintf(" %s", ev.ErrorText)
		}
	case "connection":
		if ev.Connection != nil {
			return fmt.Sprintf(" spid=%d server=%s db=%s", ev.Connection.SPID, ev.Connection.Server, ev.Connection.Database)
		}
	case "bpl_list":
		return fmt.Sprintf(" %d module(s)", len(ev.BPL))
	}
	return ""
}

// FormatUnifiedTimeline объединяет серверные вызовы и клиентские события в
// единую хронологическую ленту (сортировка по времени), с пометкой источника.
func FormatUnifiedTimeline(calls []*RTICall, events []*RTIClientEvent) string {
	return FormatUnifiedTimelineEnriched(calls, events, nil)
}

// FormatUnifiedTimelineEnriched — то же, но с обогащением клиентских событий.
func FormatUnifiedTimelineEnriched(calls []*RTICall, events []*RTIClientEvent, enrichMap map[string]*ClientEnrichment) string {
	type entry struct {
		timestampUnixNano int64
		line              string
	}
	entries := make([]entry, 0, len(calls)+len(events))
	for _, c := range calls {
		if c.EnterTime.IsZero() {
			continue
		}
		line := fmt.Sprintf("[server] %s %s ← %s (%d)",
			c.EnterTime.Format("15:04:05.000"), c.Procedure, c.ModuleName, c.ModuleID)
		if c.RetVal != nil {
			line += fmt.Sprintf(" RetVal=%d", *c.RetVal)
		}
		entries = append(entries, entry{c.EnterTime.UnixNano(), line})
	}
	for _, ev := range events {
		if ev.Timestamp.IsZero() {
			continue
		}
		line := fmt.Sprintf("[client] %s %s.%s (%s)%s",
			ev.Timestamp.Format("15:04:05.000"), ev.ClassName, ev.MethodName, ev.Kind, clientEventDescription(ev))
		if ev.ServerCallID != nil {
			line += fmt.Sprintf(" → server call #%d", *ev.ServerCallID)
		}
		if enrichMap != nil {
			key := ev.ClassName + "." + ev.MethodName
			if enrich, ok := enrichMap[key]; ok && enrich != nil && enrich.Found {
				line += fmt.Sprintf(" → %s:%d", enrich.SourceFile, enrich.LineNumber)
			}
		}
		entries = append(entries, entry{ev.Timestamp.UnixNano(), line})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].timestampUnixNano < entries[j].timestampUnixNano })

	var sb strings.Builder
	for _, e := range entries {
		sb.WriteString(e.line)
		sb.WriteByte('\n')
	}
	return sb.String()
}

func formatTreeNode(sb *strings.Builder, node *RTITreeNode, depth int, prefix string) {
	call := node.Call
	// Отступ
	indent := strings.Repeat("  ", depth)

	// Основная строка: procedure [elapsed, RetVal] ← module (module_id)
	retVal := ""
	if call.RetVal != nil {
		retVal = fmt.Sprintf(", RetVal=%d", *call.RetVal)
	}
	elapsed := ""
	if call.ElapsedMs > 0 {
		elapsed = fmt.Sprintf(" [%dms%s]", call.ElapsedMs, retVal)
	} else if retVal != "" {
		elapsed = fmt.Sprintf(" [%s]", strings.TrimPrefix(retVal, ", "))
	}

	module := call.ModuleName
	if module == "" {
		module = "Unknown"
	}

	fmt.Fprintf(sb, "%s%s%s ← %s (%d)\n", indent, call.Procedure, elapsed, module, call.ModuleID)

	// Дочерние
	for i, child := range node.Children {
		childPrefix := prefix
		if depth > 0 {
			childPrefix += prefix
		}
		_ = i
		_ = childPrefix
		formatTreeNode(sb, child, depth+1, prefix)
	}
}
