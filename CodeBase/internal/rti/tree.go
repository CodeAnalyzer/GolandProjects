package rti

import (
	"fmt"
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
