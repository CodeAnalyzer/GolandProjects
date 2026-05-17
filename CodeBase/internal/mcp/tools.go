package mcp

import "sort"

func tools() []toolDefinition {
	result := make([]toolDefinition, 0, len(toolRegistry))
	for _, tool := range toolRegistry {
		result = append(result, tool.Definition)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}
