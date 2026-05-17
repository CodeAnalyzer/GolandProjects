package mcp

import "sort"

var defaultToolOutputSchema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"content": map[string]interface{}{
			"type": "array",
			"items": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"type": map[string]interface{}{"type": "string"},
					"text": map[string]interface{}{"type": "string"},
				},
			},
		},
		"isError": map[string]interface{}{"type": "boolean"},
	},
}

func tools() []toolDefinition {
	result := make([]toolDefinition, 0, len(toolRegistry))
	for _, tool := range toolRegistry {
		def := tool.Definition
		if def.OutputSchema == nil {
			def.OutputSchema = defaultToolOutputSchema
		}
		result = append(result, def)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}
