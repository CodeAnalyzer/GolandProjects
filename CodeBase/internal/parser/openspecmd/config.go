package openspecmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// ValidateSpecDrivenRoot проверяет наличие config.yaml со schema: spec-driven.
func ValidateSpecDrivenRoot(rootDir string) (bool, error) {
	content, err := os.ReadFile(filepath.Join(filepath.FromSlash(rootDir), "config.yaml"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, _, ok := ParseConfigYAML(string(content))
	return ok, nil
}

// ParseConfigYAML разбирает config.yaml openspec-корня. Формат финпродуктов
// минимален: `schema: <name>` и блочный скаляр `context: |`. Возвращает
// schema_name и текст секции context (с выровненным отступом); ok=false,
// если schema отсутствует или не spec-driven — такой корень не индексируется.
func ParseConfigYAML(content string) (schemaName string, contextText string, ok bool) {
	// UTF-8 config.yaml may contain a BOM before the first YAML key.
	content = strings.TrimPrefix(content, "\uFEFF")
	lines := splitLines(content)

	schema := ""
	context := parseBlockScalar(lines, "context")

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "schema:") {
			schema = strings.TrimSpace(strings.TrimPrefix(trimmed, "schema:"))
			schema = strings.Trim(strings.TrimSpace(schema), `"'`)
		}
	}

	if !strings.EqualFold(schema, "spec-driven") {
		return "", "", false
	}
	return schema, context, true
}

// parseBlockScalar извлекает блочный скаляр `key: |` — все последующие строки
// с большим отступом (или пустые) до первого ключа верхнего уровня.
func parseBlockScalar(lines []string, key string) string {
	prefix := key + ":"
	start := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, prefix) {
			start = i + 1
			break
		}
	}
	if start < 0 {
		return ""
	}

	var blockLines []string
	indent := -1
	for i := start; i < len(lines); i++ {
		line := lines[i]
		if strings.TrimSpace(line) == "" {
			blockLines = append(blockLines, "")
			continue
		}
		lineIndent := len(line) - len(strings.TrimLeft(line, " \t"))
		if indent < 0 {
			indent = lineIndent
		}
		if lineIndent < indent {
			break
		}
		blockLines = append(blockLines, line[indent:])
	}
	// обрезаем хвостовые пустые строки
	for len(blockLines) > 0 && strings.TrimSpace(blockLines[len(blockLines)-1]) == "" {
		blockLines = blockLines[:len(blockLines)-1]
	}
	return strings.Join(blockLines, "\n")
}

func splitLines(content string) []string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	return strings.Split(content, "\n")
}
