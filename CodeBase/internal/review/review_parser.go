package review

import (
	"regexp"
	"sort"
	"strings"
)

func parseUpdateSetStatement(queryText string) (updateSetStatement, bool) {
	text := strings.TrimSpace(queryText)
	if text == "" {
		return updateSetStatement{}, false
	}
	re := regexp.MustCompile(`(?is)^update\s+([a-z_#][a-z0-9_#]*)(?:\s+([a-z_][a-z0-9_]*))?\s+set\s+(.+)$`)
	m := re.FindStringSubmatch(text)
	if len(m) != 4 {
		return updateSetStatement{}, false
	}

	targetTable := strings.TrimSpace(m[1])
	targetAlias := strings.TrimSpace(m[2])
	remainder := strings.TrimSpace(m[3])
	if targetTable == "" || remainder == "" {
		return updateSetStatement{}, false
	}

	fromPos := findTopLevelKeywordPosition(remainder, "from")
	wherePos := findTopLevelKeywordPosition(remainder, "where")

	setPart := remainder
	fromClause := ""
	if fromPos >= 0 {
		setPart = strings.TrimSpace(remainder[:fromPos])
		fromStart := fromPos + len("from")
		fromEnd := len(remainder)
		if wherePos > fromPos {
			fromEnd = wherePos
		}
		fromClause = strings.TrimSpace(remainder[fromStart:fromEnd])
	} else if wherePos >= 0 {
		setPart = strings.TrimSpace(remainder[:wherePos])
	}

	parts := splitTopLevelCSV(setPart)
	assignments := make([]updateAssignment, 0, len(parts))
	for _, part := range parts {
		eqIdx := strings.Index(part, "=")
		if eqIdx <= 0 {
			continue
		}
		target := strings.TrimSpace(part[:eqIdx])
		expression := strings.TrimSpace(part[eqIdx+1:])
		if target == "" || expression == "" {
			continue
		}
		assignments = append(assignments, updateAssignment{Target: target, Expression: expression})
	}
	if len(assignments) == 0 {
		return updateSetStatement{}, false
	}

	return updateSetStatement{
		TargetTable: targetTable,
		TargetAlias: targetAlias,
		Assignments: assignments,
		FromClause:  fromClause,
	}, true
}

func hasPrefixFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}

func findTopLevelKeywordPosition(text string, keyword string) int {
	parenDepth := 0
	caseDepth := 0
	for i := 0; i < len(text); i++ {
		ch := text[i]
		if ch == '(' {
			parenDepth++
		} else if ch == ')' && parenDepth > 0 {
			parenDepth--
		}
		if isWordBoundary(text, i-1) && hasPrefixFold(text[i:], "case") && isWordBoundary(text, i+4) {
			caseDepth++
		}
		if isWordBoundary(text, i-1) && hasPrefixFold(text[i:], "end") && isWordBoundary(text, i+3) && caseDepth > 0 {
			caseDepth--
		}
		if parenDepth == 0 && caseDepth == 0 && hasPrefixFold(text[i:], keyword) && isWordBoundary(text, i-1) && isWordBoundary(text, i+len(keyword)) {
			return i
		}
	}
	return -1
}

func parseInsertSelectStatement(queryText string) (insertSelectStatement, bool) {
	text := strings.TrimSpace(queryText)
	if text == "" {
		return insertSelectStatement{}, false
	}
	re := regexp.MustCompile(`(?is)insert\s+(?:into\s+)?([a-z_#][a-z0-9_#]*)[^\(]*\((.*?)\)\s*select\s+(.*?)\s+from\s+(.*)$`)
	m := re.FindStringSubmatch(text)
	if len(m) != 5 {
		return insertSelectStatement{}, false
	}

	targetTable := strings.TrimSpace(m[1])
	columns := splitTopLevelCSV(m[2])
	selectExpressions := splitTopLevelCSV(m[3])
	if targetTable == "" || len(columns) == 0 || len(selectExpressions) == 0 {
		return insertSelectStatement{}, false
	}

	return insertSelectStatement{
		TargetTable:       targetTable,
		TargetColumns:     columns,
		SelectExpressions: selectExpressions,
		FromClause:        strings.TrimSpace(m[4]),
	}, true
}

func parseSelectAssignStatement(queryText string) (selectAssignStatement, bool) {
	text := strings.TrimSpace(queryText)
	if text == "" {
		return selectAssignStatement{}, false
	}
	lower := strings.ToLower(text)
	if !strings.HasPrefix(lower, "select") {
		return selectAssignStatement{}, false
	}

	fromPos := findTopLevelKeywordPosition(text, "from")
	if fromPos < 0 {
		return selectAssignStatement{}, false
	}

	selectPart := strings.TrimSpace(text[len("select"):fromPos])
	fromClause := strings.TrimSpace(text[fromPos:])
	if selectPart == "" || fromClause == "" {
		return selectAssignStatement{}, false
	}

	parts := splitTopLevelCSV(selectPart)
	assignments := make([]selectAssignment, 0, len(parts))
	assignRe := regexp.MustCompile(`(?is)^\s*(@[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
	for _, part := range parts {
		m := assignRe.FindStringSubmatch(strings.TrimSpace(part))
		if len(m) != 3 {
			continue
		}
		targetVariable := strings.TrimSpace(m[1])
		expression := strings.TrimSpace(m[2])
		if targetVariable == "" || expression == "" {
			continue
		}
		assignments = append(assignments, selectAssignment{TargetVariable: targetVariable, Expression: expression})
	}

	if len(assignments) == 0 {
		return selectAssignStatement{}, false
	}

	return selectAssignStatement{Assignments: assignments, FromClause: fromClause}, true
}

func parseFetchIntoStatement(queryText string) (fetchIntoStatement, bool) {
	text := strings.TrimSpace(queryText)
	if text == "" {
		return fetchIntoStatement{}, false
	}

	prefixRe := regexp.MustCompile(`(?is)(?:__fetch_next__|fetch)(?:\s+next)?(?:\s+from)?\s+([a-z_#][a-z0-9_#]*)\s+into\s+`)
	loc := prefixRe.FindStringSubmatchIndex(text)
	if len(loc) < 4 {
		return fetchIntoStatement{}, false
	}

	cursorName := strings.TrimSpace(text[loc[2]:loc[3]])
	if cursorName == "" {
		return fetchIntoStatement{}, false
	}

	variablesPart := text[loc[1]:]
	if boundary := findFetchIntoTailBoundary(variablesPart); boundary >= 0 {
		variablesPart = variablesPart[:boundary]
	}
	variablesPart = strings.TrimSpace(variablesPart)
	if variablesPart == "" {
		return fetchIntoStatement{}, false
	}

	parts := splitTopLevelCSV(variablesPart)
	variables := make([]string, 0, len(parts))
	varRe := regexp.MustCompile(`(?is)^\s*(@[A-Za-z_][A-Za-z0-9_]*)\s*$`)
	for _, part := range parts {
		match := varRe.FindStringSubmatch(strings.TrimSpace(part))
		if len(match) != 2 {
			continue
		}
		variables = append(variables, strings.TrimSpace(match[1]))
	}

	if len(variables) == 0 {
		return fetchIntoStatement{}, false
	}

	return fetchIntoStatement{CursorName: cursorName, Variables: variables}, true
}

func findFetchIntoTailBoundary(value string) int {
	if strings.TrimSpace(value) == "" {
		return -1
	}

	boundaryRe := regexp.MustCompile(`(?im)\n\s*(?:while|open|close|deallocate|__deallocate_cursor__|fetch|__fetch_next__|if|begin|end|return|go)\b`)
	loc := boundaryRe.FindStringIndex(value)
	if len(loc) != 2 {
		return -1
	}
	return loc[0]
}

func parseCursorDeclarations(content string) map[string]cursorDeclaration {
	result := make(map[string]cursorDeclaration)
	if strings.TrimSpace(content) == "" {
		return result
	}

	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	lines := strings.Split(content, "\n")

	explicitDeclRe := regexp.MustCompile(`(?is)^declare\s+([a-z_#][a-z0-9_#]*)\s+(?:insensitive\s+)?cursor\s+for\s*(.*)$`)
	macroDeclRe := regexp.MustCompile(`(?is)^__declare_cursor__\s*\(\s*([a-z_#][a-z0-9_#]*)\s*\)\s*(.*)$`)

	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" {
			continue
		}

		var (
			cursorName string
			remainder  string
			matched    bool
		)

		if m := explicitDeclRe.FindStringSubmatch(trimmed); len(m) == 3 {
			cursorName = strings.TrimSpace(m[1])
			remainder = strings.TrimSpace(m[2])
			matched = true
		} else if m := macroDeclRe.FindStringSubmatch(trimmed); len(m) == 3 {
			cursorName = strings.TrimSpace(m[1])
			remainder = strings.TrimSpace(m[2])
			matched = true
		}

		if !matched || cursorName == "" {
			continue
		}

		statementLines := make([]string, 0)
		if remainder != "" {
			statementLines = append(statementLines, remainder)
		}

		j := i + 1
		for ; j < len(lines); j++ {
			nextLine := strings.TrimSpace(lines[j])
			if isCursorDeclarationBoundary(nextLine) {
				break
			}
			statementLines = append(statementLines, lines[j])
		}
		i = j - 1

		queryText := strings.TrimSpace(strings.Join(statementLines, " "))
		if queryText == "" {
			continue
		}

		selectStmt, ok := parseSelectSourceStatement(queryText)
		if !ok {
			continue
		}

		result[normalizeIdentifier(cursorName)] = cursorDeclaration{
			CursorName:        cursorName,
			SelectExpressions: selectStmt.SelectExpressions,
			FromClause:        selectStmt.FromClause,
		}
	}

	return result
}

func parseDeallocateStatements(content string) []deallocateStatement {
	result := make([]deallocateStatement, 0)
	if strings.TrimSpace(content) == "" {
		return result
	}

	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	lines := strings.Split(content, "\n")

	re := regexp.MustCompile(`(?is)^(?:__deallocate_cursor__|deallocate(?:\s+cursor)?)\s+([a-z_#][a-z0-9_#]*)`)
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if m := re.FindStringSubmatch(trimmed); len(m) == 2 {
			result = append(result, deallocateStatement{CursorName: strings.TrimSpace(m[1]), Line: i + 1})
		}
	}
	return result
}

func parseAllFetchIntoStatements(content string) map[string][]string {
	result := make(map[string][]string)
	if strings.TrimSpace(content) == "" {
		return result
	}

	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	lines := strings.Split(content, "\n")

	prefixRe := regexp.MustCompile(`(?is)^\s*(?:__fetch_next__|fetch(?:\s+next)?(?:\s+from)?)\s+([a-z_#][a-z0-9_#]*)\s+into\s+(.*)$`)
	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" {
			continue
		}

		m := prefixRe.FindStringSubmatch(trimmed)
		if len(m) != 3 {
			continue
		}

		cursorName := normalizeIdentifier(strings.TrimSpace(m[1]))
		varsPart := strings.TrimSpace(m[2])

		// Собираем продолжение переменных на следующих строках
		j := i + 1
		for ; j < len(lines); j++ {
			nextLine := strings.TrimSpace(lines[j])
			if nextLine == "" {
				continue
			}
			if isCursorDeclarationBoundary(nextLine) {
				break
			}
			if varsPart != "" {
				varsPart += " " + nextLine
			} else {
				varsPart = nextLine
			}
		}
		i = j - 1

		if varsPart == "" {
			continue
		}

		if boundary := findFetchIntoTailBoundary(varsPart); boundary >= 0 {
			varsPart = varsPart[:boundary]
		}
		varsPart = strings.TrimSpace(varsPart)

		parts := splitTopLevelCSV(varsPart)
		varRe := regexp.MustCompile(`(?is)^\s*(@[A-Za-z_][A-Za-z0-9_]*)$`)
		for _, part := range parts {
			match := varRe.FindStringSubmatch(strings.TrimSpace(part))
			if len(match) == 2 {
				result[cursorName] = append(result[cursorName], strings.TrimSpace(match[1]))
			}
		}
	}
	return result
}

func parseSelectSourceStatement(queryText string) (insertSelectStatement, bool) {
	text := strings.TrimSpace(queryText)
	if text == "" {
		return insertSelectStatement{}, false
	}
	lower := strings.ToLower(text)
	if !strings.HasPrefix(lower, "select") {
		return insertSelectStatement{}, false
	}

	fromPos := findTopLevelKeywordPosition(text, "from")
	if fromPos < 0 {
		return insertSelectStatement{}, false
	}

	selectPart := strings.TrimSpace(text[len("select"):fromPos])
	fromClause := strings.TrimSpace(text[fromPos:])
	if selectPart == "" || fromClause == "" {
		return insertSelectStatement{}, false
	}

	items := splitTopLevelCSV(selectPart)
	if len(items) == 0 {
		return insertSelectStatement{}, false
	}

	return insertSelectStatement{SelectExpressions: items, FromClause: fromClause}, true
}

func isCursorDeclarationBoundary(line string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(line))
	if trimmed == "" {
		return false
	}

	keywords := []string{"open", "fetch", "__fetch_next__", "close", "deallocate", "__deallocate_cursor__", "declare", "__declare_cursor__", "if", "while", "return", "begin", "end", "go"}
	for _, kw := range keywords {
		if strings.HasPrefix(trimmed, kw+" ") || strings.HasPrefix(trimmed, kw+"\t") || strings.HasPrefix(trimmed, kw+"(") || trimmed == kw {
			return true
		}
	}

	return false
}

func splitTopLevelCSV(value string) []string {
	items := make([]string, 0)
	b := strings.Builder{}
	parenDepth := 0
	caseDepth := 0
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '(' {
			parenDepth++
		} else if ch == ')' && parenDepth > 0 {
			parenDepth--
		}

		if isWordBoundary(value, i-1) && strings.HasPrefix(strings.ToLower(value[i:]), "case") && isWordBoundary(value, i+4) {
			caseDepth++
		}
		if isWordBoundary(value, i-1) && strings.HasPrefix(strings.ToLower(value[i:]), "end") && isWordBoundary(value, i+3) && caseDepth > 0 {
			caseDepth--
		}

		if ch == ',' && parenDepth == 0 && caseDepth == 0 {
			item := strings.TrimSpace(b.String())
			if item != "" {
				items = append(items, item)
			}
			b.Reset()
			continue
		}
		b.WriteByte(ch)
	}
	last := strings.TrimSpace(b.String())
	if last != "" {
		items = append(items, last)
	}
	return items
}

func isWordBoundary(value string, index int) bool {
	if index < 0 || index >= len(value) {
		return true
	}
	r := value[index]
	if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
		return false
	}
	return true
}

func parseAliasMap(fromClause string) map[string]string {
	result := make(map[string]string)
	if strings.TrimSpace(fromClause) == "" {
		return result
	}
	// Сначала ищем table alias (с алиасом)
	re := regexp.MustCompile(`(?is)\b(?:from|join)\s+([a-z_#][a-z0-9_#]*)\s+([a-z_][a-z0-9_]*)`)
	matches := re.FindAllStringSubmatch(fromClause, -1)
	for _, m := range matches {
		if len(m) < 3 {
			continue
		}
		tableName := strings.TrimSpace(m[1])
		aliasName := strings.TrimSpace(m[2])
		if tableName == "" || aliasName == "" {
			continue
		}
		result[strings.ToLower(aliasName)] = tableName
		result[strings.ToLower(tableName)] = tableName
	}
	// Также ищем таблицы без алиаса: from/join table (без второго слова-алиаса)
	reNoAlias := regexp.MustCompile(`(?is)\b(?:from|join)\s+([a-z_#][a-z0-9_#]*)(?:\s|$)`)
	noAliasMatches := reNoAlias.FindAllStringSubmatch(fromClause, -1)
	for _, m := range noAliasMatches {
		if len(m) < 2 {
			continue
		}
		tableName := strings.TrimSpace(m[1])
		if tableName == "" {
			continue
		}
		if _, exists := result[strings.ToLower(tableName)]; !exists {
			result[strings.ToLower(tableName)] = tableName
		}
	}
	return result
}

func normalizeAssignmentTargetColumn(target string, stmt updateSetStatement) string {
	clean := strings.TrimSpace(target)
	if clean == "" {
		return ""
	}
	clean = strings.Trim(clean, "[]\"")
	if dotIdx := strings.LastIndex(clean, "."); dotIdx > 0 {
		prefix := strings.TrimSpace(clean[:dotIdx])
		column := strings.TrimSpace(clean[dotIdx+1:])
		prefixLower := strings.ToLower(strings.Trim(prefix, "[]\""))
		tableLower := strings.ToLower(stmt.TargetTable)
		aliasLower := strings.ToLower(stmt.TargetAlias)
		if prefixLower == tableLower || (aliasLower != "" && prefixLower == aliasLower) {
			return strings.Trim(column, "[]\"")
		}
		return ""
	}
	return clean
}

type macroDefinition struct {
	Name          string
	Params        []string
	BodyLines     []string
	DeclStartLine int
	DeclEndLine   int
}

type macroReplaceResult struct {
	Content   string
	SourceMap []int
	Macros    []macroDefinition
}

func replaceMacros(content string) macroReplaceResult {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")

	lines := strings.Split(content, "\n")
	macros := collectMacroDefinitions(lines)

	withoutDecl, withoutDeclMap := stripMacroDeclarations(lines, macros)
	expandedLines, expandedMap, usedNames := expandMacrosInLines(withoutDecl, withoutDeclMap, macros)

	usedMacros := make([]macroDefinition, 0, len(usedNames))
	for _, macro := range macros {
		if _, exists := usedNames[strings.ToLower(macro.Name)]; exists {
			usedMacros = append(usedMacros, macro)
		}
	}

	return macroReplaceResult{
		Content:   strings.Join(expandedLines, "\n"),
		SourceMap: expandedMap,
		Macros:    usedMacros,
	}
}

func collectMacroDefinitions(lines []string) []macroDefinition {
	result := make([]macroDefinition, 0)

	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trimmed, "#define") {
			continue
		}

		start := i
		definitionLines := make([]string, 0)

		for {
			raw := strings.TrimRight(lines[i], " \t")
			hasContinuation := strings.HasSuffix(raw, "\\")
			if hasContinuation {
				raw = strings.TrimRight(raw[:len(raw)-1], " \t")
			}
			definitionLines = append(definitionLines, raw)

			if !hasContinuation || i >= len(lines)-1 {
				break
			}
			i++
		}

		name, params, bodyLines := parseMacroDefinition(definitionLines)
		if name == "" {
			continue
		}

		result = append(result, macroDefinition{
			Name:          name,
			Params:        params,
			BodyLines:     bodyLines,
			DeclStartLine: start + 1,
			DeclEndLine:   i + 1,
		})
	}

	return result
}

func parseMacroDefinition(definitionLines []string) (string, []string, []string) {
	if len(definitionLines) == 0 {
		return "", nil, nil
	}

	header := strings.TrimSpace(definitionLines[0])
	header = strings.TrimSpace(strings.TrimPrefix(header, "#define"))
	if header == "" {
		return "", nil, nil
	}

	nameEnd := 0
	for nameEnd < len(header) {
		ch := header[nameEnd]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			nameEnd++
			continue
		}
		break
	}
	if nameEnd == 0 {
		return "", nil, nil
	}

	name := strings.TrimSpace(header[:nameEnd])
	rest := strings.TrimLeft(header[nameEnd:], " \t")

	params := make([]string, 0)
	bodyLines := make([]string, 0)

	if strings.HasPrefix(rest, "(") {
		depth := 0
		end := -1
	scanParams:
		for i := 0; i < len(rest); i++ {
			switch rest[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					end = i
					break scanParams
				}
			}
		}
		if end > 0 {
			paramPart := strings.TrimSpace(rest[1:end])
			if paramPart != "" {
				for _, p := range splitTopLevelCSV(paramPart) {
					param := strings.TrimSpace(p)
					if param != "" {
						params = append(params, param)
					}
				}
			}
			rest = strings.TrimSpace(rest[end+1:])
		}
	}

	if rest != "" {
		bodyLines = append(bodyLines, rest)
	}
	for _, line := range definitionLines[1:] {
		bodyLines = append(bodyLines, strings.TrimSpace(line))
	}

	if len(bodyLines) == 0 {
		bodyLines = []string{""}
	}

	return name, params, bodyLines
}

func stripMacroDeclarations(lines []string, macros []macroDefinition) ([]string, []int) {
	skip := make(map[int]struct{})
	for _, macro := range macros {
		for ln := macro.DeclStartLine; ln <= macro.DeclEndLine; ln++ {
			skip[ln] = struct{}{}
		}
	}

	resultLines := make([]string, 0, len(lines))
	resultMap := make([]int, 0, len(lines))
	for i, line := range lines {
		lineNo := i + 1
		if _, shouldSkip := skip[lineNo]; shouldSkip {
			continue
		}
		resultLines = append(resultLines, line)
		resultMap = append(resultMap, lineNo)
	}

	return resultLines, resultMap
}

type macroCall struct {
	Macro macroDefinition
	Start int
	End   int
	Args  []string
}

func expandMacrosInLines(lines []string, sourceMap []int, macros []macroDefinition) ([]string, []int, map[string]struct{}) {
	byName := make(map[string]macroDefinition, len(macros))
	names := make([]string, 0, len(macros))
	for _, macro := range macros {
		key := strings.ToLower(strings.TrimSpace(macro.Name))
		if key == "" {
			continue
		}
		byName[key] = macro
		names = append(names, key)
	}

	sort.Slice(names, func(i, j int) bool {
		if len(names[i]) == len(names[j]) {
			return names[i] < names[j]
		}
		return len(names[i]) > len(names[j])
	})

	usedNames := make(map[string]struct{})
	resultLines := make([]string, 0, len(lines))
	resultMap := make([]int, 0, len(sourceMap))

	for idx := 0; idx < len(lines); idx++ {
		lineSource := idx + 1
		if idx < len(sourceMap) && sourceMap[idx] > 0 {
			lineSource = sourceMap[idx]
		}

		currentLines := []string{lines[idx]}
		currentMap := []int{lineSource}

		for pass := 0; pass < 32; pass++ {
			changedAny := false
			nextLines := make([]string, 0, len(currentLines))
			nextMap := make([]int, 0, len(currentMap))

			for lineIdx, line := range currentLines {
				srcLine := currentMap[lineIdx]
				updatedLines, updatedMap, changed := expandFirstMacroCall(line, srcLine, names, byName, usedNames)
				nextLines = append(nextLines, updatedLines...)
				nextMap = append(nextMap, updatedMap...)
				if changed {
					changedAny = true
				}
			}

			currentLines = nextLines
			currentMap = nextMap
			if !changedAny {
				break
			}
		}

		resultLines = append(resultLines, currentLines...)
		resultMap = append(resultMap, currentMap...)
	}

	return resultLines, resultMap, usedNames
}

func expandFirstMacroCall(line string, sourceLine int, names []string, byName map[string]macroDefinition, usedNames map[string]struct{}) ([]string, []int, bool) {
	call, ok := findEarliestMacroCall(line, names, byName)
	if !ok {
		return []string{line}, []int{sourceLine}, false
	}

	usedNames[strings.ToLower(call.Macro.Name)] = struct{}{}
	replacement := applyMacroArguments(call.Macro, call.Args)
	if len(replacement) == 0 {
		replacement = []string{""}
	}

	prefix := line[:call.Start]
	suffix := line[call.End:]

	if len(replacement) == 1 {
		return []string{prefix + replacement[0] + suffix}, []int{sourceLine}, true
	}

	updatedLines := make([]string, 0, len(replacement))
	updatedMap := make([]int, 0, len(replacement))
	for i := 0; i < len(replacement); i++ {
		part := replacement[i]
		if i == 0 {
			part = prefix + part
		}
		if i == len(replacement)-1 {
			part += suffix
		}
		updatedLines = append(updatedLines, part)
		updatedMap = append(updatedMap, sourceLine)
	}

	return updatedLines, updatedMap, true
}

func findEarliestMacroCall(line string, names []string, byName map[string]macroDefinition) (macroCall, bool) {
	best := macroCall{}
	found := false

	for _, name := range names {
		macro := byName[name]
		call, ok := findMacroCall(line, macro)
		if !ok {
			continue
		}
		if !found || call.Start < best.Start || (call.Start == best.Start && call.End > best.End) {
			best = call
			found = true
		}
	}

	return best, found
}

func findMacroCall(line string, macro macroDefinition) (macroCall, bool) {
	name := strings.TrimSpace(macro.Name)
	if name == "" {
		return macroCall{}, false
	}

	lowerLine := strings.ToLower(line)
	lowerName := strings.ToLower(name)
	searchPos := 0

	for {
		idx := strings.Index(lowerLine[searchPos:], lowerName)
		if idx < 0 {
			return macroCall{}, false
		}
		idx += searchPos
		afterName := idx + len(name)

		if idx > 0 && isWordChar(line[idx-1]) {
			searchPos = idx + 1
			continue
		}

		if len(macro.Params) == 0 {
			if afterName < len(line) && isWordChar(line[afterName]) {
				searchPos = idx + 1
				continue
			}
			return macroCall{Macro: macro, Start: idx, End: afterName}, true
		}

		openPos := afterName
		for openPos < len(line) && (line[openPos] == ' ' || line[openPos] == '\t') {
			openPos++
		}
		if openPos >= len(line) || line[openPos] != '(' {
			searchPos = idx + 1
			continue
		}

		argsText, closePos, ok := parseMacroCallArguments(line, openPos)
		if !ok {
			searchPos = idx + 1
			continue
		}

		args := splitTopLevelCSV(argsText)
		for i := range args {
			args[i] = strings.TrimSpace(args[i])
		}

		return macroCall{Macro: macro, Start: idx, End: closePos + 1, Args: args}, true
	}
}

func parseMacroCallArguments(line string, openPos int) (string, int, bool) {
	if openPos < 0 || openPos >= len(line) || line[openPos] != '(' {
		return "", 0, false
	}

	depth := 0
	inString := false
	for i := openPos; i < len(line); i++ {
		ch := line[i]
		if ch == '\'' {
			if inString && i+1 < len(line) && line[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return line[openPos+1 : i], i, true
			}
		}
	}

	return "", 0, false
}

func applyMacroArguments(macro macroDefinition, args []string) []string {
	result := make([]string, len(macro.BodyLines))
	copy(result, macro.BodyLines)

	for i := range result {
		line := result[i]
		for pIdx, param := range macro.Params {
			if strings.TrimSpace(param) == "" {
				continue
			}
			replacement := ""
			if pIdx < len(args) {
				replacement = args[pIdx]
			}
			line = replaceIdentifierToken(line, param, replacement)
		}
		result[i] = line
	}

	return result
}

func replaceIdentifierToken(text string, token string, replacement string) string {
	trimmedToken := strings.TrimSpace(token)
	if trimmedToken == "" {
		return text
	}

	re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(trimmedToken) + `\b`)
	return re.ReplaceAllString(text, replacement)
}

// maskBlockCommentsKeepLines маскирует блочные комментарии /* */ с сохранением строк
func maskBlockCommentsKeepLines(text string) string {
	if text == "" {
		return text
	}

	runes := []rune(text)
	masked := make([]rune, len(runes))
	inBlock := false

	for i := 0; i < len(runes); i++ {
		ch := runes[i]

		if !inBlock && i+1 < len(runes) && runes[i] == '/' && runes[i+1] == '*' {
			inBlock = true
			masked[i] = ' '
			masked[i+1] = ' '
			i++
			continue
		}

		if inBlock {
			if i+1 < len(runes) && runes[i] == '*' && runes[i+1] == '/' {
				inBlock = false
				masked[i] = ' '
				masked[i+1] = ' '
				i++
				continue
			}
			if ch == '\n' || ch == '\r' {
				masked[i] = ch
			} else {
				masked[i] = ' '
			}
			continue
		}

		masked[i] = ch
	}

	return string(masked)
}

// isCharWordBoundary проверяет, является ли символ границей слова
func isCharWordBoundary(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == ';' || ch == '(' || ch == ')' || ch == ',' || ch == '\x00'
}

// hasFromClauseEnded проверяет, закончился ли FROM clause
func hasFromClauseEnded(lowerLine string) bool {
	keywords := []string{"where", "group by", "having", "order by", "union", "except", "intersect"}
	for _, kw := range keywords {
		if strings.Contains(lowerLine, " "+kw+" ") ||
			strings.HasPrefix(lowerLine, kw+" ") ||
			strings.HasSuffix(lowerLine, " "+kw) ||
			lowerLine == kw {
			return true
		}
	}
	// Проверяем точку с запятой (конец запроса)
	if strings.Contains(lowerLine, ";") {
		return true
	}
	return false
}

// isNewSQLStatement проверяет, начинается ли строка с нового SQL оператора (не INSERT)
func isNewSQLStatement(line string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(line))
	// Ключевые слова начала операторов (кроме INSERT который мы уже обрабатываем)
	keywords := []string{"if", "exec", "execute", "select", "update", "delete", "begin", "end", "return", "goto", "while", "declare", "fetch", "close", "open", "commit", "rollback"}
	for _, kw := range keywords {
		if strings.HasPrefix(trimmed, kw+" ") ||
			strings.HasPrefix(trimmed, kw+"\t") ||
			trimmed == kw {
			return true
		}
	}
	return false
}

// maskSingleQuotedStringContent заменяет символы внутри одинарных кавычек
// на символ '?' чтобы терминаторы внутри строковых литералов (;, go, begin и т.д.)
// не приводили к ложному разделению SQL-операторов. Учитывает экранирование кавычек (”).
func maskSingleQuotedStringContent(s string) string {
	result := []byte(s)
	inString := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			if inString && i+1 < len(s) && s[i+1] == '\'' {
				// Экранированная одинарная кавычка — оставляем как есть
				result[i] = '\''
				result[i+1] = '\''
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			result[i] = '?'
		}
	}
	return string(result)
}

// hasStatementEnded проверяет, закончился ли SQL оператор
// stmtBuffer передаётся для контекста: если новый DML начинается, разрываем только если предыдущий уже "полный"
func hasStatementEnded(lower string, stmtBuffer []string) bool {
	// Используем regex с границами слова, чтобы избежать ложных срабатываний на подстроках
	// Например, "dependantinfo" содержит "end", но это не ключевое слово
	// UNION не разрывает оператор - он часть составного оператора
	re := regexp.MustCompile(`(?i)([;]|\b(?:go|begin|if|while|declare|exec|execute|return)\b)`)
	if re.MatchString(maskSingleQuotedStringContent(lower)) {
		return true
	}
	trimmed := strings.TrimSpace(lower)
	endRe := regexp.MustCompile(`(?i)^end(?:\s|;|$)`)
	if endRe.MatchString(trimmed) {
		currentStmt := strings.ToLower(strings.Join(stmtBuffer, " "))
		if strings.Contains(currentStmt, "update") && strings.Contains(currentStmt, " set ") {
			hasCase := strings.Contains(currentStmt, " case") || strings.Contains(currentStmt, " case ")
			hasFrom := strings.Contains(currentStmt, " from ")
			hasWhere := strings.Contains(currentStmt, " where ")
			if hasCase && !hasFrom && !hasWhere {
				return false
			}
		}

		// END CASE внутри выражения (ON/WHERE/SELECT-list) не завершает оператор
		// Standalone END без блоковых конструкций в буфере — тоже END CASE
		trimmedLower := strings.ToLower(strings.TrimSpace(trimmed))
		isStandaloneEnd := trimmedLower == "end" || strings.HasPrefix(trimmedLower, "end;") || strings.HasPrefix(trimmedLower, "end --")
		if strings.Contains(currentStmt, " case") {
			if !isStandaloneEnd {
				return false
			}
			// Standalone END: если в буфере нет BEGIN/IF/WHILE/DECLARE — это END CASE
			hasBlock := regexp.MustCompile(`(?i)\b(begin|if|while|declare)\b`).MatchString(currentStmt)
			if !hasBlock {
				return false
			}
		}

		return true
	}

	// Если начинается новый DML оператор (select/update/delete/insert), проверяем нужно ли разрывать предыдущий
	dmlRe := regexp.MustCompile(`(?i)^\s*(?:select|update|delete|insert)\b`)
	if !dmlRe.MatchString(lower) {
		return false
	}

	// Если stmtBuffer пустой - нечего разрывать
	if len(stmtBuffer) == 0 {
		return false
	}

	// Собираем текущий буфер в одну строку для анализа
	currentStmt := strings.Join(stmtBuffer, " ")
	currentLower := strings.ToLower(currentStmt)

	// INSERT может продолжаться SELECT - не разрываем
	if strings.Contains(currentLower, "insert") && !strings.Contains(currentLower, "from") {
		return false
	}

	// UNION - не разрываем только если новая строка начинается с SELECT
	// (SELECT ... UNION SELECT — это один оператор).
	// DELETE/UPDATE/INSERT после UNION — это новый оператор.
	if strings.Contains(currentLower, "union") {
		newSelectRe := regexp.MustCompile(`(?i)^\s*select\b`)
		if newSelectRe.MatchString(lower) {
			return false
		}
	}

	// SELECT/UPDATE/DELETE с FROM/SET считаются полными - разрываем
	if strings.Contains(currentLower, "select") && strings.Contains(currentLower, "from") {
		return true
	}
	if strings.Contains(currentLower, "update") && strings.Contains(currentLower, "set") {
		return true
	}
	if strings.Contains(currentLower, "delete") && strings.Contains(currentLower, "from") {
		return true
	}

	// В остальных случаях разрываем при новом DML
	return true
}

// findConditionStart находит начало условия (WHERE/ON/HAVING)
func findConditionStart(lower string) int {
	kws := []string{"where", "on", "having"}
	for _, kw := range kws {
		idx := findKeywordPosition(lower, kw)
		if idx >= 0 {
			return idx
		}
	}
	return -1
}

// hasConditionEnded проверяет, закончилось ли условие
func hasConditionEnded(lower string) bool {
	kws := []string{"group by", "order by", "union", "except", "intersect", ";"}
	for _, kw := range kws {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// hasJoinCondition проверяет наличие ON условия
func hasJoinCondition(lower string) bool {
	return strings.Contains(lower, " on ")
}

// containsBitwiseOperator проверяет наличие битовых операторов (&, |, ^) в выражении
func containsBitwiseOperator(expr string) bool {
	return strings.Contains(expr, "&") ||
		strings.Contains(expr, "|") ||
		strings.Contains(expr, "^")
}

// isNumericLiteral проверяет, является ли строка числовым литералом
func isNumericLiteral(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

// isInsideConvert проверяет, находится ли позиция внутри convert(...) или cast(...)
func isInsideConvert(expr string, pos int) bool {
	// Проверяем, находится ли позиция внутри convert(...) или cast(...)
	lower := strings.ToLower(expr[:pos])

	// Ищем последний convert( или cast( перед позицией
	lastConvert := strings.LastIndex(lower, "convert(")
	lastCast := strings.LastIndex(lower, "cast(")

	var funcNameLen int
	var lastFunc int

	if lastCast > lastConvert {
		lastFunc = lastCast
		funcNameLen = 5 // len("cast(")
	} else if lastConvert > lastCast {
		lastFunc = lastConvert
		funcNameLen = 8 // len("convert(")
	} else {
		return false
	}

	// Считаем скобки: если открывающих больше закрывающих — мы внутри
	// Начинаем с 1, т.к. convert( или cast( уже содержат открывающую скобку
	parenDepth := 1
	for i := lastFunc + funcNameLen; i < pos; i++ {
		if i >= len(expr) {
			break
		}
		switch expr[i] {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		}
	}

	return parenDepth > 0
}

// isOperandChar проверяет, является ли символ операндом
func isOperandChar(ch byte) bool {
	// Операнд может начинаться с цифры, буквы или @
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == '@'
}

// isInsertInSubquery проверяет, находится ли INSERT внутри подзапроса/CTE
func isInsertInSubquery(line string) bool {
	// Если перед INSERT есть открывающая скобка - это подзапрос
	lower := strings.ToLower(line)
	insertIdx := strings.Index(lower, "insert")
	if insertIdx == -1 {
		return false
	}

	// Считаем скобки до INSERT
	parenDepth := 0
	for i := 0; i < insertIdx; i++ {
		switch line[i] {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		}
	}

	return parenDepth > 0
}

// hasRowLock проверяет наличие M_WITH_ROWLOCK или WITH (ROWLOCK)
func hasRowLock(line string) bool {
	lower := strings.ToLower(line)

	// Проверяем M_WITH_ROWLOCK
	if strings.Contains(lower, "m_with_rowlock") {
		return true
	}

	// Проверяем WITH (ROWLOCK)
	if strings.Contains(lower, "with") && strings.Contains(lower, "rowlock") {
		return true
	}

	return false
}

type execArgument struct {
	Raw      string
	IsNamed  bool
	Name     string // Имя параметра без '@'
	Value    string
	IsOutput bool   // Указывает, передан ли параметр как OUTPUT/OUT
	VarName  string // Имя передаваемой переменной (например, "@locObjectID")
}

func parseExecArguments(line string, calleeName string) []execArgument {
	cleaned := removeComments(line)

	lowerLine := strings.ToLower(cleaned)
	lowerCallee := strings.ToLower(calleeName)

	idx := strings.Index(lowerLine, lowerCallee)
	if idx < 0 {
		return nil
	}

	argsPart := strings.TrimSpace(cleaned[idx+len(calleeName):])
	if argsPart == "" {
		return nil
	}

	rawArgs := splitByCommasRespectingParens(argsPart)

	var result []execArgument
	for _, raw := range rawArgs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		if strings.HasPrefix(raw, "@") {
			eqIdx := strings.Index(raw, "=")
			if eqIdx > 0 {
				paramName := strings.TrimSpace(raw[1:eqIdx])
				val := strings.TrimSpace(raw[eqIdx+1:])

				isOutput := false
				varName := ""
				valClean := val

				outRe := regexp.MustCompile(`(?i)\s+\b(out(?:put)?)\b\s*$`)
				if m := outRe.FindStringSubmatch(valClean); m != nil {
					isOutput = true
					valClean = strings.TrimSpace(valClean[:len(valClean)-len(m[0])])
				}

				if strings.HasPrefix(valClean, "@") {
					varName = strings.ToLower(valClean)
				}

				result = append(result, execArgument{
					Raw:      raw,
					IsNamed:  true,
					Name:     normalizeIdentifier(paramName),
					Value:    valClean,
					IsOutput: isOutput,
					VarName:  varName,
				})
				continue
			}
		}

		isOutput := false
		varName := ""
		valClean := raw

		outRe := regexp.MustCompile(`(?i)\s+\b(out(?:put)?)\b\s*$`)
		if m := outRe.FindStringSubmatch(valClean); m != nil {
			isOutput = true
			valClean = strings.TrimSpace(valClean[:len(valClean)-len(m[0])])
		}

		if strings.HasPrefix(valClean, "@") {
			varName = strings.ToLower(valClean)
		}

		result = append(result, execArgument{
			Raw:      raw,
			IsNamed:  false,
			Value:    valClean,
			IsOutput: isOutput,
			VarName:  varName,
		})
	}
	return result
}

func cleanAndTrim(line string) string {
	return strings.TrimSpace(removeComments(line))
}

func collectExecCallLines(lines []string, startLine int) string {
	if startLine < 1 || startLine > len(lines) {
		return ""
	}

	var collected []string
	firstLine := lines[startLine-1]
	collected = append(collected, firstLine)

	for i := startLine; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		if strings.HasPrefix(trimmed, "/*") && strings.HasSuffix(trimmed, "*/") {
			continue
		}

		cleanedPrev := cleanAndTrim(collected[len(collected)-1])
		cleanedCur := cleanAndTrim(line)

		isPrevLineDangling := strings.HasSuffix(cleanedPrev, ",")
		isCurLineContinuing := strings.HasPrefix(cleanedCur, ",") || strings.HasPrefix(cleanedCur, "@")

		if !isPrevLineDangling && !isCurLineContinuing {
			break
		}

		lower := strings.ToLower(trimmed)
		ends := strings.HasSuffix(lower, ";")

		if !strings.HasPrefix(trimmed, "@") {
			isNew := false
			keywords := []string{"select", "insert", "update", "delete", "truncate", "declare", "begin", "end", "return", "go", "set", "if", "while", "exec", "execute"}
			for _, kw := range keywords {
				if strings.HasPrefix(lower, kw+" ") || strings.HasPrefix(lower, kw+"\t") || lower == kw {
					isNew = true
					break
				}
			}
			if isNew {
				break
			}
		}

		collected = append(collected, line)
		if ends {
			break
		}
	}

	return strings.Join(collected, "\n")
}

// splitStatementsWithOffsets аналогична splitStatementsForHintType, но дополнительно
// сохраняет байтовый offset начала каждого оператора в исходном тексте.
func splitStatementsWithOffsets(text string) []stmtWithOffset {
	lower := toLowerASCII(text)
	result := make([]stmtWithOffset, 0)
	depth := 0
	inString := false
	startIdx := 0

	for i := 0; i < len(lower); i++ {
		ch := lower[i]
		if ch == '\'' {
			if i+1 < len(lower) && lower[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && i > 0 {
			if keywordMatchAt(lower, i, "select") || keywordMatchAt(lower, i, "update") || keywordMatchAt(lower, i, "delete") {
				if i > 0 && isCharWordBoundary(lower[i-1]) {
					if startIdx < i {
						stmt := strings.TrimSpace(text[startIdx:i])
						if stmt != "" {
							result = append(result, stmtWithOffset{stmt: stmt, offset: startIdx})
						}
					}
					startIdx = i
				}
			}
		}
	}
	if startIdx < len(text) {
		stmt := strings.TrimSpace(text[startIdx:])
		if stmt != "" {
			result = append(result, stmtWithOffset{stmt: stmt, offset: startIdx})
		}
	}
	return result
}

// countTopLevelJoins считает количество JOIN-ов на верхнем уровне вложенности (вне подзапросов).
func countTopLevelJoins(stmt string) int {
	lower := strings.ToLower(stmt)
	count := 0
	depth := 0
	inString := false

	for i := 0; i < len(lower); i++ {
		ch := lower[i]

		if ch == '\'' {
			if i+1 < len(lower) && lower[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}

		if depth == 0 && keywordMatchAt(lower, i, "join") {
			count++
		}
	}

	return count
}

// splitStatementsForHintType разбивает текст на отдельные операторы SELECT/UPDATE/DELETE на нулевом уровне вложенности
func splitStatementsForHintType(text string) []statementRange {
	lower := strings.ToLower(text)
	statements := make([]statementRange, 0)
	depth := 0
	inString := false
	startIdx := 0
	// Отслеживаем, начинается ли текущий оператор с INSERT (для INSERT...SELECT)
	currentStartsWithInsert := false

	for i := 0; i < len(lower); i++ {
		ch := lower[i]
		if ch == '\'' {
			if i+1 < len(lower) && lower[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}

		// Если на нулевом уровне и встречаем новое ключевое слово SELECT/UPDATE/DELETE
		if depth == 0 && i > 0 {
			if keywordMatchAt(lower, i, "select") || keywordMatchAt(lower, i, "update") || keywordMatchAt(lower, i, "delete") {
				// Проверяем, что это не часть другого слова
				if i > 0 && isCharWordBoundary(lower[i-1]) {
					// Для INSERT...SELECT не разрываем на внутреннем SELECT
					if currentStartsWithInsert && keywordMatchAt(lower, i, "select") {
						continue
					}
					// Добавляем предыдущий оператор
					if startIdx < i {
						stmt := strings.TrimSpace(text[startIdx:i])
						if stmt != "" {
							statements = append(statements, statementRange{Text: stmt, StartPos: startIdx})
						}
					}
					startIdx = i
					currentStartsWithInsert = false
				}
			}
		}

		// Определяем, начинается ли текущий оператор с INSERT
		if i == startIdx && keywordMatchAt(lower, i, "insert") {
			currentStartsWithInsert = true
		}
	}

	// Добавляем последний оператор
	if startIdx < len(text) {
		stmt := strings.TrimSpace(text[startIdx:])
		if stmt != "" {
			statements = append(statements, statementRange{Text: stmt, StartPos: startIdx})
		}
	}

	return statements
}

func findInSubqueryPositions(stmt string) []int {
	positions := make([]int, 0)
	lower := strings.ToLower(stmt)
	depth := 0
	inString := false

	for i := 0; i < len(lower); i++ {
		ch := lower[i]
		if ch == '\'' {
			if i+1 < len(lower) && lower[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}

		if depth == 0 {
			if keywordMatchAt(lower, i, "not in") {
				j := i + len("not in")
				for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\n' || lower[j] == '\r') {
					j++
				}
				if j < len(lower) && lower[j] == '(' {
					if hasSelectInsideParens(lower, j) {
						positions = append(positions, i)
					}
					i = j
					continue
				}
			}
			if keywordMatchAt(lower, i, "in") {
				j := i + len("in")
				for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\n' || lower[j] == '\r') {
					j++
				}
				if j < len(lower) && lower[j] == '(' {
					if hasSelectInsideParens(lower, j) {
						positions = append(positions, i)
					}
					i = j
					continue
				}
			}
		}
	}
	return positions
}

func parseVarDeclaration(item string) (string, string, string) {
	item = strings.TrimSpace(item)
	if item == "" {
		return "", "", ""
	}
	// @name [AS] type[(size)]
	if !strings.HasPrefix(item, "@") {
		return "", "", ""
	}
	// Находим конец имени переменной
	nameEnd := 1
	for nameEnd < len(item) && (isWordChar(item[nameEnd]) || item[nameEnd] == '_') {
		nameEnd++
	}
	varName := item[:nameEnd]
	rest := strings.TrimSpace(item[nameEnd:])

	// Пропускаем AS если есть
	if strings.HasPrefix(strings.ToLower(rest), "as ") {
		rest = strings.TrimSpace(rest[3:])
	}

	// Извлекаем тип (слово до whitespace, (, или конца)
	if rest == "" {
		return varName, "", ""
	}
	typeEnd := 0
	for typeEnd < len(rest) && (isWordChar(rest[typeEnd]) || rest[typeEnd] == '_') {
		typeEnd++
	}
	varType := rest[:typeEnd]
	return varName, varType, rest
}

func extractInsertTableName(text string, pos int) string {
	lower := toLowerASCIIPreservingLen(text)
	j := pos + len("insert")
	for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\n' || lower[j] == '\r') {
		j++
	}
	if keywordMatchAt(lower, j, "into") {
		j += len("into")
		for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\n' || lower[j] == '\r') {
			j++
		}
	}
	start := j
	for j < len(lower) && (isWordChar(lower[j]) || lower[j] == '.' || lower[j] == '[' || lower[j] == ']' || lower[j] == '#') {
		j++
	}
	if start >= j || start >= len(lower) {
		return ""
	}
	// Не должно начинаться с символов, не допустимых для имени таблицы
	if !(isWordChar(lower[start]) || lower[start] == '#' || lower[start] == '[') {
		return ""
	}
	tableName := strings.TrimSpace(text[start:j])
	return tableName
}

func findInsertWithoutColumns(text string) []int {
	positions := make([]int, 0)
	lower := toLowerASCIIPreservingLen(text)
	depth := 0
	inString := false
	inBlockComment := false

	for i := 0; i < len(lower); i++ {
		ch := lower[i]

		// Пропускаем блочные комментарии /* ... */
		if inBlockComment {
			if ch == '*' && i+1 < len(lower) && lower[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if ch == '/' && i+1 < len(lower) && lower[i+1] == '*' {
			inBlockComment = true
			i++
			continue
		}
		// Пропускаем строчные комментарии -- ...
		if ch == '-' && i+1 < len(lower) && lower[i+1] == '-' {
			for i < len(lower) && lower[i] != '\n' {
				i++
			}
			continue
		}
		if ch == '\'' {
			if i+1 < len(lower) && lower[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}

		if keywordMatchAt(lower, i, "insert") {
			j := i + len("insert")
			// пропускаем whitespace
			for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\n' || lower[j] == '\r') {
				j++
			}
			// опционально пропускаем INTO
			if keywordMatchAt(lower, j, "into") {
				j += len("into")
				for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\n' || lower[j] == '\r') {
					j++
				}
			}
			// пропускаем имя таблицы (слово, точка, квадратные скобки, #)
			for j < len(lower) && (isWordChar(lower[j]) || lower[j] == '.' || lower[j] == '[' || lower[j] == ']' || lower[j] == '#') {
				j++
			}
			// пропускаем whitespace
			for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\n' || lower[j] == '\r') {
				j++
			}
			// пропускаем макросы M_* (возможно со скобками)
			macroHadParens := false
			for {
				macroStart := j
				if j < len(lower) && lower[j] == 'm' && j+1 < len(lower) && lower[j+1] == '_' {
					for j < len(lower) && (isWordChar(lower[j]) || lower[j] == '_') {
						j++
					}
					if j < len(lower) && lower[j] == '(' {
						macroHadParens = true
						j++
						macroDepth := 1
						macroString := false
						for j < len(lower) && macroDepth > 0 {
							c := lower[j]
							if c == '\'' {
								if j+1 < len(lower) && lower[j+1] == '\'' {
									j += 2
									continue
								}
								macroString = !macroString
								j++
								continue
							}
							if macroString {
								j++
								continue
							}
							switch c {
							case '(':
								macroDepth++
							case ')':
								macroDepth--
							}
							j++
						}
					}
					for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\n' || lower[j] == '\r') {
						j++
					}
					if macroStart == j {
						break
					}
				} else {
					break
				}
			}

			// Если найден макрос со скобками (например M_WITH_ROWLOCK(col1, col2)),
			// колонки перечислены внутри макроса — не reporting
			if macroHadParens {
				i = j
				continue
			}

			// Пропускаем whitespace и строчные комментарии перед (
			for {
				for j < len(lower) && (lower[j] == ' ' || lower[j] == '\t' || lower[j] == '\n' || lower[j] == '\r') {
					j++
				}
				if j+1 < len(lower) && lower[j] == '-' && lower[j+1] == '-' {
					for j < len(lower) && lower[j] != '\n' {
						j++
					}
					continue
				}
				break
			}

			if j < len(lower) && lower[j] == '(' {
				i = j
				continue
			}
			if keywordMatchAt(lower, j, "values") || keywordMatchAt(lower, j, "select") ||
				keywordMatchAt(lower, j, "exec") || keywordMatchAt(lower, j, "execute") ||
				keywordMatchAt(lower, j, "default") {
				positions = append(positions, i)
				i = j
				continue
			}
			// Не распознали следующий токен — тоже finding
			positions = append(positions, i)
			i = j
		}
	}
	return positions
}

func extractSetColumns(stmt string) []string {
	columns := make([]string, 0)
	lower := strings.ToLower(stmt)

	setIdx := findKeywordPosition(lower, "set")
	if setIdx == -1 {
		return columns
	}

	setPart := stmt[setIdx+3:]
	lowerSetPart := strings.ToLower(setPart)

	endMarkers := []string{" where ", " from ", ";", " output ", " option "}
	endIdx := len(setPart)
	for _, marker := range endMarkers {
		if idx := findKeywordPosition(lowerSetPart, strings.TrimSpace(marker)); idx >= 0 {
			if idx < endIdx {
				endIdx = idx
			}
		}
	}
	setPart = setPart[:endIdx]

	assignments := splitTopLevelSetAssignments(setPart)
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}
		eqIdx := strings.Index(assignment, "=")
		if eqIdx == -1 {
			continue
		}
		leftPart := strings.TrimSpace(assignment[:eqIdx])
		leftPart = normalizeIdentifier(leftPart)
		if idx := strings.LastIndex(leftPart, "."); idx >= 0 {
			leftPart = leftPart[idx+1:]
		}
		if leftPart != "" && !strings.HasPrefix(leftPart, "@") {
			columns = append(columns, leftPart)
		}
	}

	return columns
}

// findAssignmentLineOffset возвращает смещение строки (0-based) от начала текста
// до позиции j-го assignment в многострочном SELECT.
func findAssignmentLineOffset(queryText string, assignments []selectAssignment, j int) int {
	if j < 0 || j >= len(assignments) {
		return 0
	}
	searchFrom := 0
	for i := 0; i <= j; i++ {
		idx := strings.Index(strings.ToLower(queryText[searchFrom:]), strings.ToLower(assignments[i].TargetVariable))
		if idx < 0 {
			return 0
		}
		if i == j {
			absPos := searchFrom + idx
			return strings.Count(queryText[:absPos], "\n")
		}
		searchFrom += idx + len(assignments[i].TargetVariable)
	}
	return 0
}

// parseCasePartAssignments извлекает assignment'ы из THEN/ELSE веток CASE-выражения.
// Возвращает selectAssignment для каждой переменной, присваиваемой внутри CASE.
// Expression — весь CASE-текст целиком, чтобы cross-reference check мог найти
// ссылки на переменные в WHEN-условиях.
func parseCasePartAssignments(part string) []selectAssignment {
	lower := strings.ToLower(strings.TrimSpace(part))
	if !strings.HasPrefix(lower, "case") {
		return nil
	}

	result := make([]selectAssignment, 0)
	seen := make(map[string]bool)

	branchRe := regexp.MustCompile(`(?i)(?:then|else)\s+(@[A-Za-z_][A-Za-z0-9_]*)\s*=`)
	matches := branchRe.FindAllStringSubmatch(part, -1)
	for _, m := range matches {
		varName := strings.TrimSpace(m[1])
		if !seen[varName] {
			seen[varName] = true
			result = append(result, selectAssignment{
				TargetVariable: varName,
				Expression:     part,
			})
		}
	}
	return result
}

// findColLineOffsetInPart находит смещение строки для colName внутри part,
// относительно начала queryText. Ищет colName в part, затем определяет
// позицию part в queryText и вычисляет line offset.
func findColLineOffsetInPart(queryText, part, colName string) int {
	partIdx := strings.Index(strings.ToLower(queryText), strings.ToLower(part))
	if partIdx < 0 {
		partIdx = 0
	}
	colIdx := strings.Index(strings.ToLower(part), strings.ToLower(colName))
	if colIdx < 0 {
		colIdx = 0
	}
	absIdx := partIdx + colIdx
	return strings.Count(queryText[:absIdx], "\n")
}

// findAllUnqualifiedColumnRefs возвращает все неквалифицированные ссылки на столбцы.
func findAllUnqualifiedColumnRefs(expr string) []string {
	// Удаляем подзапросы в скобках (select ...) — алиасы и имена таблиц внутри
	// не должны считаться неквалифицированными колонками
	cleaned := stripSubqueries(expr)
	// Удаляем однострочные комментарии //...
	cleaned = regexp.MustCompile(`//[^\n]*`).ReplaceAllString(cleaned, "")
	// Удаляем однострочные комментарии --...
	cleaned = regexp.MustCompile(`--[^\n]*`).ReplaceAllString(cleaned, "")
	// Удаляем многострочные комментарии /* ... */
	cleaned = regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllString(cleaned, "")
	// Удаляем qualified refs (alias.column)
	cleaned = regexp.MustCompile(`(?i)\b[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\b`).ReplaceAllString(cleaned, "")
	// Удаляем @variables и @@variables
	cleaned = regexp.MustCompile(`(?i)@@?[a-z_][a-z0-9_]*`).ReplaceAllString(cleaned, "")
	// Удаляем строковые литералы
	cleaned = regexp.MustCompile(`'[^']*'`).ReplaceAllString(cleaned, "")
	// Удаляем числовые литералы
	cleaned = regexp.MustCompile(`\b\d+(\.\d+)?\b`).ReplaceAllString(cleaned, "")
	// Удаляем макросы-хинты M_*_INDEX(...) вместе с содержимым скобок
	cleaned = regexp.MustCompile(`(?i)\bM_\w+_INDEX\s*\([^)]*\)`).ReplaceAllString(cleaned, "")

	idRe := regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*)\b`)
	matches := idRe.FindAllStringSubmatch(cleaned, -1)
	result := make([]string, 0, len(matches))
	seen := make(map[string]bool)
	for _, m := range matches {
		name := strings.ToLower(m[1])
		if sqlKeywordsMap[name] {
			continue
		}
		if sqlFunctionsMap[name] {
			continue
		}
		if sqlDataTypesMap[name] {
			continue
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		result = append(result, m[1])
	}
	return result
}

// truncateAtStatementBoundary обрезает выражение на первой границе оператора
// (end, else, begin, insert, select, update, delete, exec, if, while и т.д.)
// на верхнем уровне (глубина скобок = 0). Это предотвращает захват кода
// из последующих операторов, когда парсер объединяет несколько операторов в один фрагмент.
func truncateAtStatementBoundary(expr string) string {
	lower := strings.ToLower(expr)
	isUpdate := strings.HasPrefix(lower, "update")
	boundaries := []string{"end", "else", "begin", "insert", "select", "update",
		"delete", "exec", "execute", "if", "while", "print", "goto", "return",
		"break", "continue", "waitfor", "raiserror", "commit", "rollback", "set"}
	depth := 0
	for i := 0; i < len(lower); i++ {
		c := lower[i]
		if c == '(' {
			depth++
			continue
		}
		if c == ')' {
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth > 0 {
			continue
		}
		// Не обрезаем в самом начале выражения — первый ключевое слово
		// является началом оператора, а не границей между операторами
		if i == 0 {
			continue
		}
		// Проверяем слово на границу
		if i > 0 && (lower[i-1] == '_' || (lower[i-1] >= 'a' && lower[i-1] <= 'z') || (lower[i-1] >= '0' && lower[i-1] <= '9')) {
			continue
		}
		// Макросы профилирования PROFILE_TIME_* — отдельные операторы
		if strings.HasPrefix(lower[i:], "profile_time") {
			return expr[:i]
		}
		for _, kw := range boundaries {
			// Для UPDATE «set» — часть синтаксиса (UPDATE ... SET), не граница оператора
			if kw == "set" && isUpdate {
				continue
			}
			if i+len(kw) <= len(lower) && lower[i:i+len(kw)] == kw {
				after := i + len(kw)
				if after >= len(lower) || lower[after] == ' ' || lower[after] == '\t' ||
					lower[after] == '\n' || lower[after] == '\r' || lower[after] == '(' {
					return expr[:i]
				}
			}
		}
	}
	return expr
}

// stripSubqueries удаляет подзапросы в скобках (select ...) из выражения.
// Это предотвращает ложные срабатывания, когда алиасы таблиц или имена таблиц
// внутри подзапросов воспринимаются как неквалифицированные ссылки на столбцы.
func stripSubqueries(expr string) string {
	lower := strings.ToLower(expr)
	var result strings.Builder
	i := 0
	for i < len(expr) {
		// Ищем открывающую скобку
		parenIdx := strings.IndexByte(expr[i:], '(')
		if parenIdx < 0 {
			result.WriteString(expr[i:])
			break
		}
		parenIdx += i
		// Копируем всё до скобки
		result.WriteString(expr[i:parenIdx])
		// Находим matching closing bracket
		depth := 1
		j := parenIdx + 1
		for j < len(expr) && depth > 0 {
			switch expr[j] {
			case '(':
				depth++
			case ')':
				depth--
			}
			j++
		}
		// Проверяем, содержит ли содержимое скобок SELECT (подзапрос)
		content := lower[parenIdx+1 : j-1]
		if strings.Contains(content, "select") {
			// Это подзапрос — пропускаем его целиком
		} else {
			// Не подзапрос — сохраняем содержимое скобок
			result.WriteString(expr[parenIdx:j])
		}
		i = j
	}
	return result.String()
}

// extractOnClauses извлекает ON-условия из JOIN-выражений в запросе.
func extractOnClauses(queryText string) []string {
	result := make([]string, 0)
	lower := strings.ToLower(queryText)

	stopWords := []string{
		"where", "group", "order", "having", "union",
		"join", "inner", "left", "right", "outer", "full", "cross",
		"option",
	}

	searchFrom := 0
	for {
		onIdx := findKeywordPosition(lower[searchFrom:], "on")
		if onIdx < 0 {
			break
		}
		onIdx += searchFrom
		start := onIdx + 2 // skip "on"
		// Пропускаем whitespace
		for start < len(lower) && (lower[start] == ' ' || lower[start] == '\t' || lower[start] == '\n' || lower[start] == '\r') {
			start++
		}
		// Ищем конец ON-условия — следующий stop-word или ; или конец строки
		end := start
		depth := 0
	stopLoop:
		for end < len(lower) {
			ch := lower[end]
			if ch == '(' {
				depth++
			} else if ch == ')' {
				if depth > 0 {
					depth--
				}
			} else if ch == ';' && depth == 0 {
				break
			} else if depth == 0 {
				for _, kw := range stopWords {
					if keywordMatchAt(lower, end, kw) {
						break stopLoop
					}
				}
			}
			end++
		}
		if end > start {
			result = append(result, strings.TrimSpace(queryText[start:end]))
		}
		searchFrom = onIdx + 2
	}
	return result
}

// extractComparisons разбивает выражение на подусловия по AND/OR (top-level)
// и для каждого находит оператор сравнения на top-level (вне скобок).
// Если подусловие заключено в скобки и не содержит top-level оператора,
// рекурсивно обрабатывает содержимое скобок.
func extractComparisons(expr string) []comparisonExpr {
	result := make([]comparisonExpr, 0)

	// Разбиваем по AND/OR на top-level
	subExprs := splitByAndOr(expr)
	for _, sub := range subExprs {
		trimmed := strings.TrimSpace(sub)
		if trimmed == "" {
			continue
		}
		cmp := findComparisonOperator(trimmed)
		if cmp.op != "" {
			result = append(result, cmp)
			continue
		}
		// Если нет top-level оператора и выражение обёрнуто в скобки — рекурсивно
		if len(trimmed) >= 2 && trimmed[0] == '(' && trimmed[len(trimmed)-1] == ')' {
			// Проверяем что внешние скобки охватывают всё выражение
			depth := 0
			allMatched := true
			for i, ch := range trimmed {
				if ch == '(' {
					depth++
				} else if ch == ')' {
					depth--
					if depth == 0 && i < len(trimmed)-1 {
						allMatched = false
						break
					}
				}
			}
			if allMatched {
				result = append(result, extractComparisons(trimmed[1:len(trimmed)-1])...)
			}
		}
	}
	return result
}

// splitByAndOr разбивает выражение по top-level AND/OR.
func splitByAndOr(expr string) []string {
	result := make([]string, 0)
	lower := strings.ToLower(expr)
	depth := 0
	caseDepth := 0
	start := 0

	for i := 0; i < len(lower); i++ {
		ch := lower[i]
		if ch == '(' {
			depth++
		} else if ch == ')' && depth > 0 {
			depth--
		}
		// Отслеживаем CASE ... END
		if isWordBoundary(lower, i-1) && strings.HasPrefix(lower[i:], "case") && isWordBoundary(lower, i+4) {
			caseDepth++
		}
		if isWordBoundary(lower, i-1) && strings.HasPrefix(lower[i:], "end") && isWordBoundary(lower, i+3) && caseDepth > 0 {
			caseDepth--
		}

		if depth == 0 && caseDepth == 0 {
			if isWordBoundary(lower, i-1) && strings.HasPrefix(lower[i:], "and") && isWordBoundary(lower, i+3) {
				result = append(result, expr[start:i])
				start = i + 3
				i += 2
			} else if isWordBoundary(lower, i-1) && strings.HasPrefix(lower[i:], "or") && isWordBoundary(lower, i+2) {
				result = append(result, expr[start:i])
				start = i + 2
				i += 1
			}
		}
	}
	result = append(result, expr[start:])
	return result
}

// findComparisonOperator находит оператор сравнения на top-level (глубина скобок = 0)
// и возвращает пару операндов с оператором.
func findComparisonOperator(expr string) comparisonExpr {
	lower := strings.ToLower(expr)
	depth := 0
	caseDepth := 0

	// Список операторов в порядке проверки (длинные сначала)
	operators := []string{"<>", "!=", "<=", ">=", "=", "<", ">"}

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' && depth > 0 {
			depth--
			continue
		}

		// Отслеживаем CASE ... END
		if isWordBoundary(lower, i-1) && strings.HasPrefix(lower[i:], "case") && isWordBoundary(lower, i+4) {
			caseDepth++
			continue
		}
		if isWordBoundary(lower, i-1) && strings.HasPrefix(lower[i:], "end") && isWordBoundary(lower, i+3) && caseDepth > 0 {
			caseDepth--
			continue
		}

		if depth > 0 || caseDepth > 0 {
			continue
		}

		// Пропускаем строковые литералы
		if ch == '\'' {
			i++
			for i < len(expr) && expr[i] != '\'' {
				i++
			}
			continue
		}

		for _, op := range operators {
			if i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
				// Проверяем что это не часть слова (например, = в != уже обработан)
				if op == "=" && i > 0 && (expr[i-1] == '<' || expr[i-1] == '>' || expr[i-1] == '!') {
					continue
				}
				left := strings.TrimSpace(expr[:i])
				right := strings.TrimSpace(expr[i+len(op):])
				if left == "" || right == "" {
					return comparisonExpr{}
				}
				// Отбрасываем сравнения, где операнд содержит SQL-ключевые слова
				// (join, on, where, from, select и т.д.) — это артефакт парсинга,
				// когда WHERE/ON часть захватила соседние конструкции
				if containsSQLStatementKeyword(left) || containsSQLStatementKeyword(right) {
					continue
				}
				return comparisonExpr{left: left, right: right, op: op}
			}
		}
	}
	return comparisonExpr{}
}

// extractCaseWhenConditions извлекает WHEN-условия из CASE ... END конструкций.
func extractCaseWhenConditions(queryText string) []string {
	result := make([]string, 0)
	lower := strings.ToLower(queryText)

	searchFrom := 0
	for {
		caseIdx := findKeywordPosition(lower[searchFrom:], "case")
		if caseIdx < 0 {
			break
		}
		caseIdx += searchFrom

		// Находим соответствующий END
		depth := 1
		endIdx := caseIdx + 4
		for endIdx < len(lower) {
			if isWordBoundary(lower, endIdx-1) && strings.HasPrefix(lower[endIdx:], "case") && isWordBoundary(lower, endIdx+4) {
				depth++
				endIdx += 4
				continue
			}
			if isWordBoundary(lower, endIdx-1) && strings.HasPrefix(lower[endIdx:], "end") && isWordBoundary(lower, endIdx+3) {
				depth--
				if depth == 0 {
					break
				}
				endIdx += 3
				continue
			}
			endIdx++
		}
		if endIdx >= len(lower) {
			// END не найден (парсер мог отрезать его в другой фрагмент) —
			// используем остаток текста как тело CASE
			endIdx = len(lower)
		}

		caseBody := queryText[caseIdx+4 : endIdx]
		caseBodyLower := lower[caseIdx+4 : endIdx]

		// Извлекаем WHEN-условия
		whenIdx := 0
		for {
			pos := findKeywordPosition(caseBodyLower[whenIdx:], "when")
			if pos < 0 {
				break
			}
			pos += whenIdx

			// Находим THEN на top-level
			thenIdx := pos + 4
			depth2 := 0
			for thenIdx < len(caseBodyLower) {
				ch := caseBodyLower[thenIdx]
				if ch == '(' {
					depth2++
				} else if ch == ')' && depth2 > 0 {
					depth2--
				}
				if depth2 == 0 && isWordBoundary(caseBodyLower, thenIdx-1) && strings.HasPrefix(caseBodyLower[thenIdx:], "then") && isWordBoundary(caseBodyLower, thenIdx+4) {
					break
				}
				thenIdx++
			}
			if thenIdx >= len(caseBodyLower) {
				break
			}

			whenCond := caseBody[pos+4 : thenIdx]
			result = append(result, strings.TrimSpace(whenCond))
			whenIdx = thenIdx + 4
		}

		searchFrom = endIdx + 3
		if searchFrom >= len(lower) {
			break
		}
	}

	return result
}

// extractParenContent извлекает содержимое скобок начиная с позиции openIdx.
// Возвращает содержимое без внешних скобок и индекс закрывающей скобки.
func extractParenContent(text string, openIdx int) (string, int) {
	if openIdx >= len(text) || text[openIdx] != '(' {
		return "", openIdx
	}
	depth := 0
	for i := openIdx; i < len(text); i++ {
		switch text[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return text[openIdx+1 : i], i + 1
			}
		}
	}
	return "", len(text)
}

// extractInsertSelectExpr извлекает SELECT-часть из INSERT...SELECT.
func extractInsertSelectExpr(queryText string) string {
	selectIdx := findTopLevelKeywordPosition(queryText, "select")
	if selectIdx < 0 {
		return ""
	}
	// Ищем FROM после SELECT
	rest := queryText[selectIdx:]
	fromIdx := findTopLevelKeywordPosition(rest, "from")
	if fromIdx < 0 {
		return ""
	}
	return strings.TrimSpace(rest[:fromIdx])
}

// extractFirstSelectBeforeUnion извлекает первый SELECT до первого UNION.
func extractFirstSelectBeforeUnion(queryText string) (string, bool) {
	lower := strings.ToLower(queryText)
	if !strings.HasPrefix(strings.TrimSpace(lower), "select") {
		return "", false
	}
	unionIdx := findTopLevelKeywordPosition(queryText, "union")
	if unionIdx < 0 {
		return "", false
	}
	return strings.TrimSpace(queryText[:unionIdx]), true
}

// extractSelectColumnNames извлекает имена колонок из SELECT-части
// (между SELECT и FROM). Возвращает map нижнего регистра.
func extractSelectColumnNames(selectStmt string) map[string]struct{} {
	result := make(map[string]struct{})
	lower := strings.ToLower(selectStmt)
	if !strings.HasPrefix(strings.TrimSpace(lower), "select") {
		return result
	}
	fromIdx := findTopLevelKeywordPosition(selectStmt, "from")
	if fromIdx < 0 {
		return result
	}
	selectPart := strings.TrimSpace(selectStmt[len("select"):fromIdx])
	if selectPart == "" {
		return result
	}
	parts := splitTopLevelCSV(selectPart)
	for _, part := range parts {
		name := extractColumnAliasName(strings.TrimSpace(part))
		if name != "" {
			result[strings.ToLower(name)] = struct{}{}
		}
	}
	return result
}

// extractColumnAliasName извлекает имя/алиас из выражения колонки SELECT.
// "col AS alias" → "alias", "col alias" → "alias", "col" → "col".
func extractColumnAliasName(expr string) string {
	// Убираем TOP N
	expr = regexp.MustCompile(`(?i)^\s*top\s+\d+\s+`).ReplaceAllString(expr, "")
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}
	// Проверяем "expr AS alias"
	asRe := regexp.MustCompile(`(?i)\bas\s+([a-z_][a-z0-9_]*)\s*$`)
	if m := asRe.FindStringSubmatch(expr); len(m) == 2 {
		return m[1]
	}
	// Проверяем "expr alias" (последний идентификатор без ключевого слова)
	// Разбиваем по пробелам, берём последний токен если он идентификатор
	tokens := strings.Fields(expr)
	if len(tokens) == 0 {
		return ""
	}
	// Если один токен — это просто имя колонки
	if len(tokens) == 1 {
		// Убираем квалификацию table.column → column
		clean := tokens[0]
		if dotIdx := strings.LastIndex(clean, "."); dotIdx >= 0 {
			clean = clean[dotIdx+1:]
		}
		// Проверяем что это идентификатор
		if regexp.MustCompile(`(?i)^[a-z_][a-z0-9_]*$`).MatchString(clean) {
			return clean
		}
		return ""
	}
	// Несколько токенов: последний может быть алиасом
	last := tokens[len(tokens)-1]
	// Предпоследний не должен быть ключевым словом (AS уже обработано выше)
	if regexp.MustCompile(`(?i)^[a-z_][a-z0-9_]*$`).MatchString(last) {
		// Проверяем что предпоследний токен не ключевое слово
		prev := strings.ToLower(tokens[len(tokens)-2])
		keywords := map[string]bool{
			"from": true, "where": true, "and": true, "or": true,
			"not": true, "is": true, "in": true, "like": true,
			"between": true, "join": true, "on": true, "as": true,
			"case": true, "when": true, "then": true, "else": true,
			"end": true, "null": true, "select": true,
		}
		if !keywords[prev] {
			return last
		}
	}
	// Имя колонки из первого токена
	clean := tokens[0]
	if dotIdx := strings.LastIndex(clean, "."); dotIdx >= 0 {
		clean = clean[dotIdx+1:]
	}
	if regexp.MustCompile(`(?i)^[a-z_][a-z0-9_]*$`).MatchString(clean) {
		return clean
	}
	return ""
}

// extractOrderByColumns извлекает список колонок из ORDER BY.
func extractOrderByColumns(queryText string) []string {
	orderIdx := findTopLevelKeywordPosition(queryText, "order")
	if orderIdx < 0 {
		return nil
	}
	rest := queryText[orderIdx+5:]
	restLower := strings.ToLower(rest)
	restTrimmed := strings.TrimSpace(restLower)
	if !strings.HasPrefix(restTrimmed, "by") {
		return nil
	}
	byEnd := orderIdx + 5 + strings.Index(restLower, "by") + 2
	// ORDER BY может быть последним — берём до конца или до следующего top-level keyword
	remaining := queryText[byEnd:]
	// Ищем конец ORDER BY (следующий top-level keyword или конец текста)
	endKeywords := []string{"for", "compute", "option", "union", "except", "intersect"}
	endIdx := len(remaining)
	for _, kw := range endKeywords {
		kwIdx := findTopLevelKeywordPosition(remaining, kw)
		if kwIdx >= 0 && kwIdx < endIdx {
			endIdx = kwIdx
		}
	}
	orderByPart := strings.TrimSpace(remaining[:endIdx])
	if orderByPart == "" {
		return nil
	}
	// Обрезаем макросы M_FORCEORDER и подобные в конце
	orderByLower := strings.ToLower(orderByPart)
	for _, macro := range forceOrderMacros {
		macroLower := strings.ToLower(macro)
		if strings.HasSuffix(orderByLower, macroLower) {
			orderByPart = strings.TrimSpace(orderByPart[:len(orderByPart)-len(macro)])
			break
		}
	}
	// Разделяем по запятым на top-level
	cols := splitTopLevelCSV(orderByPart)
	result := make([]string, 0, len(cols))
	for _, col := range cols {
		colTrimmed := strings.TrimSpace(col)
		// Убираем ASC/DESC
		colTrimmed = regexp.MustCompile(`(?i)\s+(asc|desc)\s*$`).ReplaceAllString(colTrimmed, "")
		colTrimmed = strings.TrimSpace(colTrimmed)
		if colTrimmed != "" {
			result = append(result, colTrimmed)
		}
	}
	return result
}
