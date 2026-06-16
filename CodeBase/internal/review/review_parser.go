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

	lower := strings.ToLower(remainder)
	fromPos := findTopLevelKeywordPosition(lower, "from")
	wherePos := findTopLevelKeywordPosition(lower, "where")

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

func findTopLevelKeywordPosition(lowerText string, keyword string) int {
	parenDepth := 0
	caseDepth := 0
	for i := 0; i < len(lowerText); i++ {
		ch := lowerText[i]
		if ch == '(' {
			parenDepth++
		} else if ch == ')' && parenDepth > 0 {
			parenDepth--
		}
		if isWordBoundary(lowerText, i-1) && strings.HasPrefix(lowerText[i:], "case") && isWordBoundary(lowerText, i+4) {
			caseDepth++
		}
		if isWordBoundary(lowerText, i-1) && strings.HasPrefix(lowerText[i:], "end") && isWordBoundary(lowerText, i+3) && caseDepth > 0 {
			caseDepth--
		}
		if parenDepth == 0 && caseDepth == 0 && strings.HasPrefix(lowerText[i:], keyword) && isWordBoundary(lowerText, i-1) && isWordBoundary(lowerText, i+len(keyword)) {
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

	fromPos := findTopLevelKeywordPosition(lower, "from")
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

	fromPos := findTopLevelKeywordPosition(lower, "from")
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

// hasStatementEnded проверяет, закончился ли SQL оператор
// stmtBuffer передаётся для контекста: если новый DML начинается, разрываем только если предыдущий уже "полный"
func hasStatementEnded(lower string, stmtBuffer []string) bool {
	// Используем regex с границами слова, чтобы избежать ложных срабатываний на подстроках
	// Например, "dependantinfo" содержит "end", но это не ключевое слово
	// UNION не разрывает оператор - он часть составного оператора
	re := regexp.MustCompile(`(?i)([;]|\b(?:go|begin|if|while|declare|exec|execute|return)\b)`)
	if re.MatchString(lower) {
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

	// UNION - не разрываем, это часть составного оператора
	if strings.Contains(currentLower, "union") {
		return false
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
