package review

import (
	"regexp"
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

	keywords := []string{"open", "fetch", "__fetch_next__", "close", "deallocate", "declare", "__declare_cursor__", "if", "while", "return", "begin", "end", "go"}
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

// removeMacros удаляет определения макросов #define и их тело
// Удаляет строку с #define, все продолжения (заканчивающиеся на \) и финальную строку
func removeMacros(content string) string {
	// Нормализуем окончания строк для Windows \r\n
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")

	lines := strings.Split(content, "\n")
	var result []string

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)

		// Начало макроса
		if strings.HasPrefix(trimmed, "#define") {
			// Пропускаем строки продолжения (оканчивающиеся на \)
			for i < len(lines)-1 && strings.HasSuffix(lines[i], "\\") {
				i++
			}
			// Пропускаем финальную строку (не оканчивается на \)
			continue
		}

		result = append(result, line)
	}

	return strings.Join(result, "\n")
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
