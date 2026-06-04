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
	re := regexp.MustCompile(`(?i)([;]|\b(?:go|begin|end|if|while|declare|exec|execute|return)\b)`)
	if re.MatchString(lower) {
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
