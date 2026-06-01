package review

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
)

type procedureRef struct {
	Name string
	Line int
}

var nonProcedureCallKeywords = map[string]struct{}{
	"on": {},
}

type columnRef struct {
	Table  string
	Column string
}

// selectAllRe находит SELECT * (с любыми пробелами между SELECT и *)
var selectAllRe = regexp.MustCompile(`(?i)\bselect\s+\*`)

// truncateTblRe находит TRUNCATE TABLE и имя таблицы (включая схему dbo.table)
var truncateTblRe = regexp.MustCompile(`(?i)\btruncate\s+table\s+(\S+)`)

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

func (r *Runner) checkForeignTables(parsed *sqlparser.ParseResult, file *indexedFile, prefix string) ([]Finding, error) {
	tables := dedupeTableRefs(parsed.Tables, prefix)
	findings := make([]Finding, 0)
	for _, table := range tables {
		if strings.EqualFold(prefix, "t") && isSharedTTable(table.Name) {
			continue
		}
		targetProductID, err := r.lookupTableProductID(table.Name)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, err
		}
		if targetProductID == 0 || targetProductID == file.DsProductID {
			continue
		}
		rule := RuleForeignTablesUsing
		if strings.EqualFold(prefix, "p") {
			rule = RuleForeignPTablesUsing
		}
		findings = append(findings, Finding{
			Rule:             rule,
			Severity:         SeverityFineCode,
			Message:          "Использование таблицы чужого продукта",
			File:             file.Path,
			Line:             table.Line,
			Object:           table.Name,
			CurrentProductID: file.DsProductID,
			TargetProductID:  targetProductID,
		})
	}
	return findings, nil
}

func (r *Runner) checkIndexWrong(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	inStatement := false
	stmtStartLine := 0
	stmtBuffer := make([]string, 0)
	parenDepth := 0
	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		if !inStatement {
			_, startIdx := findStatementStartHint(lower)
			if startIdx >= 0 && !isInComment(line, startIdx) {
				depthBefore := 0
				for j := 0; j < startIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParens(line)

		if hasStatementEnded(lower) {
			items, err := r.analyzeStatementForIndexWrong(stmtBuffer, stmtStartLine, file)
			if err != nil {
				return nil, err
			}
			findings = append(findings, items...)
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0
		}
	}

	if inStatement && len(stmtBuffer) > 0 {
		items, err := r.analyzeStatementForIndexWrong(stmtBuffer, stmtStartLine, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}

	return findings, nil
}

func (r *Runner) analyzeStatementForIndexWrong(lines []string, startLine int, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	if len(lines) == 0 {
		return findings, nil
	}

	fullText := strings.Join(lines, " ")
	trimmedText := normalizeHintStatementText(strings.TrimSpace(fullText))
	tables := extractTablesFromFromClause(trimmedText)
	if len(tables) == 0 {
		return findings, nil
	}

	conditionColumns := extractConditionColumnsForIndexWrong(trimmedText, tables)
	seen := make(map[string]struct{})

	for _, table := range tables {
		if shouldSkipTableCheck(table.TableName) {
			continue
		}
		if table.Hint == "" || table.IndexName == "" {
			continue
		}

		tableName := normalizeIdentifier(table.TableName)
		indexName := normalizeIdentifier(table.IndexName)
		if tableName == "" || indexName == "" {
			continue
		}

		key := tableName + "|" + indexName
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}

		cols := conditionColumns[tableConditionKey(table)]
		if len(cols) == 0 {
			continue
		}

		candidates, err := r.lookupTableIndexCandidates(tableName)
		if err != nil {
			return nil, err
		}
		if len(candidates) == 0 {
			continue
		}

		chosenFound := false
		chosenScore := 0
		bestScore := 0
		bestNames := make([]string, 0)

		for _, candidate := range candidates {
			score := calculateIndexPrefixMatch(candidate.Fields, cols)
			if score > bestScore {
				bestScore = score
				bestNames = []string{candidate.Name}
			} else if score == bestScore && score > 0 {
				bestNames = append(bestNames, candidate.Name)
			}

			if strings.EqualFold(normalizeIdentifier(candidate.Name), indexName) {
				chosenFound = true
				// Шаг 1: chosenScore как максимум по всем кандидатам с тем же именем
				if score > chosenScore {
					chosenScore = score
				}
			}
		}

		if !chosenFound || bestScore <= chosenScore {
			continue
		}

		bestNames = normalizeIndexNameList(bestNames)

		// Шаг 2: проверяем, что среди bestNames есть индекс, отличный от выбранного
		// (не просто другая версия/регистр того же индекса)
		hasDifferentIndex := false
		for _, name := range bestNames {
			if !strings.EqualFold(normalizeIdentifier(name), indexName) {
				hasDifferentIndex = true
				break
			}
		}
		if !hasDifferentIndex {
			continue
		}
		findings = append(findings, Finding{
			Rule:             RuleIndexWrong,
			Severity:         SeverityDeployStopper,
			Message:          fmt.Sprintf("Для таблицы %s указан индекс %s, но по условиям лучше подходит %s", tableName, indexName, strings.Join(bestNames, ", ")),
			File:             file.Path,
			Line:             startLine,
			Object:           fmt.Sprintf("%s.%s", tableName, indexName),
			CurrentProductID: file.DsProductID,
		})
	}

	return findings, nil
}

func calculateIndexPrefixMatch(indexFields []string, columns map[string]struct{}) int {
	if len(indexFields) == 0 || len(columns) == 0 {
		return 0
	}

	matched := 0
	for _, field := range indexFields {
		normalized := normalizeIdentifier(field)
		if normalized == "" {
			break
		}
		if _, exists := columns[normalized]; !exists {
			break
		}
		matched++
	}

	return matched
}

func normalizeIndexNameList(names []string) []string {
	set := make(map[string]struct{})
	result := make([]string, 0, len(names))
	for _, name := range names {
		normalized := strings.TrimSpace(name)
		if normalized == "" {
			continue
		}
		key := strings.ToLower(normalized)
		if _, exists := set[key]; exists {
			continue
		}
		set[key] = struct{}{}
		result = append(result, normalized)
	}
	sort.Slice(result, func(i, j int) bool {
		return strings.ToLower(result[i]) < strings.ToLower(result[j])
	})
	return result
}

func tableConditionKey(table tableFromClause) string {
	if alias := normalizeIdentifier(table.Alias); alias != "" {
		return alias
	}
	return normalizeIdentifier(table.TableName)
}

func extractConditionColumnsForIndexWrong(fullText string, tables []tableFromClause) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{})
	for _, table := range tables {
		result[tableConditionKey(table)] = make(map[string]struct{})
	}

	wherePart := extractWherePartForIndexWrong(fullText)
	if strings.TrimSpace(wherePart) != "" {
		mergeTableColumns(result, collectColumnsFromConditionExpression(wherePart, tables))
	}

	onParts := extractOnPartsForIndexWrong(fullText)
	for _, onPart := range onParts {
		mergeTableColumns(result, collectColumnsFromConditionExpression(onPart, tables))
	}

	return result
}

func extractWherePartForIndexWrong(fullText string) string {
	lower := strings.ToLower(fullText)
	whereIdx := strings.Index(lower, " where ")
	if whereIdx == -1 {
		return ""
	}

	part := fullText[whereIdx+7:]
	lowerPart := strings.ToLower(part)
	endMarkers := []string{" order by ", " group by ", " having ", " union ", " except ", " intersect "}
	endIdx := len(part)
	for _, marker := range endMarkers {
		if idx := strings.Index(lowerPart, marker); idx > 0 && idx < endIdx {
			endIdx = idx
		}
	}

	return part[:endIdx]
}

func extractOnPartsForIndexWrong(fullText string) []string {
	result := make([]string, 0)
	onRe := regexp.MustCompile(`(?i)\s+on\s+`)
	parts := onRe.Split(fullText, -1)
	if len(parts) <= 1 {
		return result
	}

	for _, part := range parts[1:] {
		lowerPart := strings.ToLower(part)
		endMarkers := []string{" join ", " where ", " order by ", " group by ", " having "}
		endIdx := len(part)
		for _, marker := range endMarkers {
			if idx := strings.Index(lowerPart, marker); idx > 0 && idx < endIdx {
				endIdx = idx
			}
		}
		result = append(result, part[:endIdx])
	}

	return result
}

func collectColumnsFromConditionExpression(expr string, tables []tableFromClause) map[string]map[string]struct{} {
	orRe := regexp.MustCompile(`(?i)\s+or\s+`)
	branches := orRe.Split(expr, -1)
	if len(branches) == 0 {
		return map[string]map[string]struct{}{}
	}

	if len(branches) == 1 {
		return collectColumnsFromConditionBranch(branches[0], tables)
	}

	intersection := collectColumnsFromConditionBranch(branches[0], tables)
	for _, branch := range branches[1:] {
		branchCols := collectColumnsFromConditionBranch(branch, tables)
		for tableKey, cols := range intersection {
			intersection[tableKey] = intersectColumns(cols, branchCols[tableKey])
		}
	}

	return intersection
}

func collectColumnsFromConditionBranch(branch string, tables []tableFromClause) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{})

	for _, table := range tables {
		tableKey := tableConditionKey(table)
		if _, exists := result[tableKey]; !exists {
			result[tableKey] = make(map[string]struct{})
		}

		identifiers := []string{normalizeIdentifier(table.TableName), normalizeIdentifier(table.Alias)}
		for _, identifier := range identifiers {
			if identifier == "" {
				continue
			}
			re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(identifier) + `\.([a-zA-Z_][a-zA-Z0-9_]*)\b`)
			for _, match := range re.FindAllStringSubmatch(branch, -1) {
				if len(match) < 2 {
					continue
				}
				col := normalizeIdentifier(match[1])
				if col != "" {
					result[tableKey][col] = struct{}{}
				}
			}
		}
	}

	if len(tables) == 1 {
		tableKey := tableConditionKey(tables[0])
		for col := range extractUnqualifiedConditionColumns(branch) {
			result[tableKey][col] = struct{}{}
		}
	}

	return result
}

func extractUnqualifiedConditionColumns(expr string) map[string]struct{} {
	result := make(map[string]struct{})
	re := regexp.MustCompile(`(?i)\b([a-zA-Z_][a-zA-Z0-9_]*)\b\s*(=|<>|!=|>=|<=|>|<|\bin\b|\bbetween\b|\blike\b|\bis\b)`)
	matches := re.FindAllStringSubmatch(expr, -1)

	keywords := map[string]struct{}{
		"and": {}, "or": {}, "not": {}, "in": {}, "between": {}, "like": {}, "is": {}, "null": {},
		"select": {}, "from": {}, "where": {}, "join": {}, "on": {}, "case": {}, "when": {}, "then": {}, "else": {}, "end": {},
	}

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		col := normalizeIdentifier(match[1])
		if col == "" {
			continue
		}
		if _, isKeyword := keywords[col]; isKeyword {
			continue
		}
		result[col] = struct{}{}
	}

	return result
}

func mergeTableColumns(base map[string]map[string]struct{}, add map[string]map[string]struct{}) {
	for tableKey, cols := range add {
		target, exists := base[tableKey]
		if !exists {
			target = make(map[string]struct{})
			base[tableKey] = target
		}
		for col := range cols {
			target[col] = struct{}{}
		}
	}
}

func intersectColumns(left map[string]struct{}, right map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{})
	if len(left) == 0 || len(right) == 0 {
		return result
	}
	for item := range left {
		if _, exists := right[item]; exists {
			result[item] = struct{}{}
		}
	}
	return result
}

func (r *Runner) checkUpdateOnlyVar(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	inStatement := false
	stmtStartLine := 0
	stmtBuffer := make([]string, 0)
	parenDepth := 0
	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		if !inStatement {
			if idx := findKeywordPosition(lower, "update"); idx >= 0 && !isInComment(line, idx) {
				depthBefore := 0
				for j := 0; j < idx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParens(line)

		if hasStatementEnded(lower) {
			finding := analyzeStatementForUpdateOnlyVar(stmtBuffer, stmtStartLine, file)
			if finding != nil {
				findings = append(findings, *finding)
			}
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0
		}
	}

	if inStatement && len(stmtBuffer) > 0 {
		finding := analyzeStatementForUpdateOnlyVar(stmtBuffer, stmtStartLine, file)
		if finding != nil {
			findings = append(findings, *finding)
		}
	}

	return findings, nil
}

func analyzeStatementForUpdateOnlyVar(lines []string, startLine int, file *indexedFile) *Finding {
	if len(lines) == 0 {
		return nil
	}

	fullText := strings.Join(lines, " ")
	lower := strings.ToLower(fullText)

	// Проверяем, что это UPDATE
	if findKeywordPosition(lower, "update") == -1 {
		return nil
	}

	// Находим SET clause
	setIdx := findKeywordPosition(lower, "set")
	if setIdx == -1 {
		return nil
	}

	// Извлекаем SET-часть
	setPart := fullText[setIdx+3:]
	lowerSetPart := strings.ToLower(setPart)

	// Обрезаем до WHERE, FROM или другого ключевого слова
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

	// Разбиваем на присваивания с учетом вложенности
	assignments := splitTopLevelSetAssignments(setPart)
	if len(assignments) == 0 {
		return nil
	}

	hasFieldUpdate := false
	hasVariableUpdate := false

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}

		// Находим левую часть присваивания
		eqIdx := strings.Index(assignment, "=")
		if eqIdx == -1 {
			continue
		}

		leftPart := strings.TrimSpace(assignment[:eqIdx])
		leftPart = normalizeIdentifier(leftPart)

		if leftPart == "" {
			continue
		}

		// Проверяем, является ли левая часть переменной
		if strings.HasPrefix(leftPart, "@") {
			hasVariableUpdate = true
		} else {
			// Это поле таблицы (с алиасом или без)
			hasFieldUpdate = true
		}
	}

	// Если есть обновление полей - всё ок
	if hasFieldUpdate {
		return nil
	}

	// Если есть только обновления переменных - это ошибка
	if hasVariableUpdate {
		return &Finding{
			Rule:             RuleUpdateOnlyVar,
			Severity:         SeverityDeployStopper,
			Message:          "UPDATE содержит только присваивания переменным, без обновления полей таблицы",
			File:             file.Path,
			Line:             startLine,
			Object:           "",
			CurrentProductID: file.DsProductID,
		}
	}

	return nil
}

func splitTopLevelSetAssignments(setPart string) []string {
	result := make([]string, 0)
	if strings.TrimSpace(setPart) == "" {
		return result
	}

	depth := 0
	start := 0
	inCase := false

	for i := 0; i < len(setPart); i++ {
		ch := setPart[i]

		// Отслеживаем вложенность скобок
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}

		// Отслеживаем CASE ... END
		if ch == ' ' || ch == '\t' || i == 0 {
			nextWord := ""
			for j := i; j < len(setPart); j++ {
				if setPart[j] == ' ' || setPart[j] == '\t' || setPart[j] == '(' {
					break
				}
				nextWord += string(setPart[j])
			}
			nextLower := strings.ToLower(strings.TrimSpace(nextWord))
			if nextLower == "case" {
				inCase = true
			} else if nextLower == "end" && inCase {
				inCase = false
			}
		}

		// Запятая на верхнем уровне - разделитель присваиваний
		if ch == ',' && depth == 0 && !inCase {
			result = append(result, setPart[start:i])
			start = i + 1
		}
	}

	if start < len(setPart) {
		result = append(result, setPart[start:])
	}

	return result
}

func (r *Runner) checkPTableSpid(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	inStatement := false
	stmtStartLine := 0
	stmtBuffer := make([]string, 0)
	parenDepth := 0
	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		if !inStatement {
			stmtType, startIdx := findStatementStart(lower)
			if startIdx >= 0 && (stmtType == "select" || stmtType == "update" || stmtType == "delete" || stmtType == "merge") && !isInComment(line, startIdx) {
				depthBefore := 0
				for j := 0; j < startIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParens(line)

		if hasStatementEnded(lower) {
			items, err := r.analyzeStatementForPTableSpid(stmtBuffer, stmtStartLine, file)
			if err != nil {
				return nil, err
			}
			findings = append(findings, items...)
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0
		}
	}

	if inStatement && len(stmtBuffer) > 0 {
		items, err := r.analyzeStatementForPTableSpid(stmtBuffer, stmtStartLine, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}

	return findings, nil
}

func (r *Runner) analyzeStatementForPTableSpid(lines []string, startLine int, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	if len(lines) == 0 {
		return findings, nil
	}

	fullText := strings.Join(lines, " ")
	trimmedText := normalizeHintStatementText(strings.TrimSpace(fullText))

	tables := extractTablesFromFromClause(trimmedText)
	if len(tables) == 0 {
		return findings, nil
	}

	// Фильтруем только p-таблицы
	pTables := make([]tableFromClause, 0)
	for _, table := range tables {
		tableName := normalizeIdentifier(table.TableName)
		if tableName == "" || !strings.HasPrefix(tableName, "p") {
			continue
		}
		pTables = append(pTables, table)
	}

	if len(pTables) == 0 {
		return findings, nil
	}

	// Проверяем условия по SPID для каждой p-таблицы
	spidConditions := extractSpidConditions(trimmedText)

	for _, table := range pTables {
		tableName := normalizeIdentifier(table.TableName)
		tableKey := tableConditionKey(table)

		if _, hasSpidCondition := spidConditions[tableKey]; !hasSpidCondition {
			findings = append(findings, Finding{
				Rule:             RulePTableSpid,
				Severity:         SeverityDeployStopper,
				Message:          fmt.Sprintf("Для p-таблицы %s отсутствует условие по полю SPID", tableName),
				File:             file.Path,
				Line:             startLine,
				Object:           tableName,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

func extractSpidConditions(fullText string) map[string]struct{} {
	result := make(map[string]struct{})

	// Извлекаем таблицы для получения их ключей
	tables := extractTablesFromFromClause(fullText)

	// Ищем SPID в WHERE и ON условиях
	wherePart := extractWherePartForIndexWrong(fullText)
	onParts := extractOnPartsForIndexWrong(fullText)
	allParts := append([]string{wherePart}, onParts...)

	spidRe := regexp.MustCompile(`(?i)\b(spid)\b`)

	for _, part := range allParts {
		partLower := strings.ToLower(part)

		// Ищем все вхождения SPID
		for _, match := range spidRe.FindAllStringIndex(partLower, -1) {
			// Определяем контекст - проверяем, относится ли к какой-либо таблице
			contextStart := 0
			if match[0] > 20 {
				contextStart = match[0] - 20
			}
			context := part[contextStart:match[0]]
			contextLower := strings.ToLower(context)

			// Ищем ближайший алиас/имя таблицы перед SPID
			for _, table := range tables {
				tableKey := tableConditionKey(table)
				identifiers := []string{normalizeIdentifier(table.TableName), normalizeIdentifier(table.Alias)}

				for _, identifier := range identifiers {
					if identifier == "" {
						continue
					}
					pattern := `(?i)\b` + regexp.QuoteMeta(identifier) + `\s*\.\s*$`
					if matched, _ := regexp.MatchString(pattern, contextLower); matched {
						result[tableKey] = struct{}{}
						break
					}
				}
			}

			// Если это единственная таблица и SPID без префикса - тоже считаем условием
			if len(tables) == 1 && !strings.Contains(contextLower, ".") {
				result[tableConditionKey(tables[0])] = struct{}{}
			}
		}
	}

	return result
}

func (r *Runner) checkForceOrder2Tbl(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	// Удаляем макросы #define перед анализом
	contentStr := removeMacros(string(content))
	lines := strings.Split(contentStr, "\n")

	inStatement := false
	stmtStartLine := 0
	stmtBuffer := make([]string, 0)
	parenDepth := 0
	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		if !inStatement {
			stmtType, startIdx := findStatementStart(lower)
			if startIdx >= 0 && (stmtType == "select" || stmtType == "update" || stmtType == "delete" || stmtType == "merge") && !isInComment(line, startIdx) {
				depthBefore := 0
				for j := 0; j < startIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParens(line)

		if hasStatementEnded(lower) {
			finding := analyzeStatementForForceOrder2Tbl(stmtBuffer, stmtStartLine, file)
			if finding != nil {
				findings = append(findings, *finding)
			}
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0
		}
	}

	if inStatement && len(stmtBuffer) > 0 {
		finding := analyzeStatementForForceOrder2Tbl(stmtBuffer, stmtStartLine, file)
		if finding != nil {
			findings = append(findings, *finding)
		}
	}

	return findings, nil
}

var forceOrderMacros = []string{
	"M_FORCEORDER",
	"M_FORCEORDER_NOSPOOL",
	"M_FORCEORDER_FAST",
	"M_FORCEORDER_WO_LOOPJOIN",
}

func analyzeStatementForForceOrder2Tbl(lines []string, startLine int, file *indexedFile) *Finding {
	if len(lines) == 0 {
		return nil
	}

	fullText := strings.Join(lines, " ")
	trimmedText := normalizeHintStatementText(strings.TrimSpace(fullText))

	// Извлекаем таблицы из FROM clause
	tables := extractTablesFromFromClause(trimmedText)
	if len(tables) < 2 {
		return nil
	}

	// Проверяем наличие макроса M_FORCEORDER*
	lower := strings.ToLower(trimmedText)
	hasForceOrderMacro := false

	for _, macro := range forceOrderMacros {
		macroLower := strings.ToLower(macro)
		if strings.Contains(lower, macroLower) {
			hasForceOrderMacro = true
			break
		}
	}

	if hasForceOrderMacro {
		return nil
	}

	// Нет макроса при ≥2 таблицах - это ошибка
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, normalizeIdentifier(t.TableName))
	}

	return &Finding{
		Rule:             RuleForceOrder2Tbl,
		Severity:         SeverityDeployStopper,
		Message:          fmt.Sprintf("Запрос с %d таблицами (%s) требует макроса M_FORCEORDER (или M_FORCEORDER_NOSPOOL, M_FORCEORDER_FAST, M_FORCEORDER_WO_LOOPJOIN)", len(tables), strings.Join(tableNames, ", ")),
		File:             file.Path,
		Line:             startLine,
		Object:           strings.Join(tableNames, ", "),
		CurrentProductID: file.DsProductID,
	}
}

func (r *Runner) checkSaveTran(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		// Ищем SAVE TRAN или SAVE TRANSACTION
		if hasSaveTran(lower) {
			findings = append(findings, Finding{
				Rule:             RuleSaveTran,
				Severity:         SeverityDeployStopper,
				Message:          "Обнаружена конструкция SAVE TRAN - использование точек сохранения запрещено",
				File:             file.Path,
				Line:             lineNum,
				Object:           "",
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

func hasSaveTran(lower string) bool {
	// Ищем "save tran" или "save transaction" как отдельные слова
	patterns := []string{"save tran", "save transaction"}
	for _, pattern := range patterns {
		idx := strings.Index(lower, pattern)
		if idx == -1 {
			continue
		}

		// Проверяем, что это не часть другого слова
		endIdx := idx + len(pattern)

		// Проверка начала: перед "save" должен быть пробел или начало строки
		if idx > 0 {
			prevChar := lower[idx-1]
			if !isCharWordBoundary(prevChar) {
				continue
			}
		}

		// Проверка конца: после "tran" или "transaction" должен быть пробел или конец строки
		if endIdx < len(lower) {
			nextChar := lower[endIdx]
			if !isCharWordBoundary(nextChar) {
				continue
			}
		}

		return true
	}
	return false
}

func isCharWordBoundary(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == ';' || ch == '(' || ch == ')' || ch == ',' || ch == '\x00'
}

func (r *Runner) checkUseDrop(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	inProcBody := false
	inBlockComment := false
	parenDepth := 0

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		// Ищем начало тела процедуры
		if !inProcBody {
			if isCreateProcStart(lower) {
				inProcBody = true
				parenDepth = countParens(line)
			}
			continue
		}

		// Мы внутри тела процедуры
		parenDepth += countParens(line)

		// Проверяем на DROP или DROP_CREATE
		if hasDropInLine(lower) {
			findings = append(findings, Finding{
				Rule:             RuleUseDrop,
				Severity:         SeverityDeployStopper,
				Message:          "Обнаружена конструкция DROP в теле процедуры — использование DROP внутри процедур запрещено",
				File:             file.Path,
				Line:             lineNum,
				Object:           "",
				CurrentProductID: file.DsProductID,
			})
		}

		// Проверяем конец тела процедуры (по GO или началу нового CREATE)
		if isProcBodyEnd(lower) {
			inProcBody = false
			parenDepth = 0
		}
	}

	return findings, nil
}

func isCreateProcStart(lower string) bool {
	// create proc или create procedure
	if strings.HasPrefix(lower, "create proc ") || strings.HasPrefix(lower, "create procedure ") {
		return true
	}
	// с ведущими пробелами
	trimmed := strings.TrimSpace(lower)
	if strings.HasPrefix(trimmed, "create proc ") || strings.HasPrefix(trimmed, "create procedure ") {
		return true
	}
	return false
}

func hasDropInLine(lower string) bool {
	// Ищем "drop" как отдельное слово
	if hasWord(lower, "drop") {
		return true
	}
	// Ищем "drop_create" макрос
	if strings.Contains(lower, "drop_create") {
		return true
	}
	return false
}

func hasWord(text, word string) bool {
	idx := strings.Index(text, word)
	if idx == -1 {
		return false
	}

	endIdx := idx + len(word)

	// Проверка начала
	if idx > 0 {
		prevChar := text[idx-1]
		if !isCharWordBoundary(prevChar) {
			return false
		}
	}

	// Проверка конца
	if endIdx < len(text) {
		nextChar := text[endIdx]
		if !isCharWordBoundary(nextChar) {
			return false
		}
	}

	return true
}

func isProcBodyEnd(lower string) bool {
	// Конец по GO
	trimmed := strings.TrimSpace(lower)
	if trimmed == "go" || strings.HasPrefix(trimmed, "go ") || strings.HasPrefix(trimmed, "go\t") {
		return true
	}
	// Конец по началу нового CREATE (вне тела)
	if isCreateProcStart(lower) {
		return true
	}
	return false
}

func (r *Runner) checkMathOperations(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		// Проверяем BETWEEN ... AND
		if hasBetweenWithMathOp(lower) {
			findings = append(findings, Finding{
				Rule:             RuleMathOperations,
				Severity:         SeverityDeployStopper,
				Message:          "Обнаружена математическая операция в условии BETWEEN — риск переполнения типа при расширении результата",
				File:             file.Path,
				Line:             lineNum,
				Object:           "",
				CurrentProductID: file.DsProductID,
			})
			continue
		}

		// Проверяем сравнения с математическими операциями
		if hasComparisonWithMathOp(lower) {
			findings = append(findings, Finding{
				Rule:             RuleMathOperations,
				Severity:         SeverityDeployStopper,
				Message:          "Обнаружена математическая операция в условии сравнения — риск переполнения типа при расширении результата",
				File:             file.Path,
				Line:             lineNum,
				Object:           "",
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

func hasBetweenWithMathOp(lower string) bool {
	// Ищем pattern: between @var and [number*@var|@var*number|@var+number|...]
	// BETWEEN может быть без ведущего пробела
	idx := strings.Index(lower, "between ")
	if idx == -1 {
		return false
	}

	// Проверяем что это не часть слова (перед between должен быть пробел или начало)
	if idx > 0 && lower[idx-1] != ' ' && lower[idx-1] != '\t' {
		return false
	}

	// Находим позицию AND
	startPos := idx + 8 // len("between ")
	andIdx := strings.Index(lower[startPos:], " and ")
	if andIdx == -1 {
		return false
	}
	andIdx += startPos

	// Проверяем выражение после AND
	afterAnd := strings.TrimSpace(lower[andIdx+5:])

	// Ищем матоперации: * / + -
	return hasMathOperator(afterAnd)
}

func hasComparisonWithMathOp(lower string) bool {
	// Операторы сравнения: >, <, >=, <=, <>, !=
	// Проверяем если с одной стороны @var, с другой — выражение с * / + -

	// Найдем все позиции операторов сравнения
	ops := []string{">=", "<=", "<>", "!=", ">", "<"}

	for _, op := range ops {
		opIdx := strings.Index(lower, op)
		if opIdx == -1 {
			continue
		}

		left := strings.TrimSpace(lower[:opIdx])
		right := strings.TrimSpace(lower[opIdx+len(op):])

		// Если с одной стороны @var, а с другой выражение с матоперацией
		leftIsVar := strings.HasPrefix(left, "@")
		rightIsVar := strings.HasPrefix(right, "@")
		leftHasMath := hasMathOperator(left)
		rightHasMath := hasMathOperator(right)

		if (leftIsVar && rightHasMath) || (rightIsVar && leftHasMath) {
			return true
		}
	}

	return false
}

func hasMathOperator(expr string) bool {
	// Ищем * / + - между операндами (с учетом пробелов)
	// Исключаем операторы внутри convert() или cast()
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch != '*' && ch != '/' && ch != '+' && ch != '-' {
			continue
		}

		// Проверяем, не находится ли оператор внутри convert(...)
		if isInsideConvert(expr, i) {
			continue
		}

		// Ищем операнд слева (пропуская пробелы)
		hasLeftOperand := false
		for j := i - 1; j >= 0; j-- {
			if expr[j] == ' ' || expr[j] == '\t' {
				continue
			}
			hasLeftOperand = isOperandChar(expr[j])
			break
		}

		// Ищем операнд справа (пропуская пробелы)
		hasRightOperand := false
		for j := i + 1; j < len(expr); j++ {
			if expr[j] == ' ' || expr[j] == '\t' {
				continue
			}
			hasRightOperand = isOperandChar(expr[j])
			break
		}

		// Оператор должен иметь хотя бы один операнд с одной стороны
		if hasLeftOperand || hasRightOperand {
			return true
		}
	}
	return false
}

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

func isOperandChar(ch byte) bool {
	// Операнд может начинаться с цифры, буквы или @
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == '@'
}

func (r *Runner) checkExistsWithAndInIf(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		// Ищем IF с условием, содержащим AND
		if hasIfWithAndAndQuery(lower) {
			findings = append(findings, Finding{
				Rule:             RuleExistsWithAndInIf,
				Severity:         SeverityDeployStopper,
				Message:          "Обнаружено условие IF с запросом к таблицам и AND — запрос может выполняться даже при ложном условии. Используйте вложенный IF",
				File:             file.Path,
				Line:             lineNum,
				Object:           "",
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

func hasIfWithAndAndQuery(lower string) bool {
	// Ищем IF с условием, содержащим AND и запрос к таблицам
	ifIdx := strings.Index(lower, "if ")
	if ifIdx == -1 {
		return false
	}

	// Проверяем, что перед IF нет букв (чтобы не ловить части слов)
	if ifIdx > 0 && isOperandChar(lower[ifIdx-1]) {
		return false
	}

	// Извлекаем условие после IF
	conditionStart := ifIdx + 3
	condition := lower[conditionStart:]

	// Ищем AND в условии
	if !strings.Contains(condition, " and ") {
		return false
	}

	// Проверяем наличие запроса к таблицам в условии
	return hasTableQuery(condition)
}

func hasTableQuery(condition string) bool {
	// Проверяем на наличие EXISTS(SELECT...)
	if strings.Contains(condition, "exists(") || strings.Contains(condition, "exists (") {
		return true
	}

	// Проверяем на IN (SELECT...)
	if strings.Contains(condition, "in (") || strings.Contains(condition, "in(") {
		// Проверяем, что внутри SELECT
		inIdx := strings.Index(condition, "in (")
		if inIdx == -1 {
			inIdx = strings.Index(condition, "in(")
		}
		if inIdx >= 0 {
			afterIn := condition[inIdx+3:]
			if strings.Contains(afterIn, "select ") {
				return true
			}
		}
	}

	// Проверяем на скалярный подзапрос (SELECT ...)
	if strings.Contains(condition, "(select ") {
		return true
	}

	return false
}

func (r *Runner) checkIndexExistsInDB(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	// Удаляем макросы #define перед анализом
	contentStr := removeMacros(string(content))
	lines := strings.Split(contentStr, "\n")

	inStatement := false
	stmtStartLine := 0
	stmtBuffer := make([]string, 0)
	parenDepth := 0
	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		if !inStatement {
			_, startIdx := findStatementStartHint(lower)
			if startIdx >= 0 && !isInComment(line, startIdx) {
				depthBefore := 0
				for j := 0; j < startIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParens(line)

		if hasStatementEnded(lower) {
			items, err := r.analyzeStatementForIndexExists(stmtBuffer, stmtStartLine, file)
			if err != nil {
				return nil, err
			}
			findings = append(findings, items...)
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0
		}
	}

	if inStatement && len(stmtBuffer) > 0 {
		items, err := r.analyzeStatementForIndexExists(stmtBuffer, stmtStartLine, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}

	return findings, nil
}

func (r *Runner) analyzeStatementForIndexExists(lines []string, startLine int, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	if len(lines) == 0 {
		return findings, nil
	}

	fullText := strings.Join(lines, " ")
	trimmedText := normalizeHintStatementText(strings.TrimSpace(fullText))
	tables := extractTablesFromFromClause(trimmedText)

	seen := make(map[string]struct{})
	for _, table := range tables {
		if shouldSkipTableCheck(table.TableName) {
			continue
		}
		if table.Hint == "" || table.IndexName == "" {
			continue
		}

		tableName := normalizeIdentifier(table.TableName)
		indexName := normalizeIdentifier(table.IndexName)
		if tableName == "" || indexName == "" {
			continue
		}

		key := tableName + "|" + indexName
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}

		exists, err := r.lookupIndexExists(tableName, indexName)
		if err != nil {
			return nil, err
		}
		if exists {
			continue
		}

		findings = append(findings, Finding{
			Rule:             RuleIndexExistsInDB,
			Severity:         SeverityDeployStopper,
			Message:          fmt.Sprintf("Для таблицы %s не найден индекс %s, указанный в %s", tableName, indexName, table.Hint),
			File:             file.Path,
			Line:             startLine,
			Object:           fmt.Sprintf("%s.%s", tableName, indexName),
			CurrentProductID: file.DsProductID,
		})
	}

	return findings, nil
}

func (r *Runner) checkForeignPTables(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	tables := dedupeTableRefs(parsed.Tables, "p")
	if len(tables) == 0 {
		return []Finding{}, nil
	}
	apiNames, err := r.findAPITableNames(tableNames(tables))
	if err != nil {
		return nil, err
	}
	filtered := make([]tableRef, 0, len(tables))
	for _, table := range tables {
		if _, exists := apiNames[strings.ToLower(table.Name)]; exists {
			continue
		}
		filtered = append(filtered, table)
	}
	findings := make([]Finding, 0)
	for _, table := range filtered {
		targetProductID, err := r.lookupTableProductID(table.Name)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, err
		}
		if targetProductID == 0 || targetProductID == file.DsProductID {
			continue
		}
		findings = append(findings, Finding{
			Rule:             RuleForeignPTablesUsing,
			Severity:         SeverityFineCode,
			Message:          "Использование p-таблицы чужого продукта",
			File:             file.Path,
			Line:             table.Line,
			Object:           table.Name,
			CurrentProductID: file.DsProductID,
			TargetProductID:  targetProductID,
		})
	}
	return findings, nil
}

func (r *Runner) checkForeignProcedures(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	calls := dedupeProcedureCalls(parsed.Calls)
	findings := make([]Finding, 0)
	for _, call := range calls {
		targetProductID, err := r.lookupProcedureProductID(call.Name)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, err
		}
		if targetProductID == 0 || targetProductID == file.DsProductID {
			continue
		}
		findings = append(findings, Finding{
			Rule:             RuleForeignProcedureUsing,
			Severity:         SeverityFineCode,
			Message:          "Использование процедуры чужого продукта",
			File:             file.Path,
			Line:             call.Line,
			Object:           call.Name,
			CurrentProductID: file.DsProductID,
			TargetProductID:  targetProductID,
		})
	}
	return findings, nil
}

func dedupeTableRefs(tables []*model.SQLTable, prefix string) []tableRef {
	result := make([]tableRef, 0)
	seen := make(map[string]struct{})
	prefix = strings.ToLower(strings.TrimSpace(prefix))
	for _, table := range tables {
		if table == nil {
			continue
		}
		name := normalizeIdentifier(table.TableName)
		if name == "" {
			continue
		}
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		key := fmt.Sprintf("%s:%d", name, table.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, tableRef{Name: table.TableName, Line: table.LineNumber})
	}
	return result
}

func dedupeProcedureCalls(calls []*model.SQLProcedureCall) []procedureRef {
	result := make([]procedureRef, 0)
	seen := make(map[string]struct{})
	for _, call := range calls {
		if call == nil {
			continue
		}
		name := normalizeIdentifier(call.CalleeName)
		if name == "" {
			continue
		}
		if _, isKeyword := nonProcedureCallKeywords[name]; isKeyword {
			continue
		}
		key := fmt.Sprintf("%s:%d", name, call.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, procedureRef{Name: call.CalleeName, Line: call.LineNumber})
	}
	return result
}

func (r *Runner) checkExecNotExistsProcedures(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	calls := dedupeProcedureCalls(parsed.Calls)
	findings := make([]Finding, 0)
	for _, call := range calls {
		_, err := r.lookupProcedureProductID(call.Name)
		if err == nil {
			continue
		}
		if err != sql.ErrNoRows {
			return nil, err
		}

		findings = append(findings, Finding{
			Rule:             RuleExecNotExistsProc,
			Severity:         SeverityDeployStopper,
			Message:          "Вызов несуществующей процедуры",
			File:             file.Path,
			Line:             call.Line,
			Object:           call.Name,
			CurrentProductID: file.DsProductID,
		})
	}
	return findings, nil
}

func (r *Runner) checkProcDuplicate(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	for _, proc := range parsed.Procedures {
		if proc == nil || proc.ProcName == "" {
			continue
		}
		fileIDs, err := r.lookupProcedureCreateFiles(proc.ProcName)
		if err != nil {
			return nil, err
		}
		if len(fileIDs) > 1 {
			findings = append(findings, Finding{
				Rule:             RuleProcDuplicate,
				Severity:         SeverityDeployStopper,
				Message:          "Процедура создаётся в нескольких файлах",
				File:             file.Path,
				Line:             proc.LineStart,
				Object:           proc.ProcName,
				CurrentProductID: file.DsProductID,
			})
		}
	}
	return findings, nil
}

func (r *Runner) checkProcParamDefValue(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	for _, proc := range parsed.Procedures {
		if proc == nil || len(proc.Params) == 0 {
			continue
		}
		// Для каждого параметра с default=null или без default проверяем защиту в теле
		for _, param := range proc.Params {
			if !r.needsDefaultAssignment(param) {
				continue
			}
			if !r.hasDefaultAssignmentInBody(proc, param.Name) {
				findings = append(findings, Finding{
					Rule:             RuleProcParamDefValue,
					Severity:         SeverityDeployStopper,
					Message:          "Параметр без значения по умолчанию требует присвоения в начале процедуры",
					File:             file.Path,
					Line:             proc.LineStart,
					Object:           param.Name,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}
	return findings, nil
}

// needsDefaultAssignment проверяет, нужен ли параметру default в начале процедуры
func (r *Runner) needsDefaultAssignment(param model.SQLParam) bool {
	// Только параметры с default=null требуют присвоения в начале процедуры
	// Параметры без default (обязательные) - это нормально
	return param.DefaultValue == "null"
}

// hasDefaultAssignmentInBody проверяет, есть ли в теле процедуры присвоение default для параметра
func (r *Runner) hasDefaultAssignmentInBody(_ *model.SQLProcedure, _ string) bool {
	// Для Варианта 1 простой реализации - всегда возвращаем false
	// TODO: в будущем реализовать анализ тела процедуры (потребуются proc и paramName)
	return false
}

func (r *Runner) checkProcElseCase(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Читаем содержимое файла
	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	for i := 0; i < len(lines); i++ {
		lineNum := i + 1
		line := lines[i]

		// Ищем CASE в строке
		caseIdx := findCaseInLine(line)
		if caseIdx == -1 {
			continue
		}

		// Проверяем, что CASE не в комментарии
		if isInComment(line, caseIdx) {
			continue
		}

		// Ищем парный END с отслеживанием вложенности
		endLine, hasElse := findCaseEndAndElse(lines, i, caseIdx)
		if endLine == -1 {
			// Нет завершающего END - возможно синтаксическая ошибка
			continue
		}

		// Если нет ELSE - добавляем finding
		if !hasElse {
			findings = append(findings, Finding{
				Rule:             RuleProcElseCase,
				Severity:         SeverityDeployStopper,
				Message:          "Оператор CASE должен содержать ветку ELSE",
				File:             file.Path,
				Line:             lineNum,
				CurrentProductID: file.DsProductID,
			})
		}

		// Пропускаем обработанные строки (от CASE до END)
		i = endLine
	}

	return findings, nil
}

// findCaseInLine ищет позицию CASE в строке (не как часть слова)
func findCaseInLine(line string) int {
	lower := strings.ToLower(line)
	for i := 0; i < len(lower)-3; i++ {
		if lower[i:i+4] == "case" {
			// Проверяем, что это не часть слова
			if i > 0 && isWordChar(lower[i-1]) {
				continue
			}
			// Проверяем, что после идет пробел, скобка или конец строки
			if i+4 < len(lower) && isWordChar(lower[i+4]) {
				continue
			}
			return i
		}
	}
	return -1
}

// isWordChar проверяет, является ли символ буквой/цифрой/подчеркиванием
func isWordChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// isInComment проверяет, находится ли позиция внутри SQL комментария
func isInComment(line string, pos int) bool {
	lower := strings.ToLower(line)
	for i := 0; i < pos && i < len(lower); i++ {
		if i+1 < len(lower) && lower[i:i+2] == "--" {
			return true
		}
	}
	return false
}

// findCaseEndAndElse ищет парный END для CASE и проверяет наличие ELSE
// Возвращает: номер строки с END, найден ли ELSE
func findCaseEndAndElse(lines []string, startLine, casePos int) (int, bool) {
	caseDepth := 1 // Вложенность CASE (начинаем с 1 для текущего CASE)
	hasElse := false
	inComment := false

	for lineIdx := startLine; lineIdx < len(lines) && lineIdx < startLine+100; lineIdx++ {
		line := lines[lineIdx]
		lower := strings.ToLower(line)

		// Ограничиваем поиск для первой строки
		startPos := 0
		if lineIdx == startLine {
			startPos = casePos + 4 // начинаем после "case"
		}

		for i := startPos; i < len(lower); i++ {
			// Обрабатываем комментарии
			if i+1 < len(lower) && lower[i:i+2] == "--" {
				inComment = true
			}
			if inComment {
				continue
			}

			// Ищем ключевые слова
			if i+4 <= len(lower) && lower[i:i+4] == "case" {
				// Проверяем, что это полное слово "case"
				if (i == 0 || !isWordChar(lower[i-1])) && (i+4 == len(lower) || !isWordChar(lower[i+4])) {
					caseDepth++
				}
			} else if i+4 <= len(lower) && lower[i:i+4] == "else" {
				// Проверяем, что это полное слово "else"
				if (i == 0 || !isWordChar(lower[i-1])) && (i+4 == len(lower) || !isWordChar(lower[i+4])) {
					// ELSE на текущем уровне вложенности
					if caseDepth == 1 {
						hasElse = true
					}
				}
			} else if i+3 <= len(lower) && lower[i:i+3] == "end" {
				// Проверяем, что это полное слово "end"
				if (i == 0 || !isWordChar(lower[i-1])) && (i+3 == len(lower) || !isWordChar(lower[i+3])) {
					caseDepth--
					if caseDepth == 0 {
						// Нашли парный END
						return lineIdx, hasElse
					}
				}
			}
		}
	}

	// END не найден
	return -1, false
}

func (r *Runner) checkUseSelectAll(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Читаем содержимое файла
	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	// Удаляем макросы #define перед анализом
	contentStr := removeMacros(string(content))
	lines := strings.Split(contentStr, "\n")

	for i, line := range lines {
		lineNum := i + 1

		// Пропускаем комментарии (trim и проверяем --)
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Ищем SELECT *
		if selectAllRe.MatchString(line) {
			findings = append(findings, Finding{
				Rule:             RuleUseSelectAll,
				Severity:         SeverityDeployStopper,
				Message:          "Запрещено использование SELECT * в запросах",
				File:             file.Path,
				Line:             lineNum,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

func (r *Runner) checkTruncTbl(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Читаем содержимое файла
	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	for i, line := range lines {
		lineNum := i + 1

		// Пропускаем комментарии (trim и проверяем --)
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Ищем TRUNCATE TABLE и извлекаем имя таблицы
		matches := truncateTblRe.FindStringSubmatch(line)
		if matches != nil {
			tableName := strings.TrimRight(matches[1], ";,")
			findings = append(findings, Finding{
				Rule:             RuleTruncTbl,
				Severity:         SeverityDeployStopper,
				Message:          "Запрещено использование TRUNCATE TABLE",
				File:             file.Path,
				Line:             lineNum,
				Object:           tableName,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

func (r *Runner) checkDatatype(parsed *sqlparser.ParseResult, file *indexedFile) []Finding {
	findings := make([]Finding, 0)
	seen := map[string]string{}
	for _, definition := range parsed.ColumnDefinitions {
		tableName := normalizeIdentifier(definition.TableName)
		columnName := normalizeIdentifier(definition.ColumnName)
		dataType := normalizeDataType(definition.DataType)
		if tableName == "" || columnName == "" || dataType == "" || dataType == "dsunknown" {
			continue
		}
		key := tableName + "." + columnName
		if prev, exists := seen[key]; exists {
			if !areEquivalentTypes(prev, dataType) {
				findings = append(findings, Finding{
					Rule:             RuleDatatype,
					Severity:         SeverityFineCode,
					Message:          "Неэквивалентные типы данных для одной колонки",
					File:             file.Path,
					Line:             definition.LineNumber,
					Object:           definition.TableName + "." + definition.ColumnName,
					CurrentProductID: file.DsProductID,
				})
			}
			continue
		}
		seen[key] = dataType
	}

	insertSelectFindings, err := r.checkDatatypeInsertSelect(parsed, file)
	if err == nil {
		findings = append(findings, insertSelectFindings...)
	}

	updateSetFindings, err := r.checkDatatypeUpdateSet(parsed, file)
	if err == nil {
		findings = append(findings, updateSetFindings...)
	}

	return findings
}

func analyzeStatementForTableHintExists(lines []string, startLine int, file *indexedFile, stmtType string) []Finding {
	findings := make([]Finding, 0)
	if len(lines) == 0 || stmtType == "" {
		return findings
	}

	fullText := strings.Join(lines, " ")
	fullText = removeBlockComments(fullText)

	tables := extractTablesFromFromClause(fullText)
	if len(tables) == 0 {
		return findings
	}

	insertTarget := ""
	if stmtType == "insert" {
		insertTarget = normalizeIdentifier(parseInsertTableName(fullText))
	}

	for _, table := range tables {
		if shouldSkipTableCheck(table.TableName) {
			continue
		}

		if insertTarget != "" && normalizeIdentifier(table.TableName) == insertTarget {
			continue
		}

		if !isHintAllowed(table.Hint, allowedTableHints) {
			findings = append(findings, Finding{
				Rule:             RuleTableHintExists,
				Severity:         SeverityDeployStopper,
				Message:          fmt.Sprintf("Таблица %s не имеет допустимого хинта индекса", table.TableName),
				File:             file.Path,
				Line:             startLine,
				Object:           table.TableName,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings
}

func (r *Runner) checkDatatypeUpdateSet(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	seen := make(map[string]struct{})

	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		stmt, ok := parseUpdateSetStatement(fragment.QueryText)
		if !ok {
			continue
		}

		aliasMap := parseAliasMap(stmt.FromClause)
		if stmt.TargetAlias != "" {
			aliasMap[strings.ToLower(stmt.TargetAlias)] = stmt.TargetTable
		}

		for _, assignment := range stmt.Assignments {
			targetColumn := normalizeAssignmentTargetColumn(assignment.Target, stmt)
			if targetColumn == "" || assignment.Expression == "" {
				continue
			}

			targetType, err := r.db.FindLatestSQLColumnDefinitionType(stmt.TargetTable, targetColumn)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, err
			}

			sourceTypes := r.resolveExpressionTypes(assignment.Expression, aliasMap)
			for _, sourceType := range sourceTypes {
				if !isPotentialPrecisionLoss(sourceType, targetType) {
					continue
				}
				key := fmt.Sprintf("%d|%s|%s|%s", fragment.LineNumber, stmt.TargetTable, targetColumn, normalizeDataType(sourceType))
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				findings = append(findings, Finding{
					Rule:             RuleDatatype,
					Severity:         SeverityFineCode,
					Message:          fmt.Sprintf("Потеря точности типов данных: %s -> %s", sourceType, targetType),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           fmt.Sprintf("%s.%s", stmt.TargetTable, targetColumn),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
}

func (r *Runner) checkDatatypeInsertSelect(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	seen := make(map[string]struct{})

	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		stmt, ok := parseInsertSelectStatement(fragment.QueryText)
		if !ok {
			continue
		}

		aliasMap := parseAliasMap(stmt.FromClause)
		count := len(stmt.TargetColumns)
		if len(stmt.SelectExpressions) < count {
			count = len(stmt.SelectExpressions)
		}
		for i := 0; i < count; i++ {
			targetColumn := strings.TrimSpace(stmt.TargetColumns[i])
			expression := strings.TrimSpace(stmt.SelectExpressions[i])
			if targetColumn == "" || expression == "" {
				continue
			}

			targetType, err := r.db.FindLatestSQLColumnDefinitionType(stmt.TargetTable, targetColumn)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, err
			}

			sourceTypes := r.resolveExpressionTypes(expression, aliasMap)
			for _, sourceType := range sourceTypes {
				if !isPotentialPrecisionLoss(sourceType, targetType) {
					continue
				}
				key := fmt.Sprintf("%d|%s|%s|%s", fragment.LineNumber, stmt.TargetTable, targetColumn, normalizeDataType(sourceType))
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				findings = append(findings, Finding{
					Rule:             RuleDatatype,
					Severity:         SeverityFineCode,
					Message:          fmt.Sprintf("Потеря точности типов данных: %s -> %s", sourceType, targetType),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           fmt.Sprintf("%s.%s", stmt.TargetTable, targetColumn),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
}

func (r *Runner) resolveExpressionTypes(expression string, aliasMap map[string]string) []string {
	candidates := extractColumnRefsFromExpression(expression)
	result := make([]string, 0, len(candidates))
	seen := make(map[string]struct{})
	for _, ref := range candidates {
		tableName := ref.Table
		if mapped, exists := aliasMap[strings.ToLower(strings.TrimSpace(tableName))]; exists {
			tableName = mapped
		}
		if strings.TrimSpace(tableName) == "" || strings.TrimSpace(ref.Column) == "" {
			continue
		}
		typeName, err := r.db.FindLatestSQLColumnDefinitionType(tableName, ref.Column)
		if err != nil {
			continue
		}
		normalized := normalizeDataType(typeName)
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, typeName)
	}
	return result
}

func extractColumnRefsFromExpression(expression string) []columnRef {
	re := regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\b`)
	matches := re.FindAllStringSubmatch(expression, -1)
	result := make([]columnRef, 0, len(matches))
	seen := make(map[string]struct{})
	for _, m := range matches {
		if len(m) < 3 {
			continue
		}
		table := strings.TrimSpace(m[1])
		column := strings.TrimSpace(m[2])
		key := strings.ToLower(table + "." + column)
		if table == "" || column == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, columnRef{Table: table, Column: column})
	}
	return result
}

func isPotentialPrecisionLoss(sourceType string, targetType string) bool {
	source := normalizeDataType(sourceType)
	target := normalizeDataType(targetType)
	if source == "" || target == "" {
		return false
	}
	if source == target {
		return false
	}
	sourceRank := datetimePrecisionRank(source)
	targetRank := datetimePrecisionRank(target)
	if sourceRank > 0 && targetRank > 0 {
		return sourceRank > targetRank
	}
	return false
}

func datetimePrecisionRank(dataType string) int {
	v := normalizeDataType(dataType)
	switch {
	case strings.Contains(v, "datetime") && !strings.Contains(v, "smalldatetime"):
		return 3
	case strings.Contains(v, "smalldatetime"):
		return 2
	case strings.Contains(v, "date") || strings.HasPrefix(v, "dsoperday"):
		return 1
	default:
		return 0
	}
}

func (r *Runner) checkAnsiInJoin(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Читаем содержимое файла
	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	// Удаляем макросы #define перед анализом
	contentStr := removeMacros(string(content))
	lines := strings.Split(contentStr, "\n")

	// Состояние парсера
	inFromClause := false
	fromStartLine := 0
	parenDepth := 0
	hasJoin := false
	hasComma := false
	commaLine := 0

	for i, line := range lines {
		lineNum := i + 1

		// Пропускаем комментарии целиком
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		lower := strings.ToLower(line)

		// Ищем начало FROM clause
		if !inFromClause {
			fromIdx := findKeywordPosition(lower, "from")
			if fromIdx >= 0 && !isInComment(line, fromIdx) {
				// Проверяем, что это не подзапрос (проверяем глубину скобок до FROM)
				depthBefore := 0
				for j := 0; j < fromIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inFromClause = true
					fromStartLine = lineNum
					parenDepth = 0
					hasJoin = false
					hasComma = false
					// Считаем скобки в оставшейся части строки после FROM
					for j := fromIdx + 4; j < len(line); j++ {
						switch line[j] {
						case '(':
							parenDepth++
						case ')':
							parenDepth--
						}
					}
				}
			}
		}

		if !inFromClause {
			continue
		}

		// Мы внутри FROM clause - анализируем строку
		fromPart := lower
		if i == fromStartLine-1 {
			// Первая строка - берем только после FROM
			fromIdx := strings.Index(lower, "from")
			if fromIdx >= 0 {
				fromPart = lower[fromIdx+4:]
			}
		}

		// Проверяем наличие JOIN ключевых слов
		if strings.Contains(fromPart, " join ") ||
			strings.Contains(fromPart, "inner join") ||
			strings.Contains(fromPart, "left join") ||
			strings.Contains(fromPart, "right join") ||
			strings.Contains(fromPart, "full join") ||
			strings.Contains(fromPart, "cross join") {
			hasJoin = true
		}

		// Считаем скобки и ищем запятую
		for j, ch := range line {
			switch ch {
			case '(':
				parenDepth++
			case ')':
				parenDepth--
			case ',':
				if parenDepth == 0 && !isInComment(line, j) {
					hasComma = true
					commaLine = lineNum
				}
			}
		}

		// Проверяем конец FROM clause (WHERE, GROUP BY, HAVING, ORDER BY, UNION, EXCEPT, INTERSECT, ;)
		if hasFromClauseEnded(lower) {
			// Проверяем результат
			if hasComma && !hasJoin {
				findings = append(findings, Finding{
					Rule:             RuleAnsiInJoin,
					Severity:         SeverityDeployStopper,
					Message:          "Используйте ANSI-синтаксис для соединения таблиц (JOIN вместо запятой в FROM)",
					File:             file.Path,
					Line:             commaLine,
					CurrentProductID: file.DsProductID,
				})
			}
			inFromClause = false
			hasJoin = false
			hasComma = false
			commaLine = 0
		}
	}

	// Проверяем, если файл закончился во время FROM clause
	if inFromClause && hasComma && !hasJoin {
		findings = append(findings, Finding{
			Rule:             RuleAnsiInJoin,
			Severity:         SeverityDeployStopper,
			Message:          "Используйте ANSI-синтаксис для соединения таблиц (JOIN вместо запятой в FROM)",
			File:             file.Path,
			Line:             commaLine,
			CurrentProductID: file.DsProductID,
		})
	}

	return findings, nil
}

// findKeywordPosition ищет позицию ключевого слова (полное слово)
func findKeywordPosition(text, keyword string) int {
	lower := strings.ToLower(text)
	keyword = strings.ToLower(keyword)
	for i := 0; i <= len(lower)-len(keyword); i++ {
		if lower[i:i+len(keyword)] == keyword {
			// Проверяем границы слова
			if i > 0 && isWordChar(lower[i-1]) {
				continue
			}
			if i+len(keyword) < len(lower) && isWordChar(lower[i+len(keyword)]) {
				continue
			}
			return i
		}
	}
	return -1
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

func (r *Runner) checkInsertRowLock(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Читаем содержимое файла
	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	// Состояние парсера
	inInsert := false
	insertStartLine := 0
	insertBuffer := make([]string, 0)
	parenDepth := 0

	for i, line := range lines {
		lineNum := i + 1

		// Пропускаем комментарии целиком (но учитываем если мы внутри INSERT)
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		lower := strings.ToLower(line)

		// Ищем начало INSERT
		if !inInsert {
			insertIdx := findKeywordPosition(lower, "insert")
			if insertIdx >= 0 && !isInComment(line, insertIdx) {
				// Проверяем, что INSERT не внутри подзапроса
				if !isInsertInSubquery(line) {
					inInsert = true
					insertStartLine = lineNum
					insertBuffer = []string{line}
					// Считаем скобки в строке
					parenDepth = countParens(line)
				}
			}
		} else {
			// Мы внутри INSERT - добавляем строку в буфер
			insertBuffer = append(insertBuffer, line)
			parenDepth += countParens(line)

			// Проверяем конец INSERT (точка с запятой, начало нового оператора, или конец запроса)
			if strings.Contains(lower, ";") || isNewSQLStatement(line) {
				// Анализируем собранный INSERT
				if finding := analyzeInsertForRowLock(insertBuffer, insertStartLine, file); finding != nil {
					findings = append(findings, *finding)
				}
				inInsert = false
				insertBuffer = nil
				parenDepth = 0

				// Если начался новый оператор, обрабатываем текущую строку как начало нового цикла
				if isNewSQLStatement(line) && !strings.Contains(lower, "insert") {
					continue
				}
			}
		}
	}

	// Проверяем, если файл закончился во время INSERT
	if inInsert && len(insertBuffer) > 0 {
		if finding := analyzeInsertForRowLock(insertBuffer, insertStartLine, file); finding != nil {
			findings = append(findings, *finding)
		}
	}

	return findings, nil
}

// countParens считает баланс скобок в строке (открывающие - закрывающие)
func countParens(line string) int {
	depth := 0
	for _, ch := range line {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		}
	}
	return depth
}

// analyzeInsertForRowLock анализирует многострочный INSERT на наличие ROWLOCK
func analyzeInsertForRowLock(lines []string, startLine int, file *indexedFile) *Finding {
	if len(lines) == 0 {
		return nil
	}

	// Объединяем все строки для анализа
	fullText := strings.Join(lines, " ")
	lower := strings.ToLower(fullText)

	// Проверяем наличие M_WITH_ROWLOCK или WITH (ROWLOCK)
	if hasRowLock(lower) {
		return nil
	}

	// Парсим имя таблицы из первой строки
	tableName := parseInsertTableName(lines[0])
	if tableName == "" {
		// Пробуем найти имя таблицы во всем тексте
		tableName = parseInsertTableName(fullText)
	}
	if tableName == "" {
		return nil
	}

	return &Finding{
		Rule:             RuleInsertRowLock,
		Severity:         SeverityDeployStopper,
		Message:          "Для INSERT необходимо использовать M_WITH_ROWLOCK для предотвращения эскалации блокировок",
		File:             file.Path,
		Line:             startLine,
		Object:           tableName,
		CurrentProductID: file.DsProductID,
	}
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

// parseInsertTableName извлекает имя таблицы из INSERT оператора
func parseInsertTableName(line string) string {
	lower := strings.ToLower(line)

	// Находим INSERT
	insertIdx := strings.Index(lower, "insert")
	if insertIdx == -1 {
		return ""
	}

	// Пропускаем INSERT
	pos := insertIdx + 6

	// Пропускаем пробелы
	for pos < len(line) && (line[pos] == ' ' || line[pos] == '\t') {
		pos++
	}

	// Пропускаем INTO если есть
	if pos+4 <= len(lower) && lower[pos:pos+4] == "into" {
		pos += 4
		// Пропускаем пробелы
		for pos < len(line) && (line[pos] == ' ' || line[pos] == '\t') {
			pos++
		}
	}

	// Извлекаем имя таблицы
	if pos >= len(line) {
		return ""
	}

	start := pos
	for pos < len(line) && (line[pos] != ' ' && line[pos] != '\t' && line[pos] != '(' && line[pos] != ';') {
		pos++
	}

	if start < pos {
		return strings.TrimSpace(line[start:pos])
	}

	return ""
}

func (r *Runner) checkUseEqColumn(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	inCondition := false
	conditionStartLine := 0
	conditionBuffer := make([]string, 0)
	parenDepth := 0

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		lower := strings.ToLower(line)

		if !inCondition {
			condIdx := findConditionStart(lower)
			if condIdx >= 0 && !isInComment(line, condIdx) {
				depthBefore := 0
				for j := 0; j < condIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inCondition = true
					conditionStartLine = lineNum
					conditionBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
			continue
		}

		conditionBuffer = append(conditionBuffer, line)
		parenDepth += countParens(line)

		if hasConditionEnded(lower) {
			items := analyzeConditionForEqColumn(conditionBuffer, conditionStartLine, file)
			findings = append(findings, items...)
			inCondition = false
			conditionBuffer = nil
			parenDepth = 0

			condIdx := findConditionStart(lower)
			if condIdx >= 0 && !isInComment(line, condIdx) && !hasConditionEnded(lower) {
				inCondition = true
				conditionStartLine = lineNum
				conditionBuffer = []string{line}
				parenDepth = countParens(line)
			}
		}
	}

	if inCondition && len(conditionBuffer) > 0 {
		items := analyzeConditionForEqColumn(conditionBuffer, conditionStartLine, file)
		findings = append(findings, items...)
	}

	return findings, nil
}

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

func hasConditionEnded(lower string) bool {
	kws := []string{"group by", "order by", "union", "except", "intersect", ";"}
	for _, kw := range kws {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// containsBitwiseOperator проверяет наличие битовых операторов (&, |, ^) в выражении
func containsBitwiseOperator(expr string) bool {
	return strings.Contains(expr, "&") ||
		strings.Contains(expr, "|") ||
		strings.Contains(expr, "^")
}

func analyzeConditionForEqColumn(lines []string, startLine int, file *indexedFile) []Finding {
	findings := make([]Finding, 0)
	if len(lines) == 0 {
		return findings
	}

	fullText := strings.Join(lines, " ")
	seen := make(map[string]struct{})

	// Проверяем ВСЁ условие на битовые операторы ДО применения regex
	// Если в условии есть &, |, ^ — пропускаем всё условие,
	// потому что это скорее всего проверка битовых флагов
	// Пример: column & mask = mask — это не сравнение столбца с самим собой
	if containsBitwiseOperator(fullText) {
		return findings
	}

	// Захватываем опциональный @ для переменных T-SQL
	eqRe := regexp.MustCompile(`(?i)(@?\w+(?:\.\w+)?)\s*=\s*(@?\w+(?:\.\w+)?)`)
	matches := eqRe.FindAllStringSubmatch(fullText, -1)

	for _, m := range matches {
		if len(m) < 3 {
			continue
		}
		// Пропускаем присваивание в SELECT: @var = column или column = @var
		// @var — переменная, не колонка, это не сравнение столбца с самим собой
		if strings.HasPrefix(m[1], "@") || strings.HasPrefix(m[2], "@") {
			continue
		}
		left := normalizeIdentifier(m[1])
		right := normalizeIdentifier(m[2])
		if left == "" || right == "" {
			continue
		}
		if left == right {
			key := left
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			findings = append(findings, Finding{
				Rule:             RuleUseEqColumn,
				Severity:         SeverityDeployStopper,
				Message:          "Нельзя сравнивать столбец с самим собой",
				File:             file.Path,
				Line:             startLine,
				Object:           left,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings
}

func (r *Runner) checkTableFullScan(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	// Удаляем макросы #define перед анализом
	contentStr := removeMacros(string(content))
	lines := strings.Split(contentStr, "\n")

	inStatement := false
	stmtStartLine := 0
	stmtBuffer := make([]string, 0)
	parenDepth := 0
	stmtType := ""

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		lower := strings.ToLower(line)

		if !inStatement {
			var startIdx int
			stmtType, startIdx = findStatementStart(lower)
			if startIdx >= 0 && !isInComment(line, startIdx) {
				depthBefore := 0
				for j := 0; j < startIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParens(line)

		if hasStatementEnded(lower) {
			if finding := analyzeStatementForFullScan(stmtBuffer, stmtStartLine, file, stmtType); finding != nil {
				findings = append(findings, *finding)
			}
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0

			var newStartIdx int
			stmtType, newStartIdx = findStatementStart(lower)
			if newStartIdx >= 0 && !isInComment(line, newStartIdx) && !hasStatementEnded(lower) {
				depthBefore := 0
				for j := 0; j < newStartIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
		}
	}

	if inStatement && len(stmtBuffer) > 0 {
		if finding := analyzeStatementForFullScan(stmtBuffer, stmtStartLine, file, stmtType); finding != nil {
			findings = append(findings, *finding)
		}
	}

	return findings, nil
}

func findStatementStart(lower string) (string, int) {
	types := []string{"select", "delete", "update", "merge"}
	for _, stmtType := range types {
		idx := findKeywordPosition(lower, stmtType)
		if idx >= 0 {
			return stmtType, idx
		}
	}
	return "", -1
}

func hasStatementEnded(lower string) bool {
	// Используем regex с границами слова, чтобы избежать ложных срабатываний на подстроках
	// Например, "dependantinfo" содержит "end", но это не ключевое слово
	re := regexp.MustCompile(`(?i)([;]|\b(?:go|begin|end|if|while|declare|exec|execute|return)\b)`)
	return re.MatchString(lower)
}

func analyzeStatementForFullScan(lines []string, startLine int, file *indexedFile, stmtType string) *Finding {
	if len(lines) == 0 || stmtType == "" {
		return nil
	}

	fullText := strings.Join(lines, " ")
	lower := strings.ToLower(fullText)

	// Для MERGE проверяем наличие ON условия
	if stmtType == "merge" {
		if !hasJoinCondition(lower) {
			tableName := extractTableNameFromStatement(fullText, stmtType)
			return &Finding{
				Rule:             RuleTableFullScan,
				Severity:         SeverityDeployStopper,
				Message:          "Операция MERGE без условия соединения (полное сканирование таблицы)",
				File:             file.Path,
				Line:             startLine,
				Object:           tableName,
				CurrentProductID: file.DsProductID,
			}
		}
		return nil
	}

	// Извлекаем все таблицы из FROM clause
	tables := extractTablesFromFromClause(fullText)

	// Для UPDATE: дополняем алиасы целевых таблиц из FROM-части
	// Если целевая таблица без алиаса, но есть в FROM с алиасом — используем тот алиас
	if stmtType == "update" {
		tables = enrichUpdateTargetAliases(tables)
	}

	if len(tables) == 0 {
		return nil
	}

	// Извлекаем условия из WHERE и ON
	whereResult := extractColumnRefsFromWhere(lower)
	onRefs := extractColumnRefsFromOn(lower)

	// Проверяем каждую таблицу
	for _, table := range tables {
		if !isTableFiltered(table, tables, whereResult, onRefs) {
			return &Finding{
				Rule:             RuleTableFullScan,
				Severity:         SeverityDeployStopper,
				Message:          fmt.Sprintf("Таблица %s не имеет условия фильтрации (полное сканирование)", table.TableName),
				File:             file.Path,
				Line:             startLine,
				Object:           table.TableName,
				CurrentProductID: file.DsProductID,
			}
		}
	}

	return nil
}

type tableFromClause struct {
	TableName string
	Alias     string
	Hint      string // Извлеченный хинт индекса
	IndexName string // Имя индекса из M_*_INDEX(...)
}

// enrichUpdateTargetAliases для UPDATE: если таблица без алиаса,
// но есть другая запись с таким же именем и с алиасом — копируем алиас
func enrichUpdateTargetAliases(tables []tableFromClause) []tableFromClause {
	// Строим мапу имя таблицы -> алиас (из записей где алиас есть)
	aliasMap := make(map[string]string)
	for _, t := range tables {
		if t.Alias != "" {
			key := strings.ToLower(t.TableName)
			if _, exists := aliasMap[key]; !exists {
				aliasMap[key] = t.Alias
			}
		}
	}

	// Дополняем записи без алиаса
	for i := range tables {
		if tables[i].Alias == "" {
			key := strings.ToLower(tables[i].TableName)
			if alias, found := aliasMap[key]; found {
				tables[i].Alias = alias
			}
		}
	}

	return tables
}

// removeComments удаляет SQL комментарии (-- ... и /* ... */)
func removeComments(text string) string {
	// Удаляем многострочные комментарии /* ... */
	blockCommentRe := regexp.MustCompile(`(?s)/\*.*?\*/`)
	text = blockCommentRe.ReplaceAllString(text, "")

	// Удаляем однострочные комментарии -- ...
	lineCommentRe := regexp.MustCompile(`(?m)--.*$`)
	text = lineCommentRe.ReplaceAllString(text, "")

	return text
}

func extractTablesFromFromClause(fullText string) []tableFromClause {
	result := make([]tableFromClause, 0)

	// Удаляем комментарии перед парсингом
	fullText = removeComments(fullText)
	lower := strings.ToLower(fullText)

	// Находим FROM clause
	fromIdx := strings.Index(lower, " from ")
	if fromIdx == -1 {
		fromIdx = strings.Index(lower, "from ")
	}
	if fromIdx == -1 {
		return result
	}

	// Извлекаем часть после FROM до WHERE, ORDER BY, GROUP BY, UNION и т.д.
	fromPart := fullText[fromIdx:]
	lowerFromPart := strings.ToLower(fromPart)

	// Находим конец FROM clause
	endMarkers := []string{" where ", " order by ", " group by ", " having ", " union ", " except ", " intersect "}
	endIdx := len(fromPart)
	for _, marker := range endMarkers {
		if idx := strings.Index(lowerFromPart, marker); idx > 0 && idx < endIdx {
			endIdx = idx
		}
	}

	fromClause := fromPart[:endIdx]
	return parseTablesInFromClause(fromClause)
}

// splitByCommasRespectingParens разбивает строку по запятым, но только те что вне скобок
// Запятые внутри функций (isnull(arg1, arg2)) не используются как разделители
func splitByCommasRespectingParens(sql string) []string {
	var parts []string
	var current strings.Builder
	depth := 0

	for _, ch := range sql {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				// Запятая на нулевом уровне - разделитель
				parts = append(parts, current.String())
				current.Reset()
				continue
			}
		}
		current.WriteRune(ch)
	}

	// Добавляем последнюю часть
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

func parseTablesInFromClause(fromClause string) []tableFromClause {
	result := make([]tableFromClause, 0)
	lower := strings.ToLower(fromClause)

	// Убираем слово FROM
	fromIdx := strings.Index(lower, "from")
	if fromIdx >= 0 {
		fromClause = fromClause[fromIdx+4:]
		lower = strings.ToLower(fromClause)
	}

	// Разбиваем по запятым (comma-join) и JOIN
	// Сначала заменяем JOIN-ы на разделители
	normalized := strings.ReplaceAll(lower, " inner join ", ",")
	normalized = strings.ReplaceAll(normalized, " left join ", ",")
	normalized = strings.ReplaceAll(normalized, " right join ", ",")
	normalized = strings.ReplaceAll(normalized, " full join ", ",")
	normalized = strings.ReplaceAll(normalized, " cross join ", ",")
	normalized = strings.ReplaceAll(normalized, " join ", ",")

	// Разбиваем по запятым с учетом вложенных скобок
	// Это предотвращает ложное разделение по запятым внутри isnull(arg1, arg2)
	parts := splitByCommasRespectingParens(normalized)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		table := parseTableWithAlias(part)
		if table.TableName != "" {
			result = append(result, table)
		}
	}

	return result
}

func parseTableWithAlias(part string) tableFromClause {
	result := tableFromClause{}

	// Извлекаем подсказку индекса типа M_ROWLOCK_INDEX(XPK...)
	idxHintExtractRe := regexp.MustCompile(`(?i)\s+(M_\w+_INDEX)\s*\(\s*([^\s,)]+)`)
	matches := idxHintExtractRe.FindStringSubmatch(part)
	if len(matches) > 1 {
		result.Hint = matches[1]
	}
	if len(matches) > 2 {
		result.IndexName = strings.Trim(matches[2], "[]\"'")
	}

	// Убираем подсказки индексов типа M_ROWLOCK_INDEX(...)
	idxHintRemoveRe := regexp.MustCompile(`(?i)\s+M_\w+_INDEX\s*\([^)]+\)`)
	part = idxHintRemoveRe.ReplaceAllString(part, "")

	// Убираем подсказки NOLOCK
	nolockRe := regexp.MustCompile(`(?i)\s+NOLOCK`)
	part = nolockRe.ReplaceAllString(part, "")

	// Убираем подсказки WITH (...)
	withHintRe := regexp.MustCompile(`(?i)\s+WITH\s*\([^)]+\)`)
	part = withHintRe.ReplaceAllString(part, "")

	part = strings.TrimSpace(part)
	if part == "" {
		return result
	}

	// Разбиваем на токены: table alias или table AS alias
	tokens := strings.Fields(part)
	if len(tokens) == 0 {
		return result
	}

	// Очищаем имя таблицы от скобок, которые могли попасть из-за неправильного разделения
	result.TableName = strings.Trim(tokens[0], "()")

	// Ищем алиас (после AS или просто следующий токен)
	for i := 1; i < len(tokens); i++ {
		token := strings.ToLower(tokens[i])
		if token == "as" && i+1 < len(tokens) {
			result.Alias = tokens[i+1]
			break
		}
		if token != "" && !strings.HasPrefix(token, "(") {
			// Проверяем, что это не подсказка индекса
			if !strings.Contains(token, "(") {
				result.Alias = tokens[i]
				break
			}
		}
	}

	return result
}

type whereAnalysisResult struct {
	Aliases                  []string
	HasUnqualifiedConditions bool
}

func extractColumnRefsFromWhere(lower string) whereAnalysisResult {
	result := whereAnalysisResult{
		Aliases:                  []string{},
		HasUnqualifiedConditions: false,
	}

	// Находим WHERE clause
	whereIdx := strings.Index(lower, " where ")
	if whereIdx == -1 {
		return result
	}

	wherePart := lower[whereIdx+7:]

	// Обрезаем до следующего ключевого слова
	endMarkers := []string{" order by ", " group by ", " having ", " union ", " except ", " intersect "}
	endIdx := len(wherePart)
	for _, marker := range endMarkers {
		if idx := strings.Index(wherePart, marker); idx > 0 && idx < endIdx {
			endIdx = idx
		}
	}
	wherePart = wherePart[:endIdx]

	// Извлекаем alias.column ссылки
	reQualified := regexp.MustCompile(`(?i)(\w+)\.(\w+)`)
	matches := reQualified.FindAllStringSubmatch(wherePart, -1)
	for _, m := range matches {
		if len(m) >= 2 {
			result.Aliases = append(result.Aliases, strings.ToLower(m[1]))
		}
	}

	// Проверяем наличие неквалифицированных условий (column = value без alias)
	// Удаляем все квалифицированные ссылки и проверяем оставшиеся условия
	cleaned := reQualified.ReplaceAllString(wherePart, "")
	reCondition := regexp.MustCompile(`(?i)\b(\w+)\s*(=|<>|!=|<|>|<=|>=|like|in|between)`)
	if reCondition.MatchString(cleaned) {
		result.HasUnqualifiedConditions = true
	}

	return result
}

func extractColumnRefsFromOn(lower string) []string {
	result := make([]string, 0)

	// Находим все ON условия
	onRe := regexp.MustCompile(`(?i)\s+on\s+`)
	parts := onRe.Split(lower, -1)

	for _, part := range parts[1:] { // пропускаем первую часть (до первого ON)
		// Обрезаем до JOIN или другого ключевого слова
		endMarkers := []string{" join ", " where ", " order by ", " group by "}
		endIdx := len(part)
		for _, marker := range endMarkers {
			if idx := strings.Index(part, marker); idx > 0 && idx < endIdx {
				endIdx = idx
			}
		}
		onPart := part[:endIdx]

		// Извлекаем alias.column ссылки
		re := regexp.MustCompile(`(?i)(\w+)\.(\w+)`)
		matches := re.FindAllStringSubmatch(onPart, -1)
		for _, m := range matches {
			if len(m) >= 2 {
				result = append(result, strings.ToLower(m[1]))
			}
		}
	}

	return result
}

func isTableFiltered(table tableFromClause, tables []tableFromClause, whereResult whereAnalysisResult, onRefs []string) bool {
	// Если одна таблица в запросе и есть неквалифицированные условия - считаем отфильтрованной
	if len(tables) == 1 && whereResult.HasUnqualifiedConditions {
		return true
	}

	// Проверяем по алиасу
	alias := strings.ToLower(table.Alias)
	if alias != "" {
		for _, ref := range whereResult.Aliases {
			if ref == alias {
				return true
			}
		}
		for _, ref := range onRefs {
			if ref == alias {
				return true
			}
		}
	}

	// Проверяем по имени таблицы (если нет алиаса)
	tableName := strings.ToLower(table.TableName)
	for _, ref := range whereResult.Aliases {
		if ref == tableName {
			return true
		}
	}
	for _, ref := range onRefs {
		if ref == tableName {
			return true
		}
	}

	return false
}

func hasJoinCondition(lower string) bool {
	return strings.Contains(lower, " on ")
}

func extractTableNameFromStatement(fullText, stmtType string) string {
	switch stmtType {
	case "select":
		return extractTableAfterFrom(fullText)
	case "delete":
		return extractTableAfterDelete(fullText)
	case "update":
		return extractTableAfterUpdate(fullText)
	case "merge":
		return extractTableAfterMerge(fullText)
	}

	return ""
}

func extractTableAfterFrom(fullText string) string {
	lower := strings.ToLower(fullText)
	idx := strings.Index(lower, " from ")
	if idx == -1 {
		return ""
	}
	return extractNextIdentifier(fullText, idx+6)
}

func extractTableAfterDelete(fullText string) string {
	lower := strings.ToLower(fullText)
	idx := strings.Index(lower, "delete")
	if idx == -1 {
		return ""
	}
	pos := idx + 6

	for pos < len(fullText) && (fullText[pos] == ' ' || fullText[pos] == '\t') {
		pos++
	}

	if pos+4 <= len(lower) && lower[pos:pos+4] == "from" {
		pos += 4
		for pos < len(fullText) && (fullText[pos] == ' ' || fullText[pos] == '\t') {
			pos++
		}
	}

	return extractNextIdentifier(fullText, pos)
}

func extractTableAfterUpdate(fullText string) string {
	lower := strings.ToLower(fullText)
	idx := strings.Index(lower, "update")
	if idx == -1 {
		return ""
	}
	pos := idx + 6

	for pos < len(fullText) && (fullText[pos] == ' ' || fullText[pos] == '\t') {
		pos++
	}

	return extractNextIdentifier(fullText, pos)
}

func extractTableAfterMerge(fullText string) string {
	lower := strings.ToLower(fullText)
	idx := strings.Index(lower, "merge")
	if idx == -1 {
		return ""
	}
	pos := idx + 5

	for pos < len(fullText) && (fullText[pos] == ' ' || fullText[pos] == '\t') {
		pos++
	}

	if pos+4 <= len(lower) && lower[pos:pos+4] == "into" {
		pos += 4
		for pos < len(fullText) && (fullText[pos] == ' ' || fullText[pos] == '\t') {
			pos++
		}
	}

	return extractNextIdentifier(fullText, pos)
}

func extractNextIdentifier(fullText string, startPos int) string {
	if startPos >= len(fullText) {
		return ""
	}

	for startPos < len(fullText) && (fullText[startPos] == ' ' || fullText[startPos] == '\t' || fullText[startPos] == '\n' || fullText[startPos] == '\r') {
		startPos++
	}

	if startPos >= len(fullText) {
		return ""
	}

	endPos := startPos
	for endPos < len(fullText) {
		ch := fullText[endPos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '(' || ch == ')' || ch == ';' || ch == ',' {
			break
		}
		endPos++
	}

	if startPos < endPos {
		return strings.TrimSpace(fullText[startPos:endPos])
	}

	return ""
}

var allowedTableHints = []string{
	"M_INDEX",
	"M_NOLOCK_INDEX",
	"M_ROWLOCK_INDEX",
	"M_ROWLOCK_READPAST_INDEX",
	"M_READPAST_INDEX",
	"M_UPDLOCK_INDEX",
	"M_UPDLOCK_READPAST_INDEX",
	"M_HOLDLOCK_INDEX",
	"M_P_ROWLOCK_INDEX",
	"M_P_ROWLOCK_READPAST_INDEX",
	"M_P_READPAST_INDEX",
	"M_P_UPDLOCK_INDEX",
	"M_P_UPDLOCK_READPAST_INDEX",
	"M_P_HOLDLOCK_INDEX",
}

func (r *Runner) checkTableHintExists(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	// Удаляем макросы #define перед анализом
	contentStr := removeMacros(string(content))
	lines := strings.Split(contentStr, "\n")

	inStatement := false
	stmtStartLine := 0
	stmtBuffer := make([]string, 0)
	parenDepth := 0
	stmtType := ""

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		lower := strings.ToLower(line)

		if !inStatement {
			var startIdx int
			stmtType, startIdx = findStatementStartForTableHintExists(lower)
			if startIdx >= 0 && !isInComment(line, startIdx) {
				depthBefore := 0
				for j := 0; j < startIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParens(line)

		if hasStatementEnded(lower) {
			items := analyzeStatementForTableHintExists(stmtBuffer, stmtStartLine, file, stmtType)
			findings = append(findings, items...)
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0

			var newStartIdx int
			stmtType, newStartIdx = findStatementStartForTableHintExists(lower)
			if newStartIdx >= 0 && !isInComment(line, newStartIdx) && !hasStatementEnded(lower) {
				depthBefore := 0
				for j := 0; j < newStartIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
		}
	}

	if inStatement && len(stmtBuffer) > 0 {
		items := analyzeStatementForTableHintExists(stmtBuffer, stmtStartLine, file, stmtType)
		findings = append(findings, items...)
	}

	return findings, nil
}

func findStatementStartForTableHintExists(lower string) (string, int) {
	types := []string{"select", "delete", "update", "merge", "insert"}
	bestIdx := -1
	bestType := ""
	for _, stmtType := range types {
		idx := findKeywordPosition(lower, stmtType)
		if idx >= 0 && (bestIdx == -1 || idx < bestIdx) {
			bestIdx = idx
			bestType = stmtType
		}
	}

	return bestType, bestIdx
}

func removeBlockComments(text string) string {
	result := text
	for {
		startIdx := strings.Index(result, "/*")
		if startIdx == -1 {
			break
		}
		endIdx := strings.Index(result[startIdx:], "*/")
		if endIdx == -1 {
			break
		}
		endIdx += startIdx + 2
		result = result[:startIdx] + " " + result[endIdx:]
	}
	return result
}

func shouldSkipTableCheck(tableName string) bool {
	lower := strings.ToLower(tableName)

	// Пропускаем переменные (@param)
	if strings.HasPrefix(tableName, "@") {
		return true
	}

	// Пропускаем только временная #-таблица
	if strings.HasPrefix(tableName, "#") {
		return true
	}

	// Пропускаем слова которые явно не таблицы
	invalidNames := []string{"file", "select", "insert", "update", "delete", "from", "where", "join"}
	for _, invalid := range invalidNames {
		if lower == invalid {
			return true
		}
	}

	return false
}

var (
	// readHints - для SELECT и вспомогательных таблиц в UPDATE/DELETE
	readHints = []string{
		"M_INDEX",
		"M_NOLOCK_INDEX",
		"M_READPAST_INDEX",
		"M_HOLDLOCK_INDEX",
		"M_P_READPAST_INDEX",
		"M_P_HOLDLOCK_INDEX",
	}
	// deleteHints - для целевой таблицы в DELETE
	deleteHints = []string{
		"M_ROWLOCK_INDEX",
		"M_ROWLOCK_READPAST_INDEX",
		"M_P_ROWLOCK_INDEX",
		"M_P_ROWLOCK_READPAST_INDEX",
	}
	// updateHints - для целевой таблицы в UPDATE
	updateHints = []string{
		"M_UPDLOCK_INDEX",
		"M_UPDLOCK_READPAST_INDEX",
		"M_P_UPDLOCK_INDEX",
		"M_P_UPDLOCK_READPAST_INDEX",
	}
)

func (r *Runner) checkTableHintIsRight(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	// Удаляем макросы #define перед анализом
	contentStr := removeMacros(string(content))
	lines := strings.Split(contentStr, "\n")

	inStatement := false
	stmtStartLine := 0
	stmtBuffer := make([]string, 0)
	parenDepth := 0
	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Отслеживаем блочные комментарии /* */
		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		// Проверяем начало блочного комментария
		for {
			if idx := strings.Index(line, "/*"); idx >= 0 {
				endIdx := strings.Index(line[idx:], "*/")
				if endIdx > 0 {
					line = line[:idx] + " " + line[idx+endIdx+2:]
				} else {
					inBlockComment = true
					line = line[:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(line)

		if !inStatement {
			_, startIdx := findStatementStartHint(lower)
			if startIdx >= 0 && !isInComment(line, startIdx) {
				depthBefore := 0
				for j := 0; j < startIdx; j++ {
					switch line[j] {
					case '(':
						depthBefore++
					case ')':
						depthBefore--
					}
				}
				if depthBefore == 0 {
					inStatement = true
					stmtStartLine = lineNum
					stmtBuffer = []string{line}
					parenDepth = countParens(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParens(line)

		if hasStatementEnded(lower) {
			items := analyzeStatementForHintType(stmtBuffer, stmtStartLine, file)
			findings = append(findings, items...)
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0
		}
	}

	if inStatement && len(stmtBuffer) > 0 {
		items := analyzeStatementForHintType(stmtBuffer, stmtStartLine, file)
		findings = append(findings, items...)
	}

	return findings, nil
}

func findStatementStartHint(lower string) (string, int) {
	keywords := []string{"select", "update", "delete"}
	for _, kw := range keywords {
		if idx := findKeywordPosition(lower, kw); idx >= 0 {
			return kw, idx
		}
	}
	return "", -1
}

func analyzeStatementForHintType(lines []string, startLine int, file *indexedFile) []Finding {
	findings := make([]Finding, 0)
	if len(lines) == 0 {
		return findings
	}

	fullText := strings.Join(lines, " ")
	trimmedText := normalizeHintStatementText(strings.TrimSpace(fullText))
	lowerFull := strings.ToLower(trimmedText)

	// Определяем тип операции
	var stmtType string
	if strings.HasPrefix(lowerFull, "select") {
		stmtType = "select"
	} else if strings.HasPrefix(lowerFull, "update") {
		stmtType = "update"
	} else if strings.HasPrefix(lowerFull, "delete") {
		stmtType = "delete"
	} else {
		return findings
	}

	// Извлекаем таблицы из FROM clause
	tables := extractTablesFromFromClause(fullText)

	// Определяем целевую таблицу
	targetTable := ""
	switch stmtType {
	case "update":
		targetTable = extractUpdateTargetTable(trimmedText)
	case "delete":
		targetTable = extractDeleteTargetTable(trimmedText)
	}

	// Проверяем все таблицы из FROM
	for _, table := range tables {
		// Пропускаем переменные и служебные
		if shouldSkipTableCheck(table.TableName) {
			continue
		}

		// Используем хинт извлеченный при парсинге FROM clause
		hint := table.Hint
		if hint == "" {
			continue // Нет хинта - это проверяется другим правилом tableHintExists
		}

		var allowedHints []string
		if strings.EqualFold(table.TableName, targetTable) || strings.EqualFold(table.Alias, targetTable) {
			// Целевая таблица
			switch stmtType {
			case "delete":
				allowedHints = deleteHints
			case "update":
				allowedHints = updateHints
			default:
				allowedHints = readHints
			}
		} else {
			// Вспомогательная таблица
			allowedHints = readHints
		}

		if !isHintAllowed(hint, allowedHints) {
			findings = append(findings, Finding{
				Rule:             RuleTableHintIsRight,
				Severity:         SeverityDeployStopper,
				Message:          fmt.Sprintf("Таблица %s имеет неправильный хинт %s для операции %s", table.TableName, hint, stmtType),
				File:             file.Path,
				Line:             startLine,
				Object:           table.TableName,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings
}

func normalizeHintStatementText(text string) string {
	lower := strings.ToLower(text)

	if strings.HasPrefix(lower, "select") {
		if idx := findKeywordPosition(lower, "delete"); idx > 0 {
			return strings.TrimSpace(text[idx:])
		}
		if idx := findKeywordPosition(lower, "update"); idx > 0 {
			return strings.TrimSpace(text[idx:])
		}
	}

	return text
}

func extractUpdateTargetTable(fullText string) string {
	lower := strings.ToLower(fullText)
	if !strings.HasPrefix(lower, "update") {
		return ""
	}

	// UPDATE table SET ...
	lower = strings.TrimPrefix(lower, "update")
	lower = strings.TrimLeft(lower, " \t")

	// Берем первое слово до пробела или SET
	endIdx := strings.Index(lower, " ")
	setIdx := strings.Index(lower, "set")
	if setIdx >= 0 && (endIdx == -1 || setIdx < endIdx) {
		endIdx = setIdx
	}

	if endIdx > 0 {
		return strings.TrimSpace(fullText[len("update") : len("update")+endIdx])
	}

	return ""
}

func extractDeleteTargetTable(fullText string) string {
	lower := strings.ToLower(fullText)

	// DELETE FROM table ...
	if strings.HasPrefix(lower, "delete from") {
		lower = strings.TrimPrefix(lower, "delete from")
		lower = strings.TrimLeft(lower, " \t")
		endIdx := strings.Index(lower, " ")
		if endIdx < 0 {
			endIdx = len(lower)
		}
		if endIdx > 0 {
			startPos := len("delete from")
			return strings.TrimSpace(fullText[startPos : startPos+endIdx])
		}
		return ""
	}

	// DELETE table FROM table, ...
	if strings.HasPrefix(lower, "delete ") {
		lower = strings.TrimPrefix(lower, "delete ")
		lower = strings.TrimLeft(lower, " \t")
		endIdx := strings.Index(lower, " ")
		fromIdx := strings.Index(lower, "from")
		if fromIdx >= 0 && (endIdx == -1 || fromIdx < endIdx) {
			endIdx = fromIdx
		}
		if endIdx > 0 {
			startPos := len("delete ")
			return strings.TrimSpace(fullText[startPos : startPos+endIdx])
		}
	}

	return ""
}

func isHintAllowed(hint string, allowed []string) bool {
	lowerHint := strings.ToLower(hint)
	for _, allowedHint := range allowed {
		if strings.ToLower(allowedHint) == lowerHint {
			return true
		}
	}
	return false
}
