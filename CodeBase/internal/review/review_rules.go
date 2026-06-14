package review

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
)

// selectAllRe находит SELECT * (с любыми пробелами между SELECT и *)
var selectAllRe = regexp.MustCompile(`(?i)\bselect\s+\*`)

// truncateTblRe находит TRUNCATE TABLE и имя таблицы (включая схему dbo.table)
var truncateTblRe = regexp.MustCompile(`(?i)\btruncate\s+table\s+(\S+)`)

func (r *Runner) checkForeignTables(parsed *sqlparser.ParseResult, file *indexedFile, prefix string) ([]Finding, error) {
	tables := dedupeTableRefs(parsed.Tables, prefix)
	findings := make([]Finding, 0)
	for _, table := range tables {
		if strings.EqualFold(prefix, "t") && isSharedTTable(table.Name) {
			continue
		}
		targetProductIDs, err := r.lookupTableProductIDs(table.Name)
		if err != nil {
			return nil, err
		}
		if len(targetProductIDs) == 0 {
			continue
		}
		if _, ok := targetProductIDs[file.DsProductID]; ok {
			continue
		}
		rule := RuleForeignTablesUsing
		if strings.EqualFold(prefix, "p") {
			rule = RuleForeignPTablesUsing
		}
		var targetProductID int64
		for id := range targetProductIDs {
			targetProductID = id
			break
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

func (r *Runner) checkDatatypeFetchInto(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	seen := make(map[string]struct{})

	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}

	contentStr := string(content)
	variableTypes := collectVariableTypes(parsed, contentStr)
	cursorDeclarations := parseCursorDeclarations(contentStr)
	if len(cursorDeclarations) == 0 {
		return findings, nil
	}

	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}

		stmt, ok := parseFetchIntoStatement(fragment.QueryText)
		if !ok {
			continue
		}

		cursorDecl, exists := cursorDeclarations[normalizeIdentifier(stmt.CursorName)]
		if !exists {
			continue
		}

		aliasMap := parseAliasMap(cursorDecl.FromClause)
		defaultTable := parseFirstFromTable(cursorDecl.FromClause)

		count := len(stmt.Variables)
		if len(cursorDecl.SelectExpressions) < count {
			count = len(cursorDecl.SelectExpressions)
		}

		for i := 0; i < count; i++ {
			targetVariable := strings.TrimSpace(stmt.Variables[i])
			if targetVariable == "" {
				continue
			}
			targetType := variableTypes[normalizeVariableName(targetVariable)]
			if targetType == "" {
				continue
			}

			sourceExpr := strings.TrimSpace(cursorDecl.SelectExpressions[i])
			if sourceExpr == "" {
				continue
			}

			if hasExplicitConversion(sourceExpr, targetType) {
				continue
			}

			sourceTypes := r.resolveCursorSourceTypes(sourceExpr, aliasMap, defaultTable)
			for _, sourceType := range sourceTypes {
				if !isPotentialPrecisionLoss(sourceType, targetType) {
					continue
				}
				key := fmt.Sprintf("%d|%s|%s", fragment.LineNumber, normalizeVariableName(targetVariable), normalizeDataType(sourceType))
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
					Object:           targetVariable,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
}

func (r *Runner) checkDatatypeSelectAssign(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	seen := make(map[string]struct{})

	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	variableTypes := collectVariableTypes(parsed, string(content))

	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		stmt, ok := parseSelectAssignStatement(fragment.QueryText)
		if !ok {
			continue
		}

		aliasMap := parseAliasMap(stmt.FromClause)
		for _, assignment := range stmt.Assignments {
			targetVariable := normalizeVariableName(assignment.TargetVariable)
			targetType, exists := variableTypes[targetVariable]
			if !exists || targetType == "" {
				continue
			}

			if hasExplicitConversion(assignment.Expression, targetType) {
				continue
			}

			sourceTypes := r.resolveExpressionTypes(assignment.Expression, aliasMap)
			for _, sourceType := range sourceTypes {
				if !isPotentialPrecisionLoss(sourceType, targetType) {
					continue
				}
				key := fmt.Sprintf("%d|%s|%s", fragment.LineNumber, targetVariable, normalizeDataType(sourceType))
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
					Object:           assignment.TargetVariable,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
}

func collectVariableTypes(parsed *sqlparser.ParseResult, content string) map[string]string {
	result := make(map[string]string)

	for _, proc := range parsed.Procedures {
		if proc == nil {
			continue
		}
		for _, p := range proc.Params {
			name := normalizeVariableName(p.Name)
			typeName := strings.TrimSpace(p.Type)
			if name == "" || typeName == "" {
				continue
			}
			result[name] = typeName
		}
	}

	declRe := regexp.MustCompile(`(?i)@([A-Za-z_][A-Za-z0-9_]*)[ \t]+([A-Za-z_][A-Za-z0-9_]*(?:\s*\([^)]*\))?)`)
	for _, m := range declRe.FindAllStringSubmatch(content, -1) {
		if len(m) < 3 {
			continue
		}
		name := normalizeVariableName(m[1])
		typeName := strings.TrimSpace(m[2])
		if name == "" || typeName == "" {
			continue
		}
		result[name] = typeName
	}

	return result
}

func normalizeVariableName(value string) string {
	v := strings.TrimSpace(value)
	v = strings.Trim(v, "[]\"")
	v = strings.TrimPrefix(v, "@")
	return strings.ToLower(v)
}

func (r *Runner) checkIndexWrong(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
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

		if hasStatementEnded(lower, stmtBuffer) {
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
	joinColumns := extractJoinColumnsForIndexWrong(trimmedText, tables)
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
		joinCols := joinColumns[tableConditionKey(table)]

		candidates, err := r.lookupTableIndexCandidates(tableName)
		if err != nil {
			return nil, err
		}
		if len(candidates) == 0 {
			continue
		}

		chosenFound := false
		chosenScore := 0
		chosenFields := make([]string, 0)
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
					chosenFields = candidate.Fields
				}
			}
		}

		if !chosenFound || bestScore <= chosenScore {
			continue
		}

		if shouldKeepChosenIndexForPKJoin(indexName, chosenFields, joinCols) {
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

func (r *Runner) checkUpdateOnlyVar(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
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

		if hasStatementEnded(lower, stmtBuffer) {
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

func (r *Runner) checkPTableSpid(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
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

		if hasStatementEnded(lower, stmtBuffer) {
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

func (r *Runner) checkForceOrder2Tbl(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
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

		if hasStatementEnded(lower, stmtBuffer) {
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

	// Проверяем наличие макроса M_FORCEORDER* во всем тексте оператора (включая UNION)
	lower := strings.ToLower(fullText)
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

	content, err := r.fileContent(file.Path)
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

func (r *Runner) checkUseDrop(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
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

	content, err := r.fileContent(file.Path)
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

func (r *Runner) checkExistsWithAndInIf(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
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

	content, err := r.fileContent(file.Path)
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

		if hasStatementEnded(lower, stmtBuffer) {
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
		targetProductIDs, err := r.lookupTableProductIDs(table.Name)
		if err != nil {
			return nil, err
		}
		if len(targetProductIDs) == 0 {
			continue
		}
		if _, ok := targetProductIDs[file.DsProductID]; ok {
			continue
		}
		var targetProductID int64
		for id := range targetProductIDs {
			targetProductID = id
			break
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

	// Читаем содержимое файла один раз для извлечения тел процедур
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	for _, proc := range parsed.Procedures {
		if proc == nil || len(proc.Params) == 0 {
			continue
		}

		// Извлекаем тело процедуры по границам строк
		procBody := r.extractProcedureBody(lines, proc.LineStart, proc.LineEnd)

		// Для каждого параметра с default=null проверяем защиту в теле
		for _, param := range proc.Params {
			if !r.needsDefaultAssignment(param) {
				continue
			}
			if !r.hasDefaultAssignmentInBody(procBody, param.Name) {
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

// extractProcedureBody извлекает тело процедуры из строк файла по границам
func (r *Runner) extractProcedureBody(lines []string, lineStart, lineEnd int) string {
	// Проверяем валидность границ
	if lineStart < 1 || lineEnd > len(lines) || lineStart > lineEnd {
		return ""
	}
	// Корректируем верхнюю границу если она выходит за пределы
	if lineEnd > len(lines) {
		lineEnd = len(lines)
	}
	// Преобразуем в 0-based индексы
	startIdx := lineStart - 1
	endIdx := lineEnd
	return strings.Join(lines[startIdx:endIdx], "\n")
}

// hasDefaultAssignmentInBody проверяет, есть ли в теле процедуры присвоение default для параметра
// до первого использования параметра
func (r *Runner) hasDefaultAssignmentInBody(procBody string, paramName string) bool {
	if procBody == "" || paramName == "" {
		return false
	}

	// Нормализуем имя параметра (убираем @ если есть)
	searchParam := paramName
	if strings.HasPrefix(paramName, "@") {
		searchParam = paramName[1:]
	}
	paramPattern := "@" + searchParam

	// Удаляем блок-комментарии /* ... */ перед анализом
	cleanedBody := removeBlockComments(procBody)

	// Разбиваем тело на строки для анализа позиций
	lines := strings.Split(cleanedBody, "\n")

	// Пропускаем строки до ключевого слова "as" (заголовок процедуры с параметрами)
	bodyStartIdx := 0
	for i, line := range lines {
		lower := strings.ToLower(strings.TrimSpace(line))
		if strings.HasPrefix(lower, "as") || strings.HasPrefix(lower, "as ") {
			bodyStartIdx = i + 1
			break
		}
	}

	// Ищем первую позицию присваивания и первую позицию использования
	firstAssignmentPos := -1
	firstUsagePos := -1

	// Отслеживаем, находимся ли внутри select-оператора
	inSelect := false

	for i := bodyStartIdx; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		lower := strings.ToLower(trimmed)

		// Пропускаем комментарии
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Определяем начало нового оператора
		if strings.HasPrefix(lower, "select ") || strings.HasPrefix(lower, "select(") {
			inSelect = true
		} else if strings.HasPrefix(lower, "set ") || strings.HasPrefix(lower, "insert ") ||
			strings.HasPrefix(lower, "update ") || strings.HasPrefix(lower, "delete ") ||
			strings.HasPrefix(lower, "if ") || strings.HasPrefix(lower, "while ") ||
			strings.HasPrefix(lower, "begin") || strings.HasPrefix(lower, "end") ||
			strings.HasPrefix(lower, "create ") || strings.HasPrefix(lower, "alter ") ||
			strings.HasPrefix(lower, "drop ") || strings.HasPrefix(lower, "declare ") {
			inSelect = false
		}

		// Если строка заканчивается запятой, это продолжение текущего оператора - не сбрасываем inSelect
		if !strings.HasSuffix(trimmed, ",") {
			// Сбрасываем inSelect только если строка НЕ заканчивается запятой
			if strings.HasPrefix(lower, "set ") || strings.HasPrefix(lower, "insert ") ||
				strings.HasPrefix(lower, "update ") || strings.HasPrefix(lower, "delete ") ||
				strings.HasPrefix(lower, "if ") || strings.HasPrefix(lower, "while ") ||
				strings.HasPrefix(lower, "begin") || strings.HasPrefix(lower, "end") ||
				strings.HasPrefix(lower, "create ") || strings.HasPrefix(lower, "alter ") ||
				strings.HasPrefix(lower, "drop ") || strings.HasPrefix(lower, "declare ") {
				inSelect = false
			}
		}

		// Ищем присваивание: @param = (внутри select или set)
		isAssignment := false
		// Проверяем @param= с любым количеством пробелов вокруг = (удаляем пробелы для проверки)
		trimmedNoSpaces := strings.ReplaceAll(lower, " ", "")
		if inSelect && strings.Contains(trimmedNoSpaces, strings.ToLower(paramPattern)+"=") {
			// Внутри select-оператора любое @param = считается присваиванием
			isAssignment = true
		} else if strings.HasPrefix(lower, "set "+strings.ToLower(paramPattern)+" =") {
			isAssignment = true
		}

		if isAssignment {
			if firstAssignmentPos == -1 {
				firstAssignmentPos = i
			}
			continue
		}

		// Ищем использование параметра (кроме присваивания)
		if strings.Contains(lower, strings.ToLower(paramPattern)) {
			// Проверяем, что это не присваивание (уже проверено выше)
			if !isAssignment {
				if firstUsagePos == -1 {
					firstUsagePos = i
				}
			}
		}
	}

	// Если присваивание найдено и происходит до первого использования - OK
	if firstAssignmentPos != -1 && firstUsagePos != -1 {
		return firstAssignmentPos < firstUsagePos
	}

	// Если присваивание есть, а использования нет (или наоборот) - считаем OK если есть присваивание
	return firstAssignmentPos != -1
}

func (r *Runner) checkProcElseCase(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Читаем содержимое файла
	content, err := r.fileContent(file.Path)
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

func (r *Runner) checkUseSelectAll(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Читаем содержимое файла
	content, err := r.fileContent(file.Path)
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
	content, err := r.fileContent(file.Path)
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

func (r *Runner) checkDatatype(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
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
	if err != nil {
		return nil, err
	}
	findings = append(findings, insertSelectFindings...)

	updateSetFindings, err := r.checkDatatypeUpdateSet(parsed, file)
	if err != nil {
		return nil, err
	}
	findings = append(findings, updateSetFindings...)

	selectAssignFindings, err := r.checkDatatypeSelectAssign(parsed, file)
	if err != nil {
		return nil, err
	}
	findings = append(findings, selectAssignFindings...)

	fetchIntoFindings, err := r.checkDatatypeFetchInto(parsed, file)
	if err != nil {
		return nil, err
	}
	findings = append(findings, fetchIntoFindings...)

	return findings, nil
}

func analyzeStatementForTableHintExists(lines []string, allLines []string, startLine int, file *indexedFile, stmtType string) []Finding {
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

	// Получаем текст строки для вывода в сообщении об ошибке
	lineText := ""
	if startLine > 0 && startLine <= len(allLines) {
		lineText = strings.TrimSpace(allLines[startLine-1])
	}

	for _, table := range tables {
		if shouldSkipTableCheck(table.TableName) {
			continue
		}

		if insertTarget != "" && normalizeIdentifier(table.TableName) == insertTarget {
			continue
		}

		if !isHintAllowed(table.Hint, allowedTableHints) {
			message := fmt.Sprintf("Таблица %s не имеет допустимого хинта индекса", table.TableName)
			if lineText != "" {
				message += fmt.Sprintf(" (строка: %s)", lineText)
			}
			findings = append(findings, Finding{
				Rule:             RuleTableHintExists,
				Severity:         SeverityDeployStopper,
				Message:          message,
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

			// Пропускаем, если выражение уже содержит явное преобразование в targetType
			if hasExplicitConversion(assignment.Expression, targetType) {
				continue
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

			// Пропускаем, если выражение уже содержит явное преобразование в targetType
			if hasExplicitConversion(expression, targetType) {
				continue
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

func (r *Runner) resolveCursorSourceTypes(expression string, aliasMap map[string]string, defaultTable string) []string {
	types := r.resolveExpressionTypes(expression, aliasMap)
	if len(types) > 0 {
		return types
	}

	columnName := extractBareColumnName(expression)
	if columnName == "" || strings.TrimSpace(defaultTable) == "" {
		return nil
	}

	typeName, err := r.db.FindLatestSQLColumnDefinitionType(defaultTable, columnName)
	if err != nil {
		return nil
	}

	return []string{typeName}
}

func parseFirstFromTable(fromClause string) string {
	if strings.TrimSpace(fromClause) == "" {
		return ""
	}

	re := regexp.MustCompile(`(?is)\bfrom\s+([a-z_#][a-z0-9_#]*)`)
	m := re.FindStringSubmatch(fromClause)
	if len(m) != 2 {
		return ""
	}

	return strings.TrimSpace(m[1])
}

func extractBareColumnName(expression string) string {
	if strings.TrimSpace(expression) == "" {
		return ""
	}

	re := regexp.MustCompile(`(?is)^\s*\[?([a-z_][a-z0-9_]*)\]?\s*$`)
	m := re.FindStringSubmatch(expression)
	if len(m) != 2 {
		return ""
	}

	return strings.TrimSpace(m[1])
}

// hasExplicitConversion проверяет, содержит ли выражение явное преобразование в targetType
// через convert() или cast(). Проверяет эквивалентность типов, а не точное совпадение.
func hasExplicitConversion(expression string, targetType string) bool {
	if expression == "" || targetType == "" {
		return false
	}
	exprLower := strings.ToLower(expression)

	// Извлекаем тип из convert(type, ...)
	convertRe := regexp.MustCompile(`(?i)\bconvert\s*\(\s*([a-z_][a-z0-9_]*(?:\s*\([^)]*\))?)\s*[\,)]`)
	convertMatches := convertRe.FindAllStringSubmatch(exprLower, -1)
	for _, m := range convertMatches {
		if len(m) > 1 {
			convertedType := normalizeDataType(m[1])
			if areEquivalentTypes(convertedType, targetType) {
				return true
			}
		}
	}

	// Извлекаем тип из cast(... as type)
	castRe := regexp.MustCompile(`(?i)\bcast\s*\([^)]+\s+as\s+([a-z_][a-z0-9_]*(?:\s*\([^)]*\))?)\s*\)`)
	castMatches := castRe.FindAllStringSubmatch(exprLower, -1)
	for _, m := range castMatches {
		if len(m) > 1 {
			castedType := normalizeDataType(m[1])
			if areEquivalentTypes(castedType, targetType) {
				return true
			}
		}
	}

	return false
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
	if sourceP, sourceS, okSource := numericPrecisionScale(source); okSource {
		if targetP, targetS, okTarget := numericPrecisionScale(target); okTarget {
			return sourceP > targetP || sourceS > targetS
		}
	}
	sourceRank := datetimePrecisionRank(source)
	targetRank := datetimePrecisionRank(target)
	if sourceRank > 0 && targetRank > 0 {
		return sourceRank > targetRank
	}
	return false
}

func numericPrecisionScale(dataType string) (int, int, bool) {
	v := normalizeDataType(dataType)
	numericRe := regexp.MustCompile(`(?i)\b(?:numeric|decimal)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)`)
	if m := numericRe.FindStringSubmatch(v); len(m) == 3 {
		precision, errP := strconv.Atoi(m[1])
		scale, errS := strconv.Atoi(m[2])
		if errP == nil && errS == nil {
			return precision, scale, true
		}
	}

	switch {
	case strings.HasPrefix(v, "dsint_key_one"), strings.HasPrefix(v, "dsint_key"), v == "int", v == "integer", strings.Contains(v, " int"):
		return 10, 0, true
	case strings.HasPrefix(v, "dssmallint"), strings.Contains(v, "smallint"):
		return 5, 0, true
	case strings.HasPrefix(v, "dstinyint"), strings.Contains(v, "tinyint"):
		return 3, 0, true
	case strings.HasPrefix(v, "dsbigint"), strings.Contains(v, "bigint"), strings.HasPrefix(v, "dsidentifier19"):
		return 19, 0, true
	case strings.HasPrefix(v, "dsidentifier"):
		return 15, 0, true
	default:
		return 0, 0, false
	}
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
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	// Сохраняем оригинальные строки для корректного определения номера строки
	originalLines := strings.Split(string(content), "\n")

	// Удаляем макросы #define перед анализом
	contentStr := removeMacros(string(content))
	contentStr = maskBlockCommentsKeepLines(contentStr)
	lines := strings.Split(contentStr, "\n")

	// Состояние парсера
	inFromClause := false
	parenDepth := 0
	hasJoin := false
	hasComma := false
	commaLine := 0
	firstTable := ""

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
					parenDepth = 0
					hasJoin = false
					hasComma = false
					firstTable = extractFirstTableFromFromClause(line[fromIdx+4:])
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
		// Проверяем наличие JOIN ключевых слов на полной строке
		if strings.Contains(lower, " join ") ||
			strings.Contains(lower, "inner join") ||
			strings.Contains(lower, "left join") ||
			strings.Contains(lower, "right join") ||
			strings.Contains(lower, "full join") ||
			strings.Contains(lower, "cross join") {
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
					// Находим реальный номер строки в оригинальном файле
					// Ищем строку с таким же содержимым, начиная с текущей позиции,
					// чтобы одинаковые строки в разных местах файла не мапились в первое вхождение
					commaLine = findOriginalLineNumber(originalLines, line, lineNum)
					if commaLine == 0 {
						commaLine = lineNum // fallback к текущему номеру
					}
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
					Object:           firstTable,
					CurrentProductID: file.DsProductID,
				})
			}
			inFromClause = false
			hasJoin = false
			hasComma = false
			commaLine = 0
			firstTable = ""
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
			Object:           firstTable,
			CurrentProductID: file.DsProductID,
		})
	}

	return findings, nil
}

func (r *Runner) checkInsertRowLock(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Читаем содержимое файла
	content, err := r.fileContent(file.Path)
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

func (r *Runner) checkUseEqColumn(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
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
		// Пропускаем числовые литералы (например, where 1=1)
		if isNumericLiteral(left) || isNumericLiteral(right) {
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

	content, err := r.fileContent(file.Path)
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

		if hasStatementEnded(lower, stmtBuffer) {
			if finding := analyzeStatementForFullScan(stmtBuffer, stmtStartLine, file, stmtType); finding != nil {
				findings = append(findings, *finding)
			}
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0

			var newStartIdx int
			stmtType, newStartIdx = findStatementStart(lower)
			if newStartIdx >= 0 && !isInComment(line, newStartIdx) && !hasStatementEnded(lower, stmtBuffer) {
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
		// UPDATE без FROM: проверяем target-таблицу напрямую
		if stmtType == "update" {
			targetName := extractUpdateTargetTable(fullText)
			if targetName == "" || strings.HasPrefix(strings.ToLower(targetName), "#") {
				return nil
			}
			// Если есть JOIN — соединение покрывает фильтрацию
			if hasJoinCondition(lower) {
				return nil
			}
			// Если в SET есть подзапрос (SELECT) — подзапрос сам фильтрует данные
			setIdx := strings.Index(lower, " set ")
			if setIdx > 0 {
				setPart := lower[setIdx+5:]
				// Проверяем есть ли SELECT в SET (подзапрос)
				if strings.Contains(setPart, "select") {
					return nil
				}
			}
			wherePart := extractWherePartForIndexWrong(lower)
			if wherePart == "" {
				return &Finding{
					Rule:             RuleTableFullScan,
					Severity:         SeverityDeployStopper,
					Message:          fmt.Sprintf("Таблица %s не имеет условия фильтрации (полное сканирование)", strings.ToLower(targetName)),
					File:             file.Path,
					Line:             startLine,
					Object:           strings.ToLower(targetName),
					CurrentProductID: file.DsProductID,
				}
			}
		}
		return nil
	}

	// Извлекаем условия из WHERE и ON
	whereResult := extractColumnRefsFromWhere(lower)
	onRefs := extractColumnRefsFromOn(lower)

	// Проверяем каждую таблицу
	for _, table := range tables {
		if strings.HasPrefix(strings.ToLower(table.TableName), "#") {
			continue
		}
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

	content, err := r.fileContent(file.Path)
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

		if hasStatementEnded(lower, stmtBuffer) {
			items := analyzeStatementForTableHintExists(stmtBuffer, lines, stmtStartLine, file, stmtType)
			findings = append(findings, items...)
			inStatement = false
			stmtBuffer = nil
			parenDepth = 0

			var newStartIdx int
			stmtType, newStartIdx = findStatementStartForTableHintExists(lower)
			if newStartIdx >= 0 && !isInComment(line, newStartIdx) && !hasStatementEnded(lower, stmtBuffer) {
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
		items := analyzeStatementForTableHintExists(stmtBuffer, lines, stmtStartLine, file, stmtType)
		findings = append(findings, items...)
	}

	return findings, nil
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

	content, err := r.fileContent(file.Path)
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

		// Отслеживаем блочные комментарии /* */
		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
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

		if _, nextStartIdx := findStatementStartHint(lower); nextStartIdx >= 0 && !isInComment(line, nextStartIdx) {
			depthBefore := 0
			for j := 0; j < nextStartIdx; j++ {
				switch line[j] {
				case '(':
					depthBefore++
				case ')':
					depthBefore--
				}
			}
			// Для tableHintIsRight всегда разрываем при новом DML на нулевом уровне
			if parenDepth == 0 && depthBefore == 0 {
				items := analyzeStatementForHintType(stmtBuffer, stmtStartLine, file)
				findings = append(findings, items...)

				stmtStartLine = lineNum
				stmtBuffer = []string{line}
				parenDepth = countParens(line)
				continue
			}
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParens(line)
	}

	if inStatement && len(stmtBuffer) > 0 {
		items := analyzeStatementForHintType(stmtBuffer, stmtStartLine, file)
		findings = append(findings, items...)
	}

	return findings, nil
}

func analyzeStatementForHintType(lines []string, startLine int, file *indexedFile) []Finding {
	findings := make([]Finding, 0)
	if len(lines) == 0 {
		return findings
	}

	fullText := strings.Join(lines, " ")
	trimmedText := normalizeHintStatementText(strings.TrimSpace(fullText))

	// Разбиваем текст на отдельные операторы SELECT/UPDATE/DELETE на нулевом уровне
	statements := splitStatementsForHintType(trimmedText)

	for _, stmt := range statements {
		lowerStmt := strings.ToLower(stmt)

		// Определяем тип операции
		var stmtType string
		if strings.HasPrefix(lowerStmt, "select") {
			stmtType = "select"
		} else if strings.HasPrefix(lowerStmt, "update") {
			stmtType = "update"
		} else if strings.HasPrefix(lowerStmt, "delete") {
			stmtType = "delete"
		} else {
			continue
		}

		// Извлекаем таблицы из FROM clause
		tables := extractTablesFromFromClause(stmt)

		// Определяем целевую таблицу
		targetTable := ""
		switch stmtType {
		case "update":
			targetTable = extractUpdateTargetTable(stmt)
		case "delete":
			targetTable = extractDeleteTargetTable(stmt)
		}

		// Подсчитываем, сколько раз каждая таблица встречается в FROM
		tableCounts := make(map[string]int)
		for _, t := range tables {
			normalized := normalizeIdentifier(t.TableName)
			if normalized != "" {
				tableCounts[normalized]++
			}
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
			normalizedTableName := normalizeIdentifier(table.TableName)
			normalizedTarget := normalizeIdentifier(targetTable)
			normalizedAlias := normalizeIdentifier(table.Alias)

			// Проверяем, является ли таблица целевой
			isTarget := false
			if sameTableReference(table.TableName, targetTable) || sameTableReference(table.Alias, targetTable) {
				// Если таблица с тем же именем встречается несколько раз,
				// целевой считается только экземпляр с совпадающим алиасом
				if tableCounts[normalizedTableName] > 1 {
					// Целевая таблица - та, у которой алиас совпадает с именем целевой таблицы
					// (или первый экземпляр без явного алиаса, когда алиас совпадает с именем таблицы)
					if normalizedAlias == normalizedTarget || (normalizedAlias == "" && normalizedTableName == normalizedTarget) {
						isTarget = true
					}
				} else {
					isTarget = true
				}
			}

			if isTarget {
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
	}

	return findings
}

// nullComparisonBinaryRe ищет сравнения вида: expr =/<>/<=/>=/</>  NULL (не IS NULL / IS NOT NULL)
var nullComparisonBinaryRe = regexp.MustCompile(`(?i)(?:^|[^a-zA-Z_])((?:=|<>|!=|<=|>=|<|>)\s*null\b|\bnull\s*(?:=|<>|!=|<=|>=|<|>))`)

// nullParamDefaultRe соответствует строкам объявления параметра или переменной с дефолтом = null:
// @Name   DSTYPE = null,   или   @Name DSTYPE = null
var nullParamDefaultRe = regexp.MustCompile(`(?i)@\w+\s+\w+\s*=\s*null\s*,?\s*$`)

// nullComparisonInRe ищет IN (..., NULL, ...) или IN (NULL)
var nullComparisonInRe = regexp.MustCompile(`(?i)\bin\s*\([^)]*\bnull\b[^)]*\)`)

func (r *Runner) checkNullComparison(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(content), "\n")
	inBlockComment := false

	for lineIdx, rawLine := range lines {
		lineNo := lineIdx + 1

		// Обрабатываем блочные комментарии /* ... */
		stripped, stillInBlock := stripLineComments(rawLine, inBlockComment)
		inBlockComment = stillInBlock

		if strings.TrimSpace(stripped) == "" {
			continue
		}

		// Пропускаем объявления параметров/переменных с дефолтом = null: @Name DSTYPE = null
		if nullParamDefaultRe.MatchString(stripped) {
			continue
		}

		// Проверяем бинарные операторы: = NULL, <> NULL, < NULL и т.д.
		if nullComparisonBinaryRe.MatchString(stripped) {
			findings = append(findings, Finding{
				Rule:             RuleNullComparison,
				Severity:         SeverityPostgreReq,
				Message:          "Сравнение с NULL через оператор недопустимо, используйте IS NULL или IS NOT NULL",
				File:             file.Path,
				Line:             lineNo,
				Object:           strings.TrimSpace(rawLine),
				CurrentProductID: file.DsProductID,
			})
			continue
		}

		// Проверяем IN (NULL) / IN (..., NULL, ...)
		if nullComparisonInRe.MatchString(stripped) {
			findings = append(findings, Finding{
				Rule:             RuleNullComparison,
				Severity:         SeverityPostgreReq,
				Message:          "Сравнение с NULL через IN недопустимо, используйте IS NULL или IS NOT NULL",
				File:             file.Path,
				Line:             lineNo,
				Object:           strings.TrimSpace(rawLine),
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

// stripLineComments удаляет из строки однострочные комментарии (--)
// и обрабатывает блочные комментарии (/* */), возвращая очищенный текст
// и флаг того, что после этой строки мы всё ещё внутри блочного комментария.
func stripLineComments(line string, inBlock bool) (string, bool) {
	var b strings.Builder
	i := 0
	for i < len(line) {
		if inBlock {
			if i+1 < len(line) && line[i] == '*' && line[i+1] == '/' {
				inBlock = false
				i += 2
				continue
			}
			i++
			continue
		}
		// однострочный комментарий
		if i+1 < len(line) && line[i] == '-' && line[i+1] == '-' {
			break
		}
		// начало блочного комментария
		if i+1 < len(line) && line[i] == '/' && line[i+1] == '*' {
			inBlock = true
			i += 2
			continue
		}
		b.WriteByte(line[i])
		i++
	}
	return b.String(), inBlock
}

func (r *Runner) checkShouldBeCP866(file *indexedFile) ([]Finding, error) {
	data, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}

	enc := detectFileEncoding(data)
	if enc == "CP866" || enc == "ASCII" {
		return nil, nil
	}

	msg := fmt.Sprintf("Файл имеет кодировку %s, требуется CP866", enc)
	return []Finding{{
		Rule:             RuleShouldBeCP866,
		Severity:         SeverityPostgreReq,
		Message:          msg,
		File:             file.Path,
		Object:           file.Path,
		CurrentProductID: file.DsProductID,
	}}, nil
}

// detectFileEncoding определяет кодировку файла: "ASCII", "CP866", "UTF-8", "CP1251" или "UNKNOWN".
// Алгоритм:
//  1. Нет байт > 0x7F → ASCII (совместим с CP866)
//  2. Валидный UTF-8 → UTF-8
//  3. Эвристика по маркерным диапазонам:
//     cp866Score  = кол-во байт 0x80–0x9F (А-Я в CP866, редкие спецсимволы в CP1251)
//     cp1251Score = кол-во байт 0xC0–0xDF (А-Я в CP1251, псевдографика в CP866 — редка в тексте)
//     Побеждает бо́льший счёт.
func detectFileEncoding(data []byte) string {
	hasHigh := false
	for _, b := range data {
		if b > 0x7F {
			hasHigh = true
			break
		}
	}
	if !hasHigh {
		return "ASCII"
	}

	if utf8.Valid(data) {
		return "UTF-8"
	}

	var cp866Score, cp1251Score int
	for _, b := range data {
		switch {
		case b >= 0x80 && b <= 0x9F:
			cp866Score++
		case b >= 0xC0 && b <= 0xDF:
			cp1251Score++
		}
	}

	if cp1251Score > cp866Score {
		return "CP1251"
	}
	return "CP866"
}

func (r *Runner) checkTooManyJoins(file *indexedFile) ([]Finding, error) {
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}

	text := string(content)
	statements := splitStatementsWithOffsets(text)
	findings := make([]Finding, 0)

	for _, entry := range statements {
		count := countTopLevelJoins(entry.stmt)
		if count <= MaxJoinsAllowed {
			continue
		}
		// Точный номер строки по сохранённому offset
		lineNo := strings.Count(text[:entry.offset], "\n") + 1
		// Object — первая непустая строка оператора
		firstLine := entry.stmt
		if nl := strings.IndexByte(entry.stmt, '\n'); nl >= 0 {
			firstLine = strings.TrimSpace(entry.stmt[:nl])
		}
		firstLine = strings.TrimSpace(firstLine)
		findings = append(findings, Finding{
			Rule:             RuleTooManyJoins,
			Severity:         SeverityPostgreReq,
			Message:          fmt.Sprintf("Количество JOIN в запросе (%d) превышает допустимое (%d)", count, MaxJoinsAllowed),
			File:             file.Path,
			Line:             lineNo,
			Object:           firstLine,
			CurrentProductID: file.DsProductID,
		})
	}

	return findings, nil
}

type stmtWithOffset struct {
	stmt   string
	offset int
}

func (r *Runner) checkMaxProcParam(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	for _, proc := range parsed.Procedures {
		if proc == nil || proc.ProcName == "" {
			continue
		}
		if len(proc.Params) <= MaxProcParamsAllowed {
			continue
		}
		findings = append(findings, Finding{
			Rule:             RuleMaxProcParam,
			Severity:         SeverityPostgreReq,
			Message:          fmt.Sprintf("Количество параметров процедуры (%d) превышает допустимое (%d)", len(proc.Params), MaxProcParamsAllowed),
			File:             file.Path,
			Line:             proc.LineStart,
			Object:           proc.ProcName,
			CurrentProductID: file.DsProductID,
		})
	}
	return findings, nil
}

// isDataOrPatchPath возвращает true, если путь содержит каталог data или patch (case-insensitive).
func isDataOrPatchPath(path string) bool {
	lower := strings.ToLower(filepath.ToSlash(path))
	for _, seg := range strings.Split(lower, "/") {
		if seg == "data" || seg == "patch" {
			return true
		}
	}
	return false
}

// isTempTable возвращает true, если имя таблицы является временной (#-таблицей).
func isTempTable(name string) bool {
	return strings.HasPrefix(name, "#")
}

// modifyOutProcInsertRe, modifyOutProcUpdateRe, modifyOutProcDeleteRe, modifyOutProcTruncateRe —
// регулярки для детектирования DML-операторов и цели.
var (
	modifyOutProcInsertRe   = regexp.MustCompile(`(?i)^\s*insert\s+(?:into\s+)?([A-Za-z_#][A-Za-z0-9_#]*)`)
	modifyOutProcUpdateRe   = regexp.MustCompile(`(?i)^\s*update\s+([A-Za-z_#][A-Za-z0-9_#]*)`)
	modifyOutProcDeleteRe   = regexp.MustCompile(`(?i)^\s*delete\s+(?:from\s+)?([A-Za-z_#][A-Za-z0-9_#]*)`)
	modifyOutProcTruncateRe = regexp.MustCompile(`(?i)^\s*truncate\s+table\s+([A-Za-z_#][A-Za-z0-9_#]*)`)
)

func (r *Runner) checkModifyOutProc(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	if isDataOrPatchPath(file.Path) {
		return nil, nil
	}

	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}

	// Убираем #define-макросы и маскируем блочные комментарии
	text := removeMacros(string(content))
	text = maskBlockCommentsKeepLines(text)

	// Строим set номеров строк, защищённых телом процедуры (1-based)
	inProc := make(map[int]bool)
	for _, proc := range parsed.Procedures {
		if proc == nil {
			continue
		}
		for ln := proc.LineStart; ln <= proc.LineEnd; ln++ {
			inProc[ln] = true
		}
	}

	lines := strings.Split(text, "\n")
	findings := make([]Finding, 0)

	for i, line := range lines {
		lineNo := i + 1
		if inProc[lineNo] {
			continue
		}

		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		// Однострочный комментарий
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		var tableName string
		var keyword string

		if m := modifyOutProcInsertRe.FindStringSubmatch(trimmed); m != nil {
			tableName = m[1]
			keyword = "INSERT"
		} else if m := modifyOutProcUpdateRe.FindStringSubmatch(trimmed); m != nil {
			// UPDATE STATISTICS — пропускаем
			if strings.HasPrefix(strings.ToLower(m[1]), "statistics") {
				continue
			}
			tableName = m[1]
			keyword = "UPDATE"
		} else if m := modifyOutProcDeleteRe.FindStringSubmatch(trimmed); m != nil {
			tableName = m[1]
			keyword = "DELETE"
		} else if m := modifyOutProcTruncateRe.FindStringSubmatch(trimmed); m != nil {
			tableName = m[1]
			keyword = "TRUNCATE"
		} else {
			continue
		}

		// Пропускаем операции над #-таблицами
		if isTempTable(tableName) {
			continue
		}

		findings = append(findings, Finding{
			Rule:             RuleModifyOutProc,
			Severity:         SeverityPostgreReq,
			Message:          fmt.Sprintf("Оператор %s над таблицей %s вне тела процедуры", keyword, tableName),
			File:             file.Path,
			Line:             lineNo,
			Object:           tableName,
			CurrentProductID: file.DsProductID,
		})
	}

	return findings, nil
}

func (r *Runner) checkEmptyReturn(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")
	inBlockComment := false

	for lineIdx, rawLine := range lines {
		lineNo := lineIdx + 1
		stripped, stillInBlock := stripLineComments(rawLine, inBlockComment)
		inBlockComment = stillInBlock

		if hasEmptyReturn(stripped) {
			findings = append(findings, Finding{
				Rule:             RuleEmptyReturn,
				Severity:         SeverityPostgreReq,
				Message:          "Использование RETURN без явного указания возвращаемого значения не допускается",
				File:             file.Path,
				Line:             lineNo,
				Object:           strings.TrimSpace(rawLine),
				CurrentProductID: file.DsProductID,
			})
		}
	}
	return findings, nil
}

func hasEmptyReturn(line string) bool {
	i := 0
	for i < len(line) {
		// Пропускаем строковые литералы
		if line[i] == '\'' {
			i++
			for i < len(line) {
				if line[i] == '\'' {
					if i+1 < len(line) && line[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}

		// Ищем слово RETURN
		if i+6 <= len(line) && strings.EqualFold(line[i:i+6], "return") {
			beforeOK := i == 0 || !isWordChar(line[i-1])
			afterPos := i + 6
			afterOK := afterPos >= len(line) || !isWordChar(line[afterPos])
			if beforeOK && afterOK {
				remainder := strings.TrimSpace(line[afterPos:])
				if remainder == "" || remainder == ";" {
					return true
				}
			}
		}
		i++
	}
	return false
}

func (r *Runner) checkRawTransactionControl(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")
	inBlockComment := false

	for lineIdx, rawLine := range lines {
		lineNo := lineIdx + 1
		stripped, stillInBlock := stripLineComments(rawLine, inBlockComment)
		inBlockComment = stillInBlock

		if hasRawTransactionControl(stripped) {
			findings = append(findings, Finding{
				Rule:             RuleRawTransactionControl,
				Severity:         SeverityPostgreReq,
				Message:          "Управление транзакциями разрешается только с использованием макросов: BEGIN_TRAN, GOEND, COMMIT_TRAN, __BEGIN_TRAN__, __ERR_TRAN__, __COMMIT_TRAN__, __END_TRAN__",
				File:             file.Path,
				Line:             lineNo,
				Object:           strings.TrimSpace(rawLine),
				CurrentProductID: file.DsProductID,
			})
		}
	}
	return findings, nil
}

func hasRawTransactionControl(line string) bool {
	i := 0
	for i < len(line) {
		// Пропускаем строковые литералы
		if line[i] == '\'' {
			i++
			for i < len(line) {
				if line[i] == '\'' {
					if i+1 < len(line) && line[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}

		// Ищем ключевые слова
		words := []string{"begin", "commit", "rollback", "save", "end"}
		for _, word := range words {
			wlen := len(word)
			if i+wlen <= len(line) && strings.EqualFold(line[i:i+wlen], word) {
				beforeOK := i == 0 || !isWordChar(line[i-1])
				afterPos := i + wlen
				afterOK := afterPos >= len(line) || !isWordChar(line[afterPos])
				if beforeOK && afterOK {
					lower := strings.ToLower(line)
					switch strings.ToLower(word) {
					case "begin":
						if matchNextWord(lower, afterPos, "tran") || matchNextWord(lower, afterPos, "transaction") {
							return true
						}
					case "save":
						if matchNextWord(lower, afterPos, "tran") || matchNextWord(lower, afterPos, "transaction") {
							return true
						}
					case "end":
						if matchNextWord(lower, afterPos, "tran") {
							return true
						}
					case "commit", "rollback":
						return true
					}
				}
			}
		}
		i++
	}
	return false
}

func matchNextWord(lower string, start int, word string) bool {
	i := start
	for i < len(lower) && (lower[i] == ' ' || lower[i] == '\t') {
		i++
	}
	if i+len(word) > len(lower) {
		return false
	}
	if lower[i:i+len(word)] != word {
		return false
	}
	after := i + len(word)
	if after < len(lower) && isWordChar(lower[after]) {
		return false
	}
	return true
}

// toLowerASCII понижает регистр только ASCII-букв (0x41–0x5A), не затрагивая байты > 0x7F.
// Это важно для корректной работы с файлами в кодировке CP866.
func toLowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
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
func splitStatementsForHintType(text string) []string {
	lower := strings.ToLower(text)
	statements := make([]string, 0)
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

		// Если на нулевом уровне и встречаем новое ключевое слово SELECT/UPDATE/DELETE
		if depth == 0 && i > 0 {
			if keywordMatchAt(lower, i, "select") || keywordMatchAt(lower, i, "update") || keywordMatchAt(lower, i, "delete") {
				// Проверяем, что это не часть другого слова
				if i > 0 && isCharWordBoundary(lower[i-1]) {
					// Добавляем предыдущий оператор
					if startIdx < i {
						stmt := strings.TrimSpace(text[startIdx:i])
						if stmt != "" {
							statements = append(statements, stmt)
						}
					}
					startIdx = i
				}
			}
		}
	}

	// Добавляем последний оператор
	if startIdx < len(text) {
		stmt := strings.TrimSpace(text[startIdx:])
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

func (r *Runner) checkDeferredUpdate(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	text := string(content)

	statements := splitStatementsWithOffsets(text)
	for _, s := range statements {
		lower := strings.ToLower(s.stmt)
		if !strings.HasPrefix(lower, "update") {
			continue
		}

		lineNo := 1
		for i := 0; i < s.offset && i < len(text); i++ {
			if text[i] == '\n' {
				lineNo++
			}
		}

		targetTable := extractUpdateTargetTable(s.stmt)
		if targetTable == "" || strings.HasPrefix(strings.ToLower(targetTable), "#") {
			continue
		}

		setColumns := extractSetColumns(s.stmt)
		if len(setColumns) == 0 {
			continue
		}

		tables := extractTablesFromFromClause(s.stmt)

		var indexName string
		for _, t := range tables {
			if sameTableReference(t.TableName, targetTable) || sameTableReference(t.Alias, targetTable) {
				if t.IndexName != "" {
					indexName = t.IndexName
					break
				}
			}
		}

		if indexName == "" {
			continue
		}

		indexColumns, err := r.lookupIndexFieldsByName(indexName)
		if err != nil {
			return nil, err
		}
		if len(indexColumns) == 0 {
			continue
		}

		for _, col := range setColumns {
			for _, idxCol := range indexColumns {
				if strings.EqualFold(col, idxCol) {
					findings = append(findings, Finding{
						Rule:             RuleDeferredUpdate,
						Severity:         SeverityDeployStopper,
						Message:          fmt.Sprintf("Колонка %s входит в индекс %s, используемый в хинте этого UPDATE. Изменение значений индексированных полей вызывает эффект deferred update.", col, indexName),
						File:             file.Path,
						Line:             lineNo,
						Object:           targetTable,
						CurrentProductID: file.DsProductID,
					})
				}
			}
		}
	}

	return findings, nil
}

func (r *Runner) checkInSubQuery(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	text := string(content)

	statements := splitStatementsWithOffsets(text)
	for _, s := range statements {
		lineNo := 1
		for i := 0; i < s.offset && i < len(text); i++ {
			if text[i] == '\n' {
				lineNo++
			}
		}

		positions := findInSubqueryPositions(s.stmt)
		for range positions {
			findings = append(findings, Finding{
				Rule:             RuleInSubQuery,
				Severity:         SeverityDeployStopper,
				Message:          "Использование IN/NOT IN с подзапросом запрещено. Используйте EXISTS для проверки наличия значения.",
				File:             file.Path,
				Line:             lineNo,
				Object:           "",
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
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

func hasSelectInsideParens(lower string, openPos int) bool {
	if openPos >= len(lower) || lower[openPos] != '(' {
		return false
	}
	depth := 1
	inString := false
	for i := openPos + 1; i < len(lower); i++ {
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
			depth--
			if depth == 0 {
				return false
			}
		}
		if depth == 1 && keywordMatchAt(lower, i, "select") {
			return true
		}
	}
	return false
}

func (r *Runner) checkVarcharSize(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Проверяем параметры процедур через parsed
	for _, proc := range parsed.Procedures {
		if proc == nil {
			continue
		}
		for _, param := range proc.Params {
			if isVarCharLikeType(param.Type) && !hasSizeInType(param.Type) {
				findings = append(findings, Finding{
					Rule:             RuleVarcharSize,
					Severity:         SeverityDeployStopper,
					Message:          fmt.Sprintf("Переменная %s объявлена как %s без указания размера.", param.Name, strings.ToUpper(param.Type)),
					File:             file.Path,
					Line:             proc.LineStart,
					Object:           proc.ProcName,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// Проверяем DECLARE в теле файла
	content, err := r.fileContent(file.Path)
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

		// DECLARE @var type, @var2 type
		if idx := findKeywordPosition(lower, "declare"); idx >= 0 && !isInComment(line, idx) {
			declPart := line[idx+len("declare"):]
			// Обрезаем до ; или до ключевых слов на нулевом уровне
			endIdx := len(declPart)
			lowerDecl := strings.ToLower(declPart)
			for _, kw := range []string{"select", "update", "delete", "insert", "begin", "if", "while", "return", "print", "exec", "execute", ";"} {
				if kwIdx := findKeywordPosition(lowerDecl, kw); kwIdx >= 0 {
					if kwIdx < endIdx {
						endIdx = kwIdx
					}
				}
			}
			declPart = declPart[:endIdx]
			items := splitTopLevelCSV(declPart)
			for _, item := range items {
				if varName, varType, fullType := parseVarDeclaration(item); varName != "" {
					if isVarCharLikeType(varType) && !hasSizeInType(fullType) {
						findings = append(findings, Finding{
							Rule:             RuleVarcharSize,
							Severity:         SeverityDeployStopper,
							Message:          fmt.Sprintf("Переменная %s объявлена как %s без указания размера.", varName, strings.ToUpper(varType)),
							File:             file.Path,
							Line:             lineNum,
							Object:           varName,
							CurrentProductID: file.DsProductID,
						})
					}
				}
			}
		}
	}

	return findings, nil
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

func isVarCharLikeType(t string) bool {
	lower := strings.ToLower(t)
	return lower == "varchar" || lower == "nvarchar" || lower == "char" || lower == "nchar"
}

func hasSizeInType(typeExpr string) bool {
	// Проверяем наличие ( сразу после типа с учётом whitespace
	lower := strings.ToLower(typeExpr)
	for i := 0; i < len(lower); i++ {
		if lower[i] == ' ' || lower[i] == '\t' {
			continue
		}
		if isWordChar(lower[i]) || lower[i] == '_' {
			// часть имени типа
			continue
		}
		if lower[i] == '(' {
			return true
		}
		return false
	}
	return false
}

func (r *Runner) checkColumnInsert(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	text := string(content)

	positions := findInsertWithoutColumns(text)
	for _, pos := range positions {
		lineNo := 1
		for i := 0; i < pos && i < len(text); i++ {
			if text[i] == '\n' {
				lineNo++
			}
		}
		findings = append(findings, Finding{
			Rule:             RuleColumnInsert,
			Severity:         SeverityDeployStopper,
			Message:          "В операторе INSERT отсутствует явное перечисление столбцов. Укажите столбцы: INSERT INTO table (col1, col2) VALUES ...",
			File:             file.Path,
			Line:             lineNo,
			Object:           "",
			CurrentProductID: file.DsProductID,
		})
	}

	return findings, nil
}

func findInsertWithoutColumns(text string) []int {
	positions := make([]int, 0)
	lower := strings.ToLower(text)
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
			for {
				macroStart := j
				if j < len(lower) && lower[j] == 'm' && j+1 < len(lower) && lower[j+1] == '_' {
					for j < len(lower) && (isWordChar(lower[j]) || lower[j] == '_') {
						j++
					}
					if j < len(lower) && lower[j] == '(' {
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

func (r *Runner) checkPostgreLabelGotoLevel(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	type gotoRef struct {
		name string
		line int
	}

	gotos := make([]gotoRef, 0)
	labels := make(map[string][]int)
	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
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

		cleaned := strings.Builder{}
		inString := false
		for j := 0; j < len(line); j++ {
			ch := line[j]
			if ch == '\'' {
				if j+1 < len(line) && line[j+1] == '\'' {
					cleaned.WriteString("  ")
					j++
					continue
				}
				inString = !inString
				cleaned.WriteByte(' ')
				continue
			}
			if inString {
				cleaned.WriteByte(' ')
				continue
			}
			cleaned.WriteByte(ch)
		}
		cleanLine := cleaned.String()
		lower := strings.ToLower(cleanLine)

		for j := 0; j < len(lower); j++ {
			if keywordMatchAt(lower, j, "goto") {
				k := j + len("goto")
				for k < len(lower) && (lower[k] == ' ' || lower[k] == '\t') {
					k++
				}
				nameStart := k
				for k < len(lower) && isWordChar(lower[k]) {
					k++
				}
				if nameStart < k {
					labelName := cleanLine[nameStart:k]
					gotos = append(gotos, gotoRef{name: labelName, line: lineNum})
				}
			}
		}

		for j := 0; j < len(lower); j++ {
			if isWordChar(lower[j]) {
				nameStart := j
				for j < len(lower) && isWordChar(lower[j]) {
					j++
				}
				if j < len(lower) && lower[j] == ':' {
					labelName := cleanLine[nameStart:j]
					labels[labelName] = append(labels[labelName], lineNum)
				}
			}
		}
	}

	for _, g := range gotos {
		positions, ok := labels[g.name]
		if !ok {
			continue
		}
		for _, labelLine := range positions {
			if labelLine <= g.line {
				findings = append(findings, Finding{
					Rule:             RulePostgreLabelGotoLevel,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Метка '%s' для GOTO расположена выше оператора перехода.", g.name),
					File:             file.Path,
					Line:             g.line,
					Object:           fmt.Sprintf("goto %s", g.name),
					CurrentProductID: file.DsProductID,
				})
				break
			}
		}
	}

	return findings, nil
}

func (r *Runner) checkDateIntoString(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	text := string(content)
	variableTypes := collectVariableTypes(parsed, text)

	// 1. SELECT @var = expr
	assignRe := regexp.MustCompile(`(?is)^\s*(@[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		var assignments []selectAssignment
		stmt, ok := parseSelectAssignStatement(fragment.QueryText)
		if ok {
			assignments = stmt.Assignments
		} else {
			// fallback: SELECT @var = expr без FROM
			text := strings.TrimSpace(fragment.QueryText)
			lower := strings.ToLower(text)
			if !strings.HasPrefix(lower, "select") {
				continue
			}
			selectPart := strings.TrimSpace(text[len("select"):])
			parts := splitTopLevelCSV(selectPart)
			for _, part := range parts {
				m := assignRe.FindStringSubmatch(strings.TrimSpace(part))
				if len(m) != 3 {
					continue
				}
				assignments = append(assignments, selectAssignment{
					TargetVariable: strings.TrimSpace(m[1]),
					Expression:     strings.TrimSpace(m[2]),
				})
			}
		}
		for _, a := range assignments {
			targetVar := normalizeVariableName(a.TargetVariable)
			targetType, exists := variableTypes[targetVar]
			if !exists || typeGroup(targetType) != "string" {
				continue
			}
			if hasExplicitConversion(a.Expression, targetType) {
				continue
			}
			if isDateExpression(a.Expression, variableTypes) {
				findings = append(findings, Finding{
					Rule:             RuleDateIntoString,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Вставка значения типа datetime в строковую переменную: %s -> %s", a.Expression, targetType),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           a.TargetVariable,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// 2. SET @var = expr
	setRe := regexp.MustCompile(`(?is)^\s*set\s+(@[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		m := setRe.FindStringSubmatch(line)
		if len(m) != 3 {
			continue
		}
		targetVar := normalizeVariableName(m[1])
		targetType, exists := variableTypes[targetVar]
		if !exists || typeGroup(targetType) != "string" {
			continue
		}
		expr := strings.TrimSpace(m[2])
		if hasExplicitConversion(expr, targetType) {
			continue
		}
		if isDateExpression(expr, variableTypes) {
			findings = append(findings, Finding{
				Rule:             RuleDateIntoString,
				Severity:         SeverityPostgreReq,
				Message:          fmt.Sprintf("Вставка значения типа datetime в строковую переменную: %s -> %s", expr, targetType),
				File:             file.Path,
				Line:             i + 1,
				Object:           "@" + targetVar,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	// 3. DECLARE @var TYPE = expr
	declRe := regexp.MustCompile(`(?is)^\s*declare\s+(@[A-Za-z_][A-Za-z0-9_]*)\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*(?:\s*\([^)]*\))?)\s*=\s*(.+)$`)
	for i, line := range lines {
		m := declRe.FindStringSubmatch(line)
		if len(m) != 4 {
			continue
		}
		targetVar := normalizeVariableName(m[1])
		targetType := strings.TrimSpace(m[2])
		if typeGroup(targetType) != "string" {
			continue
		}
		expr := strings.TrimSpace(m[3])
		if hasExplicitConversion(expr, targetType) {
			continue
		}
		if isDateExpression(expr, variableTypes) {
			findings = append(findings, Finding{
				Rule:             RuleDateIntoString,
				Severity:         SeverityPostgreReq,
				Message:          fmt.Sprintf("Вставка значения типа datetime в строковую переменную: %s -> %s", expr, targetType),
				File:             file.Path,
				Line:             i + 1,
				Object:           "@" + targetVar,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	// 4. UPDATE ... SET col = expr
	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		stmt, ok := parseUpdateSetStatement(fragment.QueryText)
		if !ok {
			continue
		}
		for _, a := range stmt.Assignments {
			col := normalizeAssignmentTargetColumn(a.Target, stmt)
			if col == "" || a.Expression == "" {
				continue
			}
			targetType, err := r.db.FindLatestSQLColumnDefinitionType(stmt.TargetTable, col)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, err
			}
			if typeGroup(targetType) != "string" {
				continue
			}
			if hasExplicitConversion(a.Expression, targetType) {
				continue
			}
			if isDateExpression(a.Expression, variableTypes) {
				findings = append(findings, Finding{
					Rule:             RuleDateIntoString,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Вставка значения типа datetime в строковый столбец: %s -> %s.%s", a.Expression, stmt.TargetTable, col),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           fmt.Sprintf("%s.%s", stmt.TargetTable, col),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// 5. INSERT ... SELECT
	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		stmt, ok := parseInsertSelectStatement(fragment.QueryText)
		if !ok {
			continue
		}
		count := len(stmt.TargetColumns)
		if len(stmt.SelectExpressions) < count {
			count = len(stmt.SelectExpressions)
		}
		for i := 0; i < count; i++ {
			col := normalizeIdentifier(stmt.TargetColumns[i])
			expr := strings.TrimSpace(stmt.SelectExpressions[i])
			if col == "" || expr == "" {
				continue
			}
			targetType, err := r.db.FindLatestSQLColumnDefinitionType(stmt.TargetTable, col)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, err
			}
			if typeGroup(targetType) != "string" {
				continue
			}
			if hasExplicitConversion(expr, targetType) {
				continue
			}
			if isDateExpression(expr, variableTypes) {
				findings = append(findings, Finding{
					Rule:             RuleDateIntoString,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Вставка значения типа datetime в строковый столбец: %s -> %s.%s", expr, stmt.TargetTable, col),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           fmt.Sprintf("%s.%s", stmt.TargetTable, col),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// 6. INSERT ... VALUES
	valuesRe := regexp.MustCompile(`(?is)insert\s+(?:into\s+)?([a-z_#][a-z0-9_#]*)[^\(]*\((.*?)\)\s*values\s*\((.*?)\)`)
	for i, line := range lines {
		m := valuesRe.FindStringSubmatch(line)
		if len(m) != 4 {
			continue
		}
		table := strings.TrimSpace(m[1])
		cols := splitTopLevelCSV(m[2])
		exprs := splitTopLevelCSV(m[3])
		count := len(cols)
		if len(exprs) < count {
			count = len(exprs)
		}
		for j := 0; j < count; j++ {
			col := normalizeIdentifier(cols[j])
			expr := strings.TrimSpace(exprs[j])
			if col == "" || expr == "" {
				continue
			}
			targetType, err := r.db.FindLatestSQLColumnDefinitionType(table, col)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, err
			}
			if typeGroup(targetType) != "string" {
				continue
			}
			if hasExplicitConversion(expr, targetType) {
				continue
			}
			if isDateExpression(expr, variableTypes) {
				findings = append(findings, Finding{
					Rule:             RuleDateIntoString,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Вставка значения типа datetime в строковый столбец: %s -> %s.%s", expr, table, col),
					File:             file.Path,
					Line:             i + 1,
					Object:           fmt.Sprintf("%s.%s", table, col),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
}

func isDateExpression(expr string, varTypes map[string]string) bool {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return false
	}
	lower := strings.ToLower(trimmed)

	dateFuncRe := regexp.MustCompile(`(?is)\b(getdate|sysdatetime|sysutcdatetime|getutcdate|datefromparts|datetimefromparts|smalldatetimefromparts|datetime2fromparts|datetimeoffsetfromparts|timefromparts|eomonth|dateadd|datediff|datediff_big|datetrunc)\s*\(`)
	if dateFuncRe.MatchString(lower) {
		return true
	}

	if regexp.MustCompile(`(?i)\bcurrent_timestamp\b`).MatchString(lower) {
		return true
	}

	varRe := regexp.MustCompile(`(?is)^\s*(@[A-Za-z_][A-Za-z0-9_]*)\s*$`)
	m := varRe.FindStringSubmatch(trimmed)
	if len(m) == 2 {
		name := normalizeVariableName(m[1])
		if vtype, ok := varTypes[name]; ok && typeGroup(vtype) == "datetime" {
			return true
		}
	}

	return false
}

func (r *Runner) checkEmptyStringDate(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	text := string(content)
	variableTypes := collectVariableTypes(parsed, text)
	lines := strings.Split(text, "\n")

	// 1. Параметры процедур с DefaultValue = пустая строка
	for _, proc := range parsed.Procedures {
		if proc == nil {
			continue
		}
		for _, p := range proc.Params {
			if typeGroup(p.Type) != "datetime" {
				continue
			}
			if isEmptyStringLiteral(p.DefaultValue) {
				findings = append(findings, Finding{
					Rule:             RuleEmptyStringDate,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Параметр %s типа %s имеет пустую строку по умолчанию", p.Name, p.Type),
					File:             file.Path,
					Line:             proc.LineStart,
					Object:           p.Name,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// 2. SELECT @var = ''
	assignRe := regexp.MustCompile(`(?is)^\s*(@[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		var assignments []selectAssignment
		stmt, ok := parseSelectAssignStatement(fragment.QueryText)
		if ok {
			assignments = stmt.Assignments
		} else {
			text := strings.TrimSpace(fragment.QueryText)
			lower := strings.ToLower(text)
			if !strings.HasPrefix(lower, "select") {
				continue
			}
			selectPart := strings.TrimSpace(text[len("select"):])
			parts := splitTopLevelCSV(selectPart)
			for _, part := range parts {
				m := assignRe.FindStringSubmatch(strings.TrimSpace(part))
				if len(m) != 3 {
					continue
				}
				assignments = append(assignments, selectAssignment{
					TargetVariable: strings.TrimSpace(m[1]),
					Expression:     strings.TrimSpace(m[2]),
				})
			}
		}
		for _, a := range assignments {
			targetVar := normalizeVariableName(a.TargetVariable)
			targetType, exists := variableTypes[targetVar]
			if !exists || typeGroup(targetType) != "datetime" {
				continue
			}
			if isEmptyStringLiteral(a.Expression) {
				findings = append(findings, Finding{
					Rule:             RuleEmptyStringDate,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Присваивание пустой строки переменной %s типа %s", a.TargetVariable, targetType),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           a.TargetVariable,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// 3. SET @var = ''
	setRe := regexp.MustCompile(`(?is)^\s*set\s+(@[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
	for i, line := range lines {
		m := setRe.FindStringSubmatch(line)
		if len(m) != 3 {
			continue
		}
		targetVar := normalizeVariableName(m[1])
		targetType, exists := variableTypes[targetVar]
		if !exists || typeGroup(targetType) != "datetime" {
			continue
		}
		expr := strings.TrimSpace(m[2])
		if isEmptyStringLiteral(expr) {
			findings = append(findings, Finding{
				Rule:             RuleEmptyStringDate,
				Severity:         SeverityPostgreReq,
				Message:          fmt.Sprintf("Присваивание пустой строки переменной @%s типа %s", targetVar, targetType),
				File:             file.Path,
				Line:             i + 1,
				Object:           "@" + targetVar,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	// 4. DECLARE @var datetime = ''
	declRe := regexp.MustCompile(`(?is)^\s*declare\s+(@[A-Za-z_][A-Za-z0-9_]*)\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*(?:\s*\([^)]*\))?)\s*=\s*(.+)$`)
	for i, line := range lines {
		m := declRe.FindStringSubmatch(line)
		if len(m) != 4 {
			continue
		}
		targetVar := normalizeVariableName(m[1])
		targetType := strings.TrimSpace(m[2])
		if typeGroup(targetType) != "datetime" {
			continue
		}
		expr := strings.TrimSpace(m[3])
		if isEmptyStringLiteral(expr) {
			findings = append(findings, Finding{
				Rule:             RuleEmptyStringDate,
				Severity:         SeverityPostgreReq,
				Message:          fmt.Sprintf("Присваивание пустой строки переменной @%s типа %s", targetVar, targetType),
				File:             file.Path,
				Line:             i + 1,
				Object:           "@" + targetVar,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	// 5. UPDATE ... SET dateCol = ''
	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		stmt, ok := parseUpdateSetStatement(fragment.QueryText)
		if !ok {
			continue
		}
		for _, a := range stmt.Assignments {
			col := normalizeAssignmentTargetColumn(a.Target, stmt)
			if col == "" || a.Expression == "" {
				continue
			}
			targetType, err := r.db.FindLatestSQLColumnDefinitionType(stmt.TargetTable, col)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, err
			}
			if typeGroup(targetType) != "datetime" {
				continue
			}
			if isEmptyStringLiteral(a.Expression) {
				findings = append(findings, Finding{
					Rule:             RuleEmptyStringDate,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Присваивание пустой строки столбцу %s.%s типа %s", stmt.TargetTable, col, targetType),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           fmt.Sprintf("%s.%s", stmt.TargetTable, col),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// 6. INSERT ... SELECT
	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		stmt, ok := parseInsertSelectStatement(fragment.QueryText)
		if !ok {
			continue
		}
		count := len(stmt.TargetColumns)
		if len(stmt.SelectExpressions) < count {
			count = len(stmt.SelectExpressions)
		}
		for i := 0; i < count; i++ {
			col := normalizeIdentifier(stmt.TargetColumns[i])
			expr := strings.TrimSpace(stmt.SelectExpressions[i])
			if col == "" || expr == "" {
				continue
			}
			targetType, err := r.db.FindLatestSQLColumnDefinitionType(stmt.TargetTable, col)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, err
			}
			if typeGroup(targetType) != "datetime" {
				continue
			}
			if isEmptyStringLiteral(expr) {
				findings = append(findings, Finding{
					Rule:             RuleEmptyStringDate,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Присваивание пустой строки столбцу %s.%s типа %s", stmt.TargetTable, col, targetType),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           fmt.Sprintf("%s.%s", stmt.TargetTable, col),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// 7. INSERT ... VALUES
	valuesRe := regexp.MustCompile(`(?is)insert\s+(?:into\s+)?([a-z_#][a-z0-9_#]*)[^\(]*\((.*?)\)\s*values\s*\((.*?)\)`)
	for i, line := range lines {
		m := valuesRe.FindStringSubmatch(line)
		if len(m) != 4 {
			continue
		}
		table := strings.TrimSpace(m[1])
		cols := splitTopLevelCSV(m[2])
		exprs := splitTopLevelCSV(m[3])
		count := len(cols)
		if len(exprs) < count {
			count = len(exprs)
		}
		for j := 0; j < count; j++ {
			col := normalizeIdentifier(cols[j])
			expr := strings.TrimSpace(exprs[j])
			if col == "" || expr == "" {
				continue
			}
			targetType, err := r.db.FindLatestSQLColumnDefinitionType(table, col)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, err
			}
			if typeGroup(targetType) != "datetime" {
				continue
			}
			if isEmptyStringLiteral(expr) {
				findings = append(findings, Finding{
					Rule:             RuleEmptyStringDate,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Присваивание пустой строки столбцу %s.%s типа %s", table, col, targetType),
					File:             file.Path,
					Line:             i + 1,
					Object:           fmt.Sprintf("%s.%s", table, col),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// 8. convert(datetime, '') / cast('' as datetime)
	convertRe := regexp.MustCompile(`(?is)\bconvert\s*\(\s*([a-z_][a-z0-9_]*(?:\s*\([^)]*\))?)\s*,\s*(.+?)\s*\)`)
	castRe := regexp.MustCompile(`(?is)\bcast\s*\(\s*(.+?)\s+as\s+([a-z_][a-z0-9_]*(?:\s*\([^)]*\))?)\s*\)`)
	for i, line := range lines {
		for _, m := range convertRe.FindAllStringSubmatch(line, -1) {
			if len(m) >= 3 {
				convType := strings.TrimSpace(m[1])
				expr := strings.TrimSpace(m[2])
				if typeGroup(convType) == "datetime" && isEmptyStringLiteral(expr) {
					findings = append(findings, Finding{
						Rule:             RuleEmptyStringDate,
						Severity:         SeverityPostgreReq,
						Message:          fmt.Sprintf("Передача пустой строки в convert(%s, ...)", convType),
						File:             file.Path,
						Line:             i + 1,
						Object:           fmt.Sprintf("convert(%s, %s)", convType, expr),
						CurrentProductID: file.DsProductID,
					})
				}
			}
		}
		for _, m := range castRe.FindAllStringSubmatch(line, -1) {
			if len(m) >= 3 {
				expr := strings.TrimSpace(m[1])
				castType := strings.TrimSpace(m[2])
				if typeGroup(castType) == "datetime" && isEmptyStringLiteral(expr) {
					findings = append(findings, Finding{
						Rule:             RuleEmptyStringDate,
						Severity:         SeverityPostgreReq,
						Message:          fmt.Sprintf("Передача пустой строки в cast(... as %s)", castType),
						File:             file.Path,
						Line:             i + 1,
						Object:           fmt.Sprintf("cast(%s as %s)", expr, castType),
						CurrentProductID: file.DsProductID,
					})
				}
			}
		}
	}

	return findings, nil
}

func isEmptyStringLiteral(expr string) bool {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return false
	}
	re := regexp.MustCompile(`(?is)^\s*(N)?\s*['"]\s*['"]\s*$`)
	if re.MatchString(trimmed) {
		return true
	}
	// Литерал с пробелами внутри: '   ' или "   "
	re2 := regexp.MustCompile(`(?is)^\s*(N)?\s*['"][\s]*['"]\s*$`)
	return re2.MatchString(trimmed)
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
