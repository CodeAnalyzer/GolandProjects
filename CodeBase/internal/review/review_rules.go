package review

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
)

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
					parenDepth = countParensRespectingStrings(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParensRespectingStrings(line)

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
					parenDepth = countParensRespectingStrings(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParensRespectingStrings(line)

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

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	contentStr := macroResult.Content
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
					parenDepth = countParensRespectingStrings(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParensRespectingStrings(line)

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

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	contentStr := macroResult.Content
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
					parenDepth = countParensRespectingStrings(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParensRespectingStrings(line)

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
				parenDepth = countParensRespectingStrings(line)
			}
			continue
		}

		// Мы внутри тела процедуры
		parenDepth += countParensRespectingStrings(line)

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
		if betweenExpr := extractBetweenWithMathOp(lower); betweenExpr != "" {
			findings = append(findings, Finding{
				Rule:             RuleMathOperations,
				Severity:         SeverityDeployStopper,
				Message:          "Обнаружена математическая операция в условии BETWEEN — риск переполнения типа при расширении результата",
				File:             file.Path,
				Line:             lineNum,
				Object:           betweenExpr,
				CurrentProductID: file.DsProductID,
			})
			continue
		}

		// Проверяем сравнения с математическими операциями
		if expr := extractComparisonWithMathOp(lower); expr != "" {
			findings = append(findings, Finding{
				Rule:             RuleMathOperations,
				Severity:         SeverityDeployStopper,
				Message:          "Обнаружена математическая операция в условии сравнения — риск переполнения типа при расширении результата",
				File:             file.Path,
				Line:             lineNum,
				Object:           expr,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

func (r *Runner) checkExistsWithAndInIf(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	inBlockComment := false

	for i := 0; i < len(lines); i++ {
		lineNum := i + 1

		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		if inBlockComment {
			if idx := strings.Index(lines[i], "*/"); idx >= 0 {
				inBlockComment = false
				lines[i] = lines[i][idx+2:]
			} else {
				continue
			}
		}

		for {
			if idx := strings.Index(lines[i], "/*"); idx >= 0 {
				endIdx := strings.Index(lines[i][idx:], "*/")
				if endIdx > 0 {
					lines[i] = lines[i][:idx] + " " + lines[i][idx+endIdx+2:]
				} else {
					inBlockComment = true
					lines[i] = lines[i][:idx]
					break
				}
			} else {
				break
			}
		}

		if inBlockComment {
			continue
		}

		lower := strings.ToLower(lines[i])

		// Ищем IF с условием, содержащим AND
		if !strings.Contains(lower, "if ") {
			continue
		}
		ifIdx := strings.Index(lower, "if ")
		if ifIdx > 0 && isOperandChar(lower[ifIdx-1]) {
			continue
		}

		// Накапливаем многострочное условие IF ... BEGIN
		conditionLines := []string{lines[i]}
		conditionLower := strings.ToLower(lines[i])
		// Если на одной строке с IF нет BEGIN — собираем последующие строки
		if !strings.Contains(conditionLower, "begin") {
			for j := i + 1; j < len(lines) && j < i+20; j++ {
				jLower := strings.ToLower(strings.TrimSpace(lines[j]))
				// Останавливаемся, если встретили начало тела IF (без BEGIN)
				// — SQL-оператор или новый IF указывают на начало тела, а не продолжение условия
				if isIfBodyStart(jLower) {
					break
				}
				conditionLines = append(conditionLines, lines[j])
				if strings.Contains(jLower, "begin") {
					break
				}
				// Если встретили конец условия без begin (например, then или else)
				if strings.HasPrefix(jLower, "then") || strings.HasPrefix(jLower, "else") {
					break
				}
			}
		}

		fullCondition := strings.Join(conditionLines, " ")
		fullLower := strings.ToLower(fullCondition)

		if hasIfWithAndAndQueryMulti(fullLower) {
			// Извлекаем условие IF (от "if" до "begin") для object
			objText := fullCondition
			if loc := beginKeywordRe.FindStringIndex(objText); loc != nil {
				objText = objText[:loc[0]]
			}
			// Нормализуем пробелы и переносы строк
			objText = regexp.MustCompile(`\s+`).ReplaceAllString(strings.TrimSpace(objText), " ")
			// Ограничиваем длину
			if len(objText) > 80 {
				objText = objText[:80] + "..."
			}
			findings = append(findings, Finding{
				Rule:             RuleExistsWithAndInIf,
				Severity:         SeverityDeployStopper,
				Message:          "Обнаружено условие IF с запросом к таблицам и AND — запрос может выполняться даже при ложном условии. Используйте вложенный IF",
				File:             file.Path,
				Line:             lineNum,
				Object:           objText,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

func (r *Runner) checkIndexExistsInDB(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	contentStr := macroResult.Content
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
					parenDepth = countParensRespectingStrings(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParensRespectingStrings(line)

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

		// Числовые значения индекса (например 0 = clustered) — это ID индекса, не имя;
		// проверку существования для них не выполняем
		if isNumericIndex(indexName) {
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

		// Ищем реальную строку с хинтом внутри буфера оператора
		hintLine := findHintLineInBuffer(lines, tableName, table.Hint, table.IndexName)
		findingLine := startLine + hintLine

		findings = append(findings, Finding{
			Rule:             RuleIndexExistsInDB,
			Severity:         SeverityDeployStopper,
			Message:          fmt.Sprintf("Для таблицы %s не найден индекс %s, указанный в %s", tableName, indexName, table.Hint),
			File:             file.Path,
			Line:             findingLine,
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

		// Пропускаем строки-макросы (M_*, PROFILE_*, __*__) — это препроцессорные
		// директивы (логирование, профилирование), а не реальное использование параметра
		if strings.HasPrefix(trimmed, "M_") || strings.HasPrefix(trimmed, "PROFILE_") || strings.HasPrefix(trimmed, "__") {
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
		// Используем проверку границ слова, чтобы @AmountPrc не матчило внутри @AmountPrcOvrDbt
		if containsParamUsage(lower, strings.ToLower(paramPattern)) {
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

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	contentStr := macroResult.Content
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

			targetType, err := r.cachedFindColumnDefinitionType(stmt.TargetTable, targetColumn)
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

			targetType, err := r.cachedFindColumnDefinitionType(stmt.TargetTable, targetColumn)
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
		typeName, err := r.cachedFindColumnDefinitionType(tableName, ref.Column)
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

	typeName, err := r.cachedFindColumnDefinitionType(defaultTable, columnName)
	if err != nil {
		return nil
	}

	return []string{typeName}
}

func (r *Runner) checkAnsiInJoin(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	contentStr := macroResult.Content

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
					parenDepth = countParensRespectingStrings(line)
				}
			}
		} else {
			// Мы внутри INSERT - добавляем строку в буфер
			insertBuffer = append(insertBuffer, line)
			parenDepth += countParensRespectingStrings(line)

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

	// Исключаем temp-таблицы (#...) — rowlock на session-local temp tables бессмысленен
	if strings.HasPrefix(tableName, "#") {
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
	inBlockComment := false

	for i, line := range lines {
		lineNum := i + 1

		// Обрабатываем блочные комментарии /* ... */
		if inBlockComment {
			if idx := strings.Index(line, "*/"); idx >= 0 {
				inBlockComment = false
				line = line[idx+2:]
				if strings.TrimSpace(line) == "" {
					continue
				}
			} else {
				continue
			}
		}
		// Проверяем начало блочного комментария
		if idx := strings.Index(line, "/*"); idx >= 0 {
			beforeComment := line[:idx]
			rest := line[idx+2:]
			if strings.Contains(rest, "*/") {
				endIdx := strings.Index(rest, "*/")
				line = beforeComment + rest[endIdx+2:]
			} else {
				line = beforeComment
				inBlockComment = true
			}
		}

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		if strings.HasPrefix(trimmed, "//") {
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
					parenDepth = countParensRespectingStrings(line)
				}
			}
			continue
		}

		conditionBuffer = append(conditionBuffer, line)
		parenDepth += countParensRespectingStrings(line)

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
				parenDepth = countParensRespectingStrings(line)
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

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	contentStr := macroResult.Content
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
					parenDepth = countParensRespectingStrings(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParensRespectingStrings(line)

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
					parenDepth = countParensRespectingStrings(line)
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

func (r *Runner) checkTableHintExists(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	contentStr := macroResult.Content
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
					parenDepth = countParensRespectingStrings(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParensRespectingStrings(line)

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
					parenDepth = countParensRespectingStrings(line)
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

func (r *Runner) checkTableHintIsRight(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	contentStr := macroResult.Content
	lines := strings.Split(contentStr, "\n")

	inStatement := false
	stmtStartLine := 0
	stmtBuffer := make([]string, 0)
	parenDepth := 0
	inBlockComment := false
	isInsertStmt := false // INSERT...SELECT: не разрываем на внутреннем SELECT

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
			kw, startIdx := findStatementStartHint(lower)
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
					parenDepth = countParensRespectingStrings(line)
					isInsertStmt = (kw == "insert")
				}
			}
			continue
		}

		if nextKw, nextStartIdx := findStatementStartHint(lower); nextStartIdx >= 0 && !isInComment(line, nextStartIdx) {
			depthBefore := 0
			for j := 0; j < nextStartIdx; j++ {
				switch line[j] {
				case '(':
					depthBefore++
				case ')':
					depthBefore--
				}
			}
			// Для INSERT...SELECT не разрываем на внутреннем SELECT
			if isInsertStmt && nextKw == "select" {
				// продолжаем накапливать буфер
			} else if parenDepth == 0 && depthBefore == 0 {
				items := analyzeStatementForHintType(stmtBuffer, stmtStartLine, file)
				findings = append(findings, items...)

				stmtStartLine = lineNum
				stmtBuffer = []string{line}
				parenDepth = countParensRespectingStrings(line)
				isInsertStmt = (nextKw == "insert")
				continue
			}
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParensRespectingStrings(line)
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

	// Вычисляем смещения строк в fullText
	offsets := make([]int, len(lines)+1)
	offsets[0] = 0
	for i := 0; i < len(lines); i++ {
		offsets[i+1] = offsets[i] + len(lines[i]) + 1 // +1 за пробел
	}

	// Вычисляем смещение обрезки от normalizeHintStatementText
	trimOffset := 0
	if len(trimmedText) < len(fullText) {
		fullLower := strings.ToLower(fullText)
		trimLower := strings.ToLower(trimmedText)
		if idx := strings.Index(fullLower, trimLower); idx >= 0 {
			trimOffset = idx
		}
	}

	// Разбиваем текст на отдельные операторы SELECT/UPDATE/DELETE на нулевом уровне
	statements := splitStatementsForHintType(trimmedText)

	for _, stmtRange := range statements {
		stmt := stmtRange.Text
		stmtStartPos := stmtRange.StartPos + trimOffset

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

		// Вычисляем actualStartLine по позиции в fullText
		actualStartLine := startLine
		for i := 0; i < len(lines); i++ {
			if offsets[i] <= stmtStartPos && stmtStartPos < offsets[i+1] {
				actualStartLine = startLine + i
				break
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
					Line:             actualStartLine,
					Object:           table.TableName,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings
}

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

		// Пропускаем строки присвоения в SELECT: @Var = null (continuation многострочного SELECT)
		if nullSelectAssignRe.MatchString(stripped) {
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

func (r *Runner) checkModifyOutProc(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	if isDataOrPatchPath(file.Path) {
		return nil, nil
	}

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	text := maskBlockCommentsKeepLines(macroResult.Content)

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

func (r *Runner) checkColumnInsert(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	text := macroResult.Content

	positions := findInsertWithoutColumns(text)
	for _, pos := range positions {
		lineNo := strings.Count(text[:pos], "\n") + 1
		tableName := extractInsertTableName(text, pos)
		findings = append(findings, Finding{
			Rule:             RuleColumnInsert,
			Severity:         SeverityDeployStopper,
			Message:          "В операторе INSERT отсутствует явное перечисление столбцов. Укажите столбцы: INSERT INTO table (col1, col2) VALUES ...",
			File:             file.Path,
			Line:             lineNo,
			Object:           tableName,
			CurrentProductID: file.DsProductID,
		})
	}

	return findings, nil
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
			targetType, err := r.cachedFindColumnDefinitionType(stmt.TargetTable, col)
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
			targetType, err := r.cachedFindColumnDefinitionType(stmt.TargetTable, col)
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
			targetType, err := r.cachedFindColumnDefinitionType(table, col)
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

func (r *Runner) checkVarUseAfterCursor(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	text := macroResult.Content
	lines := strings.Split(text, "\n")

	cursorVars := parseAllFetchIntoStatements(text)
	if len(cursorVars) == 0 {
		return findings, nil
	}

	deallocs := parseDeallocateStatements(text)

	// Для каждого DEALLOCATE ищем использование переменных после него
	for _, da := range deallocs {
		cursorName := normalizeIdentifier(da.CursorName)
		vars, exists := cursorVars[cursorName]
		if !exists || len(vars) == 0 {
			continue
		}

		// Сканируем строки после DEALLOCATE
		for i := da.Line; i < len(lines); i++ {
			line := lines[i]
			lower := strings.ToLower(line)

			// Пропускаем строки с declare/fetch/open/close/deallocate/для этого курсора
			if strings.Contains(lower, "declare") && strings.Contains(lower, cursorName) {
				continue
			}
			if strings.Contains(lower, "open") && strings.Contains(lower, cursorName) {
				continue
			}
			if strings.Contains(lower, "close") && strings.Contains(lower, cursorName) {
				continue
			}
			if strings.Contains(lower, "fetch") && strings.Contains(lower, cursorName) {
				continue
			}
			if strings.Contains(lower, "__fetch_next__") && strings.Contains(lower, cursorName) {
				continue
			}
			if strings.Contains(lower, "deallocate") && strings.Contains(lower, cursorName) {
				continue
			}
			if strings.Contains(lower, "__deallocate_cursor__") && strings.Contains(lower, cursorName) {
				continue
			}

			// Ищем использование любой переменной из курсора
			for _, v := range vars {
				varLower := strings.ToLower(v)
				// Проверяем что переменная используется как слово (не declare/fetch etc)
				if !strings.Contains(lower, varLower) {
					continue
				}
				// Проверяем что это не declare с инициализацией той же переменной
				if strings.Contains(lower, "declare") && strings.Contains(lower, varLower) {
					continue
				}
				// Проверяем что это не set/fetch с той же переменной (переопределение)
				if strings.HasPrefix(lower, "set") && strings.Contains(lower, varLower) {
					continue
				}
				// Проверяем что это не select @var = ... (переопределение)
				if strings.Contains(lower, "select") && strings.Contains(lower, varLower+"=") {
					continue
				}

				findings = append(findings, Finding{
					Rule:             RuleVarUseAfterCursor,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Использование переменной %s из курсора %s после его деалокации", v, da.CursorName),
					File:             file.Path,
					Line:             i + 1,
					Object:           v,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
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
			targetType, err := r.cachedFindColumnDefinitionType(stmt.TargetTable, col)
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
			targetType, err := r.cachedFindColumnDefinitionType(stmt.TargetTable, col)
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
			targetType, err := r.cachedFindColumnDefinitionType(table, col)
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

func (r *Runner) checkExcessProcParams(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	calls := dedupeProcedureCalls(parsed.Calls)
	findings := make([]Finding, 0)

	for _, call := range calls {
		params, err := r.lookupProcedureParams(call.Name)
		if err != nil {
			if err == sql.ErrNoRows {
				continue // Процедура отсутствует в БД — это ловит execNotExistsProc
			}
			return nil, err
		}

		if call.Line < 1 || call.Line > len(r.exec.lines) {
			continue
		}
		callText := collectExecCallLines(r.exec.lines, call.Line)

		args := parseExecArguments(callText, call.Name)
		if hasFinding, detail := validateExecArguments(args, params); hasFinding {
			findings = append(findings, Finding{
				Rule:             RuleExcessProcParams,
				Severity:         SeverityPostgreReq,
				Message:          "Передача лишних параметров или дублирование параметров в вызове процедуры" + detail,
				File:             file.Path,
				Line:             call.Line,
				Object:           call.Name,
				CurrentProductID: file.DsProductID,
			})
		}
	}
	return findings, nil
}

func (r *Runner) checkDuplicateOutputVariable(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	calls := dedupeProcedureCalls(parsed.Calls)
	findings := make([]Finding, 0)

	for _, call := range calls {
		if call.Line < 1 || call.Line > len(r.exec.lines) {
			continue
		}
		callText := collectExecCallLines(r.exec.lines, call.Line)

		args := parseExecArguments(callText, call.Name)

		params, err := r.lookupProcedureParams(call.Name)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}

		paramMap := make(map[string]model.SQLParam)
		for _, p := range params {
			paramMap[normalizeIdentifier(p.Name)] = p
		}

		seenOutVars := make(map[string][]string) // varName -> list of parameter names
		for i, arg := range args {
			isOut := arg.IsOutput

			if !isOut && len(params) > 0 {
				if arg.IsNamed {
					if p, exists := paramMap[arg.Name]; exists {
						dir := strings.ToLower(p.Direction)
						if dir == "out" || dir == "inout" {
							isOut = true
						}
					}
				} else {
					if i < len(params) {
						p := params[i]
						dir := strings.ToLower(p.Direction)
						if dir == "out" || dir == "inout" {
							isOut = true
						}
					}
				}
			}

			if isOut && arg.VarName != "" {
				paramLabel := fmt.Sprintf("#%d", i+1)
				if arg.IsNamed {
					paramLabel = "@" + arg.Name
				}
				seenOutVars[arg.VarName] = append(seenOutVars[arg.VarName], paramLabel)
			}
		}

		for varName, params := range seenOutVars {
			if len(params) > 1 {
				findings = append(findings, Finding{
					Rule:             RuleDuplicateOutputVariable,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("В вызове процедуры в качестве OUT параметров используется одна и та же переменная %s для параметров: %s", varName, strings.Join(params, ", ")),
					File:             file.Path,
					Line:             call.Line,
					Object:           call.Name,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}
	return findings, nil
}

// checkUseOnlyDeclaredCursors проверяет, что все используемые курсоры были объявлены в том же scope
func (r *Runner) checkUseOnlyDeclaredCursors(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	// Читаем файл построчно
	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")

	// Регулярки для поиска объявлений и использований курсоров
	// DECLARE name CURSOR FOR или declare name insensitive cursor for
	declareCursorRe := regexp.MustCompile(`(?i)\bDECLARE\s+(#?\w+)\s+(?:INSENSITIVE\s+)?CURSOR\s+FOR\b`)
	// __DECLARE_CURSOR__(NAME)
	declareMacroRe := regexp.MustCompile(`(?i)\b__DECLARE_CURSOR__\s*\(\s*(#?\w+)\s*\)`)

	// OPEN name, FETCH ... FROM name, FETCH name INTO, CLOSE name, DEALLOCATE name
	openCursorRe := regexp.MustCompile(`(?i)\bOPEN\s+(#?\w+)`)
	fetchCursorRe := regexp.MustCompile(`(?i)\bFETCH\s+(?:\w+\s+)*?FROM\s+(#?\w+)`)
	// FETCH без FROM: fetch cursor_name into ... или __FETCH_NEXT__ cursor_name into ...
	// Ищем имя сразу после FETCH/__FETCH_NEXT__ (или NEXT/PRIOR) перед INTO - без FROM
	fetchCursorDirectRe := regexp.MustCompile(`(?i)\b(?:FETCH|__FETCH_NEXT__)\s+(?:NEXT\s+|PRIOR\s+|FIRST\s+|LAST\s+)?(#?\w+)\s+INTO\b`)
	closeCursorRe := regexp.MustCompile(`(?i)\bCLOSE\s+(#?\w+)`)
	deallocCursorRe := regexp.MustCompile(`(?i)\bDEALLOCATE\s+(?:CURSOR\s+)?(#?\w+)`)
	// Макрос для DEALLOCATE
	deallocMacroRe := regexp.MustCompile(`(?i)\b__DEALLOCATE_CURSOR__\s*\(\s*(#?\w+)\s*\)`)

	// Системные курсоры начинаются с @@ - исключаем их
	systemCursorRe := regexp.MustCompile(`^@@`)

	// Проверяем курсоры для каждой процедуры
	for _, proc := range parsed.Procedures {
		declaredCursors := make(map[string]int) // имя курсора -> номер строки объявления

		// Определяем границы процедуры (индексы строк 0-based)
		startLine := proc.LineStart - 1
		endLine := proc.LineEnd
		if startLine < 0 {
			startLine = 0
		}
		if endLine > len(lines) {
			endLine = len(lines)
		}

		// Первый проход: собираем объявленные курсоры
		for i := startLine; i < endLine && i < len(lines); i++ {
			line := lines[i]
			lineNum := i + 1

			// Удаляем комментарии для чистого анализа
			cleanLine := removeComments(line)

			// Ищем DECLARE ... CURSOR FOR
			matches := declareCursorRe.FindStringSubmatch(cleanLine)
			if len(matches) > 1 {
				cursorName := strings.ToLower(matches[1])
				// Пропускаем временные курсоры (начинаются с #)
				if !strings.HasPrefix(cursorName, "#") {
					declaredCursors[cursorName] = lineNum
				}
				continue
			}

			// Ищем __DECLARE_CURSOR__(NAME)
			macroMatches := declareMacroRe.FindStringSubmatch(cleanLine)
			if len(macroMatches) > 1 {
				cursorName := strings.ToLower(macroMatches[1])
				if !strings.HasPrefix(cursorName, "#") {
					declaredCursors[cursorName] = lineNum
				}
			}
		}

		// Второй проход: проверяем использования курсоров
		for i := startLine; i < endLine && i < len(lines); i++ {
			line := lines[i]
			lineNum := i + 1

			cleanLine := removeComments(line)

			// Проверяем OPEN
			if matches := openCursorRe.FindStringSubmatch(cleanLine); len(matches) > 1 {
				cursorName := strings.ToLower(matches[1])
				if !systemCursorRe.MatchString(cursorName) && !strings.HasPrefix(cursorName, "#") {
					if _, declared := declaredCursors[cursorName]; !declared {
						findings = append(findings, Finding{
							Rule:             RuleUseOnlyDeclaredCursors,
							Severity:         SeverityPostgreReq,
							Message:          fmt.Sprintf("Использование необъявленного курсора '%s' в операции OPEN", matches[1]),
							File:             file.Path,
							Line:             lineNum,
							Object:           proc.ProcName,
							CurrentProductID: file.DsProductID,
						})
					}
				}
			}

			// Проверяем FETCH
			if matches := fetchCursorRe.FindStringSubmatch(cleanLine); len(matches) > 1 {
				cursorName := strings.ToLower(matches[1])
				if !systemCursorRe.MatchString(cursorName) && !strings.HasPrefix(cursorName, "#") {
					if _, declared := declaredCursors[cursorName]; !declared {
						findings = append(findings, Finding{
							Rule:             RuleUseOnlyDeclaredCursors,
							Severity:         SeverityPostgreReq,
							Message:          fmt.Sprintf("Использование необъявленного курсора '%s' в операции FETCH", matches[1]),
							File:             file.Path,
							Line:             lineNum,
							Object:           proc.ProcName,
							CurrentProductID: file.DsProductID,
						})
					}
				}
			}

			// Проверяем FETCH без FROM (fetch cursor_name into ...)
			if matches := fetchCursorDirectRe.FindStringSubmatch(cleanLine); len(matches) > 1 {
				cursorName := strings.ToLower(matches[1])
				if !systemCursorRe.MatchString(cursorName) && !strings.HasPrefix(cursorName, "#") {
					if _, declared := declaredCursors[cursorName]; !declared {
						findings = append(findings, Finding{
							Rule:             RuleUseOnlyDeclaredCursors,
							Severity:         SeverityPostgreReq,
							Message:          fmt.Sprintf("Использование необъявленного курсора '%s' в операции FETCH (возможно опечатка?)", matches[1]),
							File:             file.Path,
							Line:             lineNum,
							Object:           proc.ProcName,
							CurrentProductID: file.DsProductID,
						})
					}
				}
			}

			// Проверяем CLOSE
			if matches := closeCursorRe.FindStringSubmatch(cleanLine); len(matches) > 1 {
				cursorName := strings.ToLower(matches[1])
				if !systemCursorRe.MatchString(cursorName) && !strings.HasPrefix(cursorName, "#") {
					if _, declared := declaredCursors[cursorName]; !declared {
						findings = append(findings, Finding{
							Rule:             RuleUseOnlyDeclaredCursors,
							Severity:         SeverityPostgreReq,
							Message:          fmt.Sprintf("Использование необъявленного курсора '%s' в операции CLOSE", matches[1]),
							File:             file.Path,
							Line:             lineNum,
							Object:           proc.ProcName,
							CurrentProductID: file.DsProductID,
						})
					}
				}
			}

			// Проверяем DEALLOCATE
			if matches := deallocCursorRe.FindStringSubmatch(cleanLine); len(matches) > 1 {
				cursorName := strings.ToLower(matches[1])
				if !systemCursorRe.MatchString(cursorName) && !strings.HasPrefix(cursorName, "#") {
					if _, declared := declaredCursors[cursorName]; !declared {
						findings = append(findings, Finding{
							Rule:             RuleUseOnlyDeclaredCursors,
							Severity:         SeverityPostgreReq,
							Message:          fmt.Sprintf("Использование необъявленного курсора '%s' в операции DEALLOCATE", matches[1]),
							File:             file.Path,
							Line:             lineNum,
							Object:           proc.ProcName,
							CurrentProductID: file.DsProductID,
						})
					}
				}
			}

			// Проверяем __DEALLOCATE_CURSOR__(NAME)
			if matches := deallocMacroRe.FindStringSubmatch(cleanLine); len(matches) > 1 {
				cursorName := strings.ToLower(matches[1])
				if !systemCursorRe.MatchString(cursorName) && !strings.HasPrefix(cursorName, "#") {
					if _, declared := declaredCursors[cursorName]; !declared {
						findings = append(findings, Finding{
							Rule:             RuleUseOnlyDeclaredCursors,
							Severity:         SeverityPostgreReq,
							Message:          fmt.Sprintf("Использование необъявленного курсора '%s' в операции DEALLOCATE (макрос)", matches[1]),
							File:             file.Path,
							Line:             lineNum,
							Object:           proc.ProcName,
							CurrentProductID: file.DsProductID,
						})
					}
				}
			}
		}
	}

	return findings, nil
}

// checkCursorFetchArguments проверяет, что FETCH INTO из курсора содержит
// столько же переменных, сколько выражений в SELECT объявления курсора.
func (r *Runner) checkCursorFetchArguments(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	content, err := r.fileContent(file.Path)
	if err != nil {
		return nil, err
	}

	contentStr := string(content)
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

		fetchCount := len(stmt.Variables)
		declareCount := len(cursorDecl.SelectExpressions)
		if fetchCount == declareCount {
			continue
		}

		findings = append(findings, Finding{
			Rule:             RuleCursorFetchArguments,
			Severity:         SeverityPostgreReq,
			Message:          fmt.Sprintf("FETCH из курсора %s: %d переменных, а в DECLARE %d выражений", stmt.CursorName, fetchCount, declareCount),
			File:             file.Path,
			Line:             fragment.LineNumber,
			Object:           stmt.CursorName,
			CurrentProductID: file.DsProductID,
		})
	}

	return findings, nil
}

// checkUsageVarInSameSelect проверяет, что в одном SELECT-операторе
// переменная, присваиваемая в одном assignment, не используется в выражении
// другого assignment, вычисляемого позже.
func (r *Runner) checkUsageVarInSameSelect(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	assignRe := regexp.MustCompile(`(?is)^\s*(@[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)

	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}

		var assignments []selectAssignment
		queryText := strings.TrimSpace(fragment.QueryText)
		queryText = truncateAtStatementBoundary(queryText)

		stmt, ok := parseSelectAssignStatement(queryText)
		if ok {
			assignments = stmt.Assignments
		} else {
			lower := strings.ToLower(queryText)
			if !strings.HasPrefix(lower, "select") {
				continue
			}
			selectPart := strings.TrimSpace(queryText[len("select"):])
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

		// Дополнительный проход: извлекаем assignment'ы из CASE-выражений
		lower := strings.ToLower(queryText)
		if strings.HasPrefix(lower, "select") {
			fromPos := findTopLevelKeywordPosition(queryText, "from")
			var selectPart string
			if fromPos >= 0 {
				selectPart = strings.TrimSpace(queryText[len("select"):fromPos])
			} else {
				selectPart = strings.TrimSpace(queryText[len("select"):])
			}
			parts := splitTopLevelCSV(selectPart)
			for _, part := range parts {
				trimmedPart := strings.TrimSpace(part)
				if !strings.HasPrefix(strings.ToLower(trimmedPart), "case") {
					continue
				}
				assignments = append(assignments, parseCasePartAssignments(trimmedPart)...)
			}
		}

		if len(assignments) < 2 {
			continue
		}

		seenVars := make(map[string]bool)
		for j, a := range assignments {
			for seenVar := range seenVars {
				if containsVarReference(a.Expression, seenVar) {
					lineOffset := findAssignmentLineOffset(queryText, assignments, j)
					findings = append(findings, Finding{
						Rule:             RuleUsageVarInSameSelect,
						Severity:         SeverityPostgreReq,
						Message:          fmt.Sprintf("Переменная %s используется в выражении для %s, но вычислена в этом же SELECT", seenVar, a.TargetVariable),
						File:             file.Path,
						Line:             fragment.LineNumber + lineOffset,
						Object:           a.TargetVariable,
						CurrentProductID: file.DsProductID,
					})
				}
			}
			seenVars[normalizeVariableName(a.TargetVariable)] = true
		}
	}

	return findings, nil
}

// checkStatementsWithJoinsRequireAliases проверяет, что в SELECT/UPDATE/DELETE
// с JOIN (2+ таблицы) все ссылки на столбцы квалифицированы алиасом.
func (r *Runner) checkStatementsWithJoinsRequireAliases(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}

		queryText := strings.TrimSpace(fragment.QueryText)
		// Обрезаем на границе следующего оператора, если парсер объединил несколько в один фрагмент
		queryText = truncateAtStatementBoundary(queryText)
		lower := strings.ToLower(queryText)

		var selectPart string
		var wherePart string
		var onParts []string
		isUpdate := false

		if strings.HasPrefix(lower, "select") {
			fromPos := findTopLevelKeywordPosition(queryText, "from")
			if fromPos < 0 {
				continue
			}
			selectPart = truncateAtStatementBoundary(strings.TrimSpace(queryText[len("select"):fromPos]))
			wherePos := findTopLevelKeywordPosition(queryText, "where")
			if wherePos >= 0 {
				wherePart = truncateAtStatementBoundary(strings.TrimSpace(queryText[wherePos+len("where"):]))
			}
			onParts = extractOnClauses(queryText)
		} else if strings.HasPrefix(lower, "update") {
			isUpdate = true
			setPos := findTopLevelKeywordPosition(queryText, "set")
			if setPos < 0 {
				continue
			}
			fromPos := findTopLevelKeywordPosition(queryText, "from")
			if fromPos < 0 {
				continue
			}
			wherePos := findTopLevelKeywordPosition(queryText, "where")
			setEnd := fromPos
			selectPart = truncateAtStatementBoundary(strings.TrimSpace(queryText[setPos+len("set") : setEnd]))
			if wherePos >= 0 {
				wherePart = truncateAtStatementBoundary(strings.TrimSpace(queryText[wherePos+len("where"):]))
			}
			onParts = extractOnClauses(queryText)
		} else if strings.HasPrefix(lower, "delete") {
			fromPos := findTopLevelKeywordPosition(queryText, "from")
			if fromPos < 0 {
				continue
			}
			wherePos := findTopLevelKeywordPosition(queryText, "where")
			if wherePos >= 0 {
				wherePart = truncateAtStatementBoundary(strings.TrimSpace(queryText[wherePos+len("where"):]))
			}
		} else {
			continue
		}

		tables := extractTablesFromFromClause(queryText)
		if len(tables) < 2 {
			continue
		}

		// Строим set известных имён таблиц и алиасов для фильтрации ложных срабатываний
		knownTableNames := make(map[string]bool)
		for _, t := range tables {
			knownTableNames[strings.ToLower(t.TableName)] = true
			if t.Alias != "" {
				knownTableNames[strings.ToLower(t.Alias)] = true
			}
			if t.IndexName != "" {
				knownTableNames[strings.ToLower(t.IndexName)] = true
			}
		}

		if selectPart != "" {
			parts := splitTopLevelCSV(selectPart)
			for _, part := range parts {
				trimmed := strings.TrimSpace(part)
				if trimmed == "" || trimmed == "*" {
					continue
				}
				// Для UPDATE SET: проверяем только правую часть присваивания (выражение)
				if isUpdate {
					eqIdx := strings.Index(trimmed, "=")
					if eqIdx >= 0 {
						trimmed = strings.TrimSpace(trimmed[eqIdx+1:])
					}
				}
				if !isUpdate {
					if asIdx := findTopLevelKeywordPosition(trimmed, "as"); asIdx >= 0 {
						trimmed = strings.TrimSpace(trimmed[:asIdx])
					}
				}
				if trimmed == "" {
					continue
				}
				colNames := findAllUnqualifiedColumnRefs(trimmed)
				colNames = r.filterKnownNames(colNames)
				colNames = filterOutTableNames(colNames, knownTableNames)
				for _, colName := range colNames {
					lineOffset := findColLineOffsetInPart(queryText, trimmed, colName)
					findings = append(findings, Finding{
						Rule:             RuleStatementsWithJoinsRequireAliases,
						Severity:         SeverityPostgreReq,
						Message:          fmt.Sprintf("Неквалифицированная ссылка на столбец %s в запросе с JOIN — укажите алиас таблицы", colName),
						File:             file.Path,
						Line:             fragment.LineNumber + lineOffset,
						Object:           colName,
						CurrentProductID: file.DsProductID,
					})
				}
			}
		}

		if wherePart != "" {
			colNames := findAllUnqualifiedColumnRefs(wherePart)
			colNames = r.filterKnownNames(colNames)
			colNames = filterOutTableNames(colNames, knownTableNames)
			for _, colName := range colNames {
				lineOffset := findColLineOffsetInPart(queryText, wherePart, colName)
				findings = append(findings, Finding{
					Rule:             RuleStatementsWithJoinsRequireAliases,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Неквалифицированная ссылка на столбец %s в WHERE запроса с JOIN — укажите алиас таблицы", colName),
					File:             file.Path,
					Line:             fragment.LineNumber + lineOffset,
					Object:           colName,
					CurrentProductID: file.DsProductID,
				})
			}
		}

		// Проверяем ON-условия JOIN
		for _, onPart := range onParts {
			colNames := findAllUnqualifiedColumnRefs(onPart)
			colNames = r.filterKnownNames(colNames)
			colNames = filterOutTableNames(colNames, knownTableNames)
			for _, colName := range colNames {
				lineOffset := findColLineOffsetInPart(queryText, onPart, colName)
				findings = append(findings, Finding{
					Rule:             RuleStatementsWithJoinsRequireAliases,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Неквалифицированная ссылка на столбец %s в ON-условии JOIN — укажите алиас таблицы", colName),
					File:             file.Path,
					Line:             fragment.LineNumber + lineOffset,
					Object:           colName,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
}

// checkVarAssignInUpdate проверяет, что в UPDATE SET не присваиваются значения
// переменным (@var = expr). Любое такое присваивание запрещено.
func (r *Runner) checkVarAssignInUpdate(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}

		stmt, ok := parseUpdateSetStatement(fragment.QueryText)
		if !ok {
			continue
		}

		queryText := strings.TrimSpace(fragment.QueryText)
		for _, a := range stmt.Assignments {
			target := strings.TrimSpace(a.Target)
			if !strings.HasPrefix(target, "@") {
				continue
			}

			lineOffset := 0
			idx := strings.Index(strings.ToLower(queryText), strings.ToLower(target))
			if idx >= 0 {
				lineOffset = strings.Count(queryText[:idx], "\n")
			}

			findings = append(findings, Finding{
				Rule:             RuleVarAssignInUpdate,
				Severity:         SeverityPostgreReq,
				Message:          fmt.Sprintf("Установка значения переменной %s в UPDATE SET запрещена", target),
				File:             file.Path,
				Line:             fragment.LineNumber + lineOffset,
				Object:           target,
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

// checkUseFuncInIndCol проверяет, что в WHERE и ON не используются функции
// от столбцов, входящих в индекс, указанный в M_*_INDEX(...).
func (r *Runner) checkUseFuncInIndCol(file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)

	macroResult, err := r.fileProcessedContent(file.Path)
	if err != nil {
		return nil, err
	}
	contentStr := macroResult.Content
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
					parenDepth = countParensRespectingStrings(line)
				}
			}
			continue
		}

		stmtBuffer = append(stmtBuffer, line)
		parenDepth += countParensRespectingStrings(line)

		if hasStatementEnded(lower, stmtBuffer) {
			items, err := r.analyzeStatementForUseFuncInIndCol(stmtBuffer, stmtStartLine, file)
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
		items, err := r.analyzeStatementForUseFuncInIndCol(stmtBuffer, stmtStartLine, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}

	return findings, nil
}

// analyzeStatementForUseFuncInIndCol анализирует один оператор на использование
// функций от индексных столбцов в WHERE и ON.
func (r *Runner) analyzeStatementForUseFuncInIndCol(lines []string, startLine int, file *indexedFile) ([]Finding, error) {
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

	wherePart := extractWherePartForIndexWrong(trimmedText)
	onParts := extractOnPartsForIndexWrong(trimmedText)

	seen := make(map[string]struct{})

	for _, table := range tables {
		if shouldSkipTableCheck(table.TableName) {
			continue
		}
		if table.IndexName == "" {
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

		indexFields, err := r.lookupIndexFieldsByName(indexName)
		if err != nil {
			return nil, err
		}
		if len(indexFields) == 0 {
			continue
		}

		indexFieldSet := make(map[string]bool)
		for _, f := range indexFields {
			indexFieldSet[normalizeIdentifier(f)] = true
		}
		alias := normalizeIdentifier(table.Alias)

		// Проверяем WHERE
		if wherePart != "" {
			funcRefs := extractFuncColumnRefs(wherePart)
			for _, fr := range funcRefs {
				if isIndexedColumn(fr.column, alias, indexFieldSet) {
					findings = append(findings, Finding{
						Rule:             RuleUseFuncInIndCol,
						Severity:         SeverityPostgreReq,
						Message:          fmt.Sprintf("Функция %s от столбца %s в WHERE разрушает использование индекса %s", fr.funcName, fr.column, indexName),
						File:             file.Path,
						Line:             startLine,
						Object:           fmt.Sprintf("%s(%s)", fr.funcName, fr.column),
						CurrentProductID: file.DsProductID,
					})
				}
			}
		}

		// Проверяем ON-условия
		for _, onPart := range onParts {
			funcRefs := extractFuncColumnRefs(onPart)
			for _, fr := range funcRefs {
				if isIndexedColumn(fr.column, alias, indexFieldSet) {
					findings = append(findings, Finding{
						Rule:             RuleUseFuncInIndCol,
						Severity:         SeverityPostgreReq,
						Message:          fmt.Sprintf("Функция %s от столбца %s в ON-условии разрушает использование индекса %s", fr.funcName, fr.column, indexName),
						File:             file.Path,
						Line:             startLine,
						Object:           fmt.Sprintf("%s(%s)", fr.funcName, fr.column),
						CurrentProductID: file.DsProductID,
					})
				}
			}
		}
	}

	return findings, nil
}

// extractFuncColumnRefs ищет в выражении вызовы функций вида funcName(arg1, arg2, ...)
// и возвращает пары (funcName, column) для аргументов, являющихся именами столбцов.
func extractFuncColumnRefs(expr string) []funcColumnRef {
	result := make([]funcColumnRef, 0)

	// Удаляем строковые литералы
	cleaned := regexp.MustCompile(`'[^']*'`).ReplaceAllString(expr, "")

	// Ищем вызовы функций: identifier(args)
	funcRe := regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*)\s*\(`)
	matches := funcRe.FindAllStringSubmatchIndex(cleaned, -1)

	for _, m := range matches {
		funcName := cleaned[m[2]:m[3]]
		lowerFunc := strings.ToLower(funcName)

		// Пропускаем CAST/CONVERT — это не функции в классическом смысле
		if lowerFunc == "cast" || lowerFunc == "convert" {
			continue
		}
		// Пропускаем ключевые слова SQL (and, or, not, in, is, like, ...)
		if sqlKeywordsMap[lowerFunc] {
			continue
		}
		// Пропускаем макросы Diasoft (M_*)
		if sqlMacrosMap[lowerFunc] {
			continue
		}

		// Извлекаем содержимое скобок с учётом вложенности
		parenStart := m[1] - 1 // позиция '('
		depth := 0
		end := parenStart
		for end < len(cleaned) {
			switch cleaned[end] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					goto foundEnd
				}
			}
			end++
		}
	foundEnd:
		if end <= parenStart {
			continue
		}
		args := cleaned[parenStart+1 : end]

		// Разбиваем аргументы по запятой (top-level)
		argParts := splitTopLevelCSV(args)
		for _, arg := range argParts {
			trimmed := strings.TrimSpace(arg)
			if trimmed == "" {
				continue
			}
			// Пропускаем переменные, числа, строки
			if strings.HasPrefix(trimmed, "@") {
				continue
			}
			if regexp.MustCompile(`^\d`).MatchString(trimmed) {
				continue
			}
			if trimmed == "*" {
				continue
			}
			// Извлекаем имя столбца: может быть alias.column или просто column
			colRe := regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*)(?:\.([a-z_][a-z0-9_]*))?\b`)
			colMatch := colRe.FindStringSubmatch(trimmed)
			if colMatch == nil {
				continue
			}
			// Если есть alias.column, берём column; иначе берём просто column
			var colName string
			if colMatch[2] != "" {
				colName = colMatch[2]
			} else {
				colName = colMatch[1]
			}
			// Пропускаем ключевые слова и функции
			lowerCol := strings.ToLower(colName)
			if sqlKeywordsMap[lowerCol] || sqlFunctionsMap[lowerCol] || sqlDataTypesMap[lowerCol] {
				continue
			}
			result = append(result, funcColumnRef{
				funcName: funcName,
				column:   colName,
			})
		}
	}

	return result
}

// checkIsNullSameTypes проверяет, что в ISNULL(expr1, expr2) оба выражения
// имеют эквивалентные типы.
func (r *Runner) checkIsNullSameTypes(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
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
		queryText := fragment.QueryText
		isnullCalls := extractIsnullCalls(queryText)
		if len(isnullCalls) == 0 {
			continue
		}

		aliasMap := parseAliasMap(extractFromClause(queryText))

		for _, call := range isnullCalls {
			type1 := r.resolveArgType(call[0], variableTypes, aliasMap)
			type2 := r.resolveArgType(call[1], variableTypes, aliasMap)

			// Пропускаем если хотя бы один тип не определён (NULL или не удалось резолвить)
			if type1 == "" || type2 == "" {
				continue
			}

			// Пропускаем литералы во втором аргументе — SQL Server неявно приводит
			// литерал к типу первого аргумента ISNULL
			if isLiteralArg(call[1]) {
				continue
			}

			if areEquivalentTypes(type1, type2) {
				continue
			}

			key := fmt.Sprintf("%d|%s|%s|%s", fragment.LineNumber, call[0], call[1], normalizeDataType(type1)+normalizeDataType(type2))
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}

			findings = append(findings, Finding{
				Rule:             RuleIsNullSameTypes,
				Severity:         SeverityPostgreReq,
				Message:          fmt.Sprintf("ISNULL с разными типами: %s (%s) и %s (%s)", strings.TrimSpace(call[0]), type1, strings.TrimSpace(call[1]), type2),
				File:             file.Path,
				Line:             fragment.LineNumber,
				Object:           fmt.Sprintf("ISNULL(%s, %s)", strings.TrimSpace(call[0]), strings.TrimSpace(call[1])),
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

// extractIsnullCalls ищет все вызовы ISNULL(...) в тексте и возвращает пары аргументов.
func extractIsnullCalls(text string) [][2]string {
	result := make([][2]string, 0)
	lower := strings.ToLower(text)

	searchFrom := 0
	for {
		idx := findKeywordPosition(lower[searchFrom:], "isnull")
		if idx < 0 {
			break
		}
		idx += searchFrom

		// Пропускаем whitespace после isnull
		pos := idx + len("isnull")
		for pos < len(text) && (text[pos] == ' ' || text[pos] == '\t' || text[pos] == '\n' || text[pos] == '\r') {
			pos++
		}
		if pos >= len(text) || text[pos] != '(' {
			searchFrom = idx + len("isnull")
			continue
		}

		// Извлекаем содержимое скобок с учётом вложенности
		depth := 0
		start := pos
		end := pos
		for end < len(text) {
			switch text[end] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					goto foundEnd
				}
			}
			end++
		}
	foundEnd:
		if end >= len(text) {
			// Закрывающая ')' не найдена — некорректный/неполный вызов ISNULL, пропускаем
			searchFrom = idx + len("isnull")
			continue
		}
		if end <= start {
			searchFrom = idx + len("isnull")
			continue
		}

		args := text[start+1 : end]
		argParts := splitTopLevelCSV(args)
		if len(argParts) >= 2 {
			result = append(result, [2]string{argParts[0], argParts[1]})
		}
		searchFrom = end + 1
	}

	return result
}

// resolveArgType определяет тип аргумента выражения.
// Возвращает пустую строку если тип не удалось определить или это NULL.
func (r *Runner) resolveArgType(arg string, variableTypes map[string]string, aliasMap map[string]string) string {
	trimmed := strings.TrimSpace(arg)
	if trimmed == "" {
		return ""
	}

	lower := strings.ToLower(trimmed)

	// NULL — пропускаем
	if lower == "null" {
		return ""
	}

	// CASE ... END — определяем тип по THEN-выражениям
	if regexp.MustCompile(`(?i)^case\b`).MatchString(trimmed) {
		return r.resolveCaseType(trimmed, variableTypes, aliasMap)
	}

	// Переменная @var
	if strings.HasPrefix(trimmed, "@") {
		varName := normalizeVariableName(trimmed)
		if typeName, exists := variableTypes[varName]; exists {
			return typeName
		}
		return ""
	}

	// Строковый литерал
	if strings.HasPrefix(trimmed, "'") {
		return "varchar"
	}

	// Числовой литерал
	if regexp.MustCompile(`^\d+(\.\d+)?$`).MatchString(trimmed) {
		return "int"
	}

	// Вызов функции: funcName(...)
	funcRe := regexp.MustCompile(`(?i)^([a-z_][a-z0-9_]*)\s*\(`)
	if m := funcRe.FindStringSubmatch(trimmed); m != nil {
		funcName := strings.ToLower(m[1])
		// Сначала проверяем известные функции с фиксированным типом возврата
		if rt, ok := knownFunctionReturnType(funcName); ok {
			return rt
		}
		// Для прочих функций пытаемся вывести тип из аргументов
		innerArgs := extractFuncInnerArgs(trimmed)
		if len(innerArgs) > 0 {
			// dateadd(datepart, number, date) — тип определяется последним аргументом
			if funcName == "dateadd" && len(innerArgs) >= 3 {
				return r.resolveArgType(innerArgs[len(innerArgs)-1], variableTypes, aliasMap)
			}
			return r.resolveArgType(innerArgs[0], variableTypes, aliasMap)
		}
		return ""
	}

	// Ссылка на столбец: alias.column или просто column
	refs := extractColumnRefsFromExpression(trimmed)
	if len(refs) > 0 {
		ref := refs[0]
		tableName := ref.Table
		if mapped, exists := aliasMap[strings.ToLower(strings.TrimSpace(tableName))]; exists {
			tableName = mapped
		}
		typeName, err := r.cachedFindColumnDefinitionType(tableName, ref.Column)
		if err != nil {
			return ""
		}
		return typeName
	}

	// Fallback: колонка без префикса таблицы — ищем по всем таблицам из aliasMap
	colNameRe := regexp.MustCompile(`(?i)^[a-z_][a-z0-9_]*$`)
	if colNameRe.MatchString(trimmed) && len(aliasMap) > 0 {
		seenTables := make(map[string]struct{})
		for _, tbl := range aliasMap {
			if _, seen := seenTables[tbl]; seen {
				continue
			}
			seenTables[tbl] = struct{}{}
			typeName, err := r.cachedFindColumnDefinitionType(tbl, trimmed)
			if err == nil && typeName != "" {
				return typeName
			}
		}
	}

	return ""
}

// resolveCaseType определяет тип результата CASE ... END по THEN-выражениям.
// Если все THEN-значения — строковые литералы, возвращает varchar.
// Если все числовые — int. Иначе берёт тип первого нетривиального THEN-выражения.
func (r *Runner) resolveCaseType(expr string, variableTypes map[string]string, aliasMap map[string]string) string {
	// Извлекаем THEN-выражения
	thenRe := regexp.MustCompile(`(?i)\bthen\b\s+(.*?)(?:\bwhen\b|\belse\b|\bend\b)`)
	matches := thenRe.FindAllStringSubmatch(expr, -1)
	if len(matches) == 0 {
		return ""
	}

	allString := true
	allNumeric := true
	var firstType string

	for _, m := range matches {
		thenExpr := strings.TrimSpace(m[1])
		if thenExpr == "" {
			continue
		}
		if !strings.HasPrefix(thenExpr, "'") {
			allString = false
		}
		if !regexp.MustCompile(`^\d+(\.\d+)?$`).MatchString(thenExpr) {
			allNumeric = false
		}
		if firstType == "" {
			firstType = r.resolveArgType(thenExpr, variableTypes, aliasMap)
		}
	}

	if allString {
		return "varchar"
	}
	if allNumeric {
		return "int"
	}
	return firstType
}

// extractFuncInnerArgs извлекает аргументы из вызова функции funcName(arg1, arg2, ...).
func extractFuncInnerArgs(expr string) []string {
	// Находим первую '(' и соответствующую ')'
	start := strings.Index(expr, "(")
	if start < 0 {
		return nil
	}
	depth := 0
	end := start
	for end < len(expr) {
		switch expr[end] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				goto found
			}
		}
		end++
	}
found:
	if end <= start {
		return nil
	}
	inner := expr[start+1 : end]
	return splitTopLevelCSV(inner)
}

// extractFromClause извлекает FROM-часть из текста запроса.
func extractFromClause(queryText string) string {
	fromIdx := findTopLevelKeywordPosition(queryText, "from")
	if fromIdx < 0 {
		return ""
	}
	endMarkers := []string{"where", "group", "order", "having", "union", "option"}
	end := len(queryText)
	for _, marker := range endMarkers {
		if idx := findTopLevelKeywordPosition(queryText[fromIdx:], marker); idx >= 0 {
			absIdx := fromIdx + idx
			if absIdx < end {
				end = absIdx
			}
		}
	}
	return queryText[fromIdx:end]
}

// checkDiffTypesComparison проверяет, что в сравнениях (WHERE, ON, IF, CASE WHEN)
// оба операнда имеют эквивалентные типы. Присвоения (UPDATE SET, SELECT @var =) не проверяются.
func (r *Runner) checkDiffTypesComparison(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
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
		queryText := removeComments(fragment.QueryText)

		aliasMap := parseAliasMap(extractFromClause(queryText))

		// Собираем все выражения для проверки: WHERE, ON, CASE WHEN
		exprParts := make([]string, 0)

		wherePart := extractWherePartForIndexWrong(queryText)
		if wherePart != "" {
			exprParts = append(exprParts, wherePart)
		}

		onParts := extractOnPartsForIndexWrong(queryText)
		exprParts = append(exprParts, onParts...)

		// CASE WHEN проверяем всегда, даже в присвоениях
		caseWhenParts := extractCaseWhenConditions(queryText)
		exprParts = append(exprParts, caseWhenParts...)

		for _, exprPart := range exprParts {
			comparisons := extractComparisons(exprPart)
			for _, cmp := range comparisons {
				// Пропускаем сравнения с литералами: SQL неявно приводит литерал
				// к типу колонки/переменной
				if isLiteralArg(cmp.left) || isLiteralArg(cmp.right) {
					continue
				}

				type1 := r.resolveArgType(cmp.left, variableTypes, aliasMap)
				type2 := r.resolveArgType(cmp.right, variableTypes, aliasMap)

				if type1 == "" || type2 == "" {
					continue
				}

				if areEquivalentTypes(type1, type2) {
					continue
				}

				key := fmt.Sprintf("%d|%s|%s|%s", fragment.LineNumber, cmp.left, cmp.right, cmp.op)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}

				findings = append(findings, Finding{
					Rule:             RuleDiffTypesComparison,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Сравнение разных типов: %s (%s) %s %s (%s)", strings.TrimSpace(cmp.left), type1, cmp.op, strings.TrimSpace(cmp.right), type2),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           fmt.Sprintf("%s %s %s", strings.TrimSpace(cmp.left), cmp.op, strings.TrimSpace(cmp.right)),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	// Построчная проверка IF ... = ...
	ifLines := strings.Split(string(content), "\n")
	inBlockComment := false
	for lineIdx, rawLine := range ifLines {
		stripped, stillInBlock := stripLineComments(rawLine, inBlockComment)
		inBlockComment = stillInBlock
		trimmed := strings.TrimSpace(stripped)
		if trimmed == "" {
			continue
		}
		lower := strings.ToLower(trimmed)
		if !strings.HasPrefix(lower, "if ") && !strings.HasPrefix(lower, "if(") {
			continue
		}

		// Отрезаем if-префикс
		ifExpr := strings.TrimSpace(trimmed[2:])
		if strings.HasPrefix(strings.ToLower(ifExpr), "(") && strings.HasSuffix(ifExpr, ")") {
			ifExpr = ifExpr[1 : len(ifExpr)-1]
		}

		comparisons := extractComparisons(ifExpr)
		for _, cmp := range comparisons {
			if isLiteralArg(cmp.left) || isLiteralArg(cmp.right) {
				continue
			}

			type1 := r.resolveArgType(cmp.left, variableTypes, nil)
			type2 := r.resolveArgType(cmp.right, variableTypes, nil)

			if type1 == "" || type2 == "" {
				continue
			}

			if areEquivalentTypes(type1, type2) {
				continue
			}

			lineNo := lineIdx + 1
			key := fmt.Sprintf("%d|%s|%s|%s", lineNo, cmp.left, cmp.right, cmp.op)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}

			findings = append(findings, Finding{
				Rule:             RuleDiffTypesComparison,
				Severity:         SeverityPostgreReq,
				Message:          fmt.Sprintf("Сравнение разных типов: %s (%s) %s %s (%s)", strings.TrimSpace(cmp.left), type1, cmp.op, strings.TrimSpace(cmp.right), type2),
				File:             file.Path,
				Line:             lineNo,
				Object:           fmt.Sprintf("%s %s %s", strings.TrimSpace(cmp.left), cmp.op, strings.TrimSpace(cmp.right)),
				CurrentProductID: file.DsProductID,
			})
		}
	}

	return findings, nil
}

// checkFloatToStringConvert проверяет, что CONVERT и CAST не используются
// для приведения float/DSFLOAT к строковому типу.
func (r *Runner) checkFloatToStringConvert(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
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
		queryText := fragment.QueryText
		aliasMap := parseAliasMap(extractFromClause(queryText))

		// Ищем CONVERT(targetType, floatExpr, ...)
		convertRe := regexp.MustCompile(`(?i)\bconvert\s*\(`)
		convertMatches := convertRe.FindAllStringIndex(queryText, -1)
		for _, m := range convertMatches {
			start := m[0]
			parenStart := strings.Index(queryText[start:], "(")
			if parenStart < 0 {
				continue
			}
			parenStart += start
			inner, endIdx := extractParenContent(queryText, parenStart)
			if inner == "" {
				continue
			}
			args := splitTopLevelCSV(inner)
			if len(args) < 2 {
				continue
			}
			targetType := strings.TrimSpace(args[0])
			sourceExpr := strings.TrimSpace(args[1])
			if targetType == "" || sourceExpr == "" {
				continue
			}
			if typeGroup(targetType) != "string" {
				continue
			}
			sourceType := r.resolveArgType(sourceExpr, variableTypes, aliasMap)
			if sourceType == "" {
				continue
			}
			if isFloatType(sourceType) {
				lineOffset := findColLineOffsetInPart(queryText, queryText[start:endIdx], "convert")
				key := fmt.Sprintf("%d|%s|%s", fragment.LineNumber, sourceExpr, targetType)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				findings = append(findings, Finding{
					Rule:             RuleFloatToStringConvert,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Использование CONVERT для приведения float к строке запрещено: CONVERT(%s, %s)", targetType, sourceExpr),
					File:             file.Path,
					Line:             fragment.LineNumber + lineOffset,
					Object:           fmt.Sprintf("CONVERT(%s, %s)", targetType, sourceExpr),
					CurrentProductID: file.DsProductID,
				})
			}
		}

		// Ищем CAST(floatExpr AS targetType)
		castRe := regexp.MustCompile(`(?i)\bcast\s*\(`)
		castMatches := castRe.FindAllStringIndex(queryText, -1)
		for _, m := range castMatches {
			start := m[0]
			parenStart := strings.Index(queryText[start:], "(")
			if parenStart < 0 {
				continue
			}
			parenStart += start
			inner, endIdx := extractParenContent(queryText, parenStart)
			if inner == "" {
				continue
			}
			// Разделяем по AS (top-level)
			asIdx := findTopLevelKeywordPosition(inner, "as")
			if asIdx < 0 {
				continue
			}
			sourceExpr := strings.TrimSpace(inner[:asIdx])
			targetType := strings.TrimSpace(inner[asIdx+2:])
			if sourceExpr == "" || targetType == "" {
				continue
			}
			if typeGroup(targetType) != "string" {
				continue
			}
			sourceType := r.resolveArgType(sourceExpr, variableTypes, aliasMap)
			if sourceType == "" {
				continue
			}
			if isFloatType(sourceType) {
				lineOffset := findColLineOffsetInPart(queryText, queryText[start:endIdx], "cast")
				key := fmt.Sprintf("%d|%s|%s", fragment.LineNumber, sourceExpr, targetType)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				findings = append(findings, Finding{
					Rule:             RuleFloatToStringConvert,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Использование CAST для приведения float к строке запрещено: CAST(%s AS %s)", sourceExpr, targetType),
					File:             file.Path,
					Line:             fragment.LineNumber + lineOffset,
					Object:           fmt.Sprintf("CAST(%s AS %s)", sourceExpr, targetType),
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
}

// checkSelectAfterSetRowcount проверяет, что после SET ROWCOUNT N (N != 0)
// SELECT-присвоения в переменные и INSERT...SELECT имеют ORDER BY.
func (r *Runner) checkSelectAfterSetRowcount(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	seen := make(map[string]struct{})

	rowcountActive := false

	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		queryText := fragment.QueryText
		lower := strings.ToLower(queryText)

		// Отслеживаем SET ROWCOUNT
		rowcountRe := regexp.MustCompile(`(?is)\bset\s+rowcount\s+(\d+)`)
		rowcountMatches := rowcountRe.FindAllStringSubmatch(lower, -1)
		for _, m := range rowcountMatches {
			if len(m) < 2 {
				continue
			}
			n := strings.TrimSpace(m[1])
			if n == "0" {
				rowcountActive = false
			} else {
				rowcountActive = true
			}
		}

		if !rowcountActive {
			continue
		}

		// Проверяем SELECT @var = expr FROM ... без ORDER BY
		if hasOrderBy(lower) {
			continue
		}

		// Case 1: SELECT @var = field FROM ...
		if stmt, ok := parseSelectAssignStatement(queryText); ok {
			for _, assign := range stmt.Assignments {
				if containsColumnRef(assign.Expression) {
					key := fmt.Sprintf("%d|%s|%s", fragment.LineNumber, assign.TargetVariable, assign.Expression)
					if _, exists := seen[key]; exists {
						continue
					}
					seen[key] = struct{}{}
					findings = append(findings, Finding{
						Rule:             RuleSelectAfterSetRowcount,
						Severity:         SeverityPostgreReq,
						Message:          fmt.Sprintf("SELECT %s = %s после SET ROWCOUNT без ORDER BY — результат недетерминирован", assign.TargetVariable, assign.Expression),
						File:             file.Path,
						Line:             fragment.LineNumber,
						Object:           fmt.Sprintf("SELECT %s = %s", assign.TargetVariable, assign.Expression),
						CurrentProductID: file.DsProductID,
					})
				}
			}
			continue
		}

		// Case 2: INSERT INTO ... SELECT ... FROM ... без ORDER BY
		if isInsertSelectFragment(queryText) {
			selectExpr := extractInsertSelectExpr(queryText)
			if selectExpr != "" && containsColumnRef(selectExpr) {
				key := fmt.Sprintf("%d|%s", fragment.LineNumber, selectExpr)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				findings = append(findings, Finding{
					Rule:             RuleSelectAfterSetRowcount,
					Severity:         SeverityPostgreReq,
					Message:          "INSERT...SELECT после SET ROWCOUNT без ORDER BY — результат недетерминирован",
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           "INSERT...SELECT",
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
}

// checkAliasWhenUsingUnion проверяет, что при использовании ORDER BY после
// SELECT-ов объединённых UNION, все имена из ORDER BY содержатся в алиасах
// первого SELECT.
func (r *Runner) checkAliasWhenUsingUnion(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
	findings := make([]Finding, 0)
	seen := make(map[string]struct{})

	for _, fragment := range parsed.Fragments {
		if fragment == nil {
			continue
		}
		queryText := fragment.QueryText
		lower := strings.ToLower(queryText)

		// Проверяем только если есть UNION на top-level
		if !containsTopLevelUnion(lower) {
			continue
		}

		// Проверяем только если есть ORDER BY на top-level
		if !hasOrderBy(lower) {
			continue
		}

		// Извлекаем первый SELECT (от начала до первого UNION)
		firstSelect, ok := extractFirstSelectBeforeUnion(queryText)
		if !ok {
			continue
		}

		// Извлекаем имена колонок первого SELECT
		firstSelectNames := extractSelectColumnNames(firstSelect)
		if len(firstSelectNames) == 0 {
			continue
		}

		// Извлекаем колонки из ORDER BY
		orderByCols := extractOrderByColumns(queryText)
		if len(orderByCols) == 0 {
			continue
		}

		// Проверяем каждую колонку ORDER BY
		for _, col := range orderByCols {
			colTrimmed := strings.TrimSpace(col)
			if colTrimmed == "" {
				continue
			}
			// Порядковый номер — всегда валиден
			if isNumericLiteral(colTrimmed) {
				continue
			}
			// Убираем квалификацию (alias.col → col)
			if dotIdx := strings.LastIndex(colTrimmed, "."); dotIdx >= 0 {
				colTrimmed = colTrimmed[dotIdx+1:]
			}
			colLower := strings.ToLower(strings.TrimSpace(colTrimmed))
			if _, exists := firstSelectNames[colLower]; !exists {
				key := fmt.Sprintf("%d|%s", fragment.LineNumber, colTrimmed)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				findings = append(findings, Finding{
					Rule:             RuleAliasWhenUsingUnion,
					Severity:         SeverityPostgreReq,
					Message:          fmt.Sprintf("Колонка ORDER BY '%s' отсутствует в алиасах первого SELECT при использовании UNION", colTrimmed),
					File:             file.Path,
					Line:             fragment.LineNumber,
					Object:           colTrimmed,
					CurrentProductID: file.DsProductID,
				})
			}
		}
	}

	return findings, nil
}
