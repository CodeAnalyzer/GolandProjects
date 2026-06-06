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
	content, err := os.ReadFile(file.Path)
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

	// Получаем текст строки для вывода в сообщении об ошибке
	lineText := ""
	content, err := os.ReadFile(file.Path)
	if err == nil {
		fileLines := strings.Split(string(content), "\n")
		if startLine > 0 && startLine <= len(fileLines) {
			lineText = strings.TrimSpace(fileLines[startLine-1])
		}
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
					// Ищем строку с таким же содержимым (без учета пробелов)
					commaLine = findOriginalLineNumber(originalLines, line)
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

		if hasStatementEnded(lower, stmtBuffer) {
			items := analyzeStatementForTableHintExists(stmtBuffer, stmtStartLine, file, stmtType)
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
		items := analyzeStatementForTableHintExists(stmtBuffer, stmtStartLine, file, stmtType)
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
