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
	lines := strings.Split(string(content), "\n")

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
