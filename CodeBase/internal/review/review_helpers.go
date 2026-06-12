package review

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/codebase/internal/model"
)

var sharedTTables = map[string]struct{}{
	"tcontract":   {},
	"tdeal":       {},
	"tmanynumber": {},
	"tseed":       {},
	"tdocmark":    {},
}

var nonProcedureCallKeywords = map[string]struct{}{
	"on": {},
}

func tableNames(items []tableRef) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names
}

func isSharedTTable(tableName string) bool {
	_, exists := sharedTTables[normalizeIdentifier(tableName)]
	return exists
}

func normalizePath(path string) string {
	return filepath.ToSlash(strings.TrimSpace(path))
}

func normalizeIdentifier(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	trimmed = strings.Trim(trimmed, "[]\"")
	return trimmed
}

func normalizeDataType(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	v = strings.Join(strings.Fields(v), " ")
	return v
}

func areEquivalentTypes(left, right string) bool {
	left = normalizeDataType(left)
	right = normalizeDataType(right)
	if left == right {
		return true
	}
	lg := typeGroup(left)
	rg := typeGroup(right)
	return lg != "" && lg == rg
}

func typeGroup(dataType string) string {
	v := normalizeDataType(dataType)
	switch {
	// INT types - все int/smallint/tinyint/bigint типы эквивалентны
	case strings.Contains(v, "int") ||
		strings.HasPrefix(v, "dsint_key") ||
		strings.HasPrefix(v, "dsint_key_one") ||
		strings.HasPrefix(v, "dssmallint") ||
		strings.HasPrefix(v, "dstinyint") ||
		strings.HasPrefix(v, "dsbigint") ||
		strings.HasPrefix(v, "dsbit0") ||
		strings.HasPrefix(v, "dsbit1"):
		return "int"
	// FLOAT types - все float/real/money/numeric/decimal типы эквивалентны
	case strings.Contains(v, "float") ||
		strings.Contains(v, "real") ||
		strings.Contains(v, "money") ||
		strings.Contains(v, "decimal") ||
		strings.Contains(v, "numeric") ||
		strings.HasPrefix(v, "dsfloat") ||
		strings.HasPrefix(v, "dsbigmoney") ||
		strings.HasPrefix(v, "dsnominal") ||
		strings.HasPrefix(v, "dspercent") ||
		strings.HasPrefix(v, "dsmoney") ||
		strings.HasPrefix(v, "dsspid") ||
		strings.HasPrefix(v, "dsidentifier") ||
		strings.HasPrefix(v, "dsidentifier19"):
		return "float"
	// STRING types - все char/varchar типы эквивалентны
	case strings.Contains(v, "char") ||
		strings.Contains(v, "varchar") ||
		strings.HasPrefix(v, "dsbriefname") ||
		strings.HasPrefix(v, "dsbriefvar") ||
		strings.HasPrefix(v, "dscomment") ||
		strings.HasPrefix(v, "dscomment300") ||
		strings.HasPrefix(v, "dscommentc") ||
		strings.HasPrefix(v, "dsday_str") ||
		strings.HasPrefix(v, "dsdephash") ||
		strings.HasPrefix(v, "dsfieldname") ||
		strings.HasPrefix(v, "dsfieldnamevar") ||
		strings.HasPrefix(v, "dsformclass") ||
		strings.HasPrefix(v, "dsfullname") ||
		strings.HasPrefix(v, "dshash") ||
		strings.HasPrefix(v, "dsidentname") ||
		strings.HasPrefix(v, "dsindexhash") ||
		strings.HasPrefix(v, "dslabel") ||
		strings.HasPrefix(v, "dsmemo") ||
		strings.HasPrefix(v, "dsmemo1000") ||
		strings.HasPrefix(v, "dsnmemo") ||
		strings.HasPrefix(v, "dsnumber") ||
		strings.HasPrefix(v, "dsnumber12") ||
		strings.HasPrefix(v, "dsnumber20") ||
		strings.HasPrefix(v, "dsnumber3") ||
		strings.HasPrefix(v, "dsnumber5") ||
		strings.HasPrefix(v, "dssymbol") ||
		strings.HasPrefix(v, "dstext") ||
		strings.HasPrefix(v, "dsusername") ||
		strings.HasPrefix(v, "dsvarfullname") ||
		strings.HasPrefix(v, "dsvarfullname160") ||
		strings.HasPrefix(v, "dsvarfullname40") ||
		strings.HasPrefix(v, "dsvarindex") ||
		strings.HasPrefix(v, "dsvarchar_max") ||
		strings.HasPrefix(v, "dsacc_swift") ||
		strings.HasPrefix(v, "dsaccnumber") ||
		strings.HasPrefix(v, "dsaccnumber35") ||
		strings.HasPrefix(v, "dsaccnumvar") ||
		strings.HasPrefix(v, "dsbic"):
		return "string"
	// DATETIME types - все datetime/smalldatetime/date типы эквивалентны
	case strings.Contains(v, "datetime") ||
		strings.Contains(v, "time") ||
		strings.Contains(v, "date") ||
		strings.HasPrefix(v, "dsdatetime") ||
		strings.HasPrefix(v, "dsoperday") ||
		strings.HasPrefix(v, "smalldatetime") ||
		strings.HasPrefix(v, "day"):
		return "datetime"
	// OBJECT/BINARY types
	case strings.Contains(v, "image") ||
		strings.Contains(v, "binary") ||
		strings.HasPrefix(v, "dsimage") ||
		strings.HasPrefix(v, "dsuuid") ||
		strings.HasPrefix(v, "dsvarbinary_max") ||
		strings.HasPrefix(v, "dsnumeric3820"):
		return "object"
	default:
		return ""
	}
}

func enabledRuleSet(rules []RuleID) map[RuleID]bool {
	result := map[RuleID]bool{
		RuleForeignTablesUsing:    true,
		RuleForeignPTablesUsing:   true,
		RuleForeignProcedureUsing: true,
		RuleExecNotExistsProc:     true,
		RuleProcDuplicate:         true,
		RuleProcParamDefValue:     true,
		RuleProcElseCase:          true,
		RuleUseSelectAll:          true,
		RuleTruncTbl:              true,
		RuleDatatype:              true,
		RuleAnsiInJoin:            true,
		RuleInsertRowLock:         true,
		RuleUseEqColumn:           true,
		RuleTableFullScan:         true,
		RuleTableHintExists:       true,
		RuleTableHintIsRight:      true,
		RuleIndexExistsInDB:       true,
		RuleIndexWrong:            true,
		RuleUpdateOnlyVar:         true,
		RulePTableSpid:            true,
		RuleForceOrder2Tbl:        true,
		RuleSaveTran:              true,
		RuleUseDrop:               true,
		RuleMathOperations:        true,
		RuleExistsWithAndInIf:     true,
		RuleNullComparison:        true,
		RuleShouldBeCP866:         true,
		RuleTooManyJoins:          true,
		RuleMaxProcParam:          true,
		RuleModifyOutProc:         true,
	}
	if len(rules) == 0 {
		return result
	}
	for key := range result {
		result[key] = false
	}
	for _, rule := range rules {
		rule = RuleID(strings.TrimSpace(string(rule)))
		if _, exists := result[rule]; exists {
			result[rule] = true
		}
	}
	return result
}

func buildSummary(findings []Finding) Summary {
	summary := Summary{
		Total:      len(findings),
		ByRule:     map[RuleID]int{},
		BySeverity: map[int]int{},
	}
	for _, finding := range findings {
		summary.ByRule[finding.Rule]++
		summary.BySeverity[finding.Severity]++
	}
	if len(summary.ByRule) == 0 {
		summary.ByRule = nil
	}
	if len(summary.BySeverity) == 0 {
		summary.BySeverity = nil
	}
	return summary
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

func extractJoinColumnsForIndexWrong(fullText string, tables []tableFromClause) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{})
	for _, table := range tables {
		result[tableConditionKey(table)] = make(map[string]struct{})
	}

	onParts := extractOnPartsForIndexWrong(fullText)
	for _, onPart := range onParts {
		collectJoinColumnsFromOnPart(onPart, tables, result)
	}

	return result
}

func collectJoinColumnsFromOnPart(onPart string, tables []tableFromClause, result map[string]map[string]struct{}) {
	eqRe := regexp.MustCompile(`(?i)\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b\s*=\s*\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b`)
	matches := eqRe.FindAllStringSubmatch(onPart, -1)
	if len(matches) == 0 {
		return
	}

	idents := make(map[string]string)
	for _, table := range tables {
		key := tableConditionKey(table)
		if key == "" {
			continue
		}
		idents[normalizeIdentifier(table.TableName)] = key
		if alias := normalizeIdentifier(table.Alias); alias != "" {
			idents[alias] = key
		}
	}

	for _, m := range matches {
		if len(m) < 5 {
			continue
		}

		leftIdent := normalizeIdentifier(m[1])
		leftCol := normalizeIdentifier(m[2])
		rightIdent := normalizeIdentifier(m[3])
		rightCol := normalizeIdentifier(m[4])

		leftKey, leftExists := idents[leftIdent]
		rightKey, rightExists := idents[rightIdent]
		if !leftExists || !rightExists || leftKey == rightKey {
			continue
		}
		if leftCol != "" {
			result[leftKey][leftCol] = struct{}{}
		}
		if rightCol != "" {
			result[rightKey][rightCol] = struct{}{}
		}
	}
}

func shouldKeepChosenIndexForPKJoin(indexName string, chosenFields []string, joinCols map[string]struct{}) bool {
	if len(joinCols) == 0 {
		return false
	}
	normalizedIndex := normalizeIdentifier(indexName)
	if !strings.HasPrefix(normalizedIndex, "xpk") {
		return false
	}
	if len(chosenFields) == 0 {
		return true
	}
	for _, field := range chosenFields {
		normalizedField := normalizeIdentifier(field)
		if normalizedField == "" {
			continue
		}
		if _, exists := joinCols[normalizedField]; exists {
			return true
		}
	}
	return false
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
	branches := splitByOrRespectingParens(expr)
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

func isInComment(line string, pos int) bool {
	lower := strings.ToLower(line)
	for i := 0; i < pos && i < len(lower); i++ {
		if i+1 < len(lower) && lower[i:i+2] == "--" {
			return true
		}
	}
	return false
}

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

	return -1, hasElse
}

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

func normalizeHintStatementText(text string) string {
	lower := strings.ToLower(text)

	if strings.HasPrefix(lower, "select") {
		if idx := findKeywordPosition(lower, "delete"); idx > 0 {
			return strings.TrimSpace(lower[idx:])
		}
		if idx := findKeywordPosition(lower, "update"); idx > 0 {
			return strings.TrimSpace(lower[idx:])
		}
	}

	return lower
}

func findOriginalLineNumber(originalLines []string, processedLine string, currentLine int) int {
	processedTrimmed := strings.TrimSpace(processedLine)
	if processedTrimmed == "" {
		return 0
	}

	startIndex := currentLine - 1
	if startIndex < 0 {
		startIndex = 0
	}
	if startIndex >= len(originalLines) {
		startIndex = len(originalLines) - 1
	}

	// Ищем строку с таким же содержимым начиная с текущей позиции.
	for i := startIndex; i < len(originalLines); i++ {
		origLine := originalLines[i]
		origTrimmed := strings.TrimSpace(origLine)
		if origTrimmed == processedTrimmed {
			return i + 1
		}
	}
	for i := 0; i < startIndex; i++ {
		origLine := originalLines[i]
		origTrimmed := strings.TrimSpace(origLine)
		if origTrimmed == processedTrimmed {
			return i + 1
		}
	}

	// Если точное совпадение не найдено, пробуем с учетом различий в пробелах.
	processedNormalized := strings.Join(strings.Fields(processedLine), " ")
	for i := startIndex; i < len(originalLines); i++ {
		origLine := originalLines[i]
		origNormalized := strings.Join(strings.Fields(origLine), " ")
		if origNormalized == processedNormalized {
			return i + 1
		}
	}
	for i := 0; i < startIndex; i++ {
		origLine := originalLines[i]
		origNormalized := strings.Join(strings.Fields(origLine), " ")
		if origNormalized == processedNormalized {
			return i + 1
		}
	}

	return 0
}

func sameTableReference(left, right string) bool {
	l := normalizeIdentifier(left)
	r := normalizeIdentifier(right)
	if l == "" || r == "" {
		return false
	}
	if l == r {
		return true
	}
	if idx := strings.LastIndex(l, "."); idx >= 0 {
		l = l[idx+1:]
	}
	if idx := strings.LastIndex(r, "."); idx >= 0 {
		r = r[idx+1:]
	}
	return l != "" && l == r
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

func isHintAllowed(hint string, allowed []string) bool {
	lowerHint := strings.ToLower(hint)
	for _, allowedHint := range allowed {
		if strings.ToLower(allowedHint) == lowerHint {
			return true
		}
	}
	return false
}

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
