package review

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/util"
)

// cachedRegexp делегирует в общий кэш util.CachedRegexp.
func cachedRegexp(pattern string) *regexp.Regexp {
	return util.CachedRegexp(pattern)
}

// Precompiled regexes for hot-path helper functions
var (
	reDateFunc         = regexp.MustCompile(`(?is)\b(getdate|sysdatetime|sysutcdatetime|getutcdate|datefromparts|datetimefromparts|smalldatetimefromparts|datetime2fromparts|datetimeoffsetfromparts|timefromparts|eomonth|dateadd|datediff|datediff_big|datetrunc)\s*\(`)
	reCurrentTimestamp = regexp.MustCompile(`(?i)\bcurrent_timestamp\b`)
	reVarOnly          = regexp.MustCompile(`(?is)^\s*(@[A-Za-z_][A-Za-z0-9_]*)\s*$`)
	reEmptyString1     = regexp.MustCompile(`(?is)^\s*(N)?\s*['"]\s*['"]\s*$`)
	reEmptyString2     = regexp.MustCompile(`(?is)^\s*(N)?\s*['"][\s]*['"]\s*$`)
	reConvertCall      = regexp.MustCompile(`(?i)\bconvert\s*\(`)
	reCastCall         = regexp.MustCompile(`(?i)\bcast\s*\(`)
	reDeclareVar       = regexp.MustCompile(`(?i)@([A-Za-z_][A-Za-z0-9_]*)[ \t]+([A-Za-z_][A-Za-z0-9_]*(?:\s*\([^)]*\))?)`)
	reAtVar            = regexp.MustCompile(`(?i)@@?[a-z_][a-z0-9_]*`)
	reNumber           = regexp.MustCompile(`(?i)\b\d+(\.\d+)?\b`)
	reNullEtc          = regexp.MustCompile(`(?i)\b(null|isnull|convert|cast|case|when|then|else|end|and|or|not)\b`)
	reFuncPrefix       = regexp.MustCompile(`(?i)\b[a-z_]+\s*\(`)
	reIdentifier       = regexp.MustCompile(`(?i)\b[a-z_][a-z0-9_]*\b`)
	reBareColumn       = regexp.MustCompile(`(?is)^\s*\[?([a-z_][a-z0-9_]*)\]?\s*$`)
	reSelectAssign     = regexp.MustCompile(`(?is)^\s*(@[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
	reSetAssign        = regexp.MustCompile(`(?is)^\s*set\s+(@[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
	reDeclareAssign    = regexp.MustCompile(`(?is)^\s*declare\s+(@[A-Za-z_][A-Za-z0-9_]*)\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*(?:\s*\([^)]*\))?)\s*=\s*(.+)$`)
	reAPICreateProc    = regexp.MustCompile(`(?i)API_CREATE_PROC\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)`)
	reBeginProcedure   = regexp.MustCompile(`(?i)__BEGIN_PROCEDURE__\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)`)

	// Regexes для join/condition парсинга.
	reHelperJoinEq               = regexp.MustCompile(`(?i)\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b\s*=\s*\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b`)
	reHelperOnParts              = regexp.MustCompile(`(?i)\s+on\s+`)
	reHelperUnqualifiedCondition = regexp.MustCompile(`(?i)\b([a-zA-Z_][a-zA-Z0-9_]*)\b\s*(=|<>|!=|>=|<=|>|<|\bin\b|\bbetween\b|\blike\b|\bis\b)`)
	reHelperSpid                 = regexp.MustCompile(`(?i)\b(spid)\b`)

	// Regexes для парсинга типов данных.
	reHelperNumericType = regexp.MustCompile(`(?i)\b(?:numeric|decimal)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)`)
	reHelperVarcharType = regexp.MustCompile(`(?i)\b(?:nvarchar|nchar|varchar|char)\s*\(\s*(\d+)\s*\)`)

	// Regexes для извлечения таблиц из FROM.
	reHelperFromClause = regexp.MustCompile(`(?is)\bfrom\s+([a-z_#][a-z0-9_#]*)`)

	// reDeletePtableMacro ищет вызовы M_DELETE_PTABLE* макросов в тексте.
	reDeletePtableMacro = regexp.MustCompile(`(?i)\b(M_DELETE_PTABLE(?:_INMEM|_PARALLEL|_INDEX|_SPID_INDEX|_SPID_UNIQUE)?)\s*\(`)
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

// isFloatType строго проверяет, что тип — это FLOAT или DSFLOAT.
// Используется в правиле floatToStringConvert для узкой проверки,
// в отличие от typeGroup, которая относит к float также money/numeric/decimal и т.д.
func isFloatType(dataType string) bool {
	v := normalizeDataType(dataType)
	return strings.Contains(v, "float") || strings.HasPrefix(v, "dsfloat")
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
		RuleForeignTablesUsing:                true,
		RuleForeignPTablesUsing:               true,
		RuleForeignProcedureUsing:             true,
		RuleExecNotExistsProc:                 true,
		RuleProcDuplicate:                     true,
		RuleProcParamDefValue:                 true,
		RuleProcElseCase:                      true,
		RuleUseSelectAll:                      true,
		RuleTruncTbl:                          true,
		RuleDatatype:                          true,
		RuleAnsiInJoin:                        true,
		RuleInsertRowLock:                     true,
		RuleUseEqColumn:                       true,
		RuleTableFullScan:                     true,
		RuleTableHintExists:                   true,
		RuleTableHintIsRight:                  true,
		RuleIndexExistsInDB:                   true,
		RuleIndexWrong:                        true,
		RuleUpdateOnlyVar:                     true,
		RulePTableSpid:                        true,
		RuleForceOrder2Tbl:                    true,
		RuleSaveTran:                          true,
		RuleUseDrop:                           true,
		RuleMathOperations:                    true,
		RuleExistsWithAndInIf:                 true,
		RuleNullComparison:                    true,
		RuleShouldBeCP866:                     true,
		RuleTooManyJoins:                      true,
		RuleMaxProcParam:                      true,
		RuleModifyOutProc:                     true,
		RuleEmptyReturn:                       true,
		RuleRawTransactionControl:             true,
		RuleDeferredUpdate:                    true,
		RuleInSubQuery:                        true,
		RuleVarcharSize:                       true,
		RuleColumnInsert:                      true,
		RulePostgreLabelGotoLevel:             true,
		RuleDateIntoString:                    true,
		RuleEmptyStringDate:                   true,
		RuleVarUseAfterCursor:                 true,
		RuleExcessProcParams:                  true,
		RuleDuplicateOutputVariable:           true,
		RuleUseOnlyDeclaredCursors:            true,
		RuleCursorFetchArguments:              true,
		RuleUsageVarInSameSelect:              true,
		RuleVarAssignInUpdate:                 true,
		RuleStatementsWithJoinsRequireAliases: true,
		RuleUseFuncInIndCol:                   true,
		RuleIsNullSameTypes:                   true,
		RuleDiffTypesComparison:               true,
		RuleFloatToStringConvert:              true,
		RuleSelectAfterSetRowcount:            true,
		RuleAliasWhenUsingUnion:               true,
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
	matches := reHelperJoinEq.FindAllStringSubmatch(onPart, -1)
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

func extractWhereOnlyColumnsForIndexWrong(fullText string, tables []tableFromClause) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{})
	for _, table := range tables {
		result[tableConditionKey(table)] = make(map[string]struct{})
	}

	wherePart := extractWherePartForIndexWrong(fullText)
	if strings.TrimSpace(wherePart) != "" {
		mergeTableColumns(result, collectColumnsFromConditionExpression(wherePart, tables))
	}

	return result
}

func containsForceOrderMacro(lowerText string) bool {
	for _, macro := range forceOrderMacros {
		if strings.Contains(lowerText, strings.ToLower(macro)) {
			return true
		}
	}
	return false
}

func extractWherePartForIndexWrong(fullText string) string {
	whereIdx := findSubstringAtDepthZero(fullText, " where ")
	if whereIdx < 0 {
		return ""
	}

	part := fullText[whereIdx+7:]
	endMarkers := []string{" order by ", " group by ", " having ", " union ", " except ", " intersect ",
		" insert ", " select ", " update ", " delete "}
	endIdx := len(part)
	for _, marker := range endMarkers {
		if idx := findSubstringAtDepthZero(part, marker); idx > 0 && idx < endIdx {
			endIdx = idx
		}
	}

	return part[:endIdx]
}

func extractOnPartsForIndexWrong(fullText string) []string {
	result := make([]string, 0)
	parts := reHelperOnParts.Split(fullText, -1)
	if len(parts) <= 1 {
		return result
	}

	for _, part := range parts[1:] {
		endMarkers := []string{" join ", " where ", " order by ", " group by ", " having "}
		endIdx := len(part)
		for _, marker := range endMarkers {
			if idx := findSubstringAtDepthZero(part, marker); idx > 0 && idx < endIdx {
				endIdx = idx
			}
		}
		result = append(result, part[:endIdx])
	}

	return result
}

func findSubstringAtDepthZero(text, substr string) int {
	lower := strings.ToLower(text)
	substr = strings.ToLower(substr)
	depth := 0
	for i := 0; i <= len(lower)-len(substr); i++ {
		ch := lower[i]
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' && depth > 0 {
			depth--
			continue
		}
		if depth > 0 {
			continue
		}
		if ch == '\'' {
			i++
			for i < len(lower) && lower[i] != '\'' {
				i++
			}
			continue
		}
		if lower[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
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
			re := cachedRegexp(`(?i)\b` + regexp.QuoteMeta(identifier) + `\.([a-zA-Z_][a-zA-Z0-9_]*)\b`)
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
	matches := reHelperUnqualifiedCondition.FindAllStringSubmatch(expr, -1)

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

	for _, part := range allParts {
		partLower := strings.ToLower(part)

		// Ищем все вхождения SPID
		for _, match := range reHelperSpid.FindAllStringIndex(partLower, -1) {
			// Определяем контекст - проверяем, относится ли к какой-либо таблице
			contextStart := 0
			if match[0] > 60 {
				contextStart = match[0] - 60
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

func isInStringLiteral(line string, pos int) bool {
	inString := false
	for i := 0; i < pos && i < len(line); i++ {
		if line[i] == '\'' {
			if inString && i+1 < len(line) && line[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
		}
	}
	return inString
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

// isNumericIndex проверяет, является ли имя индекса числовым ID (например 0 = clustered).
// Числовые ID не нужно искать в БД по имени.
func isNumericIndex(indexName string) bool {
	s := strings.TrimSpace(indexName)
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

// findHintLineInBuffer ищет смещение строки внутри буфера оператора,
// содержащей хинт индекса для указанной таблицы.
// Возвращает 0, если строка не найдена.
func findHintLineInBuffer(lines []string, tableName, hint, indexName string) int {
	lowerTable := strings.ToLower(tableName)
	lowerHint := strings.ToLower(hint)
	lowerIndex := strings.ToLower(indexName)
	for i, line := range lines {
		lowerLine := strings.ToLower(line)
		if strings.Contains(lowerLine, lowerTable) &&
			strings.Contains(lowerLine, lowerHint) &&
			strings.Contains(lowerLine, lowerIndex) {
			return i
		}
	}
	// Если не нашли все три компонента, ищем хотя бы таблицу + хинт
	for i, line := range lines {
		lowerLine := strings.ToLower(line)
		if strings.Contains(lowerLine, lowerTable) &&
			strings.Contains(lowerLine, lowerHint) {
			return i
		}
	}
	return 0
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

func countParensRespectingStrings(line string) int {
	depth := 0
	inString := false
	for i := 0; i < len(line); i++ {
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
		}
	}
	return depth
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
	if m := reHelperNumericType.FindStringSubmatch(v); len(m) == 3 {
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

// literalFitsType проверяет, помещается ли числовой литерал в целевой тип.
// Возвращает true, если литерал fits в диапазон/точность targetType.
func literalFitsType(literal string, targetType string) bool {
	v := strings.TrimSpace(literal)
	if v == "" {
		return false
	}
	target := normalizeDataType(targetType)

	// Целочисленные типы — проверяем диапазон
	if p, s, ok := numericPrecisionScale(target); ok && s == 0 {
		// Если литерал имеет дробную часть, а цель — целый тип, проверяем отдельно
		hasDecimal := strings.Contains(v, ".")
		if hasDecimal {
			// Для целочисленного target литерал с дробной частью не помещается
			// только если есть ненулевая дробная часть
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return false
			}
			// Если дробная часть ненулевая — не помещается в целый тип
			if f != float64(int64(f)) {
				return false
			}
			v = strconv.FormatInt(int64(f), 10)
		}

		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			// Может быть слишком большое число — не помещается
			return false
		}

		switch {
		case strings.HasPrefix(target, "dstinyint"), strings.Contains(target, "tinyint"):
			return n >= 0 && n <= 255
		case strings.HasPrefix(target, "dssmallint"), strings.Contains(target, "smallint"):
			return n >= -32768 && n <= 32767
		case strings.HasPrefix(target, "dsint_key_one"), strings.HasPrefix(target, "dsint_key"),
			target == "int", target == "integer", strings.Contains(target, " int"):
			return n >= -2147483648 && n <= 2147483647
		case strings.HasPrefix(target, "dsbigint"), strings.Contains(target, "bigint"),
			strings.HasPrefix(target, "dsidentifier19"):
			return true
		case strings.HasPrefix(target, "dsidentifier"):
			return n >= 0 && n <= 999999999999999
		case p > 0:
			// numeric(p, 0) — проверяем что помещается в p цифр
			return n >= 0 && len(strings.TrimLeft(v, "-0")) <= p
		}
	}

	// numeric(p, s) / decimal(p, s) — проверяем точность и масштаб
	if m := reHelperNumericType.FindStringSubmatch(target); len(m) == 3 {
		precision, _ := strconv.Atoi(m[1])
		scale, _ := strconv.Atoi(m[2])
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false
		}
		_ = f
		// Проверяем количество цифр
		absStr := strings.TrimLeft(v, "-+")
		dotIdx := strings.Index(absStr, ".")
		intDigits := absStr
		fracDigits := ""
		if dotIdx >= 0 {
			intDigits = absStr[:dotIdx]
			fracDigits = absStr[dotIdx+1:]
		}
		intDigits = strings.TrimLeft(intDigits, "0")
		if intDigits == "" {
			intDigits = "0"
		}
		if len(intDigits) > precision-scale {
			return false
		}
		if len(fracDigits) > scale {
			return false
		}
		return true
	}

	return false
}

func datetimePrecisionRank(dataType string) int {
	v := normalizeDataType(dataType)
	switch {
	case strings.Contains(v, "datetime") && !strings.Contains(v, "smalldatetime"):
		return 3
	case strings.Contains(v, "smalldatetime") || strings.HasPrefix(v, "dsoperday"):
		return 2
	case strings.Contains(v, "date"):
		return 1
	default:
		return 0
	}
}

// hasPrecisionLoss классифицирует отношение между sourceType и targetType.
// Возвращает ("loss", true) при сужении точности, ("incompatible", true) при несовместимых типах,
// ("", false) если типы совместимы или сужения нет.
func hasPrecisionLoss(sourceType, targetType string) (kind string, ok bool) {
	source := normalizeDataType(sourceType)
	target := normalizeDataType(targetType)
	if source == "" || target == "" || source == target {
		return "", false
	}

	sg := typeGroup(source)
	tg := typeGroup(target)
	if sg == "" || tg == "" {
		return "", false
	}

	// Разные группы типов — несовместимые
	if sg != tg {
		return "incompatible", true
	}

	// Одна группа — проверяем сужение
	if isPotentialPrecisionLoss(source, target) {
		return "loss", true
	}

	// Проверка сужения varchar/char по длине
	if sg == "string" {
		srcLen, srcOk := varcharLength(source)
		tgtLen, tgtOk := varcharLength(target)
		if srcOk && tgtOk && srcLen > tgtLen {
			return "loss", true
		}
	}

	return "", false
}

// varcharLength извлекает длину из типа varchar(N) / char(N) / nvarchar(N).
// Возвращает (0, false) если длина не указана.
func varcharLength(dataType string) (int, bool) {
	v := normalizeDataType(dataType)
	if m := reHelperVarcharType.FindStringSubmatch(v); len(m) == 2 {
		n, err := strconv.Atoi(m[1])
		if err == nil {
			return n, true
		}
	}
	return 0, false
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

func isDateExpression(expr string, varTypes map[string]string) bool {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return false
	}
	lower := strings.ToLower(trimmed)

	if reDateFunc.MatchString(lower) {
		return true
	}

	if reCurrentTimestamp.MatchString(lower) {
		return true
	}

	m := reVarOnly.FindStringSubmatch(trimmed)
	if len(m) == 2 {
		name := normalizeVariableName(m[1])
		if vtype, ok := varTypes[name]; ok && typeGroup(vtype) == "datetime" {
			return true
		}
	}

	return false
}

func isEmptyStringLiteral(expr string) bool {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return false
	}
	if reEmptyString1.MatchString(trimmed) {
		return true
	}
	// Литерал с пробелами внутри: '   ' или "   "
	return reEmptyString2.MatchString(trimmed)
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

func hasBetweenWithMathOp(lower string) bool {
	return extractBetweenWithMathOp(lower) != ""
}

// extractBetweenWithMathOp находит BETWEEN ... AND с матоперацией и возвращает
// выражение после AND (для использования в Object finding-а).
func extractBetweenWithMathOp(lower string) string {
	idx := strings.Index(lower, "between ")
	if idx == -1 {
		return ""
	}
	if idx > 0 && lower[idx-1] != ' ' && lower[idx-1] != '\t' {
		return ""
	}

	startPos := idx + 8
	andIdx := strings.Index(lower[startPos:], " and ")
	if andIdx == -1 {
		return ""
	}
	andIdx += startPos

	afterAnd := strings.TrimSpace(lower[andIdx+5:])
	if hasMathOperator(afterAnd) {
		return afterAnd
	}
	return ""
}

func hasComparisonWithMathOp(lower string) bool {
	return extractComparisonWithMathOp(lower) != ""
}

// extractComparisonWithMathOp находит сравнение с матоперацией и возвращает
// выражение стороны с матоперацией (для использования в Object finding-а).
func extractComparisonWithMathOp(lower string) string {
	// Операторы сравнения: >, <, >=, <=, <>, !=
	ops := []string{">=", "<=", "<>", "!=", ">", "<"}

	for _, op := range ops {
		opIdx := strings.Index(lower, op)
		if opIdx == -1 {
			continue
		}

		left := strings.TrimSpace(lower[:opIdx])
		right := strings.TrimSpace(lower[opIdx+len(op):])

		leftIsVar := strings.HasPrefix(left, "@")
		rightIsVar := strings.HasPrefix(right, "@")
		leftHasMath := hasMathOperator(left)
		rightHasMath := hasMathOperator(right)

		if leftIsVar && rightHasMath {
			return right
		}
		if rightIsVar && leftHasMath {
			return left
		}
	}

	return ""
}

func hasMathOperator(expr string) bool {
	// Ищем * / + - между операндами (с учетом пробелов)
	// Исключаем операторы внутри convert() или cast()
	// Исключаем операторы внутри вызовов функций (внутри скобок)
	parenDepth := 0
	for i := 0; i < len(expr); i++ {
		ch := expr[i]

		// Отслеживаем глубину скобок
		if ch == '(' {
			parenDepth++
			continue
		}
		if ch == ')' {
			if parenDepth > 0 {
				parenDepth--
			}
			continue
		}

		// Проверяем математические операторы только на верхнем уровне (parenDepth == 0)
		if parenDepth > 0 {
			continue
		}
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

	// Обрезаем до BEGIN (конец условия)
	if beginIdx := strings.Index(condition, " begin"); beginIdx >= 0 {
		condition = condition[:beginIdx]
	}

	// Ищем AND на top-level в условии
	if !hasTopLevelAnd(condition) {
		return false
	}

	// Проверяем наличие запроса к таблицам в условии
	return hasTableQuery(condition)
}

// isIfBodyStart проверяет, начинается ли строка с SQL-оператора,
// что указывает на начало тела IF (без BEGIN), а не на продолжение условия.
func isIfBodyStart(lower string) bool {
	for _, kw := range []string{"if ", "if(", "delete ", "select ", "select(", "insert ",
		"update ", "exec ", "set ", "print ", "goto ", "return ", "break",
		"continue ", "waitfor ", "raiserror ", "commit ", "rollback ",
		"save ", "truncate ", "merge ", "grant ", "revoke ", "alter ",
		"create ", "drop "} {
		if strings.HasPrefix(lower, kw) {
			return true
		}
	}
	return false
}

// hasIfWithAndAndQueryMulti проверяет многострочное IF условие.
// Принимает уже склеенное условие (все строки объединены пробелами).
func hasIfWithAndAndQueryMulti(lower string) bool {
	ifIdx := strings.Index(lower, "if ")
	if ifIdx == -1 {
		return false
	}

	if ifIdx > 0 && isOperandChar(lower[ifIdx-1]) {
		return false
	}

	conditionStart := ifIdx + 3
	condition := lower[conditionStart:]

	// Обрезаем до BEGIN (конец условия)
	if beginIdx := strings.Index(condition, " begin"); beginIdx >= 0 {
		condition = condition[:beginIdx]
	}

	// Проверяем AND только на top-level (глубина скобок = 0)
	if !hasTopLevelAnd(condition) {
		return false
	}

	return hasTableQuery(condition)
}

// hasTopLevelAnd проверяет, есть ли " and " на top-level (вне скобок).
func hasTopLevelAnd(condition string) bool {
	depth := 0
	for i := 0; i < len(condition); i++ {
		ch := condition[i]
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' && depth > 0 {
			depth--
			continue
		}
		if depth > 0 {
			continue
		}
		// Ищем " and " на top-level
		if i+5 <= len(condition) && condition[i:i+5] == " and " {
			return true
		}
	}
	return false
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

// hasExplicitConversion проверяет, содержит ли выражение явное преобразование
// через convert() или cast() к целевому типу (или типу той же группы без потери точности).
// Если разработчик осознанно привёл к целевому типу — проверка потери точности не нужна.
// convert к другому типу (например convert(numeric, ...) при target DSOPERDAY) не считается.
func hasExplicitConversion(expression string, targetType string) bool {
	if expression == "" || targetType == "" {
		return false
	}
	exprLower := strings.ToLower(expression)
	target := normalizeDataType(targetType)
	targetGroup := typeGroup(target)

	// Проверяем все convert(type, ...) вызовы в выражении
	for _, m := range reConvertCall.FindAllStringIndex(exprLower, -1) {
		args := extractFuncInnerArgs(exprLower[m[0]:])
		if len(args) >= 2 {
			convType := normalizeDataType(strings.TrimSpace(args[0]))
			if convType == target {
				return true
			}
			if typeGroup(convType) == targetGroup && targetGroup != "" && !isPotentialPrecisionLoss(convType, targetType) {
				return true
			}
		}
	}

	// Проверяем все cast(... AS type) вызовы в выражении
	for _, m := range reCastCall.FindAllStringIndex(exprLower, -1) {
		args := extractFuncInnerArgs(exprLower[m[0]:])
		if len(args) >= 1 {
			inner := args[0]
			asIdx := findTopLevelKeywordPosition(inner, "as")
			if asIdx >= 0 {
				castType := normalizeDataType(strings.TrimSpace(inner[asIdx+2:]))
				if castType == target {
					return true
				}
				if typeGroup(castType) == targetGroup && targetGroup != "" && !isPotentialPrecisionLoss(castType, targetType) {
					return true
				}
			}
		}
	}

	return false
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

func mapProcessedLineNumber(sourceMap []int, processedLineNumber int) int {
	if processedLineNumber <= 0 {
		return 0
	}
	idx := processedLineNumber - 1
	if idx < 0 || idx >= len(sourceMap) {
		return processedLineNumber
	}
	if sourceMap[idx] <= 0 {
		return processedLineNumber
	}
	return sourceMap[idx]
}

func validateExecArguments(args []execArgument, params []model.SQLParam) (bool, string) {
	if len(params) == 0 && len(args) > 0 {
		return true, fmt.Sprintf(": процедура не принимает параметры, но передано %d", len(args))
	}

	seenNamed := make(map[string]int)
	for _, arg := range args {
		if arg.IsNamed {
			seenNamed[arg.Name]++
			if seenNamed[arg.Name] > 1 {
				return true, fmt.Sprintf(": параметр @%s дублируется в вызове", arg.Name)
			}
		}
	}

	allowedParams := make(map[string]bool)
	for _, p := range params {
		allowedParams[normalizeIdentifier(p.Name)] = true
	}

	for _, arg := range args {
		if arg.IsNamed {
			if !allowedParams[arg.Name] {
				return true, fmt.Sprintf(": передан лишний параметр @%s, отсутствующий в объявлении процедуры", arg.Name)
			}
		}
	}

	posCount := 0
	for _, arg := range args {
		if !arg.IsNamed {
			posCount++
		}
	}
	if posCount > len(params) {
		return true, fmt.Sprintf(": передано %d позиционных параметров, а процедура принимает максимум %d", posCount, len(params))
	}

	return false, ""
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

	for _, block := range extractDeclareBlocks(content) {
		for _, m := range reDeclareVar.FindAllStringSubmatch(block, -1) {
			if len(m) < 3 {
				continue
			}
			name := normalizeVariableName(m[1])
			typeName := strings.TrimSpace(m[2])
			if name == "" || typeName == "" {
				continue
			}
			if isSQLKeywordNotType(typeName) {
				continue
			}
			result[name] = typeName
		}
	}

	return result
}

// extractCurrentProcName определяет имя текущей процедуры из parsed-результата
// или, если парсер не извлёк имя (например, для API_CREATE_PROC), ищет в контенте
// макросы API_CREATE_PROC(name) или __BEGIN_PROCEDURE__(name).
func extractCurrentProcName(parsed *sqlparser.ParseResult, content string) string {
	for _, proc := range parsed.Procedures {
		if proc != nil && strings.TrimSpace(proc.ProcName) != "" {
			return strings.TrimSpace(proc.ProcName)
		}
	}
	if m := reAPICreateProc.FindStringSubmatch(content); len(m) >= 2 {
		return m[1]
	}
	if m := reBeginProcedure.FindStringSubmatch(content); len(m) >= 2 {
		return m[1]
	}
	return ""
}

// enrichVariableTypesFromAPI дополняет карту variableTypes параметрами процедуры,
// полученными из БД индекса (включая fallback к API-контрактам).
// Не перезаписывает типы, уже определённые через declare-блоки или параметры парсера.
func (r *Runner) enrichVariableTypesFromAPI(variableTypes map[string]string, parsed *sqlparser.ParseResult, content string) {
	if r == nil || r.db == nil {
		return
	}
	procName := extractCurrentProcName(parsed, content)
	if procName == "" {
		return
	}
	params, err := r.lookupProcedureParams(procName)
	if err != nil || len(params) == 0 {
		return
	}
	for _, p := range params {
		name := normalizeVariableName(p.Name)
		typeName := strings.TrimSpace(p.Type)
		if name == "" || typeName == "" {
			continue
		}
		if _, exists := variableTypes[name]; !exists {
			variableTypes[name] = typeName
		}
	}
}

// extractDeclareBlocks извлекает текст блоков declare из контента.
// Блок начинается с ключевого слова declare и заканчивается на следующем
// операторе SQL (exec, select, insert, update, delete, set, if, while, и т.д.).
func extractDeclareBlocks(content string) []string {
	var blocks []string
	lower := strings.ToLower(content)
	searchFrom := 0
	stmtKeywords := []string{
		"exec", "select", "insert", "update", "delete", "set",
		"if", "while", "print", "return", "create", "alter",
		"drop", "go", "begin", "end", "declare", "truncate",
		"merge", "with", "waitfor", "raiserror", "throw",
	}
	for {
		idx := findKeywordPosition(lower[searchFrom:], "declare")
		if idx < 0 {
			break
		}
		idx += searchFrom
		start := idx + len("declare")
		end := len(content)
		for _, kw := range stmtKeywords {
			kwIdx := findKeywordPosition(lower[start:], kw)
			if kwIdx >= 0 {
				absIdx := start + kwIdx
				if absIdx < end {
					end = absIdx
				}
			}
		}
		blocks = append(blocks, content[start:end])
		searchFrom = end
	}
	return blocks
}

// isSQLKeywordNotType проверяет, является ли строка SQL-ключевым словом,
// которое не может быть типом данных (например output, out, readonly).
func isSQLKeywordNotType(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "output", "out", "readonly", "default", "null", "into", "from", "where", "and", "or", "not":
		return true
	}
	return false
}

func normalizeVariableName(value string) string {
	v := strings.TrimSpace(value)
	v = strings.Trim(v, "[]\"")
	v = strings.TrimPrefix(v, "@")
	return strings.ToLower(v)
}

func containsParamUsage(line, paramLower string) bool {
	start := 0
	for {
		idx := strings.Index(line[start:], paramLower)
		if idx == -1 {
			return false
		}
		pos := idx + start
		after := pos + len(paramLower)
		if after >= len(line) || !isWordCharByte(line[after]) {
			return true
		}
		start = pos + 1
	}
}

func isWordCharByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// detectFileEncoding определяет кодировку файла: "ASCII", "CP866", "UTF-8", "CP1251" или "UNKNOWN".
// Делегирует в encoding.DetectFromBytes, возвращая строковое представление для совместимости.
func detectFileEncoding(data []byte) string {
	enc := encoding.DetectFromBytes(data)
	switch enc {
	case encoding.CP866:
		return "CP866"
	case encoding.WIN1251:
		return "CP1251"
	case encoding.UTF8:
		return "UTF-8"
	default:
		return "UNKNOWN"
	}
}

// isIndexedColumn проверяет, входит ли столбец в набор полей индекса.
// Учитывает алиас: если column содержит точку (alias.column), проверяет
// совпадение prefix с алиасом таблицы и извлекает часть после точки.
func isIndexedColumn(column, alias string, indexFieldSet map[string]bool) bool {
	normalizedCol := normalizeIdentifier(column)
	if normalizedCol == "" {
		return false
	}

	// Если column содержит точку (alias.column), извлекаем часть после точки
	if idx := strings.Index(normalizedCol, "."); idx >= 0 {
		prefix := normalizeIdentifier(normalizedCol[:idx])
		colOnly := normalizeIdentifier(normalizedCol[idx+1:])
		// Если алиас таблицы задан, проверяем совпадение prefix
		if alias != "" && prefix != alias {
			return false
		}
		if indexFieldSet[colOnly] {
			return true
		}
		return false
	}

	// column без точки — прямое совпадение
	if indexFieldSet[normalizedCol] {
		return true
	}

	return false
}

// knownFunctionReturnType возвращает тип результата для известных SQL-функций.
func knownFunctionReturnType(funcName string) (string, bool) {
	switch funcName {
	case "getdate", "getutcdate", "sysdatetime", "sysutcdatetime":
		return "datetime", true
	case "upper", "lower", "ltrim", "rtrim", "left", "right", "substring",
		"replace", "replicate", "stuff", "char", "space", "str":
		return "varchar", true
	case "len", "datalength", "charindex", "patindex":
		return "int", true
	case "rand", "datediff", "datepart", "day", "month", "year":
		return "int", true
	case "dateadd":
		return "", false // тип определяется вторым аргументом (датой)
	case "cast", "convert":
		return "", false // тип определяется аргументами
	default:
		return "", false
	}
}

// inferTypeFromMacroSignature определяет тип результата макроса по его телу.
// Поддерживает convert(тип, ...) и cast(... as тип).
func inferTypeFromMacroSignature(signature string) string {
	trimmed := strings.TrimSpace(signature)
	if trimmed == "" {
		return ""
	}
	lower := strings.ToLower(trimmed)

	// convert(тип, ...) — тип определяется первым аргументом
	if idx := strings.Index(lower, "convert("); idx >= 0 {
		inner, _ := extractParenContent(trimmed, idx+7)
		if inner == "" {
			return ""
		}
		args := splitTopLevelCSV(inner)
		if len(args) > 0 {
			return normalizeDataType(strings.TrimSpace(args[0]))
		}
	}

	// cast(... as тип) — тип после AS
	if idx := strings.Index(lower, "cast("); idx >= 0 {
		inner, _ := extractParenContent(trimmed, idx+4)
		if inner == "" {
			return ""
		}
		if asIdx := findTopLevelKeywordPosition(inner, "as"); asIdx >= 0 {
			return normalizeDataType(strings.TrimSpace(inner[asIdx+2:]))
		}
	}

	return ""
}

// isLiteralArg проверяет, является ли аргумент литералом (числовым, строковым или NULL).
// SQL неявно приводит литералы к типу другого операнда, поэтому такие сравнения не являются ошибкой.
func isLiteralArg(arg string) bool {
	trimmed := strings.TrimSpace(arg)
	if trimmed == "" {
		return false
	}
	// NULL
	if strings.ToLower(trimmed) == "null" {
		return true
	}
	// Строковый литерал
	if strings.HasPrefix(trimmed, "'") {
		return true
	}
	// Числовой литерал (включая отрицательные)
	if reNumericSigned.MatchString(trimmed) {
		return true
	}
	return false
}

// containsSQLStatementKeyword проверяет, содержит ли выражение
// SQL-ключевые слова конструкций (join, on, where, from, select и т.д.),
// что указывает на артефакт парсинга — операнд не является корректным выражением.
func containsSQLStatementKeyword(expr string) bool {
	lower := strings.ToLower(expr)
	// Ключевые слова-конструкции, которых не должно быть в операнде сравнения
	wordKeywords := []string{"join", "where", "from", "select", "insert", "update", "delete", "having", "union", "except", "intersect", "on", "left", "right", "outer", "inner", "cross"}
	for _, kw := range wordKeywords {
		if findKeywordPosition(lower, kw) >= 0 {
			return true
		}
	}
	// "group by" и "order by" проверяем как подстроки (с пробелами)
	substrings := []string{"group by", "order by"}
	for _, s := range substrings {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}

// isAssignmentFragment проверяет, является ли фрагмент присвоением (UPDATE SET или SELECT @var =).
func isAssignmentFragment(queryText string) bool {
	_, ok1 := parseUpdateSetStatement(queryText)
	if ok1 {
		return true
	}
	_, ok2 := parseSelectAssignStatement(queryText)
	return ok2
}

// containsColumnRef проверяет, содержит ли выражение ссылку на колонку таблицы
// (а не только литералы или переменные).
func containsColumnRef(expr string) bool {
	// Убираем qualified refs (alias.column) — они тоже считаются ссылками на колонки
	refs := extractColumnRefsFromExpression(expr)
	if len(refs) > 0 {
		return true
	}
	// Проверяем неквалифицированные ссылки на колонки
	// Убираем переменные (@var), числа, строки, функции
	cleaned := reAtVar.ReplaceAllString(expr, "")
	cleaned = reNumber.ReplaceAllString(cleaned, "")
	cleaned = reStringLiteral.ReplaceAllString(cleaned, "")
	cleaned = reNullEtc.ReplaceAllString(cleaned, "")
	// Убираем известные функции
	cleaned = reFuncPrefix.ReplaceAllString(cleaned, "")
	// Остаёмся ли с идентификатором?
	return reIdentifier.MatchString(strings.TrimSpace(cleaned))
}

// isInsertSelectFragment проверяет, является ли фрагмент INSERT...SELECT.
func isInsertSelectFragment(queryText string) bool {
	trimmed := strings.TrimSpace(queryText)
	return hasPrefixFold(trimmed, "insert") && findTopLevelKeywordPosition(trimmed, "select") >= 0
}

// containsTopLevelUnion проверяет наличие UNION на top-level.
func containsTopLevelUnion(lowerQuery string) bool {
	return findTopLevelKeywordPosition(lowerQuery, "union") >= 0
}

// hasOrderBy проверяет наличие ORDER BY в тексте запроса.
func hasOrderBy(lowerQuery string) bool {
	idx := findTopLevelKeywordPosition(lowerQuery, "order")
	if idx < 0 {
		return false
	}
	rest := lowerQuery[idx+5:]
	return strings.HasPrefix(strings.TrimSpace(rest), "by")
}

// containsVarReference проверяет, содержит ли выражение ссылку на переменную @varName
// с учётом word boundary (чтобы @a не матчило @ab).
func containsVarReference(expr, varName string) bool {
	v := strings.TrimPrefix(strings.TrimSpace(varName), "@")
	if v == "" {
		return false
	}
	re := cachedRegexp(`(?i)@` + regexp.QuoteMeta(v) + `\b`)
	return re.MatchString(expr)
}

// filterOutTableNames удаляет из списка имён те, которые являются именами таблиц,
// алиасами или индексами из FROM clause (чтобы не путать их с неквалифицированными колонками).
func filterOutTableNames(names []string, knownTableNames map[string]bool) []string {
	if len(names) == 0 || len(knownTableNames) == 0 {
		return names
	}
	result := make([]string, 0, len(names))
	for _, name := range names {
		if knownTableNames[strings.ToLower(name)] {
			continue
		}
		result = append(result, name)
	}
	return result
}

func parseFirstFromTable(fromClause string) string {
	if strings.TrimSpace(fromClause) == "" {
		return ""
	}

	m := reHelperFromClause.FindStringSubmatch(fromClause)
	if len(m) != 2 {
		return ""
	}

	return strings.TrimSpace(m[1])
}

func extractBareColumnName(expression string) string {
	if strings.TrimSpace(expression) == "" {
		return ""
	}

	m := reBareColumn.FindStringSubmatch(expression)
	if len(m) != 2 {
		return ""
	}

	return strings.TrimSpace(m[1])
}

// extractDeletePtableCalls сканирует строки текста на наличие вызовов
// макросов семейства M_DELETE_PTABLE* и извлекает имя таблицы и индекса.
func extractDeletePtableCalls(lines []string) []deletePtableCall {
	result := make([]deletePtableCall, 0)

	for i, line := range lines {
		lineNum := i + 1
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		matches := reDeletePtableMacro.FindAllStringSubmatchIndex(line, -1)
		for _, m := range matches {
			macroName := line[m[2]:m[3]]
			argsStart := m[1] - 1 // позиция '('
			argsText, _, ok := parseMacroCallArguments(line, argsStart)
			if !ok {
				continue
			}
			args := splitTopLevelCSV(argsText)
			for j := range args {
				args[j] = strings.TrimSpace(args[j])
			}

			lowerMacro := strings.ToLower(macroName)

			// M_DELETE_PTABLE_RUN — не вызов удаления, пропускаем
			if lowerMacro == "m_delete_ptable_run" {
				continue
			}

			var tableName, indexName string

			switch lowerMacro {
			case "m_delete_ptable", "m_delete_ptable_inmem":
				if len(args) >= 1 {
					tableName = args[0]
					indexName = "XPK" + tableName
				}
			case "m_delete_ptable_parallel":
				// M_DELETE_PTABLE_PARALLEL(table, spid, col, batch, parallel)
				if len(args) >= 1 {
					tableName = args[0]
					indexName = "XPK" + tableName
				}
			case "m_delete_ptable_index":
				// M_DELETE_PTABLE_INDEX(table, index)
				if len(args) >= 2 {
					tableName = args[0]
					indexName = args[1]
				}
			case "m_delete_ptable_spid_index":
				// M_DELETE_PTABLE_SPID_INDEX(table, index, spid)
				if len(args) >= 2 {
					tableName = args[0]
					indexName = args[1]
				}
			case "m_delete_ptable_spid_unique":
				// M_DELETE_PTABLE_SPID_UNIQUE(table, index, spid, unique)
				if len(args) >= 2 {
					tableName = args[0]
					indexName = args[1]
				}
			default:
				continue
			}

			if tableName == "" || indexName == "" {
				continue
			}

			result = append(result, deletePtableCall{
				TableName: tableName,
				IndexName: indexName,
				MacroName: macroName,
				Line:      lineNum,
			})
		}
	}

	return result
}

// positionToLine переводит смещение в объединённом тексте lines в номер строки (1-based).
func positionToLine(lines []string, pos int) int {
	if pos < 0 {
		return 0
	}
	current := 0
	for i, line := range lines {
		lineLen := len(line)
		if pos < current+lineLen {
			return i + 1
		}
		current += lineLen + 1 // +1 for пробел-разделитель
	}
	return len(lines)
}
