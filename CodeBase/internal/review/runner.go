package review

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store"
	"github.com/lib/pq"
)

type Runner struct {
	db     *store.DB
	parser *sqlparser.Parser
}

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

var sharedTTables = map[string]struct{}{
	"tcontract":   {},
	"tdeal":       {},
	"tmanynumber": {},
	"tseed":       {},
	"tdocmark":    {},
}

type indexedFile struct {
	ID          int64
	Path        string
	RelPath     string
	DsProductID int64
}

func NewRunner(db *store.DB) *Runner {
	return &Runner{db: db, parser: sqlparser.NewParser()}
}

func (r *Runner) RunSQLFile(path string, opts Options) (*Result, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("sql file path is required")
	}
	if r.db == nil {
		return nil, fmt.Errorf("database is not initialized")
	}

	normalizedPath := normalizePath(path)
	file, err := r.getIndexedFile(normalizedPath)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("file is not indexed: %s", normalizedPath)
		}
		return nil, err
	}
	if file.DsProductID == 0 {
		return nil, fmt.Errorf("ds_product_id is not set for file: %s", normalizedPath)
	}

	parsed, err := r.parser.ParseFile(normalizedPath)
	if err != nil {
		return nil, err
	}

	ruleSet := enabledRuleSet(opts.Rules)
	findings := make([]Finding, 0)

	if ruleSet[RuleForeignTablesUsing] {
		items, err := r.checkForeignTables(parsed, file, "t")
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleForeignPTablesUsing] {
		items, err := r.checkForeignPTables(parsed, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleForeignProcedureUsing] {
		items, err := r.checkForeignProcedures(parsed, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleExecNotExistsProc] {
		items, err := r.checkExecNotExistsProcedures(parsed, file)
		if err != nil {
			return nil, err
		}
		findings = append(findings, items...)
	}
	if ruleSet[RuleDatatype] {
		items := r.checkDatatype(parsed, file)
		findings = append(findings, items...)
	}

	minSeverity := opts.MinSeverity
	if minSeverity <= 0 {
		minSeverity = SeverityFineCode
	}

	filtered := make([]Finding, 0, len(findings))
	for _, finding := range findings {
		if finding.Severity <= minSeverity {
			filtered = append(filtered, finding)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Line == filtered[j].Line {
			return filtered[i].Rule < filtered[j].Rule
		}
		return filtered[i].Line < filtered[j].Line
	})

	result := &Result{
		AnalyzedFile: normalizedPath,
		Summary:      buildSummary(filtered),
		Findings:     filtered,
	}
	return result, nil
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

type tableRef struct {
	Name string
	Line int
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

type procedureRef struct {
	Name string
	Line int
}

var nonProcedureCallKeywords = map[string]struct{}{
	"on": {},
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

type insertSelectStatement struct {
	TargetTable       string
	TargetColumns     []string
	SelectExpressions []string
	FromClause        string
}

type updateAssignment struct {
	Target     string
	Expression string
}

type updateSetStatement struct {
	TargetTable string
	TargetAlias string
	Assignments []updateAssignment
	FromClause  string
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

type columnRef struct {
	Table  string
	Column string
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

func (r *Runner) getIndexedFile(path string) (*indexedFile, error) {
	variants := []string{path, filepath.ToSlash(path), strings.ReplaceAll(path, "/", `\`)}
	for _, candidate := range variants {
		var item indexedFile
		var dsProduct sql.NullInt64
		err := r.db.QueryRow(`
			SELECT id, path, rel_path, ds_product_id
			FROM files
			WHERE LOWER(path) = LOWER($1) OR LOWER(rel_path) = LOWER($1)
			ORDER BY id DESC
			LIMIT 1
		`, candidate).Scan(&item.ID, &item.Path, &item.RelPath, &dsProduct)
		if err == nil {
			if dsProduct.Valid {
				item.DsProductID = dsProduct.Int64
			}
			item.Path = normalizePath(item.Path)
			return &item, nil
		}
		if err != sql.ErrNoRows {
			return nil, err
		}
	}
	return nil, sql.ErrNoRows
}

func (r *Runner) lookupTableProductID(tableName string) (int64, error) {
	var productID int64
	err := r.db.QueryRow(`
		SELECT f.ds_product_id
		FROM sql_tables t
		JOIN files f ON f.id = t.file_id
		WHERE LOWER(t.table_name) = LOWER($1)
		  AND t.context = 'create'
		  AND f.ds_product_id IS NOT NULL
		ORDER BY t.id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName)).Scan(&productID)
	if err == nil {
		return productID, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}

	err = r.db.QueryRow(`
		SELECT f.ds_product_id
		FROM sql_tables t
		JOIN files f ON f.id = t.file_id
		WHERE LOWER(t.table_name) = LOWER($1)
		  AND t.context = 'dfm_embedded'
		  AND f.ds_product_id IS NOT NULL
		ORDER BY t.id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName)).Scan(&productID)
	if err != nil {
		return 0, err
	}
	return productID, nil
}

func (r *Runner) lookupProcedureProductID(procName string) (int64, error) {
	var productID int64
	err := r.db.QueryRow(`
		SELECT f.ds_product_id
		FROM sql_procedures p
		JOIN files f ON f.id = p.file_id
		WHERE LOWER(p.proc_name) = LOWER($1)
		  AND f.ds_product_id IS NOT NULL
		ORDER BY p.id DESC
		LIMIT 1
	`, strings.TrimSpace(procName)).Scan(&productID)
	if err != nil {
		return 0, err
	}
	return productID, nil
}

func (r *Runner) findAPITableNames(names []string) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	normalized := make([]string, 0, len(names))
	seen := map[string]struct{}{}
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, key)
	}
	if len(normalized) == 0 {
		return result, nil
	}

	load := func(query string) error {
		rows, err := r.db.Query(query, pq.Array(normalized))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			result[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
		}
		return rows.Err()
	}

	if err := load(`SELECT LOWER(table_name) FROM api_business_object_tables WHERE LOWER(table_name) = ANY($1)`); err != nil {
		return nil, err
	}
	if err := load(`SELECT LOWER(table_name) FROM api_contract_tables WHERE LOWER(table_name) = ANY($1)`); err != nil {
		return nil, err
	}
	return result, nil
}

func enabledRuleSet(rules []RuleID) map[RuleID]bool {
	result := map[RuleID]bool{
		RuleForeignTablesUsing:    true,
		RuleForeignPTablesUsing:   true,
		RuleForeignProcedureUsing: true,
		RuleExecNotExistsProc:     true,
		RuleDatatype:              true,
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

func tableNames(items []tableRef) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names
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
	case strings.Contains(v, "int") || strings.HasPrefix(v, "dsidentifier"):
		return "int"
	case strings.Contains(v, "decimal") || strings.Contains(v, "numeric") || strings.Contains(v, "money") || strings.Contains(v, "float") || strings.Contains(v, "real"):
		return "number"
	case strings.Contains(v, "char") || strings.Contains(v, "text") || strings.HasPrefix(v, "dsbriefname") || strings.HasPrefix(v, "dscomment"):
		return "string"
	case strings.Contains(v, "date") || strings.Contains(v, "time"):
		return "datetime"
	case strings.Contains(v, "bit") || strings.Contains(v, "bool"):
		return "bool"
	default:
		return ""
	}
}

func isSharedTTable(tableName string) bool {
	_, exists := sharedTTables[normalizeIdentifier(tableName)]
	return exists
}
