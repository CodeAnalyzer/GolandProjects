package review

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

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

func (r *Runner) lookupTableProductIDs(tableName string) (map[int64]struct{}, error) {
	result := make(map[int64]struct{})
	name := strings.TrimSpace(tableName)

	scanRows := func(query string) error {
		rows, err := r.db.Query(query, name)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return err
			}
			if id > 0 {
				result[id] = struct{}{}
			}
		}
		return rows.Err()
	}

	if err := scanRows(`
		SELECT DISTINCT f.ds_product_id
		FROM sql_tables t
		JOIN files f ON f.id = t.file_id
		WHERE LOWER(t.table_name) = LOWER($1)
		  AND t.context IN ('create', 'select_into')
		  AND f.ds_product_id IS NOT NULL
	`); err != nil {
		return nil, err
	}

	if len(result) > 0 {
		return result, nil
	}

	if err := scanRows(`
		SELECT DISTINCT f.ds_product_id
		FROM sql_column_definitions scd
		JOIN files f ON f.id = scd.file_id
		WHERE LOWER(scd.table_name) = LOWER($1)
		  AND scd.definition_kind IN ('create', 'select_into')
		  AND f.ds_product_id IS NOT NULL
	`); err != nil {
		return nil, err
	}

	if len(result) > 0 {
		return result, nil
	}

	if err := scanRows(`
		SELECT DISTINCT f.ds_product_id
		FROM sql_tables t
		JOIN files f ON f.id = t.file_id
		WHERE LOWER(t.table_name) = LOWER($1)
		  AND t.context = 'dfm_embedded'
		  AND f.ds_product_id IS NOT NULL
	`); err != nil {
		return nil, err
	}

	return result, nil
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

func (r *Runner) lookupProcedureCreateFiles(procName string) ([]int64, error) {
	rows, err := r.db.Query(`
		SELECT DISTINCT f.id
		FROM sql_procedures p
		JOIN files f ON f.id = p.file_id
		WHERE LOWER(p.proc_name) = LOWER($1)
		  AND LOWER(f.path) NOT LIKE '%/upload/%'
		  AND LOWER(f.path) NOT LIKE '%\upload\%'
		  AND LOWER(f.path) NOT LIKE '%.t01'
		ORDER BY f.id
	`, strings.TrimSpace(procName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *Runner) lookupIndexExists(tableName, indexName string) (bool, error) {
	normalizedTable := strings.TrimSpace(tableName)
	normalizedIndex := strings.TrimSpace(indexName)
	if normalizedTable == "" || normalizedIndex == "" {
		return false, nil
	}

	var exists bool
	err := r.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM sql_index_definitions i
			WHERE LOWER(i.table_name) = LOWER($1)
			  AND LOWER(i.index_name) = LOWER($2)
		)
	`, normalizedTable, normalizedIndex).Scan(&exists)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	err = r.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM api_business_object_table_indexes i
			JOIN api_business_object_tables t ON t.id = i.business_table_id
			WHERE LOWER(t.table_name) = LOWER($1)
			  AND LOWER(i.index_name) = LOWER($2)
		)
	`, normalizedTable, normalizedIndex).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (r *Runner) lookupIndexFieldsByName(indexName string) ([]string, error) {
	normalized := strings.TrimSpace(indexName)
	if normalized == "" {
		return nil, nil
	}

	rows, err := r.db.Query(`
		SELECT f.field_name
		FROM sql_index_definitions i
		LEFT JOIN sql_index_definition_fields f ON f.table_index_id = i.id
		WHERE LOWER(i.index_name) = LOWER($1)
		ORDER BY f.field_order, f.id
	`, normalized)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fields []string
	for rows.Next() {
		var field sql.NullString
		if err := rows.Scan(&field); err != nil {
			return nil, err
		}
		if field.Valid {
			f := normalizeIdentifier(field.String)
			if f != "" {
				fields = append(fields, f)
			}
		}
	}

	// Также проверяем API-индексы
	if len(fields) == 0 {
		rows2, err := r.db.Query(`
			SELECT f.field_name
			FROM api_business_object_table_indexes i
			JOIN api_business_object_tables t ON t.id = i.business_table_id
			LEFT JOIN api_business_object_table_index_fields f ON f.table_index_id = i.id
			WHERE LOWER(i.index_name) = LOWER($1)
			ORDER BY f.field_order, f.id
		`, normalized)
		if err != nil {
			return nil, err
		}
		defer rows2.Close()

		for rows2.Next() {
			var field sql.NullString
			if err := rows2.Scan(&field); err != nil {
				return nil, err
			}
			if field.Valid {
				f := normalizeIdentifier(field.String)
				if f != "" {
					fields = append(fields, f)
				}
			}
		}
		if err := rows2.Err(); err != nil {
			return nil, err
		}
	}

	return fields, rows.Err()
}

func (r *Runner) lookupTableIndexCandidates(tableName string) ([]tableIndexCandidate, error) {
	normalizedTable := strings.TrimSpace(tableName)
	if normalizedTable == "" {
		return nil, nil
	}

	type aggregate struct {
		name       string
		fieldsByNo map[int]string
	}

	items := make(map[string]*aggregate)
	order := make([]string, 0)

	consumeRows := func(query string) error {
		rows, err := r.db.Query(query, normalizedTable)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var indexID int64
			var indexName string
			var fieldName sql.NullString
			var fieldOrder sql.NullInt64
			if err := rows.Scan(&indexID, &indexName, &fieldName, &fieldOrder); err != nil {
				return err
			}

			key := fmt.Sprintf("%d:%s", indexID, normalizeIdentifier(indexName))
			agg, exists := items[key]
			if !exists {
				agg = &aggregate{name: strings.TrimSpace(indexName), fieldsByNo: map[int]string{}}
				items[key] = agg
				order = append(order, key)
			}

			if fieldName.Valid {
				field := normalizeIdentifier(fieldName.String)
				if field != "" {
					no := len(agg.fieldsByNo) + 1
					if fieldOrder.Valid && fieldOrder.Int64 > 0 {
						no = int(fieldOrder.Int64)
					}
					agg.fieldsByNo[no] = field
				}
			}
		}
		return rows.Err()
	}

	if err := consumeRows(`
		SELECT i.id, i.index_name, f.field_name, f.field_order
		FROM sql_index_definitions i
		LEFT JOIN sql_index_definition_fields f ON f.table_index_id = i.id
		WHERE LOWER(i.table_name) = LOWER($1)
		ORDER BY i.id, f.field_order, f.id
	`); err != nil {
		return nil, err
	}

	if err := consumeRows(`
		SELECT i.id, i.index_name, f.field_name, f.field_order
		FROM api_business_object_table_indexes i
		JOIN api_business_object_tables t ON t.id = i.business_table_id
		LEFT JOIN api_business_object_table_index_fields f ON f.table_index_id = i.id
		WHERE LOWER(t.table_name) = LOWER($1)
		ORDER BY i.id, f.field_order, f.id
	`); err != nil {
		return nil, err
	}

	result := make([]tableIndexCandidate, 0, len(order))
	for _, key := range order {
		agg := items[key]
		if agg == nil {
			continue
		}
		fields := make([]string, 0, len(agg.fieldsByNo))
		for i := 1; i <= len(agg.fieldsByNo); i++ {
			if field, exists := agg.fieldsByNo[i]; exists {
				fields = append(fields, field)
			}
		}
		result = append(result, tableIndexCandidate{Name: agg.name, Fields: fields})
	}

	return result, nil
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

func isWordChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func keywordMatchAt(lower string, pos int, keyword string) bool {
	if pos < 0 || pos+len(keyword) > len(lower) {
		return false
	}
	if lower[pos:pos+len(keyword)] != keyword {
		return false
	}
	if pos > 0 && isWordChar(lower[pos-1]) {
		return false
	}
	after := pos + len(keyword)
	if after < len(lower) && isWordChar(lower[after]) {
		return false
	}
	return true
}

func toLowerASCIIPreservingLen(text string) string {
	b := []byte(text)
	for i := 0; i < len(b); i++ {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

func safeSliceByBounds(text string, start, end int) (string, bool) {
	if start < 0 {
		start = 0
	}
	if start > len(text) {
		start = len(text)
	}
	if end < start {
		end = start
	}
	if end > len(text) {
		end = len(text)
	}
	if start >= end {
		return "", false
	}
	return text[start:end], true
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

// splitByOrRespectingParens разбивает строку по OR, но только те что вне скобок
// OR внутри скобок (al.DateLast = '19000101' or al.DateLast > @Date) не используются как разделители верхнего уровня
func splitByOrRespectingParens(sql string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	lower := strings.ToLower(sql)

	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		}

		// Проверяем OR на нулевом уровне
		if depth == 0 && i+2 < len(sql) {
			if lower[i] == ' ' && lower[i+1] == 'o' && lower[i+2] == 'r' {
				// Проверяем, что это полное слово OR (перед пробел, после пробел или конец)
				isOrWord := false
				if i+3 >= len(sql) || lower[i+3] == ' ' {
					isOrWord = true
				}
				if isOrWord {
					parts = append(parts, current.String())
					current.Reset()
					i += 2 // Пропускаем 'or'
					continue
				}
			}
		}
		current.WriteByte(ch)
	}

	// Добавляем последнюю часть
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

func findTopLevelFromClauseBounds(text string) (int, int, bool) {
	lower := toLowerASCIIPreservingLen(text)
	fromIdx := -1
	depth := 0
	inString := false

	for i := 0; i < len(lower); i++ {
		ch := lower[i]
		if ch == '\'' {
			if inString && i+1 < len(lower) && lower[i+1] == '\'' {
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

		if depth == 0 && keywordMatchAt(lower, i, "from") {
			fromIdx = i
			break
		}
	}

	if fromIdx < 0 {
		return 0, 0, false
	}

	// Ищем конец FROM clause
	endIdx := len(lower)
	for i := fromIdx + 4; i < len(lower); i++ {
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
			if keywordMatchAt(lower, i, "where") ||
				keywordMatchAt(lower, i, "group by") ||
				keywordMatchAt(lower, i, "order by") ||
				keywordMatchAt(lower, i, "having") ||
				keywordMatchAt(lower, i, "union") ||
				keywordMatchAt(lower, i, "except") ||
				keywordMatchAt(lower, i, "intersect") {
				endIdx = i
				break
			}
		}
	}

	return fromIdx, endIdx, true
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

func extractTablesFromFromClause(fullText string) []tableFromClause {
	result := make([]tableFromClause, 0)

	// Удаляем комментарии перед парсингом
	fullText = removeComments(fullText)
	lower := toLowerASCIIPreservingLen(fullText)

	// Для UPDATE ... FROM синтаксиса: ищем FROM после SET,
	// пропуская FROM внутри скобок (подзапросы в SET)
	if strings.HasPrefix(lower, "update") {
		setIdx := strings.Index(lower, " set ")
		if setIdx > 0 {
			fromIdx := -1
			depth := 0
			for i := setIdx; i < len(lower)-5; i++ {
				switch lower[i] {
				case '(':
					depth++
				case ')':
					if depth > 0 {
						depth--
					}
				}
				if depth == 0 && lower[i] == ' ' && lower[i+1:i+5] == "from" && (i+5 >= len(lower) || lower[i+5] == ' ') {
					fromIdx = i
					break
				}
			}
			if fromIdx >= 0 {
				fromStart := fromIdx
				// Ищем конец FROM clause (до WHERE, GROUP BY, ORDER BY и т.д.)
				fromEnd := len(lower)
				for i := fromStart + 5; i < len(lower); i++ {
					if keywordMatchAt(lower, i, "where") ||
						keywordMatchAt(lower, i, "group by") ||
						keywordMatchAt(lower, i, "order by") ||
						keywordMatchAt(lower, i, "having") {
						fromEnd = i
						break
					}
				}
				fromClause, ok := safeSliceByBounds(fullText, fromStart, fromEnd)
				if !ok {
					return result
				}
				return parseTablesInFromClause(fromClause)
			}
		}
	}

	// Для обычных SELECT/DELETE: ищем FROM на верхнем уровне
	fromStart, fromEnd, found := findTopLevelFromClauseBounds(fullText)
	if !found {
		return result
	}

	fromClause, ok := safeSliceByBounds(fullText, fromStart, fromEnd)
	if !ok {
		return result
	}
	// Проверяем, что fromClause действительно начинается с FROM
	if !strings.HasPrefix(strings.ToLower(fromClause), "from") {
		return result
	}

	return parseTablesInFromClause(fromClause)
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

func extractFirstTableFromFromClause(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}

	lower := strings.ToLower(trimmed)

	// Пропускаем открывающую скобку подзапроса
	if strings.HasPrefix(trimmed, "(") {
		return ""
	}

	// Ищем конец имени таблицы (пробел, запятая, JOIN, WHERE, etc.)
	endMarkers := []string{" ", "\t", ",", "\n", "\r", "inner", "left", "right", "full", "cross", "join", "where", "group", "having", "order", "union", "except", "intersect", ";"}
	endIdx := len(trimmed)
	for _, marker := range endMarkers {
		if idx := strings.Index(lower, marker); idx >= 0 && idx < endIdx {
			endIdx = idx
		}
	}

	if endIdx > 0 {
		table := strings.TrimSpace(trimmed[:endIdx])
		// Убираем возможные хинты вида "table M_INDEX(...)" или "table WITH (...)"
		if spaceIdx := strings.Index(table, " "); spaceIdx > 0 {
			table = table[:spaceIdx]
		}
		return table
	}

	return trimmed
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

func extractUpdateTargetTable(fullText string) string {
	fullText = strings.TrimSpace(fullText)
	lower := strings.ToLower(fullText)
	if !strings.HasPrefix(lower, "update") {
		return ""
	}

	remainder := strings.TrimSpace(fullText[len("update"):])
	if remainder == "" {
		return ""
	}

	parts := strings.Fields(remainder)
	if len(parts) == 0 {
		return ""
	}

	if strings.EqualFold(parts[0], "top") {
		if len(parts) < 2 {
			return ""
		}
		i := 1
		if strings.HasPrefix(parts[i], "(") {
			for i < len(parts) && !strings.Contains(parts[i], ")") {
				i++
			}
			if i+1 < len(parts) {
				return strings.TrimSpace(parts[i+1])
			}
			return ""
		}
		if len(parts) >= 3 {
			return strings.TrimSpace(parts[2])
		}
		return ""
	}

	return strings.TrimSpace(parts[0])
}

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

func findStatementStartHint(lower string) (string, int) {
	keywords := []string{"select", "update", "delete", "insert"}
	for _, kw := range keywords {
		if idx := findKeywordPosition(lower, kw); idx >= 0 {
			return kw, idx
		}
	}
	return "", -1
}

func (r *Runner) lookupProcedureParams(procName string) ([]model.SQLParam, error) {
	var paramsJSON []byte
	err := r.db.QueryRow(`
		SELECT parameters
		FROM sql_procedures
		WHERE LOWER(proc_name) = LOWER($1)
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(procName)).Scan(&paramsJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err == nil && len(paramsJSON) > 0 {
		var params []model.SQLParam
		if err := json.Unmarshal(paramsJSON, &params); err != nil {
			return nil, err
		}
		if len(params) > 0 {
			return params, nil
		}
	}
	// fallback: API-контракты, где параметры живут в api_contract_params
	return r.lookupAPIContractParams(procName)
}

func (r *Runner) lookupAPIContractParams(procName string) ([]model.SQLParam, error) {
	var contractID int64
	err := r.db.QueryRow(`
		SELECT id FROM api_contracts
		WHERE LOWER(contract_name) = LOWER($1)
		ORDER BY id DESC LIMIT 1
	`, strings.TrimSpace(procName)).Scan(&contractID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(`
		SELECT param_name, COALESCE(type_name,''), direction
		FROM api_contract_params
		WHERE contract_id = $1
		ORDER BY param_order, id
	`, contractID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var params []model.SQLParam
	for rows.Next() {
		var paramName, typeName, direction string
		if err := rows.Scan(&paramName, &typeName, &direction); err != nil {
			return nil, err
		}
		paramName = strings.TrimPrefix(strings.TrimSpace(paramName), "@")
		dir := "in"
		switch strings.ToLower(direction) {
		case "output":
			dir = "out"
		case "input", "context":
			dir = "in"
		}
		params = append(params, model.SQLParam{
			Name:      paramName,
			Type:      typeName,
			Direction: dir,
		})
	}
	return params, rows.Err()
}

// filterKnownNames фильтрует список имён, удаляя известные Diasoft макросы (M_*)
// и константы из H-файлов (h_files_defines).
func (r *Runner) filterKnownNames(names []string) []string {
	if len(names) == 0 {
		return names
	}
	result := make([]string, 0, len(names))
	for _, name := range names {
		lower := strings.ToLower(name)
		if sqlMacrosMap[lower] {
			continue
		}
		// Проверяем в h_files_defines только если есть подключение к БД
		if r.db != nil {
			exists, err := r.db.FindHDefineExistsByName(name)
			if err == nil && exists {
				continue
			}
		}
		result = append(result, name)
	}
	return result
}
