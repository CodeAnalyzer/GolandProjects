package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/codebase/internal/fswalk"
	"github.com/codebase/internal/model"
	pasparser "github.com/codebase/internal/parser/pas"
	"github.com/codebase/internal/parser/retcode"
	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/util"
)

// Precompiled regexps for indexer_sql_pas (static patterns).
var (
	reIdxAliasFromJoin    = regexp.MustCompile(`(?i)\b(?:from|join)\s+([A-Za-z_#][A-Za-z0-9_#]*)\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*)\b`)
	reIdxAsAliasSuffix    = regexp.MustCompile(`(?i)\bas\s+([A-Za-z_#][A-Za-z0-9_#]*)\s*$`)
	reIdxStripAsAlias     = regexp.MustCompile(`(?i)\s+as\s+[A-Za-z_#][A-Za-z0-9_#]*\s*$`)
	reIdxDirectColRef     = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*$`)
	reIdxIsnullCoalesce   = regexp.MustCompile(`(?i)^\s*(?:isnull|coalesce)\s*\(`)
	reIdxQualifiedColRef  = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)`)
)

// parseSQLFile парсит SQL-файл с использованием batch-вставки
func (idx *Indexer) parseSQLFile(ctx context.Context, file fswalk.FileInfo, fileID int64, stats *model.ScanStats) error {
	return idx.parseSQLLikeFile(ctx, file, fileID, stats, true, false)
}

func (idx *Indexer) parseSQLLikeFile(ctx context.Context, file fswalk.FileInfo, fileID int64, stats *model.ScanStats, includeAPIMacros bool, includeGeneratedSubscriberCalls bool) error {
	path := file.Path
	content, err := decodeIndexedFileContent(file)
	if err != nil {
		return fmt.Errorf("failed to decode SQL file: %w", err)
	}
	parser := sqlparser.NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		return fmt.Errorf("failed to parse SQL file: %w", err)
	}
	if err := idx.enrichSelectIntoDataTypes(ctx, result); err != nil {
		return fmt.Errorf("failed to enrich select_into data types: %w", err)
	}

	// Готовим batch-срезы для всех сущностей
	proceduresBatch := make([]*model.SQLProcedure, 0, len(result.Procedures))
	tablesBatch := make([]*model.SQLTable, 0, len(result.Tables))
	columnsBatch := make([]*model.SQLColumn, 0, len(result.Columns))
	columnDefinitionsBatch := make([]*model.SQLColumnDefinition, 0, len(result.ColumnDefinitions))
	indexDefinitionsBatch := make([]*model.SQLIndexDefinition, 0, len(result.IndexDefinitions))
	indexDefinitionFieldsBatch := make([]*model.SQLIndexDefinitionField, 0, len(result.IndexFields))
	fragmentsBatch := make([]*model.QueryFragment, 0, len(result.Fragments))
	definesBatch := make([]*model.HDefine, 0, len(result.Defines))
	symbolsBatch := make([]*model.Symbol, 0, len(result.Procedures)+len(result.Tables)+len(result.Defines))

	// Собираем процедуры в batch
	for _, proc := range result.Procedures {
		proc.FileID = fileID
		proceduresBatch = append(proceduresBatch, proc)

		// Готовим символ для процедуры
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: proc.ProcName,
			SymbolType: "procedure",
			EntityType: "sql",
			LineNumber: proc.LineStart,
			Signature:  proc.ProcName,
		})
	}

	// Собираем таблицы в batch
	for _, table := range result.Tables {
		table.FileID = fileID
		tablesBatch = append(tablesBatch, table)

		// Готовим символ для таблицы
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: table.TableName,
			SymbolType: "table",
			EntityType: "sql",
			LineNumber: table.LineNumber,
			SQLContext: table.Context,
			Signature:  table.TableName,
		})
	}

	// Собираем колонки в batch
	for _, column := range result.Columns {
		column.FileID = fileID
		columnsBatch = append(columnsBatch, column)
	}

	for _, columnDefinition := range result.ColumnDefinitions {
		columnDefinition.FileID = fileID
		columnDefinitionsBatch = append(columnDefinitionsBatch, columnDefinition)
	}

	for _, indexDefinition := range result.IndexDefinitions {
		indexDefinition.FileID = fileID
		indexDefinitionsBatch = append(indexDefinitionsBatch, indexDefinition)
	}

	indexDefinitionFieldsBatch = append(indexDefinitionFieldsBatch, result.IndexFields...)

	for _, fragment := range result.Fragments {
		if fragment == nil {
			continue
		}
		fragment.FileID = fileID
		fragment.QueryHash = computeQueryHash(fragment.QueryText)
		fragment.ParentType = "sql_file"
		fragment.ParentID = 0
		tablesReferenced := make([]string, 0)
		for _, table := range result.Tables {
			if table == nil {
				continue
			}
			if table.LineNumber < fragment.LineNumber {
				continue
			}
			if fragment.LineEnd > 0 && table.LineNumber > fragment.LineEnd {
				continue
			}
			tablesReferenced = append(tablesReferenced, table.TableName)
		}
		fragment.TablesReferenced = uniqueStrings(tablesReferenced)
		fragmentsBatch = append(fragmentsBatch, fragment)
	}

	for _, define := range result.Defines {
		if define == nil {
			continue
		}
		define.FileID = fileID
		definesBatch = append(definesBatch, define)
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: define.DefineName,
			SymbolType: "define",
			EntityType: "sql",
			LineNumber: define.LineNumber,
			Signature:  define.DefineValue,
		})
	}

	// Выполняем batch-вставку процедур
	if len(proceduresBatch) > 0 {
		if err := idx.db.BatchInsertSQLProcedures(ctx, proceduresBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting procedures: %v", err)
			stats.Errors += len(proceduresBatch)
			return err
		}
		stats.Procedures += len(proceduresBatch)
	}

	// Выполняем batch-вставку таблиц
	if len(tablesBatch) > 0 {
		if err := idx.db.BatchInsertSQLTables(ctx, tablesBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting tables: %v", err)
			stats.Errors += len(tablesBatch)
			return err
		}
		stats.Tables += len(tablesBatch)
	}

	// Выполняем batch-вставку колонок
	if len(columnsBatch) > 0 {
		if err := idx.db.BatchInsertSQLColumns(ctx, columnsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting columns: %v", err)
			stats.Errors += len(columnsBatch)
			return err
		}
		stats.Columns += len(columnsBatch)
	}

	if len(columnDefinitionsBatch) > 0 {
		if err := idx.db.BatchInsertSQLColumnDefinitions(ctx, columnDefinitionsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting column definitions: %v", err)
			stats.Errors += len(columnDefinitionsBatch)
			return err
		}
		columnDefinitionIDs, err := idx.db.FindSQLColumnDefinitionIDsByFile(ctx, fileID)
		if err != nil {
			return fmt.Errorf("failed to resolve SQL column definition ids for symbols: %w", err)
		}
		columnDefinitionSymbolsBatch := make([]*model.Symbol, 0, len(columnDefinitionsBatch))
		for _, columnDefinition := range columnDefinitionsBatch {
			if columnDefinition == nil {
				continue
			}
			columnDefinitionID := columnDefinitionIDs[store.BuildSQLColumnDefinitionLookupKey(columnDefinition.TableName, columnDefinition.ColumnName, columnDefinition.LineNumber, columnDefinition.ColumnOrder)]
			if columnDefinitionID == 0 {
				continue
			}
			signature := strings.TrimSpace(columnDefinition.TableName + "." + columnDefinition.ColumnName + " " + columnDefinition.DataType)
			columnDefinitionSymbolsBatch = append(columnDefinitionSymbolsBatch, &model.Symbol{
				FileID:     fileID,
				SymbolName: columnDefinition.ColumnName,
				SymbolType: "column_definition",
				EntityType: "sql",
				EntityID:   columnDefinitionID,
				LineNumber: columnDefinition.LineNumber,
				Signature:  signature,
			})
		}
		if len(columnDefinitionSymbolsBatch) > 0 {
			if err := idx.db.BatchInsertSymbols(ctx, columnDefinitionSymbolsBatch, idx.config.Indexer.BatchSize); err != nil {
				return fmt.Errorf("failed to save SQL column definition symbols: %w", err)
			}
		}
	}

	var indexIDs map[string]int64
	if len(indexDefinitionsBatch) > 0 {
		if err := idx.db.BatchInsertSQLIndexDefinitions(ctx, indexDefinitionsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting SQL index definitions: %v", err)
			stats.Errors += len(indexDefinitionsBatch)
			return err
		}
		var err error
		indexIDs, err = idx.db.FindSQLIndexDefinitionIDsByFile(ctx, fileID)
		if err != nil {
			return fmt.Errorf("failed to resolve SQL index definition ids for symbols: %w", err)
		}
		indexSymbolsBatch := make([]*model.Symbol, 0, len(indexDefinitionsBatch))
		for _, indexDefinition := range indexDefinitionsBatch {
			if indexDefinition == nil {
				continue
			}
			indexID := indexIDs[store.BuildSQLIndexDefinitionLookupKey(indexDefinition.TableName, indexDefinition.IndexName, indexDefinition.LineNumber)]
			if indexID == 0 {
				continue
			}
			signature := strings.TrimSpace(indexDefinition.TableName + "(" + indexDefinition.IndexFields + ")")
			if signature == "()" {
				signature = indexDefinition.IndexType
			}
			indexSymbolsBatch = append(indexSymbolsBatch, &model.Symbol{
				FileID:     fileID,
				SymbolName: indexDefinition.IndexName,
				SymbolType: "index",
				EntityType: "sql",
				EntityID:   indexID,
				LineNumber: indexDefinition.LineNumber,
				Signature:  signature,
			})
		}
		if len(indexSymbolsBatch) > 0 {
			if err := idx.db.BatchInsertSymbols(ctx, indexSymbolsBatch, idx.config.Indexer.BatchSize); err != nil {
				return fmt.Errorf("failed to save SQL index definition symbols: %w", err)
			}
		}
	}

	if len(indexDefinitionFieldsBatch) > 0 {
		if indexIDs == nil {
			var err error
			indexIDs, err = idx.db.FindSQLIndexDefinitionIDsByFile(ctx, fileID)
			if err != nil {
				return fmt.Errorf("failed to resolve SQL index definition ids: %w", err)
			}
		}
		fieldsToPersist := make([]*model.SQLIndexDefinitionField, 0, len(indexDefinitionFieldsBatch))
		for _, field := range indexDefinitionFieldsBatch {
			if field == nil {
				continue
			}
			key := store.BuildSQLIndexDefinitionLookupKey(field.ParentTableName, field.ParentIndexName, field.LineNumber)
			field.TableIndexID = indexIDs[key]
			if field.TableIndexID == 0 {
				continue
			}
			fieldsToPersist = append(fieldsToPersist, field)
		}
		if len(fieldsToPersist) > 0 {
			if err := idx.db.BatchInsertSQLIndexDefinitionFields(ctx, fieldsToPersist, idx.config.Indexer.BatchSize); err != nil {
				idx.logError(path, "Error batch inserting SQL index definition fields: %v", err)
				stats.Errors += len(fieldsToPersist)
				return err
			}
		}
	}

	if len(definesBatch) > 0 {
		if err := idx.db.BatchInsertHDefines(ctx, definesBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting SQL defines: %v", err)
			stats.Errors += len(definesBatch)
			return err
		}
		stats.Defines += len(definesBatch)
	}

	procedureIDs, err := idx.db.FindSQLProcedureIDsByFile(ctx, fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve SQL procedure ids for symbols: %w", err)
	}
	tableIDs, err := idx.db.FindSQLTableIDsByFileAndLine(ctx, fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve SQL table ids for symbols: %w", err)
	}
	defineIDs, err := idx.db.FindHDefineIDsByFile(ctx, fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve SQL define ids for symbols: %w", err)
	}
	for _, symbol := range symbolsBatch {
		switch symbol.SymbolType {
		case "procedure":
			symbol.EntityID = procedureIDs[strings.ToLower(strings.TrimSpace(symbol.SymbolName))]
		case "table":
			key := store.BuildSQLTableLookupKey(symbol.SymbolName, symbol.SQLContext, symbol.LineNumber)
			symbol.EntityID = tableIDs[key]
		case "define":
			symbol.EntityID = defineIDs[store.BuildHDefineLookupKey(symbol.SymbolName, symbol.LineNumber)]
		}
	}
	for _, fragment := range fragmentsBatch {
		if fragment == nil {
			continue
		}
		for _, proc := range proceduresBatch {
			if proc == nil {
				continue
			}
			if fragment.LineNumber < proc.LineStart || (proc.LineEnd > 0 && fragment.LineNumber > proc.LineEnd) {
				continue
			}
			fragment.ParentType = "sql_procedure"
			fragment.ParentID = procedureIDs[strings.ToLower(strings.TrimSpace(proc.ProcName))]
			fragment.ComponentName = proc.ProcName
			fragment.ComponentType = "sql_procedure"
			break
		}
		if fragment.ParentID == 0 && strings.TrimSpace(fragment.ComponentName) == "" {
			fragment.ComponentName = "sql_script"
		}
		if fragment.ParentID == 0 && strings.TrimSpace(fragment.ComponentType) == "" {
			fragment.ComponentType = "sql_script"
		}
	}

	// Выполняем batch-вставку символов
	if len(symbolsBatch) > 0 {
		if err := idx.db.BatchInsertSymbols(ctx, symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting symbols: %v", err)
			stats.Errors += len(symbolsBatch)
			return err
		}
	}
	if len(fragmentsBatch) > 0 {
		if err := idx.db.BatchInsertQueryFragments(ctx, fragmentsBatch, idx.config.Indexer.BatchSize); err != nil {
			idx.logError(path, "Error batch inserting SQL query fragments: %v", err)
			stats.Errors += len(fragmentsBatch)
			return err
		}
		stats.QueryFragments += len(fragmentsBatch)
	}

	idx.addPendingSQLCalls(fileID, path, proceduresBatch, procedureIDs, result.Calls)

	relations, err := buildSQLProcedureTableRelations(procedureIDs, tableIDs, proceduresBatch, tablesBatch)
	if err != nil {
		return fmt.Errorf("failed to build SQL relations: %w", err)
	}
	if includeAPIMacros {
		macroRelations, err := idx.indexAPIMacros(ctx, path, content, fileID)
		if err != nil {
			return fmt.Errorf("failed to index SQL API macros: %w", err)
		}
		relations = append(relations, macroRelations...)
	}
	queryRelations, err := idx.buildQueryFragmentRelations(ctx, fileID, fragmentsBatch)
	if err != nil {
		return fmt.Errorf("failed to build SQL query relations: %w", err)
	}
	relations = append(relations, queryRelations...)
	if includeGeneratedSubscriberCalls {
		generatedRelations, err := idx.buildT01GeneratedSubscriberRelations(ctx, content, fileID, proceduresBatch, result.Calls)
		if err != nil {
			return fmt.Errorf("failed to build T01 generated subscriber relations: %w", err)
		}
		relations = append(relations, generatedRelations...)
	}
	if err := idx.saveRelations(ctx, relations, path, stats); err != nil {
		return err
	}

	// Обрабатываем include директивы (остаются как есть)
	for _, inc := range result.Includes {
		if err := idx.saveIncludeDirective(ctx, fileID, path, inc.IncludePath, inc.LineNumber); err != nil {
			return fmt.Errorf("failed to save include directive %s: %w", inc.IncludePath, err)
		}
	}

	// Retcode parsing: prescreen content, then parse and batch insert.
	// Контент уже прочитан walker'ом и декодирован выше — повторное чтение не нужно.
	if retcode.HasReturnCodes(content) {
		entries := retcode.Parse(content)
		for _, e := range entries {
			e.FileID = fileID
		}
		if len(entries) > 0 {
			if err := idx.db.BatchInsertRetCodes(ctx, entries, idx.config.Indexer.BatchSize); err != nil {
				idx.logError(path, "Error batch inserting return codes: %v", err)
			}
		}
	}

	return nil
}

type selectIntoFragmentInfo struct {
	ProjectionSegments []string
	AliasToTable       map[string]string
}

func (idx *Indexer) enrichSelectIntoDataTypes(ctx context.Context, result *sqlparser.ParseResult) error {
	if result == nil || len(result.ColumnDefinitions) == 0 || len(result.Fragments) == 0 {
		return nil
	}
	fragmentCache := make(map[int64]*selectIntoFragmentInfo)
	typeCache := make(map[string]string)
	for _, definition := range result.ColumnDefinitions {
		if definition == nil || !strings.EqualFold(strings.TrimSpace(definition.DefinitionKind), "select_into") {
			continue
		}
		fragment := findSelectIntoFragment(result.Fragments, definition.LineNumber, definition.TableName)
		if fragment == nil {
			continue
		}
		cacheKey := int64(fragment.LineNumber)*1000000 + int64(fragment.LineEnd)
		info, ok := fragmentCache[cacheKey]
		if !ok {
			parsed, parseOk := parseSelectIntoFragmentInfo(fragment.QueryText, definition.TableName)
			if !parseOk {
				fragmentCache[cacheKey] = nil
				continue
			}
			fragmentCache[cacheKey] = parsed
			info = parsed
		}
		if info == nil || definition.ColumnOrder <= 0 || definition.ColumnOrder > len(info.ProjectionSegments) {
			continue
		}
		segment := strings.TrimSpace(info.ProjectionSegments[definition.ColumnOrder-1])
		if segment == "" {
			continue
		}
		if outputName := inferSelectIntoOutputName(segment); outputName != "" {
			definition.ColumnName = outputName
		}
		if !strings.EqualFold(strings.TrimSpace(definition.DataType), "DSUNKNOWN") {
			continue
		}
		qualifier, sourceColumn, ok := extractSimpleSourceColumn(segment)
		if !ok {
			continue
		}
		resolvedTable := strings.TrimSpace(qualifier)
		if mapped, hasAlias := info.AliasToTable[strings.ToLower(resolvedTable)]; hasAlias {
			resolvedTable = mapped
		}
		if resolvedTable == "" || sourceColumn == "" {
			continue
		}
		typeKey := strings.ToLower(strings.TrimSpace(resolvedTable)) + "|" + strings.ToLower(strings.TrimSpace(sourceColumn))
		if cachedType, hasCached := typeCache[typeKey]; hasCached {
			if cachedType != "" {
				definition.DataType = cachedType
			}
			continue
		}
		resolvedType, err := idx.db.FindLatestSQLColumnDefinitionType(ctx, resolvedTable, sourceColumn)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				typeCache[typeKey] = ""
				continue
			}
			return err
		}
		resolvedType = strings.TrimSpace(resolvedType)
		typeCache[typeKey] = resolvedType
		if resolvedType != "" {
			definition.DataType = resolvedType
		}
	}
	return nil
}

func findSelectIntoFragment(fragments []*model.QueryFragment, lineNumber int, tableName string) *model.QueryFragment {
	if lineNumber <= 0 || strings.TrimSpace(tableName) == "" {
		return nil
	}
	tablePattern := util.CachedRegexp(`(?i)\binto\s+` + regexp.QuoteMeta(strings.TrimSpace(tableName)) + `\b`)
	for _, fragment := range fragments {
		if fragment == nil {
			continue
		}
		if lineNumber < fragment.LineNumber {
			continue
		}
		if fragment.LineEnd > 0 && lineNumber > fragment.LineEnd {
			continue
		}
		if !tablePattern.MatchString(fragment.QueryText) {
			continue
		}
		return fragment
	}
	return nil
}

func parseSelectIntoFragmentInfo(queryText string, targetTable string) (*selectIntoFragmentInfo, bool) {
	projection, tail, ok := extractTopLevelSelectIntoParts(queryText, targetTable)
	if !ok {
		return nil, false
	}
	segments := splitSQLByTopLevelCommaLocal(projection)
	if len(segments) == 0 {
		return nil, false
	}
	aliasToTable := make(map[string]string)
	aliasMatches := reIdxAliasFromJoin.FindAllStringSubmatch(tail, -1)
	for _, match := range aliasMatches {
		if len(match) < 3 {
			continue
		}
		tableName := strings.TrimSpace(match[1])
		alias := strings.TrimSpace(match[2])
		if tableName == "" || alias == "" {
			continue
		}
		aliasToTable[strings.ToLower(alias)] = tableName
	}
	return &selectIntoFragmentInfo{ProjectionSegments: segments, AliasToTable: aliasToTable}, true
}

func extractTopLevelSelectIntoParts(text string, targetTable string) (string, string, bool) {
	runes := []rune(text)
	parenDepth := 0
	inSingleQuote := false
	for i := 0; i+4 <= len(runes); i++ {
		r := runes[i]
		if r == '\'' {
			if inSingleQuote && i+1 < len(runes) && runes[i+1] == '\'' {
				i++
				continue
			}
			inSingleQuote = !inSingleQuote
			continue
		}
		if inSingleQuote {
			continue
		}
		switch r {
		case '(':
			parenDepth++
			continue
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
			continue
		}
		if parenDepth != 0 {
			continue
		}
		if !strings.EqualFold(string(runes[i:i+4]), "into") {
			continue
		}
		if i > 0 {
			prev := runes[i-1]
			if !(prev == ' ' || prev == '\t' || prev == '\n' || prev == '\r' || prev == ',') {
				continue
			}
		}
		j := i + 4
		for j < len(runes) && (runes[j] == ' ' || runes[j] == '\t' || runes[j] == '\n' || runes[j] == '\r') {
			j++
		}
		if j >= len(runes) {
			continue
		}
		start := j
		for j < len(runes) {
			ch := runes[j]
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '#' {
				j++
				continue
			}
			break
		}
		tableName := strings.TrimSpace(string(runes[start:j]))
		if strings.TrimSpace(targetTable) != "" && !strings.EqualFold(tableName, strings.TrimSpace(targetTable)) {
			continue
		}
		prefix := strings.TrimSpace(string(runes[:i]))
		selectPos := strings.Index(strings.ToLower(prefix), "select")
		if selectPos < 0 {
			continue
		}
		projection := strings.TrimSpace(prefix[selectPos+len("select"):])
		if projection == "" {
			continue
		}
		tail := strings.TrimSpace(string(runes[j:]))
		return projection, tail, true
	}
	return "", "", false
}

func splitSQLByTopLevelCommaLocal(text string) []string {
	parts := make([]string, 0)
	var current strings.Builder
	parenDepth := 0
	inSingleQuote := false
	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if r == '\'' {
			if inSingleQuote && i+1 < len(runes) && runes[i+1] == '\'' {
				current.WriteRune(r)
				current.WriteRune(runes[i+1])
				i++
				continue
			}
			inSingleQuote = !inSingleQuote
			current.WriteRune(r)
			continue
		}
		if !inSingleQuote {
			if r == '-' && i+1 < len(runes) && runes[i+1] == '-' {
				part := strings.TrimSpace(current.String())
				if part != "" {
					parts = append(parts, part)
				}
				current.Reset()
				for i < len(runes) && runes[i] != '\n' {
					i++
				}
				continue
			}
			switch r {
			case '(':
				parenDepth++
			case ')':
				if parenDepth > 0 {
					parenDepth--
				}
			case ',':
				if parenDepth == 0 {
					part := strings.TrimSpace(current.String())
					if part != "" {
						parts = append(parts, part)
					}
					current.Reset()
					continue
				}
			}
		}
		current.WriteRune(r)
	}
	last := strings.TrimSpace(current.String())
	if last != "" {
		parts = append(parts, last)
	}
	return parts
}

func inferSelectIntoOutputName(segment string) string {
	value := strings.TrimSpace(segment)
	if value == "" {
		return ""
	}
	if idx := strings.Index(value, "--"); idx >= 0 {
		value = strings.TrimSpace(value[:idx])
	}
	value = strings.TrimSpace(strings.TrimSuffix(value, ","))
	if value == "" {
		return ""
	}
	if matches := reIdxAsAliasSuffix.FindStringSubmatch(value); len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}
	parts := strings.Fields(value)
	if len(parts) >= 2 {
		candidate := strings.Trim(parts[len(parts)-1], "[]`\",()")
		if candidate != "" {
			return candidate
		}
	}
	if dotIdx := strings.LastIndex(value, "."); dotIdx >= 0 && dotIdx+1 < len(value) {
		candidate := strings.TrimSpace(value[dotIdx+1:])
		candidate = strings.Trim(candidate, "[]`\",()")
		if candidate != "" {
			return candidate
		}
	}
	candidate := strings.Trim(value, "[]`\",()")
	return strings.TrimSpace(candidate)
}

func extractSimpleSourceColumn(segment string) (string, string, bool) {
	value := strings.TrimSpace(segment)
	if value == "" {
		return "", "", false
	}
	if idx := strings.Index(value, "--"); idx >= 0 {
		value = strings.TrimSpace(value[:idx])
	}
	value = strings.TrimSpace(strings.TrimSuffix(value, ","))
	if value == "" {
		return "", "", false
	}
	value = reIdxStripAsAlias.ReplaceAllString(value, "")
	if direct := reIdxDirectColRef.FindStringSubmatch(value); len(direct) == 3 {
		return strings.TrimSpace(direct[1]), strings.TrimSpace(direct[2]), true
	}
	if reIdxIsnullCoalesce.MatchString(value) {
		if ref := reIdxQualifiedColRef.FindStringSubmatch(value); len(ref) == 3 {
			return strings.TrimSpace(ref[1]), strings.TrimSpace(ref[2]), true
		}
	}
	return "", "", false
}

func (idx *Indexer) buildT01GeneratedSubscriberRelations(ctx context.Context, content string, fileID int64, procedures []*model.SQLProcedure, calls []*model.SQLProcedureCall) ([]*model.Relation, error) {
	lines := strings.Split(content, "\n")
	procedureIDs, err := idx.db.FindSQLProcedureIDsByFile(ctx, fileID)
	if err != nil {
		return nil, err
	}

	pendingRefs := make([]*PendingT01SubscriberRef, 0)
	for _, call := range calls {
		if call == nil {
			continue
		}
		if !hasGlobalProcessIDBinding(lines, call.LineNumber) {
			continue
		}
		sourceProc := findProcedureForLine(procedures, call.LineNumber, call.CallerName)
		if sourceProc == nil {
			continue
		}
		sourceID := procedureIDs[strings.ToLower(strings.TrimSpace(sourceProc.ProcName))]
		if sourceID == 0 {
			continue
		}
		calleeName := strings.TrimSpace(call.CalleeName)
		if calleeName == "" {
			continue
		}
		pendingRefs = append(pendingRefs, &PendingT01SubscriberRef{
			SourceID:   sourceID,
			CalleeName: calleeName,
			LineNumber: call.LineNumber,
		})
	}

	idx.addPendingT01SubscriberRefs(pendingRefs)
	return nil, nil
}

func hasGlobalProcessIDBinding(lines []string, lineNumber int) bool {
	if lineNumber <= 0 || len(lines) == 0 {
		return false
	}
	start := lineNumber - 1
	if start < 0 {
		start = 0
	}
	end := start + 6
	if end > len(lines) {
		end = len(lines)
	}
	for i := start; i < end; i++ {
		lineLower := strings.ToLower(lines[i])
		if strings.Contains(lineLower, "@processid") && strings.Contains(lineLower, "@globalprocessid") {
			return true
		}
	}
	return false
}

func findProcedureForLine(procedures []*model.SQLProcedure, lineNumber int, callerName string) *model.SQLProcedure {
	callerName = strings.TrimSpace(callerName)
	if callerName != "" {
		for _, proc := range procedures {
			if proc != nil && strings.EqualFold(strings.TrimSpace(proc.ProcName), callerName) {
				return proc
			}
		}
	}
	for _, proc := range procedures {
		if proc == nil {
			continue
		}
		if lineNumber < proc.LineStart || (proc.LineEnd > 0 && lineNumber > proc.LineEnd) {
			continue
		}
		return proc
	}
	return nil
}

func (idx *Indexer) parsePASFile(ctx context.Context, file fswalk.FileInfo, fileID int64, stats *model.ScanStats) error {
	path := file.Path
	content, err := decodeIndexedFileContent(file)
	if err != nil {
		return fmt.Errorf("failed to decode PAS file: %w", err)
	}
	parser := pasparser.NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		return fmt.Errorf("failed to parse PAS file: %w", err)
	}

	if len(result.Units) == 0 {
		return nil
	}

	unit := result.Units[0]
	unit.FileID = fileID
	if err := idx.db.BatchInsertPASUnits(ctx, []*model.PASUnit{unit}, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS unit: %w", err)
	}
	unitIDs, err := idx.db.FindPASUnitIDsByFile(ctx, fileID)
	if err != nil {
		return fmt.Errorf("failed to resolve PAS unit ids: %w", err)
	}
	unitID := unitIDs[store.BuildPASUnitLookupKey(unit.UnitName, unit.LineStart)]
	if unitID == 0 {
		return fmt.Errorf("failed to resolve persisted PAS unit id for %s", unit.UnitName)
	}
	stats.Units++

	classIDs := make(map[string]int64)
	methodIDs := make(map[string]int64)
	symbolsBatch := make([]*model.Symbol, 0, 1+len(result.Classes)+len(result.Methods))
	symbolsBatch = append(symbolsBatch, &model.Symbol{
		FileID:     fileID,
		SymbolName: unit.UnitName,
		SymbolType: "unit",
		EntityType: "pas",
		EntityID:   unitID,
		LineNumber: unit.LineStart,
	})
	if err := idx.db.BatchInsertSymbols(ctx, symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS unit symbol: %w", err)
	}
	unitSymbolCount := len(symbolsBatch)
	classesBatch := make([]*model.PASClass, 0, len(result.Classes))
	for _, class := range result.Classes {
		class.UnitID = unitID
		classesBatch = append(classesBatch, class)
	}
	if err := idx.db.BatchInsertPASClasses(ctx, classesBatch, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS classes: %w", err)
	}
	stats.Classes += len(classesBatch)

	classIDByLookup, err := idx.db.FindPASClassIDsByUnit(ctx, unitID)
	if err != nil {
		return fmt.Errorf("failed to resolve PAS class ids: %w", err)
	}
	for _, class := range classesBatch {
		classID := classIDByLookup[store.BuildPASClassLookupKey(class.ClassName, class.LineStart)]
		if classID == 0 {
			continue
		}
		classIDs[strings.ToLower(strings.TrimSpace(class.ClassName))] = classID
		idx.addPendingClass(classID, class.ClassName, path)
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: class.ClassName,
			SymbolType: "class",
			EntityType: "pas",
			EntityID:   classID,
			LineNumber: class.LineStart,
			Signature:  class.ParentClass,
		})
	}
	if len(symbolsBatch) > unitSymbolCount {
		if err := idx.db.BatchInsertSymbols(ctx, symbolsBatch[unitSymbolCount:], idx.config.Indexer.BatchSize); err != nil {
			return fmt.Errorf("failed to save PAS class symbols: %w", err)
		}
	}
	classSymbolCount := len(symbolsBatch)

	methodsBatch := make([]*model.PASMethod, 0, len(result.Methods))
	for _, method := range result.Methods {
		method.UnitID = unitID
		if method.ClassName != "" {
			method.ClassID = classIDs[strings.ToLower(strings.TrimSpace(method.ClassName))]
		}
		methodsBatch = append(methodsBatch, method)
	}
	if err := idx.db.BatchInsertPASMethods(ctx, methodsBatch, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS methods: %w", err)
	}
	stats.Methods += len(methodsBatch)

	methodIDByLookup, err := idx.db.FindPASMethodIDsByUnit(ctx, unitID)
	if err != nil {
		return fmt.Errorf("failed to resolve PAS method ids: %w", err)
	}
	for _, method := range methodsBatch {
		methodID := methodIDByLookup[store.BuildPASMethodLookupKey(method.ClassName, method.MethodName, method.LineNumber)]
		if methodID == 0 {
			continue
		}
		methodKey := strings.ToLower(strings.TrimSpace(method.ClassName)) + "|" + strings.ToLower(strings.TrimSpace(method.MethodName))
		methodIDs[methodKey] = methodID
		signature := strings.TrimSpace(method.Signature)
		if signature == "" {
			signature = strings.Trim(strings.TrimSpace(method.ClassName)+"."+strings.TrimSpace(method.MethodName), ".")
		}
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: method.MethodName,
			SymbolType: "method",
			EntityType: "pas",
			EntityID:   methodID,
			LineNumber: method.LineNumber,
			Signature:  signature,
		})
		if method.ClassID == 0 && strings.TrimSpace(method.ClassName) != "" {
			idx.addPendingMethod(methodID, method.ClassName, method.MethodName, path)
		}
	}
	if len(symbolsBatch) > classSymbolCount {
		if err := idx.db.BatchInsertSymbols(ctx, symbolsBatch[classSymbolCount:], idx.config.Indexer.BatchSize); err != nil {
			return fmt.Errorf("failed to save PAS method symbols: %w", err)
		}
	}

	fieldsBatch := make([]*model.PASField, 0, len(result.Fields))
	for _, field := range result.Fields {
		if field.ClassName != "" {
			field.ClassID = classIDs[strings.ToLower(strings.TrimSpace(field.ClassName))]
		}
		fieldsBatch = append(fieldsBatch, field)
	}
	if err := idx.db.BatchInsertPASFields(ctx, fieldsBatch, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS fields: %w", err)
	}
	stats.PASFields += len(fieldsBatch)

	fieldIDByLookup, err := idx.db.FindPASFieldIDsByUnit(ctx, unitID)
	if err != nil {
		return fmt.Errorf("failed to resolve PAS field ids: %w", err)
	}
	for _, field := range fieldsBatch {
		fieldID := fieldIDByLookup[store.BuildPASFieldLookupKey(field.ClassName, field.FieldName, field.LineNumber)]
		if fieldID == 0 {
			continue
		}
		if field.ClassID == 0 && strings.TrimSpace(field.ClassName) != "" {
			idx.addPendingField(fieldID, field.ClassName, field.FieldName, path)
		}
	}

	for _, table := range result.Tables {
		table.FileID = fileID
	}
	if err := idx.db.BatchInsertSQLTables(ctx, result.Tables, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS SQL tables: %w", err)
	}
	stats.Tables += len(result.Tables)

	tablesByLine := make(map[int][]string)
	for _, table := range result.Tables {
		if table == nil {
			continue
		}
		if table.LineNumber <= 0 || strings.TrimSpace(table.TableName) == "" {
			continue
		}
		tablesByLine[table.LineNumber] = append(tablesByLine[table.LineNumber], table.TableName)
	}

	unitName := ""
	for _, unit := range result.Units {
		if unit == nil {
			continue
		}
		unitName = strings.TrimSpace(unit.UnitName)
		if unitName != "" {
			break
		}
	}

	fragmentsBatch := make([]*model.QueryFragment, 0, len(result.SQLFragments))
	for _, fragment := range result.SQLFragments {
		parentType := "pas_unit"
		parentID := unitID
		tablesReferenced := uniqueStrings(tablesByLine[fragment.LineNumber])
		componentName := ""
		componentType := "pas_unit"
		className := strings.TrimSpace(fragment.ClassName)
		methodName := strings.TrimSpace(fragment.MethodName)
		if methodName != "" {
			componentType = "pas_method"
			if className != "" {
				componentName = className + "." + methodName
			} else {
				componentName = methodName
			}
		} else if className != "" {
			componentType = "pas_class"
			componentName = className
		} else {
			componentName = unitName
		}
		if fragment.MethodName != "" {
			methodKey := strings.ToLower(strings.TrimSpace(fragment.ClassName)) + "|" + strings.ToLower(strings.TrimSpace(fragment.MethodName))
			if methodID := methodIDs[methodKey]; methodID > 0 {
				parentType = "pas_method"
				parentID = methodID
			}
		}
		fragmentsBatch = append(fragmentsBatch, &model.QueryFragment{
			FileID:           fileID,
			ParentType:       parentType,
			ParentID:         parentID,
			ComponentName:    componentName,
			ComponentType:    componentType,
			QueryText:        fragment.QueryText,
			QueryHash:        computeQueryHash(fragment.QueryText),
			TablesReferenced: tablesReferenced,
			Context:          fragment.Context,
			LineNumber:       fragment.LineNumber,
		})
	}
	if err := idx.db.BatchInsertQueryFragments(ctx, fragmentsBatch, idx.config.Indexer.BatchSize); err != nil {
		return fmt.Errorf("failed to save PAS query fragments: %w", err)
	}
	stats.QueryFragments += len(fragmentsBatch)

	relations, err := idx.buildQueryFragmentRelations(ctx, fileID, fragmentsBatch)
	if err != nil {
		return fmt.Errorf("failed to build PAS query relations: %w", err)
	}
	if err := idx.saveRelations(ctx, relations, path, stats); err != nil {
		return err
	}

	return nil
}
