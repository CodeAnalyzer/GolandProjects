package sql

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
)

// Parser SQL-парсер
type Parser struct {
	// Регулярки для парсинга
	procBeginRe        *regexp.Regexp
	procEndRe          *regexp.Regexp
	procDeclRe         *regexp.Regexp
	procCreateRe       *regexp.Regexp
	createTableRe      *regexp.Regexp
	alterTableAddRe    *regexp.Regexp
	createIndexRe      *regexp.Regexp
	createIndexStartRe *regexp.Regexp
	createIndexOnRe    *regexp.Regexp
	mAddFieldRe        *regexp.Regexp
	mCreateIndexRe     *regexp.Regexp
	selectIntoRe       *regexp.Regexp
	selectIntoTableRe  *regexp.Regexp
	insertTableRe      *regexp.Regexp
	deleteTableRe      *regexp.Regexp
	deletePTableRe     *regexp.Regexp
	logTableRe         *regexp.Regexp
	selectTempRe       *regexp.Regexp
	tableNameRe        *regexp.Regexp
	continuedTableRe   *regexp.Regexp
	tableAliasRe       *regexp.Regexp
	columnNameRe       *regexp.Regexp
	insertColumnsRe    *regexp.Regexp
	updateColumnsRe    *regexp.Regexp
	varDeclRe          *regexp.Regexp
	procParamRe        *regexp.Regexp
	execRe             *regexp.Regexp
	selectRe           *regexp.Regexp
	insertRe           *regexp.Regexp
	updateRe           *regexp.Regexp
	deleteRe           *regexp.Regexp
	includeRe          *regexp.Regexp
	defineRe           *regexp.Regexp
	commentRe          *regexp.Regexp
	pTableRe           *regexp.Regexp
	tempTableRe        *regexp.Regexp
}

func extractSelectIntoColumnNames(projection string) []string {
	segments := splitSQLByTopLevelComma(projection)
	result := make([]string, 0, len(segments))
	seen := make(map[string]struct{}, len(segments))
	for _, segment := range segments {
		name := inferSelectColumnName(segment)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, name)
	}
	return result
}

func splitSQLByTopLevelComma(text string) []string {
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

func inferSelectColumnName(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}
	if idx := strings.Index(expr, "--"); idx >= 0 {
		expr = strings.TrimSpace(expr[:idx])
	}
	if expr == "" {
		return ""
	}

	asAliasRe := regexp.MustCompile(`(?i)\bas\s+([A-Za-z_#][A-Za-z0-9_#]*)\s*$`)
	if matches := asAliasRe.FindStringSubmatch(expr); matches != nil {
		return strings.TrimSpace(matches[1])
	}

	parts := strings.Fields(expr)
	if len(parts) >= 2 {
		candidate := strings.Trim(parts[len(parts)-1], "[]`\",()")
		if candidate != "" && !isSQLKeyword(candidate) {
			return candidate
		}
	}

	if dotIdx := strings.LastIndex(expr, "."); dotIdx >= 0 && dotIdx+1 < len(expr) {
		candidate := strings.TrimSpace(expr[dotIdx+1:])
		candidate = strings.Trim(candidate, "[]`\",()")
		if candidate != "" && !isSQLKeyword(candidate) {
			return candidate
		}
	}

	candidate := strings.Trim(expr, "[]`\",()")
	if candidate == "" || isSQLKeyword(candidate) {
		return ""
	}
	return candidate
}

func startsWithAnyCI(value string, prefixes ...string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	for _, prefix := range prefixes {
		prefix = strings.TrimSpace(strings.ToLower(prefix))
		if prefix != "" && strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	return false
}

func findTopLevelIntoClause(text string) (string, string, bool) {
	runes := []rune(text)
	parenDepth := 0
	inSingleQuote := false

	for i := 0; i < len(runes); i++ {
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
		if i+4 > len(runes) || !strings.EqualFold(string(runes[i:i+4]), "into") {
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
		if !((runes[j] >= 'A' && runes[j] <= 'Z') || (runes[j] >= 'a' && runes[j] <= 'z') || runes[j] == '_' || runes[j] == '#') {
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

		projection := strings.TrimSpace(string(runes[:i]))
		tableName := strings.TrimSpace(string(runes[start:j]))
		if projection == "" || tableName == "" {
			continue
		}
		return projection, tableName, true
	}

	return "", "", false
}

func isSQLKeyword(word string) bool {
	keywords := map[string]bool{
		"SELECT": true,
		"FROM":   true,
		"WHERE":  true,
		"JOIN":   true,
		"LEFT":   true,
		"RIGHT":  true,
		"INNER":  true,
		"OUTER":  true,
		"CASE":   true,
		"WHEN":   true,
		"THEN":   true,
		"ELSE":   true,
		"END":    true,
		"AS":     true,
		"INTO":   true,
	}
	return keywords[strings.ToUpper(strings.TrimSpace(word))]
}

// ParseResult результат парсинга SQL-файла
type ParseResult struct {
	Procedures        []*model.SQLProcedure
	Tables            []*model.SQLTable
	Columns           []*model.SQLColumn
	ColumnDefinitions []*model.SQLColumnDefinition
	IndexDefinitions  []*model.SQLIndexDefinition
	IndexFields       []*model.SQLIndexDefinitionField
	Calls             []*model.SQLProcedureCall
	Fragments         []*model.QueryFragment
	Includes          []model.IncludeRef
	Defines           map[string]string
	Errors            []ParseError
}

// ParseError ошибка парсинга
type ParseError struct {
	Line     int
	Column   int
	Message  string
	Severity string // error, warning, info
}

type pendingSQLColumn struct {
	TableAlias string
	ColumnName string
	LineNumber int
	ColNumber  int
}

// NewParser создаёт новый SQL-парсер
func NewParser() *Parser {
	return &Parser{
		// DCL_PROC_BEGIN(proc_name) или DCL_PROC_BEGIN proc_name
		procBeginRe: regexp.MustCompile(`(?i)(?:DCL_PROC_BEGIN\s*[\(]?|__BEGIN_PROCEDURE__\s*\()([A-Za-z_][A-Za-z0-9_]*)[\)]?`),
		// __END_PROCEDURE__(proc_name)
		procEndRe: regexp.MustCompile(`(?i)(?:__END_PROCEDURE__|X_ANYMODE)\s*\(([A-Za-z_][A-Za-z0-9_]*)\)`),
		// create procedure proc_name или create proc proc_name
		procDeclRe:   regexp.MustCompile(`(?i)create\s+(?:procedure|proc)\s+([A-Za-z_][A-Za-z0-9_]*)`),
		procCreateRe: regexp.MustCompile(`(?i)API_CREATE_PROC\s*\(([A-Za-z_][A-Za-z0-9_]*)\)`),
		// create table table_name
		createTableRe: regexp.MustCompile(`(?i)\bcreate\s+table\s+([A-Za-z_#][A-Za-z0-9_#]*)`),
		// alter table table_name add column_definition
		alterTableAddRe: regexp.MustCompile(`(?i)^\s*alter\s+table\s+([A-Za-z_#][A-Za-z0-9_#]*)\s+add\s+(.+?)\s*$`),
		// create [unique] index index_name on table_name(field1, field2)
		createIndexRe: regexp.MustCompile(`(?i)^\s*create\s+(unique\s+)?index\s+([A-Za-z_#][A-Za-z0-9_#]*)\s+on\s+([A-Za-z_#][A-Za-z0-9_#]*)\s*\(([^\)]*)\)`),
		// start of multiline create [unique] index definition
		createIndexStartRe: regexp.MustCompile(`(?i)^\s*create\s+(unique\s+)?index\s+([A-Za-z_#][A-Za-z0-9_#]*)\b(.*)$`),
		// ON table_name part (can be placed on the same or next lines)
		createIndexOnRe: regexp.MustCompile(`(?i)^\s*on\s+([A-Za-z_#][A-Za-z0-9_#]*)\b(.*)$`),
		// M_ADD_FIELD('table','column_definition')
		mAddFieldRe: regexp.MustCompile(`(?i)\bM_ADD_FIELD\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)`),
		// M_CRT_INDEX('UNIQUE','index','table','field1,field2')
		mCreateIndexRe: regexp.MustCompile(`(?i)\bM_CRT_INDEX\s*\(\s*'([^']*)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']*)'\s*\)`),
		// select ... into table_name
		selectIntoRe: regexp.MustCompile(`(?i)\bselect\b.*\binto\s+([A-Za-z_#][A-Za-z0-9_#]*)`),
		// into table_name (for multiline SELECT ... INTO)
		selectIntoTableRe: regexp.MustCompile(`(?i)\binto\s+([A-Za-z_#][A-Za-z0-9_#]*)`),
		// insert [into] table_name [hint] - поддерживаем оба формата
		insertTableRe: regexp.MustCompile(`(?i)^\s*insert\s+(?:into\s+)?([A-Za-z_#][A-Za-z0-9_#]*)\b`),
		// delete table_name
		deleteTableRe: regexp.MustCompile(`(?i)^\s*delete\s+([A-Za-z_#][A-Za-z0-9_#]*)\b`),
		// M_DELETE_PTABLE(table_name) / M_DELETE_PTABLE_INDEX(table_name, index_name)
		deletePTableRe: regexp.MustCompile(`(?i)\bM_DELETE_PTABLE(?:_INDEX)?\s*\(\s*([A-Za-z_#][A-Za-z0-9_#]*)\s*(?:,\s*[A-Za-z_#][A-Za-z0-9_#]*)?\)`),
		// M_LOG_TABLE(table_name)
		logTableRe: regexp.MustCompile(`(?i)\bM_LOG_TABLE\s*\(\s*([A-Za-z_#][A-Za-z0-9_#]*)\s*\)`),
		// SELECT_TEMP(table_name)
		selectTempRe: regexp.MustCompile(`(?i)\bSELECT_TEMP\s*\(\s*([A-Za-z_#][A-Za-z0-9_#]*)\s*\)`),
		// Имена таблиц: from table, join table, into table, update table
		tableNameRe: regexp.MustCompile(`(?i)\b(?:FROM|JOIN|INTO|UPDATE)\s+([A-Za-z_#][A-Za-z0-9_#]*)`),
		// Продолжение многострочного списка таблиц после FROM/JOIN
		continuedTableRe: regexp.MustCompile(`^\s*,?\s*([A-Za-z_#][A-Za-z0-9_#]*)\b`),
		// Алиасы таблиц: from table t, from table as t, join table t, update table t
		tableAliasRe: regexp.MustCompile(`(?i)\b(?:FROM|JOIN|UPDATE)\s+([A-Za-z_#][A-Za-z0-9_#]*)\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)\b`),
		// Имена полей: table.column или просто column в контексте
		columnNameRe: regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)`),
		// Поля в INSERT: INSERT INTO table (field1, field2, ...)
		insertColumnsRe: regexp.MustCompile(`(?i)^\s*insert\s+(?:into\s+)?[A-Za-z_#][A-Za-z0-9_#]*\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)(?:\s*,\s*[A-Za-z_][A-Za-z0-9_]*)*\s*\)`),
		// Поля в UPDATE: UPDATE table SET field1 = value1, field2 = value2
		updateColumnsRe: regexp.MustCompile(`(?i)^\s*update\s+[A-Za-z_#][A-Za-z0-9_#]*\s+set\s+([A-Za-z_][A-Za-z0-9_]*\s*=\s*[^,]+(?:\s*,\s*[A-Za-z_][A-Za-z0-9_]*\s*=\s*[^,]+)*)`),
		// Объявление переменных: declare @var type, @var2 type2
		varDeclRe: regexp.MustCompile(`(?i)@([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*)`),
		// Параметры процедуры: @ParamName DSType [output]
		procParamRe: regexp.MustCompile(`@([A-Za-z_][A-Za-z0-9_]*)\s+(DS[A-Za-z0-9_]*)`),
		// Exec procedure
		execRe: regexp.MustCompile(`(?i)exec(?:ute)?\s+(?:@\w+\s*=\s*)?(?:\[?[A-Za-z_][A-Za-z0-9_]*\]?\.)?(\[?[A-Za-z_][A-Za-z0-9_]*\]?)`),
		// Select
		selectRe: regexp.MustCompile(`(?i)^\s*select\b`),
		// Insert
		insertRe: regexp.MustCompile(`(?i)^\s*insert\b`),
		// Update
		updateRe: regexp.MustCompile(`(?i)^\s*update\b`),
		// Delete
		deleteRe: regexp.MustCompile(`(?i)^\s*delete\b`),
		// #include <file.h> или #include "file.h"
		includeRe: regexp.MustCompile(`(?i)#include\s*[<"]([^>"]+)[>"]`),
		// #define NAME value
		defineRe: regexp.MustCompile(`(?i)#define\s+([A-Za-z_][A-Za-z0-9_]*)\s+(.*)`),
		// Комментарии --
		commentRe: regexp.MustCompile(`^\s*--.*$`),
		// Временные таблицы: pAPI_*, #temp
		pTableRe: regexp.MustCompile(`(?i)\b(pAPI_[A-Za-z_][A-Za-z0-9_]*)`),
		// Temp tables
		tempTableRe: regexp.MustCompile(`(?i)\b#[A-Za-z_][A-Za-z0-9_]*`),
	}
}

// ParseFile парсит SQL-файл
func (p *Parser) ParseFile(path string) (*ParseResult, error) {
	content, err := encoding.ReadFile(path, encoding.CP866)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return p.ParseContent(content)
}

// ParseContent парсит содержимое SQL-файла
func (p *Parser) ParseContent(content string) (*ParseResult, error) {
	result := &ParseResult{
		// Инициализация пустых коллекций упрощает дальнейший persist layer:
		// indexer может итерироваться по ним без дополнительных nil-check.
		Procedures:        make([]*model.SQLProcedure, 0),
		Tables:            make([]*model.SQLTable, 0),
		Columns:           make([]*model.SQLColumn, 0),
		ColumnDefinitions: make([]*model.SQLColumnDefinition, 0),
		IndexDefinitions:  make([]*model.SQLIndexDefinition, 0),
		IndexFields:       make([]*model.SQLIndexDefinitionField, 0),
		Calls:             make([]*model.SQLProcedureCall, 0),
		Fragments:         make([]*model.QueryFragment, 0),
		Includes:          make([]model.IncludeRef, 0),
		Defines:           make(map[string]string),
		Errors:            make([]ParseError, 0),
	}

	lines := strings.Split(content, "\n")
	scanner := bufio.NewScanner(strings.NewReader(content))

	var (
		currentProc             *model.SQLProcedure
		inProcedure             bool
		inProcSignature         bool
		procLineStart           int
		procName                string
		inCreateTableDefinition bool
		currentCreateTableName  string
		currentCreateTableOrder int
		inFromTableList         bool
		pendingColumns          []pendingSQLColumn
		collectingSelectInto    bool
		selectIntoProjection    strings.Builder
		inCreateIndexDefinition bool
		createIndexIsUnique     bool
		createIndexName         string
		createIndexTableName    string
		createIndexFields       strings.Builder
		createIndexStartLine    int
		lineNum                 int
		statementStart          int
		statementLines          []string
	)

	appendColumnDefinition := func(tableName, definition, definitionKind string, currentLine int, columnOrder int) {
		columnName, dataType, ok := p.parseColumnDefinition(definition)
		if !ok {
			return
		}
		result.ColumnDefinitions = append(result.ColumnDefinitions, &model.SQLColumnDefinition{
			TableName:      tableName,
			ColumnName:     columnName,
			DataType:       dataType,
			DefinitionKind: definitionKind,
			LineNumber:     currentLine,
			ColumnOrder:    columnOrder,
		})
	}
	appendIndexDefinition := func(tableName, indexName, indexFields, indexType, definitionKind string, isUnique bool, currentLine int) {
		normalizedFields := normalizeIndexFields(indexFields)
		result.IndexDefinitions = append(result.IndexDefinitions, &model.SQLIndexDefinition{
			TableName:      tableName,
			IndexName:      indexName,
			IndexFields:    normalizedFields,
			IndexType:      strings.TrimSpace(indexType),
			IsUnique:       isUnique,
			DefinitionKind: definitionKind,
			LineNumber:     currentLine,
		})
		for idx, fieldName := range splitIndexFields(normalizedFields) {
			result.IndexFields = append(result.IndexFields, &model.SQLIndexDefinitionField{
				ParentIndexName: indexName,
				ParentTableName: tableName,
				FieldName:       fieldName,
				FieldOrder:      idx + 1,
				LineNumber:      currentLine,
			})
		}
	}
	flushSelectIntoProjection := func(tableName string, currentLine int) {
		projection := strings.TrimSpace(selectIntoProjection.String())
		if projection == "" || strings.TrimSpace(tableName) == "" {
			collectingSelectInto = false
			selectIntoProjection.Reset()
			return
		}
		columnNames := extractSelectIntoColumnNames(projection)
		for idx, columnName := range columnNames {
			appendColumnDefinition(tableName, fmt.Sprintf("%s DSUNKNOWN", columnName), "select_into", currentLine, idx+1)
		}
		collectingSelectInto = false
		selectIntoProjection.Reset()
	}
	appendMultilineCreateIndex := func(currentLine int) {
		if !inCreateIndexDefinition {
			return
		}
		if strings.TrimSpace(createIndexName) == "" || strings.TrimSpace(createIndexTableName) == "" {
			inCreateIndexDefinition = false
			createIndexIsUnique = false
			createIndexName = ""
			createIndexTableName = ""
			createIndexFields.Reset()
			createIndexStartLine = 0
			return
		}
		indexType := ""
		if createIndexIsUnique {
			indexType = "UNIQUE"
		}
		lineForDefinition := createIndexStartLine
		if lineForDefinition <= 0 {
			lineForDefinition = currentLine
		}
		appendIndexDefinition(createIndexTableName, createIndexName, createIndexFields.String(), indexType, "create_index", createIndexIsUnique, lineForDefinition)
		inCreateIndexDefinition = false
		createIndexIsUnique = false
		createIndexName = ""
		createIndexTableName = ""
		createIndexFields.Reset()
		createIndexStartLine = 0
	}
	consumeCreateIndexTail := func(tail string, currentLine int) {
		remaining := strings.TrimSpace(tail)
		for remaining != "" {
			if createIndexTableName == "" {
				onMatch := p.createIndexOnRe.FindStringSubmatch(remaining)
				if onMatch == nil {
					break
				}
				createIndexTableName = strings.TrimSpace(onMatch[1])
				remaining = strings.TrimSpace(onMatch[2])
				continue
			}

			openIdx := strings.Index(remaining, "(")
			if openIdx < 0 {
				break
			}
			afterOpen := remaining[openIdx+1:]
			closeIdx := strings.Index(afterOpen, ")")
			if closeIdx >= 0 {
				chunk := strings.TrimSpace(afterOpen[:closeIdx])
				if chunk != "" {
					if createIndexFields.Len() > 0 {
						createIndexFields.WriteString(",")
					}
					createIndexFields.WriteString(chunk)
				}
				appendMultilineCreateIndex(currentLine)
				break
			}
			chunk := strings.TrimSpace(afterOpen)
			if chunk != "" {
				if createIndexFields.Len() > 0 {
					createIndexFields.WriteString(",")
				}
				createIndexFields.WriteString(chunk)
			}
			break
		}
	}

	tableContexts := make(map[string]string) // table -> context (select, insert, update, delete)
	tableAliases := make(map[string]string)  // alias -> real table name
	flushPendingColumns := func() {
		for _, pending := range pendingColumns {
			tableName := pending.TableAlias
			if realTableName, ok := tableAliases[strings.ToLower(tableName)]; ok {
				tableName = realTableName
			}
			result.Columns = append(result.Columns, &model.SQLColumn{
				TableName:  tableName,
				ColumnName: pending.ColumnName,
				LineNumber: pending.LineNumber,
				ColNumber:  pending.ColNumber,
			})
		}
		pendingColumns = nil
	}
	appendStatementLine := func(text string, currentLine int) {
		trimmedText := strings.TrimSpace(text)
		if trimmedText == "" {
			return
		}
		if inProcedure && inProcSignature {
			return
		}
		lower := strings.ToLower(trimmedText)
		if strings.EqualFold(trimmedText, "go") ||
			trimmedText == "as" ||
			strings.EqualFold(trimmedText, "begin") ||
			strings.EqualFold(trimmedText, "end") ||
			strings.HasPrefix(lower, "#include") ||
			strings.HasPrefix(lower, "#define") ||
			p.procCreateRe.MatchString(trimmedText) ||
			p.procBeginRe.MatchString(trimmedText) ||
			p.procEndRe.MatchString(trimmedText) ||
			p.procDeclRe.MatchString(trimmedText) ||
			p.commentRe.MatchString(trimmedText) {
			return
		}
		if statementStart == 0 {
			statementStart = currentLine
		}
		statementLines = append(statementLines, text)
	}
	flushStatement := func(endLine int) {
		if statementStart == 0 || len(statementLines) == 0 {
			statementStart = 0
			statementLines = nil
			return
		}
		queryText := strings.TrimSpace(strings.Join(statementLines, "\n"))
		if queryText == "" {
			statementStart = 0
			statementLines = nil
			return
		}
		componentName := "sql_script"
		componentType := "sql_script"
		context := "sql_statement"
		if currentProc != nil {
			componentName = currentProc.ProcName
			componentType = "sql_procedure"
			context = "sql_procedure_statement"
		}
		result.Fragments = append(result.Fragments, &model.QueryFragment{
			ComponentName: componentName,
			ComponentType: componentType,
			QueryText:     queryText,
			Context:       context,
			LineNumber:    statementStart,
			LineEnd:       endLine,
		})
		statementStart = 0
		statementLines = nil
	}
	// Инициализация структур для отслеживания контекстов на весь файл
	fileSeenContexts := make(map[string]bool) // fileSeenContexts[contextKey] где contextKey = tableName_context_position
	var inMultiStatement bool                 // Флаг для многострочных операторов типа INSERT...SELECT
	var inInsertFields bool                   // Флаг для сбора полей INSERT
	var insertFieldsBuffer []string           // Буфер для полей из нескольких строк

	// Вспомогательная функция для проверки что контекст еще не использовался для таблицы
	// с учетом позиции (чтобы разрешать повторные упоминания в разных частях кода)
	isNewContext := func(tableName, context string) bool {
		positionKey := (lineNum - 1) / 10
		contextKey := fmt.Sprintf("%s_%s_%d", strings.ToLower(tableName), context, positionKey)

		if _, exists := fileSeenContexts[contextKey]; exists {
			return false
		}
		fileSeenContexts[contextKey] = true
		return true
	}

	resetStatementState := func() {
		flushPendingColumns()
		inFromTableList = false
		inInsertFields = false
		insertFieldsBuffer = nil
		if !inMultiStatement {
			tableContexts["current"] = ""
		}
		tableAliases = make(map[string]string)
	}

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		endOfStatement := strings.HasSuffix(trimmed, ";")
		appendStatementLine(line, lineNum)

		// Пропускаем пустые строки и комментарии
		if trimmed == "" || p.commentRe.MatchString(trimmed) {
			if trimmed == "" && statementStart > 0 && inProcedure && !inProcSignature {
				flushStatement(lineNum - 1)
				resetStatementState()
				inMultiStatement = false
			}
			continue
		}

		if p.selectRe.MatchString(trimmed) {
			lowerTrimmed := strings.ToLower(trimmed)
			selectKeywordIndex := strings.Index(lowerTrimmed, "select")
			remainder := strings.TrimSpace(trimmed)
			if selectKeywordIndex >= 0 {
				remainder = strings.TrimSpace(trimmed[selectKeywordIndex+len("select"):])
			}
			if strings.TrimSpace(remainder) != "" && !strings.HasPrefix(strings.TrimSpace(remainder), "@") {
				collectingSelectInto = true
				selectIntoProjection.Reset()
				selectIntoProjection.WriteString(remainder)
			}
		}

		if collectingSelectInto {
			candidate := strings.TrimSpace(trimmed)
			if !p.selectRe.MatchString(trimmed) && startsWithAnyCI(trimmed, "insert", "update", "delete", "exec", "execute", "truncate", "declare", "set", "if", "begin", "end", "profile_time", "grant", "x_anymode", "go") {
				collectingSelectInto = false
				selectIntoProjection.Reset()
				continue
			}
			if p.selectRe.MatchString(candidate) {
				lowerCandidate := strings.ToLower(candidate)
				if idx := strings.Index(lowerCandidate, "select"); idx >= 0 {
					candidate = strings.TrimSpace(candidate[idx+len("select"):])
				}
			}
			if projectionPart, tableName, ok := findTopLevelIntoClause(candidate); ok {
				if projectionPart != "" {
					if selectIntoProjection.Len() > 0 {
						selectIntoProjection.WriteString(" ")
					}
					selectIntoProjection.WriteString(projectionPart)
				}
				flushSelectIntoProjection(strings.TrimSpace(tableName), lineNum)
			} else if !p.selectRe.MatchString(trimmed) {
				if startsWithAnyCI(trimmed, "from", "join", "where", "group", "order", "union", "go") {
					collectingSelectInto = false
					selectIntoProjection.Reset()
				} else {
					if candidate != "" {
						if selectIntoProjection.Len() > 0 {
							selectIntoProjection.WriteString(" ")
						}
						selectIntoProjection.WriteString(candidate)
					}
				}
			}
		}

		// Проверяем #include
		if matches := p.includeRe.FindStringSubmatch(trimmed); matches != nil {
			flushStatement(lineNum)
			result.Includes = append(result.Includes, model.IncludeRef{IncludePath: matches[1], LineNumber: lineNum})
			continue
		}

		// Проверяем #define
		if matches := p.defineRe.FindStringSubmatch(trimmed); matches != nil {
			flushStatement(lineNum)
			result.Defines[matches[1]] = matches[2]
			continue
		}

		if matches := p.procCreateRe.FindStringSubmatch(trimmed); matches != nil {
			flushStatement(lineNum - 1)
			continue
		}

		// Проверяем начало процедуры
		// Границы процедуры важны для line range (диапазона строк) и для того,
		// чтобы later relation builder (построитель связей) мог привязать найденные сущности к процедуре.
		if matches := p.procBeginRe.FindStringSubmatch(trimmed); matches != nil {
			if inProcedure && strings.Contains(strings.ToUpper(trimmed), "__BEGIN_PROCEDURE__") {
				continue
			}
			flushStatement(lineNum - 1)
			procName = matches[1]
			procLineStart = lineNum
			inProcedure = true
			inProcSignature = !strings.Contains(strings.ToUpper(trimmed), "__BEGIN_PROCEDURE__")
			currentProc = &model.SQLProcedure{
				ProcName:  procName,
				LineStart: procLineStart,
				Params:    make([]model.SQLParam, 0),
			}
			continue
		}

		// Если внутри процедуры, собираем параметры
		if inProcedure && currentProc != nil {
			if inProcSignature {
				paramMatches := p.procParamRe.FindAllStringSubmatch(trimmed, -1)
				for _, paramMatch := range paramMatches {
					paramName := paramMatch[1]
					paramType := paramMatch[2]
					if p.hasProcedureParam(currentProc, paramName) {
						continue
					}
					direction := "in"
					if strings.Contains(strings.ToLower(trimmed), "output") {
						direction = "out"
					}
					currentProc.Params = append(currentProc.Params, model.SQLParam{
						Name:      paramName,
						Type:      paramType,
						Direction: direction,
					})
				}
			}

			trimmedLower := strings.ToLower(trimmed)
			if trimmedLower == "as" || strings.HasPrefix(trimmedLower, "as ") || strings.HasPrefix(trimmedLower, "begin") {
				inProcSignature = false
			}
		}

		// Проверяем конец процедуры
		if matches := p.procEndRe.FindStringSubmatch(trimmed); matches != nil {
			flushPendingColumns()
			flushStatement(lineNum)
			if inProcedure && currentProc != nil {
				currentProc.LineEnd = lineNum
				currentProc.BodyHash = computeBodyHash(lines, currentProc.LineStart, currentProc.LineEnd)
				result.Procedures = append(result.Procedures, currentProc)
			}
			inProcedure = false
			inProcSignature = false
			currentProc = nil
			continue
		}

		// Создаем процедуру из create procedure, если нет DCL_PROC_BEGIN
		if matches := p.procDeclRe.FindStringSubmatch(trimmed); matches != nil && !inProcedure {
			flushPendingColumns()
			flushStatement(lineNum - 1)
			procName = matches[1]
			procLineStart = lineNum
			currentProc = &model.SQLProcedure{
				ProcName:  procName,
				LineStart: procLineStart,
				Params:    make([]model.SQLParam, 0),
			}
			inProcedure = true
			inProcSignature = true
			continue
		}

		if strings.EqualFold(trimmed, "go") {
			inCreateIndexDefinition = false
			createIndexIsUnique = false
			createIndexName = ""
			createIndexTableName = ""
			createIndexFields.Reset()
			createIndexStartLine = 0
			flushStatement(lineNum - 1)
			resetStatementState()
			inCreateTableDefinition = false
			currentCreateTableName = ""
			currentCreateTableOrder = 0
			inMultiStatement = false
			continue
		}

		if inCreateTableDefinition {
			definitionLine := strings.TrimSpace(trimmed)
			definitionLine = strings.TrimSuffix(definitionLine, ",")
			if idx := strings.Index(definitionLine, "--"); idx >= 0 {
				definitionLine = strings.TrimSpace(definitionLine[:idx])
			}
			upperDefinitionLine := strings.ToUpper(definitionLine)
			if definitionLine == ")" || strings.HasPrefix(definitionLine, ")") {
				inCreateTableDefinition = false
				currentCreateTableName = ""
				currentCreateTableOrder = 0
				continue
			}
			if definitionLine != "" &&
				!strings.HasPrefix(upperDefinitionLine, "CONSTRAINT ") &&
				!strings.HasPrefix(upperDefinitionLine, "PRIMARY KEY") &&
				!strings.HasPrefix(upperDefinitionLine, "UNIQUE") &&
				!strings.HasPrefix(upperDefinitionLine, "FOREIGN KEY") &&
				!strings.HasPrefix(upperDefinitionLine, "CHECK ") &&
				!strings.HasPrefix(upperDefinitionLine, "INDEX ") {
				currentCreateTableOrder++
				appendColumnDefinition(currentCreateTableName, definitionLine, "create_table", lineNum, currentCreateTableOrder)
			}
		}

		if matches := p.alterTableAddRe.FindStringSubmatch(trimmed); matches != nil {
			tableName := strings.TrimSpace(matches[1])
			definition := strings.TrimSpace(matches[2])
			appendColumnDefinition(tableName, definition, "alter_add", lineNum, 0)
		}
		if matches := p.mAddFieldRe.FindStringSubmatch(trimmed); matches != nil {
			tableName := strings.TrimSpace(matches[1])
			definition := strings.TrimSpace(matches[2])
			appendColumnDefinition(tableName, definition, "macro_add_field", lineNum, 0)
		}
		if inCreateIndexDefinition {
			consumeCreateIndexTail(trimmed, lineNum)
			if inCreateIndexDefinition && createIndexTableName != "" && !strings.Contains(trimmed, "(") {
				chunk := strings.TrimSpace(strings.TrimSuffix(trimmed, ","))
				chunk = strings.TrimSpace(strings.TrimPrefix(chunk, ","))
				if closeIdx := strings.Index(chunk, ")"); closeIdx >= 0 {
					chunk = strings.TrimSpace(chunk[:closeIdx])
				}
				if chunk != "" && !strings.EqualFold(chunk, "on") && !strings.EqualFold(chunk, ")") {
					if p.createIndexOnRe.MatchString(chunk) {
						chunk = ""
					}
				}
				if chunk != "" && !strings.EqualFold(chunk, "on") && !strings.EqualFold(chunk, ")") {
					if createIndexFields.Len() > 0 {
						createIndexFields.WriteString(",")
					}
					createIndexFields.WriteString(chunk)
				}
			}
			if inCreateIndexDefinition && strings.Contains(trimmed, ")") {
				appendMultilineCreateIndex(lineNum)
			}
			continue
		}
		if matches := p.createIndexRe.FindStringSubmatch(trimmed); matches != nil {
			isUnique := strings.TrimSpace(matches[1]) != ""
			indexName := strings.TrimSpace(matches[2])
			tableName := strings.TrimSpace(matches[3])
			indexFields := strings.TrimSpace(matches[4])
			indexType := ""
			if isUnique {
				indexType = "UNIQUE"
			}
			appendIndexDefinition(tableName, indexName, indexFields, indexType, "create_index", isUnique, lineNum)
			continue
		}
		if matches := p.createIndexStartRe.FindStringSubmatch(trimmed); matches != nil {
			inCreateIndexDefinition = true
			createIndexIsUnique = strings.TrimSpace(matches[1]) != ""
			createIndexName = strings.TrimSpace(matches[2])
			createIndexTableName = ""
			createIndexFields.Reset()
			createIndexStartLine = lineNum
			consumeCreateIndexTail(strings.TrimSpace(matches[3]), lineNum)
			if inCreateIndexDefinition && strings.Contains(trimmed, ")") {
				appendMultilineCreateIndex(lineNum)
			}
			continue
		}
		if matches := p.mCreateIndexRe.FindStringSubmatch(trimmed); matches != nil {
			indexType := strings.TrimSpace(matches[1])
			indexName := strings.TrimSpace(matches[2])
			tableName := strings.TrimSpace(matches[3])
			indexFields := strings.TrimSpace(matches[4])
			appendIndexDefinition(tableName, indexName, indexFields, indexType, "macro_create_index", strings.EqualFold(indexType, "UNIQUE"), lineNum)
		}

		// Определяем контекст запроса
		if p.selectRe.MatchString(trimmed) {
			// Если это SELECT внутри INSERT, не меняем контекст
			if tableContexts["current"] != "insert" {
				tableContexts["current"] = "select"
			}
		} else if p.insertRe.MatchString(trimmed) {
			tableContexts["current"] = "insert"
			inMultiStatement = true // INSERT может быть многострочным
		} else if p.updateRe.MatchString(trimmed) {
			tableContexts["current"] = "update"
			inMultiStatement = true // UPDATE может быть многострочным
		} else if p.deleteRe.MatchString(trimmed) {
			tableContexts["current"] = "delete"
			inMultiStatement = true // DELETE может быть многострочным
		}

		// Ищем имена таблиц
		// Таблицы и поля извлекаются независимо от того, находимся ли мы внутри процедуры:
		// это позволяет индексировать и процедурный код, и standalone SQL scripts.
		lineSeenTables := make(map[string]bool)

		if matches := p.createTableRe.FindStringSubmatch(line); matches != nil {
			tableName := matches[1]
			if !p.isKeyword(tableName) {
				colNumber := 0
				if matchIndexes := p.createTableRe.FindStringSubmatchIndex(line); len(matchIndexes) >= 4 {
					colNumber = matchIndexes[2] + 1
				}
				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     "create",
					IsTemporary: p.isTemporaryTableName(tableName),
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				inCreateTableDefinition = true
				currentCreateTableName = tableName
				currentCreateTableOrder = 0
				lineSeenTables[strings.ToLower(tableName)] = true
			}
		}
		if matches := p.selectIntoRe.FindStringSubmatch(line); matches != nil {
			tableName := matches[1]
			if !p.isKeyword(tableName) && isNewContext(tableName, "select") {
				colNumber := 0
				if matchIndexes := p.selectIntoRe.FindStringSubmatchIndex(line); len(matchIndexes) >= 4 {
					colNumber = matchIndexes[2] + 1
				}
				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     "select",
					IsTemporary: p.isTemporaryTableName(tableName),
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				lineSeenTables[strings.ToLower(tableName)] = true
			}
		}
		if matches := p.insertTableRe.FindStringSubmatch(line); matches != nil {
			tableName := matches[1]
			if !p.isKeyword(tableName) && isNewContext(tableName, "insert") {
				colNumber := 0
				if matchIndexes := p.insertTableRe.FindStringSubmatchIndex(line); len(matchIndexes) >= 4 {
					colNumber = matchIndexes[2] + 1
				}
				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     "insert",
					IsTemporary: p.isTemporaryTableName(tableName),
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				lineSeenTables[strings.ToLower(tableName)] = true

				// Начинаем сбор полей, если найдена открывающая скобка
				openParen := strings.Index(line, "(")
				closeParen := strings.Index(line, ")")
				if openParen >= 0 {
					inInsertFields = true
					insertFieldsBuffer = nil // Очищаем буфер
					// Извлекаем поля после открывающей скобки в текущей строке
					fieldsStart := openParen + 1
					fieldsEnd := len(line)
					if closeParen >= 0 {
						fieldsEnd = closeParen
					}
					fieldsLine := line[fieldsStart:fieldsEnd]
					for _, field := range strings.Split(fieldsLine, ",") {
						field = strings.TrimSpace(field)
						if field != "" && !strings.Contains(field, "(") && !strings.Contains(field, ")") {
							insertFieldsBuffer = append(insertFieldsBuffer, field)
						}
					}
				}

				// Если список полей начат ранее и продолжается на следующей строке
				if inInsertFields && openParen < 0 {
					fieldsEnd := len(line)
					closeParen = strings.Index(line, ")")
					if closeParen >= 0 {
						fieldsEnd = closeParen
					}
					fieldsLine := line[:fieldsEnd]
					for _, field := range strings.Split(fieldsLine, ",") {
						field = strings.TrimSpace(field)
						if field != "" && !strings.Contains(field, "(") && !strings.Contains(field, ")") {
							insertFieldsBuffer = append(insertFieldsBuffer, field)
						}
					}
				}

				// Проверяем заканчивается ли список полей в этой строке
				if closeParen >= 0 && inInsertFields {
					// Добавляем собранные поля в результат
					for _, field := range insertFieldsBuffer {
						result.Columns = append(result.Columns, &model.SQLColumn{
							TableName:  tableName,
							ColumnName: field,
							LineNumber: lineNum,
							ColNumber:  colNumber,
						})
					}
					inInsertFields = false
					insertFieldsBuffer = nil
				}
			}
		}
		if matches := p.deleteTableRe.FindStringSubmatch(line); matches != nil {
			tableName := matches[1]
			if !p.isKeyword(tableName) && isNewContext(tableName, "delete") {
				colNumber := 0
				if matchIndexes := p.deleteTableRe.FindStringSubmatchIndex(line); len(matchIndexes) >= 4 {
					colNumber = matchIndexes[2] + 1
				}
				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     "delete",
					IsTemporary: p.isTemporaryTableName(tableName),
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				lineSeenTables[strings.ToLower(tableName)] = true
			}
		}
		if matches := p.deletePTableRe.FindStringSubmatch(line); matches != nil {
			tableName := matches[1]
			if !p.isKeyword(tableName) && isNewContext(tableName, "delete") {
				colNumber := 0
				if matchIndexes := p.deletePTableRe.FindStringSubmatchIndex(line); len(matchIndexes) >= 4 {
					colNumber = matchIndexes[2] + 1
				}
				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     "delete",
					IsTemporary: p.isTemporaryTableName(tableName),
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				lineSeenTables[strings.ToLower(tableName)] = true
			}
		}
		if matches := p.logTableRe.FindStringSubmatch(line); matches != nil {
			tableName := matches[1]
			if !p.isKeyword(tableName) && isNewContext(tableName, "log") {
				colNumber := 0
				if matchIndexes := p.logTableRe.FindStringSubmatchIndex(line); len(matchIndexes) >= 4 {
					colNumber = matchIndexes[2] + 1
				}
				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     "log",
					IsTemporary: p.isTemporaryTableName(tableName),
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				lineSeenTables[strings.ToLower(tableName)] = true
			}
		}
		if matches := p.selectTempRe.FindStringSubmatch(line); matches != nil {
			tableName := matches[1]
			if !p.isKeyword(tableName) && !p.isIgnoredTableName(tableName) && !lineSeenTables[strings.ToLower(tableName)] {
				colNumber := 0
				if matchIndexes := p.selectTempRe.FindStringSubmatchIndex(line); len(matchIndexes) >= 4 {
					colNumber = matchIndexes[2] + 1
				}
				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     "select",
					IsTemporary: p.isTemporaryTableName(tableName),
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				lineSeenTables[strings.ToLower(tableName)] = true
			}
		}
		matchIndexes := p.tableNameRe.FindAllStringSubmatchIndex(line, -1)
		if matches := p.tableNameRe.FindAllStringSubmatch(line, -1); matches != nil {
			context := tableContexts["current"]
			if context == "" {
				flushPendingColumns()
				inFromTableList = false
				continue
			}
			for i, m := range matches {
				tableName := m[1]
				if lineSeenTables[strings.ToLower(tableName)] {
					continue
				}
				// Пропускаем ключевые слова
				if p.isKeyword(tableName) || p.isIgnoredTableName(tableName) {
					continue
				}

				// Определяем реальный контекст таблицы на основе положения
				realContext := context
				if context == "insert" {
					// Для INSERT...SELECT: все таблицы из SELECT/FROM/JOIN имеют контекст select
					// кроме таблицы из INSERT INTO
					if !strings.Contains(strings.ToLower(line), "insert into") {
						realContext = "select"
					}
				}

				// Проверяем что этот контекст еще не использовался для таблицы
				if !isNewContext(tableName, realContext) {
					continue
				}
				colNumber := 0
				if i < len(matchIndexes) && len(matchIndexes[i]) >= 4 {
					colNumber = matchIndexes[i][2] + 1
				}
				if aliasMatches := p.tableAliasRe.FindAllStringSubmatch(line, -1); aliasMatches != nil {
					for _, aliasMatch := range aliasMatches {
						aliasTableName := aliasMatch[1]
						aliasName := aliasMatch[2]
						if p.isKeyword(aliasTableName) || p.isKeyword(aliasName) {
							continue
						}
						tableAliases[strings.ToLower(aliasName)] = aliasTableName
					}
				}
				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     realContext,
					IsTemporary: p.isTemporaryTableName(tableName),
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				lineSeenTables[strings.ToLower(tableName)] = true

				// Извлекаем поля из UPDATE table SET field1 = value1, field2 = value2
				if realContext == "update" {
					if updateColsMatches := p.updateColumnsRe.FindStringSubmatch(line); updateColsMatches != nil {
						// Находим часть после SET
						setRe := regexp.MustCompile(`(?i)^\s*update\s+[A-Za-z_#][A-Za-z0-9_#]*\s+set\s+(.+)`)
						if setMatch := setRe.FindStringSubmatch(line); setMatch != nil {
							setClause := setMatch[1]
							// Разделяем по запятым, но учитываем что значения могут содержать запятые
							assignments := strings.Split(setClause, ",")
							for _, assignment := range assignments {
								assignment = strings.TrimSpace(assignment)
								if eqIndex := strings.Index(assignment, "="); eqIndex > 0 {
									field := strings.TrimSpace(assignment[:eqIndex])
									if field != "" {
										result.Columns = append(result.Columns, &model.SQLColumn{
											TableName:  tableName,
											ColumnName: field,
											LineNumber: lineNum,
											ColNumber:  colNumber,
										})
									}
								}
							}
						}
					}
				}
			}
			inFromTableList = true
		} else if inFromTableList {
			context := tableContexts["current"]
			if context != "" {
				// Пропускаем строки внутри скобок (списки полей)
				if strings.Contains(line, "(") && !strings.Contains(line, ")") {
					// Начало списка полей - выходим из режима таблиц
					inFromTableList = false
					continue
				}
				if strings.Contains(line, ")") && !strings.Contains(line, "(") {
					// Конец списка полей - выходим из режима таблиц
					inFromTableList = false
					continue
				}
				// Если строка содержит запятые но это похоже на список полей, пропускаем
				if strings.Contains(line, ",") && (strings.Contains(line, "(") || strings.Contains(line, ")")) && !strings.Contains(strings.ToUpper(line), "#M_") {
					continue
				}
				trimmedLine := strings.TrimSpace(line)
				trimmedLower := strings.ToLower(trimmedLine)
				if strings.HasPrefix(trimmedLower, "insert ") ||
					strings.HasPrefix(trimmedLower, "select ") ||
					strings.HasPrefix(trimmedLower, "update ") ||
					strings.HasPrefix(trimmedLower, "delete ") ||
					strings.HasPrefix(trimmedLower, "if ") ||
					strings.HasPrefix(trimmedLower, "case ") ||
					strings.HasPrefix(trimmedLower, "else") ||
					strings.HasPrefix(trimmedLower, "end") ||
					strings.HasPrefix(trimmedLower, "exec ") ||
					strings.HasPrefix(trimmedLower, "where ") ||
					strings.HasPrefix(trimmedLower, "group ") ||
					strings.HasPrefix(trimmedLower, "order ") ||
					strings.HasPrefix(trimmedLower, "union ") {
					inFromTableList = false
					continue
				}
				if !strings.HasPrefix(trimmedLine, ",") && !p.continuedTableRe.MatchString(line) {
					inFromTableList = false
				} else if matches := p.continuedTableRe.FindStringSubmatch(line); matches != nil {
					tableName := matches[1]
					normalizedLine := strings.TrimSpace(line)
					normalizedLine = strings.TrimSpace(strings.TrimLeft(normalizedLine, ","))
					if aliasMatches := p.tableAliasRe.FindAllStringSubmatch("from "+normalizedLine, -1); aliasMatches != nil {
						for _, aliasMatch := range aliasMatches {
							aliasTableName := aliasMatch[1]
							aliasName := aliasMatch[2]
							if p.isKeyword(aliasTableName) || p.isKeyword(aliasName) {
								continue
							}
							tableAliases[strings.ToLower(aliasName)] = aliasTableName
						}
					}
					if !p.isKeyword(tableName) && !p.isIgnoredTableName(tableName) && !lineSeenTables[strings.ToLower(tableName)] {
						colNumber := 0
						if matchIndexes := p.continuedTableRe.FindStringSubmatchIndex(line); len(matchIndexes) >= 4 {
							colNumber = matchIndexes[2] + 1
						}
						result.Tables = append(result.Tables, &model.SQLTable{
							TableName:   tableName,
							Context:     context,
							IsTemporary: p.isTemporaryTableName(tableName),
							LineNumber:  lineNum,
							ColNumber:   colNumber,
						})
						lineSeenTables[strings.ToLower(tableName)] = true
					}
				}
			}
		} else {
			inFromTableList = false
		}

		// Ищем временные таблицы pAPI_* вне обычного table context, чтобы не терять
		// упоминания, но и не плодить дубли для уже найденных FROM/JOIN/UPDATE/INTO.
		if matches := p.pTableRe.FindAllStringSubmatch(line, -1); matches != nil {
			matchIndexes := p.pTableRe.FindAllStringSubmatchIndex(line, -1)
			for i, m := range matches {
				tableName := m[1]
				if lineSeenTables[strings.ToLower(tableName)] {
					continue
				}
				colNumber := 0
				if i < len(matchIndexes) && len(matchIndexes[i]) >= 4 {
					colNumber = matchIndexes[i][2] + 1
				}
				if colNumber > 0 && p.isInsideSingleQuotedString(line, colNumber-1) {
					continue
				}
				if colNumber > 0 && p.isInsideInlineComment(line, colNumber-1) {
					continue
				}
				if colNumber > 0 && p.isInsideBlockComment(content, lineNum, colNumber-1) {
					continue
				}

				// Определяем контекст на основе текущего состояния
				context := tableContexts["current"]
				if context == "" {
					context = "unknown"
				} else if context == "insert" && !strings.Contains(strings.ToLower(line), "insert into") {
					// Для INSERT...SELECT: таблицы из SELECT/FROM/JOIN имеют контекст select
					context = "select"
				}

				// Проверяем умную дедупликацию
				if !isNewContext(tableName, context) {
					continue
				}

				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     context,
					IsTemporary: true,
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				lineSeenTables[strings.ToLower(tableName)] = true
			}
		}

		if matches := p.tempTableRe.FindAllStringSubmatch(line, -1); matches != nil {
			matchIndexes := p.tempTableRe.FindAllStringSubmatchIndex(line, -1)
			for i, m := range matches {
				tableName := m[0]
				if lineSeenTables[strings.ToLower(tableName)] {
					continue
				}
				colNumber := 0
				if i < len(matchIndexes) && len(matchIndexes[i]) >= 2 {
					colNumber = matchIndexes[i][0] + 1
				}
				if colNumber > 0 && p.isInsideSingleQuotedString(line, colNumber-1) {
					continue
				}
				if colNumber > 0 && p.isInsideInlineComment(line, colNumber-1) {
					continue
				}
				if colNumber > 0 && p.isInsideBlockComment(content, lineNum, colNumber-1) {
					continue
				}

				// Определяем контекст на основе текущего состояния
				context := tableContexts["current"]
				if context == "" {
					context = "unknown"
				}

				// Проверяем умную дедупликацию
				if !isNewContext(tableName, context) {
					continue
				}

				result.Tables = append(result.Tables, &model.SQLTable{
					TableName:   tableName,
					Context:     context,
					IsTemporary: true,
					LineNumber:  lineNum,
					ColNumber:   colNumber,
				})
				lineSeenTables[strings.ToLower(tableName)] = true
			}
		}

		// Ищем поля table.column
		if matches := p.columnNameRe.FindAllStringSubmatch(line, -1); matches != nil {
			matchIndexes := p.columnNameRe.FindAllStringSubmatchIndex(line, -1)
			for i, m := range matches {
				tableName := m[1]
				colNumber := 0
				tablePos := 0
				if i < len(matchIndexes) && len(matchIndexes[i]) >= 6 {
					colNumber = matchIndexes[i][4] + 1
					tablePos = matchIndexes[i][2]
				}
				if tablePos >= 0 && p.isInsideSingleQuotedString(line, tablePos) {
					continue
				}
				if tablePos >= 0 && p.isInsideInlineComment(line, tablePos) {
					continue
				}
				if tablePos >= 0 && p.isInsideBlockComment(content, lineNum, tablePos) {
					continue
				}
				if realTableName, ok := tableAliases[strings.ToLower(tableName)]; ok {
					tableName = realTableName
					result.Columns = append(result.Columns, &model.SQLColumn{
						TableName:  tableName,
						ColumnName: m[2],
						LineNumber: lineNum,
						ColNumber:  colNumber,
					})
					continue
				}
				pendingColumns = append(pendingColumns, pendingSQLColumn{
					TableAlias: tableName,
					ColumnName: m[2],
					LineNumber: lineNum,
					ColNumber:  colNumber,
				})
			}
		}

		// Найденные exec-вызовы не резолвятся здесь в ID.
		// Parser отвечает только за text extraction, а indexer позже связывает имена с сущностями БД.
		// Exec procedure calls
		if matches := p.execRe.FindStringSubmatch(trimmed); matches != nil {
			callerName := ""
			if currentProc != nil {
				callerName = currentProc.ProcName
			}
			calleeName := strings.TrimSpace(strings.Trim(matches[1], "[]"))
			if calleeName == "" {
				continue
			}
			result.Calls = append(result.Calls, &model.SQLProcedureCall{
				CallerName: callerName,
				CalleeName: calleeName,
				LineNumber: lineNum,
			})
		}

		if endOfStatement {
			flushStatement(lineNum)
			resetStatementState()
			inMultiStatement = false // Сбрасываем флаг многострочности
		}
	}

	if err := scanner.Err(); err != nil {
		// Как и в H parser, частичный результат сохраняется даже при ошибке чтения scanner-а.
		result.Errors = append(result.Errors, ParseError{
			Line:     lineNum,
			Message:  err.Error(),
			Severity: "error",
		})
	}

	flushPendingColumns()
	flushStatement(lineNum)
	if collectingSelectInto {
		collectingSelectInto = false
		selectIntoProjection.Reset()
	}
	if inCreateIndexDefinition {
		appendMultilineCreateIndex(lineNum)
	}

	if inProcedure && currentProc != nil && currentProc.LineEnd == 0 {
		currentProc.LineEnd = len(lines)
		currentProc.BodyHash = computeBodyHash(lines, currentProc.LineStart, currentProc.LineEnd)
		result.Procedures = append(result.Procedures, currentProc)
	}

	return result, nil
}

func computeBodyHash(lines []string, lineStart, lineEnd int) string {
	if lineStart <= 0 || lineEnd <= 0 || lineStart > lineEnd || lineStart > len(lines) {
		return ""
	}

	if lineEnd > len(lines) {
		lineEnd = len(lines)
	}

	body := strings.Join(lines[lineStart-1:lineEnd], "\n")
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}

	hash := sha256.Sum256([]byte(body))
	return hex.EncodeToString(hash[:])
}

func (p *Parser) isTemporaryTableName(tableName string) bool {
	lowerTableName := strings.ToLower(strings.TrimSpace(tableName))
	return strings.HasPrefix(lowerTableName, "p") || strings.HasPrefix(lowerTableName, "#")
}

func (p *Parser) isInsideSingleQuotedString(line string, pos int) bool {
	if pos < 0 || pos >= len(line) {
		return false
	}
	inString := false
	for i := 0; i < len(line); i++ {
		if line[i] != '\'' {
			if i == pos {
				return inString
			}
			continue
		}
		if inString && i+1 < len(line) && line[i+1] == '\'' {
			if i == pos || i+1 == pos {
				return true
			}
			i++
			continue
		}
		if i == pos {
			return inString
		}
		inString = !inString
	}
	return false
}

func (p *Parser) isInsideInlineComment(line string, pos int) bool {
	if pos < 0 || pos >= len(line) {
		return false
	}
	inString := false
	for i := 0; i < len(line)-1; i++ {
		if line[i] == '\'' {
			if inString && i+1 < len(line) && line[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if !inString && line[i] == '-' && line[i+1] == '-' {
			return pos >= i
		}
	}
	return false
}

func (p *Parser) isInsideBlockComment(content string, lineNum int, pos int) bool {
	if lineNum <= 0 || pos < 0 {
		return false
	}
	lines := strings.Split(content, "\n")
	if lineNum > len(lines) || pos >= len(lines[lineNum-1]) {
		return false
	}
	absolutePos := 0
	for i := 0; i < lineNum-1; i++ {
		absolutePos += len(lines[i]) + 1
	}
	absolutePos += pos

	inString := false
	inBlockComment := false
	for i := 0; i < len(content); i++ {
		if i == absolutePos {
			return inBlockComment
		}
		if content[i] == '\'' {
			if inString && i+1 < len(content) && content[i+1] == '\'' {
				i++
				continue
			}
			if !inBlockComment {
				inString = !inString
			}
			continue
		}
		if inString {
			continue
		}
		if !inBlockComment && i+1 < len(content) && content[i] == '/' && content[i+1] == '*' {
			inBlockComment = true
			i++
			continue
		}
		if inBlockComment && i+1 < len(content) && content[i] == '*' && content[i+1] == '/' {
			inBlockComment = false
			i++
			continue
		}
	}
	return false
}

func (p *Parser) isIgnoredTableName(tableName string) bool {
	ignored := map[string]bool{
		"TBL": true,
	}
	return ignored[strings.ToUpper(strings.TrimSpace(tableName))]
}

func (p *Parser) hasProcedureParam(proc *model.SQLProcedure, paramName string) bool {
	for _, param := range proc.Params {
		if strings.EqualFold(param.Name, paramName) {
			return true
		}
	}
	return false
}

func (p *Parser) parseColumnDefinition(definition string) (string, string, bool) {
	definition = strings.TrimSpace(definition)
	definition = strings.TrimSuffix(definition, ",")
	if idx := strings.Index(definition, "/*"); idx >= 0 {
		definition = strings.TrimSpace(definition[:idx])
	}
	if idx := strings.Index(definition, "--"); idx >= 0 {
		definition = strings.TrimSpace(definition[:idx])
	}
	parts := strings.Fields(definition)
	if len(parts) < 2 {
		return "", "", false
	}
	columnName := strings.Trim(parts[0], "([]`\"")
	dataType := strings.Trim(parts[1], ",")
	if columnName == "" || dataType == "" || p.isKeyword(columnName) {
		return "", "", false
	}
	return columnName, dataType, true
}

func normalizeIndexFields(fields string) string {
	parts := splitIndexFields(fields)
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ",")
}

func splitIndexFields(fields string) []string {
	rawParts := strings.Split(fields, ",")
	result := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		field := strings.TrimSpace(strings.Trim(part, "[]`\""))
		if field == "" {
			continue
		}
		result = append(result, field)
	}
	return result
}

func (p *Parser) isKeyword(word string) bool {
	keywords := map[string]bool{
		"SELECT": true, "FROM": true, "WHERE": true, "JOIN": true, "INNER": true,
		"LEFT": true, "RIGHT": true, "OUTER": true, "ON": true, "AND": true,
		"OR": true, "NOT": true, "IN": true, "EXISTS": true, "BETWEEN": true,
		"LIKE": true, "IS": true, "NULL": true, "AS": true, "DISTINCT": true,
		"GROUP": true, "BY": true, "HAVING": true, "ORDER": true, "ASC": true,
		"DESC": true, "UNION": true, "ALL": true, "TOP": true, "CASE": true,
		"WHEN": true, "THEN": true, "END": true, "INSERT": true,
		"INTO": true, "VALUES": true, "UPDATE": true, "SET": true, "DELETE": true,
		"CREATE": true, "TABLE": true, "INDEX": true, "VIEW": true, "PROC": true,
		"PROCEDURE": true, "FUNCTION": true, "DECLARE": true, "BEGIN": true,
		"RETURN": true, "IF": true, "WHILE": true, "FOR": true,
		"EXEC": true, "EXECUTE": true, "WITH": true, "RECURSIVE": true,
		"XPK": true, "XIE": true, "NOLOCK": true, "UPDLOCK": true,
		"ROWLOCK": true, "READPAST": true, "FORCE": true, "OPTION": true,
	}
	return keywords[strings.ToUpper(word)]
}
