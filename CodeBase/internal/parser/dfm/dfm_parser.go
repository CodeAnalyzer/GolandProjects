package dfm

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
)

// Parser DFM-парсер
type Parser struct {
	// DFM-объекты
	objectRe     *regexp.Regexp // object tbSelect: DsTextBox
	inheritedRe  *regexp.Regexp // inherited tbSelect: DsTextBox
	itemRe       *regexp.Regexp // item
	strArrayRe   *regexp.Regexp // StrArray = <
	collectionRe *regexp.Regexp // Params = < / Items = < / Columns = <
	angleEndRe   *regexp.Regexp // >
	endRe        *regexp.Regexp // end

	// Свойства объектов
	nameRe     *regexp.Regexp // Name = 'tbSelect'
	captionRe  *regexp.Regexp // Caption = '...'/#1040...
	linesRe    *regexp.Regexp // Lines.Strings = (
	linesOldRe *regexp.Regexp // Lines = (
	sqlRe      *regexp.Regexp // SQL.Strings = (

	// SQL в строках
	stringRe *regexp.Regexp // 'string value'

	// Комментарии
	commentRe *regexp.Regexp // { comment }
}

// ParseResult результат парсинга DFM-файла
type ParseResult struct {
	Components []*model.DFMComponent
	Forms      []*model.DFMForm
	Queries    []*model.DFMQuery
	Tables     []*model.SQLTable
	Errors     []ParseError
}

// ParseError ошибка парсинга
type ParseError struct {
	Line     int
	Column   int
	Message  string
	Severity string
}

// NewParser создаёт новый DFM-парсер
func NewParser() *Parser {
	return &Parser{
		// DFM-объекты
		objectRe:     regexp.MustCompile(`(?i)^\s*object\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z_][A-Za-z0-9_]*)`),
		inheritedRe:  regexp.MustCompile(`(?i)^\s*inherited\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z_][A-Za-z0-9_]*)`),
		itemRe:       regexp.MustCompile(`(?i)^\s*item\s*$`),
		strArrayRe:   regexp.MustCompile(`(?i)^\s*StrArray\s*=\s*<\s*$`),
		collectionRe: regexp.MustCompile(`(?i)^\s*[A-Za-z_][A-Za-z0-9_.]*\s*=\s*<\s*$`),
		angleEndRe:   regexp.MustCompile(`^\s*>\s*$`),
		endRe:        regexp.MustCompile(`(?i)^\s*end\s*$`),

		// Свойства объектов
		nameRe:     regexp.MustCompile(`(?i)^\s*Name\s*=\s*['"]([^'"]+)['"]`),
		captionRe:  regexp.MustCompile(`(?i)^\s*Caption\s*=\s*(.+)$`),
		linesRe:    regexp.MustCompile(`(?i)^\s*Lines\.Strings\s*=\s*\(`),
		linesOldRe: regexp.MustCompile(`(?i)^\s*Lines\s*=\s*\(`),
		sqlRe:      regexp.MustCompile(`(?i)^\s*SQL\.Strings\s*=\s*\(`),

		// Строки
		stringRe: regexp.MustCompile(`'([^']*(?:''[^']*)*)'`),

		// Комментарии
		commentRe: regexp.MustCompile(`\{[^}]*\}`),
	}
}

// ParseFile парсит DFM-файл
func (p *Parser) ParseFile(path string) (*ParseResult, error) {
	content, err := encoding.ReadFile(path, encoding.WIN1251)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return p.ParseContent(content)
}

// ParseContent парсит содержимое DFM-файла
func (p *Parser) ParseContent(content string) (*ParseResult, error) {
	result := &ParseResult{
		Components: make([]*model.DFMComponent, 0),
		Forms:      make([]*model.DFMForm, 0),
		Queries:    make([]*model.DFMQuery, 0),
		Tables:     make([]*model.SQLTable, 0),
		Errors:     make([]ParseError, 0),
	}

	scanner := bufio.NewScanner(strings.NewReader(content))

	type objectFrame struct {
		Name       string
		Type       string
		ParentName string
		LineStart  int
		Caption    string
	}

	var (
		currentForm      *model.DFMForm
		currentQuery     *model.DFMQuery
		objectStack      []objectFrame
		formDepth        int
		rootFormCaptured bool
		inStrArray       bool
		inCollection     bool
		inItem           bool
		currentItemName  string
		inLines          bool
		lineNum          int
		queryLines       []string
	)

	currentObjectName := func() string {
		if inItem && currentItemName != "" {
			return currentItemName
		}
		if len(objectStack) == 0 {
			return ""
		}
		return objectStack[len(objectStack)-1].Name
	}

	currentObjectType := func() string {
		if inItem {
			return "StrArrayItem"
		}
		if len(objectStack) == 0 {
			return ""
		}
		return objectStack[len(objectStack)-1].Type
	}

	currentObjectFrame := func() *objectFrame {
		if inItem || len(objectStack) == 0 {
			return nil
		}
		return &objectStack[len(objectStack)-1]
	}

	decodeDFMString := func(raw string) string {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return ""
		}

		var result strings.Builder
		for i := 0; i < len(raw); {
			switch raw[i] {
			case '\'':
				j := i + 1
				var chunk strings.Builder
				for j < len(raw) {
					if raw[j] == '\'' {
						if j+1 < len(raw) && raw[j+1] == '\'' {
							chunk.WriteByte('\'')
							j += 2
							continue
						}
						break
					}
					chunk.WriteByte(raw[j])
					j++
				}
				result.WriteString(chunk.String())
				if j < len(raw) && raw[j] == '\'' {
					j++
				}
				i = j
			case '#':
				j := i + 1
				for j < len(raw) && raw[j] >= '0' && raw[j] <= '9' {
					j++
				}
				if j > i+1 {
					var code int
					fmt.Sscanf(raw[i+1:j], "%d", &code)
					if code > 0 {
						result.WriteRune(rune(code))
					}
				}
				i = j
			default:
				result.WriteByte(raw[i])
				i++
			}
		}

		return strings.TrimSpace(result.String())
	}

	flushQuery := func() {
		if currentQuery == nil {
			queryLines = nil
			return
		}

		currentQuery.QueryText = strings.TrimSpace(strings.Join(queryLines, "\n"))
		if currentQuery.QueryText != "" && isLikelySQLText(currentQuery.QueryText) {
			result.Queries = append(result.Queries, currentQuery)
			p.extractTablesFromSQL(currentQuery.QueryText, currentQuery.LineNumber, result)
		}

		currentQuery = nil
		queryLines = nil
	}

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Пропускаем пустые строки
		if trimmed == "" {
			continue
		}

		// Пропускаем комментарии
		if p.commentRe.MatchString(trimmed) {
			continue
		}

		// ========================================
		// Объекты (формы и компоненты)
		// ========================================

		// object ComponentName: ComponentType
		if matches := p.objectRe.FindStringSubmatch(trimmed); matches != nil {
			parentName := ""
			if len(objectStack) > 0 {
				parentName = objectStack[len(objectStack)-1].Name
			}
			objectStack = append(objectStack, objectFrame{Name: matches[1], Type: matches[2], ParentName: parentName, LineStart: lineNum})
			if len(objectStack) == 1 && !rootFormCaptured {
				formDepth = 1
				currentForm = &model.DFMForm{
					FormName:  matches[1],
					FormClass: matches[2],
					LineStart: lineNum,
				}
			}
			continue
		}

		// inherited ComponentName: ComponentType
		if matches := p.inheritedRe.FindStringSubmatch(trimmed); matches != nil {
			parentName := ""
			if len(objectStack) > 0 {
				parentName = objectStack[len(objectStack)-1].Name
			}
			objectStack = append(objectStack, objectFrame{Name: matches[1], Type: matches[2], ParentName: parentName, LineStart: lineNum})
			if len(objectStack) == 1 && !rootFormCaptured {
				formDepth = 1
				currentForm = &model.DFMForm{
					FormName:  matches[1],
					FormClass: matches[2],
					LineStart: lineNum,
				}
			}
			continue
		}

		if p.strArrayRe.MatchString(trimmed) || p.collectionRe.MatchString(trimmed) {
			inStrArray = true
			inCollection = true
			continue
		}

		if inStrArray && p.itemRe.MatchString(trimmed) {
			inItem = true
			currentItemName = ""
			continue
		}

		if inStrArray && p.angleEndRe.MatchString(trimmed) {
			inStrArray = false
			inCollection = false
			inItem = false
			currentItemName = ""
			continue
		}

		// end - конец объекта
		if p.endRe.MatchString(trimmed) {
			if inLines {
				flushQuery()
				inLines = false
			}
			if inItem {
				inItem = false
				currentItemName = ""
				continue
			}
			if len(objectStack) > 0 {
				closing := objectStack[len(objectStack)-1]
				if len(objectStack) == formDepth && currentForm != nil {
					currentForm.Caption = closing.Caption
					currentForm.LineEnd = lineNum
					result.Forms = append(result.Forms, currentForm)
					rootFormCaptured = true
					currentForm = nil
					formDepth = 0
				} else if len(objectStack) > formDepth && formDepth > 0 {
					formName := ""
					if currentForm != nil {
						formName = currentForm.FormName
					} else if len(objectStack) > 0 {
						formName = objectStack[0].Name
					}
					result.Components = append(result.Components, &model.DFMComponent{
						FormName:      formName,
						ComponentName: closing.Name,
						ComponentType: closing.Type,
						ParentName:    closing.ParentName,
						Caption:       closing.Caption,
						LineStart:     closing.LineStart,
						LineEnd:       lineNum,
					})
				}
				objectStack = objectStack[:len(objectStack)-1]
			}
			continue
		}

		// ========================================
		// Имя объекта
		// ========================================

		if matches := p.nameRe.FindStringSubmatch(trimmed); matches != nil {
			if inItem || inCollection {
				currentItemName = matches[1]
				continue
			}
			if len(objectStack) > 0 {
				objectStack[len(objectStack)-1].Name = matches[1]
				if len(objectStack) == 1 && currentForm != nil {
					currentForm.FormName = matches[1]
				}
			}
			continue
		}

		if matches := p.captionRe.FindStringSubmatch(trimmed); matches != nil {
			caption := decodeDFMString(matches[1])
			if frame := currentObjectFrame(); frame != nil {
				frame.Caption = caption
				objectStack[len(objectStack)-1] = *frame
				if len(objectStack) == 1 && currentForm != nil {
					currentForm.Caption = caption
				}
			}
			continue
		}

		// ========================================
		// Секция Lines.Strings = ( или SQL.Strings = (
		// ========================================

		if p.linesRe.MatchString(trimmed) || p.linesOldRe.MatchString(trimmed) {
			inLines = true
			currentQuery = &model.DFMQuery{
				ComponentName: currentObjectName(),
				ComponentType: currentObjectType(),
				LineNumber:    lineNum,
			}
			queryLines = make([]string, 0)
			continue
		}

		// SQL.Strings = ( - для компонентов DsQuery
		if p.sqlRe.MatchString(trimmed) {
			inLines = true
			currentQuery = &model.DFMQuery{
				ComponentName: currentObjectName(),
				ComponentType: currentObjectType(),
				LineNumber:    lineNum,
			}
			queryLines = make([]string, 0)
			continue
		}

		// ========================================
		// Строки SQL-запроса
		// ========================================

		if inLines {
			// Конец секции строк
			if trimmed == ")" {
				inLines = false
				flushQuery()
				continue
			}

			// item - начало новой строки в старом формате
			if p.itemRe.MatchString(trimmed) {
				continue
			}

			// Извлекаем строковое содержимое
			if matches := p.stringRe.FindAllStringSubmatch(line, -1); matches != nil {
				for _, m := range matches {
					strContent := m[1]
					// Unescape '' -> '
					strContent = strings.ReplaceAll(strContent, "''", "'")
					if strContent != "" {
						queryLines = append(queryLines, strContent)
					}
				}
			}
		}
	}

	// Финализация
	if currentQuery != nil && len(queryLines) > 0 {
		flushQuery()
	}

	if currentForm != nil {
		if len(objectStack) > 0 {
			currentForm.Caption = objectStack[0].Caption
		}
		if currentForm.LineEnd == 0 {
			currentForm.LineEnd = lineNum
		}
		result.Forms = append(result.Forms, currentForm)
		rootFormCaptured = true
	}

	if err := scanner.Err(); err != nil {
		result.Errors = append(result.Errors, ParseError{
			Line:     lineNum,
			Message:  err.Error(),
			Severity: "error",
		})
	}

	return result, nil
}

func isLikelySQLText(s string) bool {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return false
	}
	sqlKeywordDetectRe := regexp.MustCompile(`(?is)\b(SELECT\b.*\bFROM|INSERT\s+INTO|UPDATE\s+[A-Za-z_#][A-Za-z0-9_#]*\s+SET|DELETE\s+FROM|DECLARE\s+@[A-Za-z_#][A-Za-z0-9_#]*|FROM\s+[A-Za-z_#][A-Za-z0-9_#]*|JOIN\s+[A-Za-z_#][A-Za-z0-9_#]*|WHERE\b|GROUP\s+BY|ORDER\s+BY|HAVING\b|UNION\b|CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE|EXEC(?:UTE)?\s+[A-Za-z_#][A-Za-z0-9_#]*)`)
	return sqlKeywordDetectRe.MatchString(trimmed)
}

// extractTablesFromSQL извлекает имена таблиц из SQL-строки
func (p *Parser) extractTablesFromSQL(sqlText string, lineNum int, result *ParseResult) {
	// Нормализуем SQL
	sqlText = strings.ReplaceAll(sqlText, "''", "'")

	// Регулярка для таблиц
	tableRe := regexp.MustCompile(`(?i)\b(?:FROM|JOIN|INTO|UPDATE|DELETE\s+FROM|INSERT\s+INTO)\s+([A-Za-z_#][A-Za-z0-9_#]*)`)

	matchIndexes := tableRe.FindAllStringSubmatchIndex(sqlText, -1)
	if matches := tableRe.FindAllStringSubmatch(sqlText, -1); matches != nil {
		seenTables := make(map[string]bool)
		for i, m := range matches {
			tableName := m[1]
			// Пропускаем ключевые слова
			if isKeyword(tableName) || isIgnoredTableName(tableName) {
				continue
			}
			if seenTables[strings.ToLower(tableName)] {
				continue
			}
			seenTables[strings.ToLower(tableName)] = true
			colNumber := 0
			if i < len(matchIndexes) && len(matchIndexes[i]) >= 4 {
				colNumber = matchIndexes[i][2] + 1
			}

			result.Tables = append(result.Tables, &model.SQLTable{
				TableName:   tableName,
				Context:     "dfm_embedded",
				IsTemporary: strings.HasPrefix(strings.ToLower(strings.TrimSpace(tableName)), "p") || strings.HasPrefix(strings.ToLower(strings.TrimSpace(tableName)), "#"),
				LineNumber:  lineNum,
				ColNumber:   colNumber,
			})
		}
	}
}

// isKeyword проверяет, является ли имя ключевым словом SQL
func isKeyword(name string) bool {
	keywords := map[string]bool{
		"select": true, "insert": true, "update": true, "delete": true,
		"from": true, "where": true, "join": true, "table": true,
		"values": true, "set": true, "into": true, "declare": true,
		"begin": true, "end": true, "if": true, "else": true,
		"while": true, "for": true, "return": true, "exec": true,
		"as": true, "on": true, "and": true, "or": true, "not": true,
		"in": true, "is": true, "null": true, "like": true,
		"group": true, "by": true, "order": true, "having": true,
		"union": true, "all": true, "distinct": true,
		"left": true, "right": true, "inner": true, "outer": true,
		"case": true, "when": true, "then": true,
	}
	return keywords[strings.ToLower(name)]
}

// isIgnoredTableName проверяет, нужно ли игнорировать имя таблицы
func isIgnoredTableName(name string) bool {
	ignored := map[string]bool{
		"a": true, "b": true, "c": true, "d": true, "e": true,
		"t": true, "tmp": true, "temp": true,
		"select": true, "insert": true, "update": true, "delete": true,
		"from": true, "where": true, "join": true, "table": true,
		"values": true, "set": true, "into": true, "declare": true,
		"begin": true, "end": true, "if": true, "else": true,
		"while": true, "for": true, "return": true, "exec": true,
		"as": true, "on": true, "and": true, "or": true, "not": true,
		"in": true, "is": true, "null": true, "like": true,
		"group": true, "by": true, "order": true, "having": true,
		"union": true, "all": true, "distinct": true,
		"left": true, "right": true, "inner": true, "outer": true,
		"case": true, "when": true, "then": true,
	}
	return ignored[strings.ToLower(name)]
}
