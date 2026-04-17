package tpr

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
)

type Parser struct {
	sqlBlockStartRe *regexp.Regexp
	fieldRe         *regexp.Regexp
	paramRe         *regexp.Regexp
	includeRe       *regexp.Regexp
}

type ParseResult struct {
	Form      *model.ReportForm
	Fields    []*model.ReportField
	Params    []*model.ReportParam
	Includes  []model.IncludeRef
	Fragments []*model.QueryFragment
	Errors    []ParseError
}

type ParseError struct {
	Line     int
	Column   int
	Message  string
	Severity string
}

func NewParser() *Parser {
	return &Parser{
		sqlBlockStartRe: regexp.MustCompile(`^\s*@([A-Za-z0-9_]+)@\s*=\s*SQL\s*\{\s*$`),
		fieldRe:         regexp.MustCompile(`^\s*@([^@]+)@\s*=\s*Field\{(.*)\}\s*$`),
		paramRe:         regexp.MustCompile(`^\s*%([^!]+)!=Param\{(.*)\}\s*$`),
		includeRe:       regexp.MustCompile(`(?i)#include\s*[<"]([^>"]+)[>"]`),
	}
}

func (p *Parser) ParseFile(path string) (*ParseResult, error) {
	encodings := []encoding.Encoding{encoding.WIN1251, encoding.CP866, encoding.UTF8}

	var content string
	var err error
	for _, enc := range encodings {
		content, err = encoding.ReadFile(path, enc)
		if err == nil && p.isValidEncoding(content) {
			return p.ParseContent(content, path)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read file with any encoding: %w", err)
	}
	return p.ParseContent(content, path)
}

func (p *Parser) ParseContent(content string, path string) (*ParseResult, error) {
	result := &ParseResult{
		Form: &model.ReportForm{
			ReportName: reportNameFromPath(path),
			ReportType: "tpr",
			LineStart:  1,
		},
		Fields:    make([]*model.ReportField, 0),
		Params:    make([]*model.ReportParam, 0),
		Includes:  make([]model.IncludeRef, 0),
		Fragments: make([]*model.QueryFragment, 0),
		Errors:    make([]ParseError, 0),
	}

	lines := strings.Split(content, "\n")
	result.Form.LineEnd = len(lines)

	inFields := false
	inSQLBlock := false
	currentSQLName := ""
	currentSQLStart := 0
	sqlLines := make([]string, 0)

	flushSQL := func(lineEnd int) {
		queryText := strings.TrimSpace(strings.Join(sqlLines, "\n"))
		fragment := &model.QueryFragment{
			ComponentName: currentSQLName,
			ComponentType: "TPR_SQL_BLOCK",
			QueryText:     queryText,
			Context:       "tpr_sql_block",
			LineNumber:    currentSQLStart,
		}
		result.Fragments = append(result.Fragments, fragment)
		for idx, line := range sqlLines {
			for _, m := range p.includeRe.FindAllStringSubmatch(line, -1) {
				if len(m) > 1 {
					result.Includes = append(result.Includes, model.IncludeRef{IncludePath: strings.TrimSpace(m[1]), LineNumber: currentSQLStart + idx})
				}
			}
		}
		_ = lineEnd
	}

	scanner := bufio.NewScanner(strings.NewReader(content))
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if inSQLBlock {
			if trimmed == "}" {
				flushSQL(lineNum)
				inSQLBlock = false
				currentSQLName = ""
				sqlLines = sqlLines[:0]
				continue
			}
			sqlLines = append(sqlLines, line)
			continue
		}

		if strings.EqualFold(trimmed, "[Fields]") {
			inFields = true
			continue
		}
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") && !strings.EqualFold(trimmed, "[Fields]") {
			inFields = false
		}
		if trimmed == "&&" {
			inFields = false
		}

		if matches := p.sqlBlockStartRe.FindStringSubmatch(line); matches != nil {
			inSQLBlock = true
			currentSQLName = strings.TrimSpace(matches[1])
			currentSQLStart = lineNum
			sqlLines = sqlLines[:0]
			continue
		}

		if inFields {
			if matches := p.fieldRe.FindStringSubmatch(line); matches != nil {
				fieldName := normalizeReportToken(matches[1])
				body := strings.TrimSpace(matches[2])
				parts := splitCSVLike(body)
				field := &model.ReportField{FieldName: fieldName, LineNumber: lineNum, RawText: strings.TrimSpace(line)}
				if len(parts) > 0 {
					field.SourceName = parts[0]
				}
				if len(parts) > 1 {
					field.FormatMask = parts[1]
				}
				if len(parts) > 2 {
					field.Options = parts[2:]
				}
				result.Fields = append(result.Fields, field)
				continue
			}
		}

		if matches := p.paramRe.FindStringSubmatch(line); matches != nil {
			paramName := strings.TrimSpace(matches[1])
			body := strings.TrimSpace(matches[2])
			parts := splitCSVLike(body)
			param := &model.ReportParam{ParamName: paramName, ParamKind: "tpr_param", LineNumber: lineNum, RawText: strings.TrimSpace(line)}
			if len(parts) > 1 {
				param.DataType = parts[1]
			}
			if len(parts) > 3 {
				param.LookupTable = parts[3]
			}
			if len(parts) > 4 {
				param.LookupColumn = parts[4]
			}
			if len(parts) > 5 {
				param.KeyColumn = parts[5]
			}
			if len(parts) > 9 {
				param.DefaultValue = parts[9]
			}
			result.Params = append(result.Params, param)
		}
	}

	if inSQLBlock {
		flushSQL(lineNum)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func splitCSVLike(input string) []string {
	parts := strings.Split(input, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		result = append(result, trimmed)
	}
	return result
}

func normalizeReportToken(value string) string {
	return strings.TrimSpace(strings.Trim(value, "@"))
}

func reportNameFromPath(path string) string {
	path = strings.ReplaceAll(path, `\\`, "/")
	parts := strings.Split(path, "/")
	name := parts[len(parts)-1]
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		return name[:idx]
	}
	return name
}

func (p *Parser) isValidEncoding(content string) bool {
	garbledPatterns := []*regexp.Regexp{
		regexp.MustCompile(`[╚╩╘╟╤╥╦╧╨╩╪╫╬═│└┘├┤]{3,}`),
		regexp.MustCompile(`[ÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞß]{3,}`),
	}
	for _, pattern := range garbledPatterns {
		if pattern.MatchString(content) {
			return false
		}
	}
	return true
}
