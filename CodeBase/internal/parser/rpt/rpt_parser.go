package rpt

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
)

type Parser struct {
	objectRe       *regexp.Regexp
	componentKVRe  *regexp.Regexp
	quotedValueRe  *regexp.Regexp
	functionStart  *regexp.Regexp
	hugeBoxNameRe  *regexp.Regexp
}

type ParseResult struct {
	Form      *model.ReportForm
	Params    []*model.ReportParam
	Functions []*model.VBFunction
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
		objectRe:      regexp.MustCompile(`(?i)^\s*object\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z_][A-Za-z0-9_]*)`),
		componentKVRe: regexp.MustCompile(`(?i)^\s*([A-Za-z0-9_.]+)\s*=\s*(.*)$`),
		quotedValueRe: regexp.MustCompile(`'([^']*(?:''[^']*)*)'`),
		functionStart: regexp.MustCompile(`(?i)^\s*(Sub|Function)\s+([A-Za-z_][A-Za-z0-9_]*)`),
		hugeBoxNameRe: regexp.MustCompile(`(?i)^\s*Name\s*=\s*'([^']+)'`),
	}
}

func (p *Parser) ParseFile(path string) (*ParseResult, error) {
	content, err := encoding.ReadFile(path, encoding.WIN1251)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return p.ParseContent(content, path)
}

func (p *Parser) ParseContent(content string, path string) (*ParseResult, error) {
	lines := strings.Split(content, "\n")
	result := &ParseResult{
		Form: &model.ReportForm{ReportName: reportNameFromPath(path), ReportType: "rpt", LineStart: 1, LineEnd: len(lines)},
		Params:    make([]*model.ReportParam, 0),
		Functions: make([]*model.VBFunction, 0),
		Fragments: make([]*model.QueryFragment, 0),
		Errors:    make([]ParseError, 0),
	}

	scanner := bufio.NewScanner(strings.NewReader(content))
	lineNum := 0
	rootCaptured := false
	currentObjectName := ""
	currentObjectType := ""
	inScriptStrings := false
	inLinesStrings := false
	currentFragmentName := ""
	currentFragmentType := ""
	currentFragmentLine := 0
	buffer := make([]string, 0)
	scriptLines := make([]string, 0)
	scriptBaseLine := 0
	objectStack := make([]string, 0)

	flushFragment := func() {
		queryText := strings.TrimSpace(strings.Join(buffer, "\n"))
		if queryText == "" {
			buffer = buffer[:0]
			return
		}
		context := "rpt_textbox_sql"
		if currentFragmentType == "TDsHugeBox" {
			context = "rpt_named_sql"
		}
		result.Fragments = append(result.Fragments, &model.QueryFragment{
			ComponentName: currentFragmentName,
			ComponentType: currentFragmentType,
			QueryText:     queryText,
			Context:       context,
			LineNumber:    currentFragmentLine,
		})
		buffer = buffer[:0]
	}

	flushScript := func() {
		if len(scriptLines) == 0 {
			return
		}
		joined := strings.Join(scriptLines, "\n")
		chunks := strings.Split(joined, "\n")
		var current []string
		var startLine int
		var fnType string
		var fnName string
		for idx, line := range chunks {
			if m := p.functionStart.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
				if len(current) > 0 {
					body := strings.Join(current, "\n")
					result.Functions = append(result.Functions, &model.VBFunction{FunctionName: fnName, FunctionType: strings.ToLower(fnType), Signature: strings.TrimSpace(current[0]), BodyText: body, LineStart: scriptBaseLine + startLine - 1, LineEnd: scriptBaseLine + startLine + len(current) - 2})
				}
				current = []string{line}
				startLine = idx + 1
				fnType = m[1]
				fnName = m[2]
				continue
			}
			if len(current) > 0 {
				current = append(current, line)
			}
		}
		if len(current) > 0 {
			body := strings.Join(current, "\n")
			result.Functions = append(result.Functions, &model.VBFunction{FunctionName: fnName, FunctionType: strings.ToLower(fnType), Signature: strings.TrimSpace(current[0]), BodyText: body, LineStart: scriptBaseLine + startLine - 1, LineEnd: scriptBaseLine + startLine + len(current) - 2})
		}
	}

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if m := p.objectRe.FindStringSubmatch(line); m != nil {
			currentObjectName = m[1]
			currentObjectType = m[2]
			objectStack = append(objectStack, currentObjectType)
			if !rootCaptured {
				rootCaptured = true
				result.Form.FormName = currentObjectName
				result.Form.FormClass = currentObjectType
			}
			if isRPTParamType(currentObjectType) {
				result.Params = append(result.Params, &model.ReportParam{ParamName: currentObjectName, ParamKind: "rpt_control", ComponentType: currentObjectType, LineNumber: lineNum, RawText: strings.TrimSpace(line)})
			}
			continue
		}
		if strings.EqualFold(trimmed, "end") {
			if inLinesStrings {
				flushFragment()
				inLinesStrings = false
			}
			if len(objectStack) > 0 {
				objectStack = objectStack[:len(objectStack)-1]
			}
			continue
		}

		if inScriptStrings {
			if trimmed == ")" {
				inScriptStrings = false
				flushScript()
				continue
			}
			if values := p.quotedValueRe.FindAllStringSubmatch(line, -1); values != nil {
				for _, v := range values {
					scriptLines = append(scriptLines, strings.ReplaceAll(v[1], "''", "'"))
				}
			}
			continue
		}

		if inLinesStrings {
			if trimmed == ")" {
				flushFragment()
				inLinesStrings = false
				continue
			}
			if values := p.quotedValueRe.FindAllStringSubmatch(line, -1); values != nil {
				for _, v := range values {
					buffer = append(buffer, strings.ReplaceAll(v[1], "''", "'"))
				}
			}
			continue
		}

		if strings.Contains(strings.ToLower(trimmed), "script.strings = (") {
			inScriptStrings = true
			scriptLines = scriptLines[:0]
			scriptBaseLine = lineNum + 1
			continue
		}
		if strings.Contains(strings.ToLower(trimmed), "lines.strings = (") {
			inLinesStrings = true
			currentFragmentName = currentObjectName
			currentFragmentType = currentObjectType
			currentFragmentLine = lineNum
			buffer = buffer[:0]
			continue
		}
		if strings.Contains(strings.ToLower(trimmed), "sql.strings = (") {
			inLinesStrings = true
			currentFragmentName = currentObjectName
			currentFragmentType = currentObjectType
			currentFragmentLine = lineNum
			buffer = buffer[:0]
			continue
		}

		if matches := p.componentKVRe.FindStringSubmatch(line); matches != nil && len(result.Params) > 0 {
			last := result.Params[len(result.Params)-1]
			if last.LineNumber > lineNum {
				continue
			}
			key := strings.TrimSpace(matches[1])
			value := strings.TrimSpace(matches[2])
			switch key {
			case "LookupForm":
				last.LookupForm = trimQuoted(value)
			case "FieldName":
				last.LookupColumn = trimQuoted(value)
			case "DataType", "PortName.DataType":
				last.DataType = trimQuoted(value)
			case "Required":
				last.Required = strings.EqualFold(value, "True")
			case "Mask":
				last.DefaultValue = trimQuoted(value)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if inLinesStrings {
		flushFragment()
	}
	flushScript()
	return result, nil
}

func isRPTParamType(componentType string) bool {
	switch strings.ToLower(strings.TrimSpace(componentType)) {
	case "dsdatetimepicker", "dsformlookup", "tcheckbox", "tmaskedit", "tcombobox":
		return true
	default:
		return false
	}
}

func trimQuoted(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "'")
	return strings.ReplaceAll(value, "''", "'")
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
