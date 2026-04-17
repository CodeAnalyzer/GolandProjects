package js

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
)

// Parser JS-парсер
type Parser struct {
	// Функции
	functionRe *regexp.Regexp

	// Переменные и объекты
	varRe          *regexp.Regexp
	createObjectRe *regexp.Regexp

	// Вызовы процедур и запросов
	execProcRe  *regexp.Regexp
	execQueryRe *regexp.Regexp

	// Константы
	constRe *regexp.Regexp

	// Комментарии
	commentLineRe  *regexp.Regexp
	commentBlockRe *regexp.Regexp
}

// ParseResult результат парсинга JS-файла
type ParseResult struct {
	Functions      []*model.JSFunction
	ScriptObjects  []*model.JSScriptObject
	ProcedureCalls []*model.JSProcedureCall
	QueryCalls     []*model.JSQueryCall
	Constants      []*model.JSConstant
	Errors         []ParseError
}

// ParseError ошибка парсинга
type ParseError struct {
	Line     int
	Column   int
	Message  string
	Severity string
}

// NewParser создаёт новый JS-парсер
func NewParser() *Parser {
	return &Parser{
		// Функции: function name(...) или function name( ...)
		functionRe: regexp.MustCompile(`function\s+(\w+)\s*\(([^)]*)\)`),

		// Переменные: var name = ...
		varRe: regexp.MustCompile(`var\s+(\w+)\s*=\s*`),

		// Sys.CreateObject("ObjectName")
		createObjectRe: regexp.MustCompile(`(\w+)\s*=\s*Sys\.CreateObject\(\s*"(\w+)"\s*\)`),

		// object.ExecProc("ProcName", ...)
		execProcRe: regexp.MustCompile(`(\w+)\.ExecProc\s*\(\s*"(\w+)"`),

		// object.ExecQuery("SQL...", ...)
		execQueryRe: regexp.MustCompile(`(\w+)\.ExecQuery\s*\(\s*"([^"]+)"`),

		// Константы: var NAME = value (верхний регистр)
		constRe: regexp.MustCompile(`var\s+([A-Z_][A-Z0-9_]*)\s*=\s*(\d+|"[^"]*")`),

		// Комментарии
		commentLineRe:  regexp.MustCompile(`^\s*//.*$`),
		commentBlockRe: regexp.MustCompile(`/\*[\s\S]*?\*/`),
	}
}

// ParseFile парсит JS-файл
func (p *Parser) ParseFile(path string) (*ParseResult, error) {
	content, err := encoding.ReadFile(path, encoding.WIN1251)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return p.ParseContent(content)
}

// ParseContent парсит содержимое JS-файла
func (p *Parser) ParseContent(content string) (*ParseResult, error) {
	result := &ParseResult{
		Functions:      make([]*model.JSFunction, 0),
		ScriptObjects:  make([]*model.JSScriptObject, 0),
		ProcedureCalls: make([]*model.JSProcedureCall, 0),
		QueryCalls:     make([]*model.JSQueryCall, 0),
		Constants:      make([]*model.JSConstant, 0),
		Errors:         make([]ParseError, 0),
	}

	// Удаляем блочные комментарии из контента для упрощения парсинга
	contentNoBlocks := p.commentBlockRe.ReplaceAllString(content, "")
	linesNoBlocks := strings.Split(contentNoBlocks, "\n")

	for idx, line := range linesNoBlocks {
		lineNum := idx + 1
		trimmed := strings.TrimSpace(line)

		// Пропускаем пустые строки и однострочные комментарии
		if trimmed == "" || p.commentLineRe.MatchString(trimmed) {
			continue
		}

		// ========================================
		// Функции
		// ========================================
		if matches := p.functionRe.FindStringSubmatch(line); matches != nil {
			funcName := matches[1]
			params := matches[2]

			// Определяем тип сценария по имени функции
			scenarioType := detectScenarioType(funcName, contentNoBlocks)

			// Находим начало и конец функции
			lineStart, lineEnd := findFunctionBounds(linesNoBlocks, lineNum-1)

			result.Functions = append(result.Functions, &model.JSFunction{
				FunctionName: funcName,
				Signature:    buildSignature(funcName, params),
				LineStart:    lineStart + 1, // 1-based
				LineEnd:      lineEnd + 1,
				ScenarioType: scenarioType,
			})
		}

		// ========================================
		// Script Objects: var Name = Sys.CreateObject("Type")
		// ========================================
		if matches := p.createObjectRe.FindStringSubmatch(line); matches != nil {
			varName := matches[1]
			objectType := matches[2]

			result.ScriptObjects = append(result.ScriptObjects, &model.JSScriptObject{
				Name: varName,
				Type: objectType,
			})
		}

		// ========================================
		// Вызовы процедур: object.ExecProc("ProcName", ...)
		// ========================================
		if matches := p.execProcRe.FindAllStringSubmatch(line, -1); matches != nil {
			for _, m := range matches {
				objectName := m[1]
				procName := m[2]

				result.ProcedureCalls = append(result.ProcedureCalls, &model.JSProcedureCall{
					ObjectName: objectName,
					ProcName:   procName,
					LineNumber: lineNum,
				})
			}
		}

		// ========================================
		// Вызовы запросов: object.ExecQuery("SQL...", ...)
		// ========================================
		if matches := p.execQueryRe.FindAllStringSubmatch(line, -1); matches != nil {
			for _, m := range matches {
				objectName := m[1]
				queryText := m[2]

				// Unescape экранированных кавычек
				queryText = strings.ReplaceAll(queryText, `\"`, `"`)
				queryText = strings.ReplaceAll(queryText, `\n`, "\n")

				result.QueryCalls = append(result.QueryCalls, &model.JSQueryCall{
					ObjectName: objectName,
					QueryText:  queryText,
					LineNumber: lineNum,
				})
			}
		}

		// ========================================
		// Константы: var NAME = value
		// ========================================
		if matches := p.constRe.FindAllStringSubmatch(line, -1); matches != nil {
			for _, m := range matches {
				constName := m[1]
				constValue := strings.Trim(m[2], `"`)

				result.Constants = append(result.Constants, &model.JSConstant{
					Name:  constName,
					Value: constValue,
				})
			}
		}
	}

	// Анализируем использование объектов и присваиваем ParentObject
	p.assignParentObjects(result.Functions, result.ScriptObjects, contentNoBlocks)

	return result, nil
}

// assignParentObjects анализирует использование объектов в функциях
func (p *Parser) assignParentObjects(functions []*model.JSFunction, objects []*model.JSScriptObject, content string) {
	for _, fn := range functions {
		if fn.ParentObject != "" {
			continue // Уже присвоен
		}

		// Извлекаем содержимое функции
		fnContent := p.extractFunctionContent(content, fn.LineStart, fn.LineEnd)

		// Проверяем использование каждого объекта
		for _, obj := range objects {
			if p.isObjectCreatedInFunction(fnContent, obj.Name, obj.Type) {
				continue
			}

			if p.isObjectUsedInFunction(fnContent, obj.Name) {
				fn.ParentObject = obj.Type
				break
			}
		}
	}
}

// extractFunctionContent извлекает содержимое функции
func (p *Parser) extractFunctionContent(content string, start, end int) string {
	lines := strings.Split(content, "\n")
	if start >= 1 && end <= len(lines) {
		return strings.Join(lines[start-1:end], "\n")
	}
	return ""
}

// isObjectUsedInFunction проверяет использование объекта в функции
func (p *Parser) isObjectUsedInFunction(fnContent, objectName string) bool {
	// Ищем вызовы методов объекта: objectName.MethodName(...)
	methodRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(objectName) + `\.\w+\s*\(`)

	// Ищем доступ к свойствам: objectName.PropertyName
	propRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(objectName) + `\.\w+\s*[^(\s]`)

	return methodRe.MatchString(fnContent) || propRe.MatchString(fnContent)
}

// isObjectCreatedInFunction проверяет, создаётся ли объект локально внутри функции
func (p *Parser) isObjectCreatedInFunction(fnContent, objectName, objectType string) bool {
	createRe := regexp.MustCompile(`\b(?:var\s+)?` + regexp.QuoteMeta(objectName) + `\s*=\s*Sys\.CreateObject\(\s*"` + regexp.QuoteMeta(objectType) + `"\s*\)`)
	return createRe.MatchString(fnContent)
}

// detectScenarioType определяет тип сценария по функциям в файле
func detectScenarioType(funcName, content string) string {
	// Массовые операции обычно содержат StepForward и MassAccrualStarter
	if strings.Contains(content, "StepForward") && strings.Contains(content, "MassAccrualStarter") {
		return "mass_operation"
	}

	// Модель Ф.О. содержит CreateInstrument
	if strings.Contains(content, "CreateInstrument") {
		return "instrument_model"
	}

	// Утилиты
	if funcName == "BeforeQuery" || funcName == "AfterProcess" || funcName == "StepBack" {
		return "mass_operation"
	}

	if funcName == "CreateInstrument" || funcName == "PostLoadInstrument" || funcName == "InitForm" {
		return "instrument_model"
	}

	return "utility"
}

// findFunctionBounds находит начало и конец функции
func findFunctionBounds(lines []string, startIdx int) (int, int) {
	lineStart := startIdx
	braceCount := 0
	inFunction := false
	lineEnd := startIdx

	for i := startIdx; i < len(lines); i++ {
		line := lines[i]

		// Считаем скобки
		for _, ch := range line {
			if ch == '{' {
				braceCount++
				inFunction = true
			} else if ch == '}' {
				braceCount--
			}
		}

		if inFunction {
			lineEnd = i
			if braceCount == 0 {
				break
			}
		}
	}

	return lineStart, lineEnd
}

// buildSignature строит сигнатуру функции
func buildSignature(name, params string) string {
	if params == "" {
		return fmt.Sprintf("function %s()", name)
	}
	return fmt.Sprintf("function %s(%s)", name, params)
}
