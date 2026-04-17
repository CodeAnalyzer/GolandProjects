package h

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
)

// Parser H-файлов парсер
type Parser struct {
	defineRe    *regexp.Regexp
	emptyDefineRe *regexp.Regexp
	includeRe   *regexp.Regexp
	commentRe   *regexp.Regexp
	macroRe     *regexp.Regexp
	emptyMacroRe *regexp.Regexp
	constRe     *regexp.Regexp
	constWithCommentRe *regexp.Regexp
}

// ParseResult результат парсинга H-файла
type ParseResult struct {
	Defines  []*model.HDefine
	Includes []model.IncludeRef
	Errors   []ParseError
}

// ParseError ошибка парсинга
type ParseError struct {
	Line     int
	Column   int
	Message  string
	Severity string
}

// NewParser создаёт новый H-парсер
func NewParser() *Parser {
	return &Parser{
		// #define NAME value
		defineRe: regexp.MustCompile(`^\s*#define\s+([A-Za-z_][A-Za-z0-9_]*)\s+(.*)$`),
		// #define NAME
		emptyDefineRe: regexp.MustCompile(`^\s*#define\s+([A-Za-z_][A-Za-z0-9_]*)\s*$`),
		// #include <file> или #include "file"
		includeRe: regexp.MustCompile(`^\s*#include\s*[<"]([^>"]+)[>"]`),
		// Комментарии -- или /* */
		commentRe: regexp.MustCompile(`^\s*(--|/\*|\*)`),
		// Макросы с параметрами: #define MACRO(a,b) body
		macroRe: regexp.MustCompile(`^\s*#define\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)\s+(.+)$`),
		// Макросы с параметрами без body: #define MACRO(a,b)
		emptyMacroRe: regexp.MustCompile(`^\s*#define\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)\s*$`),
		// Константы: #define NAME number
		constRe: regexp.MustCompile(`^\s*#define\s+([A-Za-z_][A-Za-z0-9_]*)\s+(\d+)$`),
		// Константы с inline comment: #define NAME 1 /* comment */
		constWithCommentRe: regexp.MustCompile(`^\s*#define\s+([A-Za-z_][A-Za-z0-9_]*)\s+([+-]?\d+)\s*(/\*.*\*/|--.*)$`),
	}
}

// ParseFile парсит H-файл
func (p *Parser) ParseFile(path string) (*ParseResult, error) {
	content, err := encoding.ReadFile(path, encoding.CP866)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return p.ParseContent(content)
}

// ParseContent парсит содержимое H-файла
func (p *Parser) ParseContent(content string) (*ParseResult, error) {
	result := &ParseResult{
		// Все срезы инициализируются сразу, чтобы вызывающий код получал
		// предсказуемый пустой результат даже если совпадений не найдено.
		Defines:  make([]*model.HDefine, 0),
		Includes: make([]model.IncludeRef, 0),
		Errors:   make([]ParseError, 0),
	}

	scanner := bufio.NewScanner(strings.NewReader(content))
	lineNum := 0
	var pendingLine string
	var pendingLineNum int

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if pendingLine != "" {
			pendingLine += "\n" + strings.TrimRight(line, " \t")
		} else {
			pendingLine = strings.TrimRight(line, " \t")
			pendingLineNum = lineNum
		}

		if strings.HasSuffix(pendingLine, "\\") {
			pendingLine = strings.TrimRight(strings.TrimSuffix(pendingLine, "\\"), " \t")
			continue
		}

		trimmed := strings.TrimSpace(pendingLine)
		defineLineNum := pendingLineNum
		pendingLine = ""
		pendingLineNum = 0

		// Парсер намеренно работает line-by-line и regex-based,
		// потому что для H-файлов нам нужен быстрый extraction pass, а не полноценный C preprocessor.

		// Пропускаем пустые строки
		if trimmed == "" {
			continue
		}

		// Пропускаем комментарии
		if p.commentRe.MatchString(trimmed) {
			// Но сохраняем комментарии с определениями
			if strings.Contains(trimmed, "/*---") || strings.Contains(trimmed, "//---") {
				// Это заголовочный комментарий секции
				continue
			}
			continue
		}

		// include директивы сохраняются отдельно, чтобы позже можно было строить
		// include graph (граф включений) и резолвить зависимости между файлами.
		if matches := p.includeRe.FindStringSubmatch(trimmed); matches != nil {
			result.Includes = append(result.Includes, model.IncludeRef{IncludePath: matches[1], LineNumber: defineLineNum})
			continue
		}

		// Проверяем макросы с параметрами
		if matches := p.macroRe.FindStringSubmatch(trimmed); matches != nil {
			result.Defines = append(result.Defines, &model.HDefine{
				DefineName:  matches[1],
				DefineValue: strings.TrimSpace(matches[3]),
				DefineType:  "macro",
				LineNumber:  defineLineNum,
			})
			continue
		}

		if matches := p.emptyMacroRe.FindStringSubmatch(trimmed); matches != nil {
			result.Defines = append(result.Defines, &model.HDefine{
				DefineName:  matches[1],
				DefineValue: "",
				DefineType:  "macro",
				LineNumber:  defineLineNum,
			})
			continue
		}

		// Обычные #define после macroRe обрабатываются как константы или текстовые определения.
		if matches := p.defineRe.FindStringSubmatch(trimmed); matches != nil {
			defineType := "macro"
			defineName := matches[1]
			defineValue := matches[2]

			// Определяем тип определения
			if p.constRe.MatchString(trimmed) {
				defineType = "const"
			} else if matches := p.constWithCommentRe.FindStringSubmatch(trimmed); matches != nil {
				defineType = "const"
				defineValue = matches[2]
			} else if strings.Contains(defineValue, "--") {
				// Это комментарий-определение
				defineType = "comment"
				defineValue = strings.Split(defineValue, "--")[0]
			}

			result.Defines = append(result.Defines, &model.HDefine{
				DefineName:  defineName,
				DefineValue: strings.TrimSpace(defineValue),
				DefineType:  defineType,
				LineNumber:  defineLineNum,
			})
			continue
		}

		if matches := p.emptyDefineRe.FindStringSubmatch(trimmed); matches != nil {
			result.Defines = append(result.Defines, &model.HDefine{
				DefineName:  matches[1],
				DefineValue: "",
				DefineType:  "const",
				LineNumber:  defineLineNum,
			})
			continue
		}
	}

	if pendingLine != "" {
		trimmed := strings.TrimSpace(pendingLine)
		if matches := p.macroRe.FindStringSubmatch(trimmed); matches != nil {
			result.Defines = append(result.Defines, &model.HDefine{
				DefineName:  matches[1],
				DefineValue: strings.TrimSpace(matches[3]),
				DefineType:  "macro",
				LineNumber:  pendingLineNum,
			})
		} else if matches := p.emptyMacroRe.FindStringSubmatch(trimmed); matches != nil {
			result.Defines = append(result.Defines, &model.HDefine{
				DefineName:  matches[1],
				DefineValue: "",
				DefineType:  "macro",
				LineNumber:  pendingLineNum,
			})
		} else if matches := p.defineRe.FindStringSubmatch(trimmed); matches != nil {
			defineType := "macro"
			defineValue := matches[2]
			if p.constRe.MatchString(trimmed) {
				defineType = "const"
			} else if constMatches := p.constWithCommentRe.FindStringSubmatch(trimmed); constMatches != nil {
				defineType = "const"
				defineValue = constMatches[2]
			} else if strings.Contains(defineValue, "--") {
				defineType = "comment"
				defineValue = strings.Split(defineValue, "--")[0]
			}
			result.Defines = append(result.Defines, &model.HDefine{
				DefineName:  matches[1],
				DefineValue: strings.TrimSpace(defineValue),
				DefineType:  defineType,
				LineNumber:  pendingLineNum,
			})
		} else if matches := p.emptyDefineRe.FindStringSubmatch(trimmed); matches != nil {
			result.Defines = append(result.Defines, &model.HDefine{
				DefineName:  matches[1],
				DefineValue: "",
				DefineType:  "const",
				LineNumber:  pendingLineNum,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		// Ошибка Scanner-а не прерывает структуру результата: мы возвращаем всё,
		// что успели извлечь, и добавляем diagnostic (диагностику) в Errors.
		result.Errors = append(result.Errors, ParseError{
			Line:     lineNum,
			Message:  err.Error(),
			Severity: "error",
		})
	}

	return result, nil
}
