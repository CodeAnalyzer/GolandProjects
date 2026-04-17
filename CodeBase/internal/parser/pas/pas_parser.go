package pas

import (
	"bufio"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
)

// Parser PAS-парсер
type Parser struct {
	// Разделители секций
	interfaceRe      *regexp.Regexp
	implementationRe *regexp.Regexp
	initializationRe *regexp.Regexp
	finalizationRe   *regexp.Regexp

	// Uses-клаузулы
	interfaceUsesRe      *regexp.Regexp
	implementationUsesRe *regexp.Regexp

	// Секция типов
	typeSectionRe *regexp.Regexp
	classDeclRe   *regexp.Regexp
	classParentRe *regexp.Regexp

	// Методы и функции
	methodDeclRe      *regexp.Regexp
	functionDeclRe    *regexp.Regexp
	procedureDeclRe   *regexp.Regexp
	constructorRe     *regexp.Regexp
	destructorRe      *regexp.Regexp
	qualifiedMethodRe *regexp.Regexp
	propertyRe        *regexp.Regexp

	// Видимость
	visibilityPublicRe    *regexp.Regexp
	visibilityPrivateRe   *regexp.Regexp
	visibilityProtectedRe *regexp.Regexp
	visibilityPublishedRe *regexp.Regexp

	// Переменные
	varSectionRe   *regexp.Regexp
	fieldDeclRe    *regexp.Regexp
	localVarDeclRe *regexp.Regexp
	constSectionRe *regexp.Regexp

	// SQL-вызовы
	apiExecRe        *regexp.Regexp
	execSQLRe        *regexp.Regexp
	openSQLRe        *regexp.Regexp
	prepareSQLRe     *regexp.Regexp
	sqlClearRe       *regexp.Regexp
	sqlAddOwnerRe    *regexp.Regexp
	sqlOpenCallRe    *regexp.Regexp
	sqlExecCallRe    *regexp.Regexp
	sqlPrepareCallRe *regexp.Regexp
	sqlPropertyRe    *regexp.Regexp
	sqlTextRe        *regexp.Regexp
	sqlQueryRe       *regexp.Regexp
	addSQLRe         *regexp.Regexp

	// SQL-вызовы Diasoft 5NT
	getQueryRe      *regexp.Regexp // tbSelect.GetQuery([...], [...])
	getQueryLinesRe *regexp.Regexp // GetQueryLines([...], hbText[...], [...])
	dfmQueryRefRe   *regexp.Regexp // hbText['name'], tbSelect

	// Строковые литералы (для извлечения SQL)
	stringLiteralRe *regexp.Regexp
	concatStringRe  *regexp.Regexp

	// Таблицы в SQL
	tableInSQLRe *regexp.Regexp

	// Комментарии
	commentLineRe       *regexp.Regexp
	blockCommentStartRe *regexp.Regexp
	blockCommentEndRe   *regexp.Regexp
	parenCommentStartRe *regexp.Regexp
	parenCommentEndRe   *regexp.Regexp
	sqlKeywordDetectRe  *regexp.Regexp

	// Директивы
	directiveRe *regexp.Regexp
	asmRe       *regexp.Regexp
	endRe       *regexp.Regexp
	beginRe     *regexp.Regexp
}

func normalizeMethodName(name string) string {
	parts := strings.Split(name, ".")
	return strings.TrimSpace(parts[len(parts)-1])
}

func resolveQualifiedOwnerFallback(rawName string, classes []*model.PASClass) (string, string, bool) {
	rawName = strings.TrimSpace(rawName)
	dotPos := strings.LastIndex(rawName, ".")
	if dotPos <= 0 || dotPos >= len(rawName)-1 {
		return "", "", false
	}

	rawOwner := strings.TrimSpace(rawName[:dotPos])
	methodName := strings.TrimSpace(rawName[dotPos+1:])
	if rawOwner == "" || methodName == "" {
		return "", "", false
	}

	type candidate struct {
		name string
		len  int
	}

	candidates := make([]candidate, 0, len(classes))
	seen := make(map[string]bool)
	for _, class := range classes {
		if class == nil {
			continue
		}
		name := strings.TrimSpace(class.ClassName)
		if name == "" || seen[strings.ToLower(name)] {
			continue
		}
		seen[strings.ToLower(name)] = true
		candidates = append(candidates, candidate{name: name, len: len(name)})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].len == candidates[j].len {
			return candidates[i].name < candidates[j].name
		}
		return candidates[i].len > candidates[j].len
	})

	rawOwnerLower := strings.ToLower(rawOwner)
	for _, candidate := range candidates {
		candidateLower := strings.ToLower(candidate.name)
		if strings.HasPrefix(rawOwnerLower, candidateLower) {
			rest := strings.TrimSpace(rawOwner[len(candidate.name):])
			if rest == "" || seen[strings.ToLower(rest)] {
				return candidate.name, methodName, true
			}
		}
	}

	return "", "", false
}

func normalizeTypeDeclarationLine(line string) string {
	line = regexp.MustCompile(`\{\$[^}]*\}`).ReplaceAllString(line, "")
	replacer := strings.NewReplacer(
		"objectrecord", "object",
		"recordobject", "record",
		"classrecord", "class",
		"recordclass", "record",
		"classobject", "class",
		"objectclass", "object",
	)
	return replacer.Replace(line)
}

func currentMethodName(method *model.PASMethod) string {
	if method == nil {
		return ""
	}
	return method.MethodName
}

func currentMethodLine(method *model.PASMethod) int {
	if method == nil {
		return 0
	}
	return method.LineNumber
}

// ParseResult результат парсинга PAS-файла
type ParseResult struct {
	Units        []*model.PASUnit
	Classes      []*model.PASClass
	Methods      []*model.PASMethod
	Fields       []*model.PASField
	SQLFragments []*model.SQLFragment
	Calls        []string
	Tables       []*model.SQLTable
	DFMQueries   []*model.DFMQuery // SQL из DFM-объектов
	Errors       []ParseError
}

// ParseError ошибка парсинга
type ParseError struct {
	Line     int
	Column   int
	Message  string
	Severity string // error, warning, info
}

// PASField поле класса
type PASField struct {
	ClassName  string // имя класса, которому принадлежит поле
	Name       string
	Type       string
	Visibility string
	LineNumber int
}

// SQLFragment SQL-фрагмент в PAS-коде
type SQLFragment struct {
	QueryText  string
	LineNumber int
	Context    string // method, property, global
	Method     string
}

// getClassSafe безопасно возвращает имя класса или пустую строку
func getClassSafe(class *model.PASClass) string {
	if class == nil {
		return ""
	}
	return class.ClassName
}

func getClassContext(currentClass *model.PASClass, currentMethod *model.PASMethod) string {
	if currentMethod != nil && strings.TrimSpace(currentMethod.ClassName) != "" {
		return currentMethod.ClassName
	}
	return getClassSafe(currentClass)
}

// getMethodSafe безопасно возвращает имя метода или пустую строку
func getMethodSafe(method *model.PASMethod) string {
	if method == nil {
		return ""
	}
	return method.MethodName
}

// NewParser создаёт новый PAS-парсер
func NewParser() *Parser {
	return &Parser{
		// Секции
		interfaceRe:      regexp.MustCompile(`(?i)^\s*interface\s*$`),
		implementationRe: regexp.MustCompile(`(?i)^\s*implementation\s*$`),
		initializationRe: regexp.MustCompile(`(?i)^\s*initialization\s*$`),
		finalizationRe:   regexp.MustCompile(`(?i)^\s*finalization\s*$`),

		// Uses-клаузулы
		interfaceUsesRe:      regexp.MustCompile(`(?i)^\s*interface\s+uses\s+(.+?)(?:;|$)`),
		implementationUsesRe: regexp.MustCompile(`(?i)^\s*implementation\s+uses\s+(.+?)(?:;|$)`),

		// Секция типов
		typeSectionRe: regexp.MustCompile(`(?i)^\s*type\s*$`),

		// Объявление owner type: class/object/record, в том числе с наследованием
		classDeclRe:   regexp.MustCompile(`(?i)^\s*(?:type\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:class|object|record)(?:\s*\(|\s*(?:\{[^}]*\})?\s*(?:;|$))`),
		classParentRe: regexp.MustCompile(`(?i)^\s*(?:type\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:class|object)\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\{[^}]*\})?\s*(?:,|\))`),

		// Методы: function/procedure/constructor/destructor
		methodDeclRe:      regexp.MustCompile(`(?i)^\s*(function|procedure)\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*(?:\(([^)]*)\))?\s*(?::\s*([A-Za-z_][A-Za-z0-9_]*))?\s*;`),
		functionDeclRe:    regexp.MustCompile(`(?i)^\s*function\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*(?:\(([^)]*)\))?\s*(?::\s*([A-Za-z_][A-Za-z0-9_]*))?\s*;`),
		procedureDeclRe:   regexp.MustCompile(`(?i)^\s*procedure\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*(?:\(([^)]*)\))?\s*;`),
		constructorRe:     regexp.MustCompile(`(?i)^\s*constructor\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*(?:\(([^)]*)\))?\s*;`),
		destructorRe:      regexp.MustCompile(`(?i)^\s*destructor\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*(?:\(([^)]*)\))?\s*;`),
		qualifiedMethodRe: regexp.MustCompile(`(?i)^\s*(function|procedure|constructor|destructor)\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)`),
		propertyRe:        regexp.MustCompile(`(?i)^\s*property\s+([A-Za-z_][A-Za-z0-9_]*)\s*:`),

		// Видимость
		visibilityPublicRe:    regexp.MustCompile(`(?i)^\s*public\s*$`),
		visibilityPrivateRe:   regexp.MustCompile(`(?i)^\s*private\s*$`),
		visibilityProtectedRe: regexp.MustCompile(`(?i)^\s*protected\s*$`),
		visibilityPublishedRe: regexp.MustCompile(`(?i)^\s*published\s*$`),

		// Переменные
		varSectionRe:   regexp.MustCompile(`(?i)^\s*var\s*$`),
		fieldDeclRe:    regexp.MustCompile(`(?i)^\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z_][A-Za-z0-9_<>(),:\s]*)\s*;?`),
		localVarDeclRe: regexp.MustCompile(`(?i)^\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z_][A-Za-z0-9_<>(),\s]*);`),
		constSectionRe: regexp.MustCompile(`(?i)^\s*const\s*$`),

		// SQL-вызовы
		apiExecRe:        regexp.MustCompile(`(?i)\bAPI_EXEC\s*\(\s*['"]([^'"]+)['"]`),
		execSQLRe:        regexp.MustCompile(`(?i)\bExecSQL\s*\(\s*['"]([^'"]+)['"]`),
		openSQLRe:        regexp.MustCompile(`(?i)\bOpen\s*\(\s*['"]([^'"]+)['"]`),
		prepareSQLRe:     regexp.MustCompile(`(?i)\bPrepare\s*\(\s*['"]([^'"]+)['"]`),
		sqlClearRe:       regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*SQL\s*\.\s*Clear\s*\(`),
		sqlAddOwnerRe:    regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*SQL\s*\.\s*Add\s*\(`),
		sqlOpenCallRe:    regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*Open\s*(?:\(|;)`),
		sqlExecCallRe:    regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*ExecSQL\s*(?:\(|;)`),
		sqlPrepareCallRe: regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*Prepare\s*(?:\(|;)`),
		sqlPropertyRe:    regexp.MustCompile(`(?i)\.SQL\s*:=\s*['"]([^'"]+)['"]`),
		sqlTextRe:        regexp.MustCompile(`(?i)\.SQL\.Text\s*:=\s*['"]([^'"]+)['"]`),
		sqlQueryRe:       regexp.MustCompile(`(?i)\.Query\s*:=\s*['"]([^'"]+)['"]`),
		addSQLRe:         regexp.MustCompile(`(?i)\.Add\s*\(\s*['"]([^'"]+)['"]`),

		// SQL-вызовы Diasoft 5NT
		// tbSelect.GetQuery([params], [args]) или GetQuery([params], [args])
		getQueryRe: regexp.MustCompile(`(?i)([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*GetQuery\s*\(`),
		// GetQueryLines([params], hbText['name'], [args])
		getQueryLinesRe: regexp.MustCompile(`(?i)\bGetQueryLines\s*\(`),
		// Ссылки на DFM-объекты: hbText['name'], tbSelect
		dfmQueryRefRe: regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\s*\[\s*['"]([^'"]+)['"]\s*\]`),

		// Строковые литералы
		stringLiteralRe: regexp.MustCompile(`'([^']*(?:''[^']*)*)'`),
		concatStringRe:  regexp.MustCompile(`'[^']*'\s*\+\s*'[^']*'`),

		// Таблицы в SQL
		tableInSQLRe: regexp.MustCompile(`(?i)\b(?:FROM|JOIN|INTO|UPDATE|DELETE\s+FROM|INSERT\s+INTO)\s+([A-Za-z_#][A-Za-z0-9_#]*)`),

		// Комментарии
		commentLineRe:       regexp.MustCompile(`^\s*//.*$`),
		blockCommentStartRe: regexp.MustCompile(`\{`),
		blockCommentEndRe:   regexp.MustCompile(`\}`),
		parenCommentStartRe: regexp.MustCompile(`\(\*`),
		parenCommentEndRe:   regexp.MustCompile(`\*\)`),
		sqlKeywordDetectRe:  regexp.MustCompile(`(?i)\b(SELECT\b.*\bFROM|INSERT\s+INTO|UPDATE\s+[A-Za-z_#][A-Za-z0-9_#]*\s+SET|DELETE\s+FROM|FROM\s+[A-Za-z_#][A-Za-z0-9_#]*|JOIN\s+[A-Za-z_#][A-Za-z0-9_#]*|WHERE\b|GROUP\s+BY|ORDER\s+BY|HAVING\b|UNION\b|CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE|EXEC(?:UTE)?\s+[A-Za-z_#][A-Za-z0-9_#]*)`),

		// Директивы
		directiveRe: regexp.MustCompile(`(?i)^\s*\{\$.*\}\s*$`),
		asmRe:       regexp.MustCompile(`(?i)^\s*asm\s*$`),
		endRe:       regexp.MustCompile(`(?i)^\s*end\s*;?\s*$`),
		beginRe:     regexp.MustCompile(`(?i)^\s*begin\s*$`),
	}
}

func stripInlinePasComments(line string) string {
	var result strings.Builder
	inString := false
	inBraceComment := false
	inParenComment := false

	for i := 0; i < len(line); i++ {
		if inString {
			result.WriteByte(line[i])
			if line[i] == '\'' {
				if i+1 < len(line) && line[i+1] == '\'' {
					result.WriteByte(line[i+1])
					i++
					continue
				}
				inString = false
			}
			continue
		}

		if inBraceComment {
			if line[i] == '}' {
				inBraceComment = false
			}
			continue
		}

		if inParenComment {
			if line[i] == '*' && i+1 < len(line) && line[i+1] == ')' {
				inParenComment = false
				i++
			}
			continue
		}

		if line[i] == '\'' {
			inString = true
			result.WriteByte(line[i])
			continue
		}

		if line[i] == '/' && i+1 < len(line) && line[i+1] == '/' {
			break
		}

		if line[i] == '{' {
			inBraceComment = true
			continue
		}

		if line[i] == '(' && i+1 < len(line) && line[i+1] == '*' {
			inParenComment = true
			i++
			continue
		}

		result.WriteByte(line[i])
	}

	return result.String()
}

// ParseFile парсит PAS-файл
func (p *Parser) ParseFile(path string) (*ParseResult, error) {
	content, err := encoding.ReadFile(path, encoding.WIN1251)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return p.ParseContent(content)
}

// ParseContent парсит содержимое PAS-файла
func (p *Parser) ParseContent(content string) (*ParseResult, error) {
	result := &ParseResult{
		Units:        make([]*model.PASUnit, 0),
		Classes:      make([]*model.PASClass, 0),
		Methods:      make([]*model.PASMethod, 0),
		Fields:       make([]*model.PASField, 0),
		SQLFragments: make([]*model.SQLFragment, 0),
		Calls:        make([]string, 0),
		Tables:       make([]*model.SQLTable, 0),
		Errors:       make([]ParseError, 0),
	}

	scanner := bufio.NewScanner(strings.NewReader(content))

	var (
		currentUnit         *model.PASUnit
		currentClass        *model.PASClass
		currentMethod       *model.PASMethod
		inInterface         bool
		inImplementation    bool
		inTypeSection       bool
		inVarSection        bool
		inUsesSection       bool
		usesTargetSection   string
		inMethodDecl        bool
		inClass             bool
		inMethod            bool
		inMethodBody        bool
		inAsm               bool
		inBraceCommentBlock bool
		inParenCommentBlock bool
		methodBlockDepth    int
		nestedDepth         int  // >0 — внутри вложенной процедуры (begin/end счётчик)
		inNestedProcDecl    bool // обнаружено объявление вложенной процедуры, ждём begin
		currentVisibility   string
		lineNum             int
		inConstSection      bool
		classLineStart      int
		methodLineStart     int
		usesBuffer          strings.Builder
		methodDeclBuffer    strings.Builder
		sqlBuilderName      string
		sqlBuilderParts     []string
		sqlBuilderLine      int
		constVars           map[string]string
		stringVars          map[string]string
		pendingAssignVar    string
		pendingAssignIsSQL  bool
		pendingAssignLine   int
		pendingAssignExpr   strings.Builder
		pendingConstName    string
		pendingConstExpr    strings.Builder
	)

	resolvePasStringExpr := func(expr string) string {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			return ""
		}

		parts := make([]string, 0)
		var token strings.Builder
		inString := false

		flushToken := func() {
			part := strings.TrimSpace(token.String())
			token.Reset()
			if part == "" {
				return
			}
			if len(part) >= 2 && part[0] == '\'' && part[len(part)-1] == '\'' {
				unquoted := part[1 : len(part)-1]
				unquoted = strings.ReplaceAll(unquoted, "''", "'")
				parts = append(parts, unquoted)
				return
			}
			if constVars != nil {
				if value, ok := constVars[strings.ToLower(part)]; ok {
					parts = append(parts, value)
					return
				}
			}
			if stringVars != nil {
				if value, ok := stringVars[strings.ToLower(part)]; ok {
					parts = append(parts, value)
				}
			}
		}

		for i := 0; i < len(expr); i++ {
			ch := expr[i]
			if ch == '\'' {
				token.WriteByte(ch)
				if i+1 < len(expr) && expr[i+1] == '\'' {
					token.WriteByte(expr[i+1])
					i++
					continue
				}
				inString = !inString
				continue
			}
			if inString {
				token.WriteByte(ch)
				continue
			}
			if ch == '+' {
				flushToken()
				continue
			}
			token.WriteByte(ch)
		}
		flushToken()

		return strings.TrimSpace(strings.Join(parts, ""))
	}

	parseSQLAddExpression := func(line string) (string, bool) {
		idx := strings.Index(strings.ToLower(line), ".add")
		if idx == -1 {
			return "", false
		}
		openIdx := strings.Index(line[idx:], "(")
		if openIdx == -1 {
			return "", false
		}
		openIdx += idx

		depth := 0
		inString := false
		var expr strings.Builder
		for i := openIdx + 1; i < len(line); i++ {
			ch := line[i]
			if ch == '\'' {
				expr.WriteByte(ch)
				if inString && i+1 < len(line) && line[i+1] == '\'' {
					expr.WriteByte(line[i+1])
					i++
					continue
				}
				inString = !inString
				continue
			}
			if inString {
				expr.WriteByte(ch)
				continue
			}
			switch ch {
			case '(':
				depth++
				expr.WriteByte(ch)
			case ')':
				if depth == 0 {
					resolved := resolvePasStringExpr(expr.String())
					return resolved, resolved != ""
				}
				depth--
				expr.WriteByte(ch)
			case ';':
				if depth == 0 {
					resolved := resolvePasStringExpr(expr.String())
					return resolved, resolved != ""
				}
				expr.WriteByte(ch)
			default:
				expr.WriteByte(ch)
			}
		}

		resolved := resolvePasStringExpr(expr.String())
		return resolved, resolved != ""
	}

	extractResolvedSQLAssignment := func(line string) (string, bool) {
		assignIdx := strings.Index(line, ":=")
		if assignIdx == -1 {
			return "", false
		}
		left := strings.ToLower(strings.TrimSpace(line[:assignIdx]))
		if !(strings.HasSuffix(left, ".sql") || strings.HasSuffix(left, ".sql.text") || strings.HasSuffix(left, ".query")) {
			return "", false
		}
		right := strings.TrimSpace(line[assignIdx+2:])
		right = strings.TrimSuffix(right, ";")
		resolved := resolvePasStringExpr(right)
		if resolved == "" || !isLikelySQL(resolved) {
			return "", false
		}
		return resolved, true
	}

	trackStringAssignment := func(line string) bool {
		if !inMethod && !inMethodBody {
			return false
		}
		assignIdx := strings.Index(line, ":=")
		if assignIdx == -1 {
			return false
		}
		left := strings.TrimSpace(line[:assignIdx])
		if left == "" || strings.Contains(left, ".") || strings.Contains(left, "[") {
			return false
		}
		for _, r := range left {
			if !(r == '_' || r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z') {
				return false
			}
		}
		right := strings.TrimSpace(line[assignIdx+2:])
		right = strings.TrimSuffix(right, ";")
		resolved := resolvePasStringExpr(right)
		if stringVars == nil {
			stringVars = make(map[string]string)
		}
		key := strings.ToLower(left)
		if resolved == "" {
			delete(stringVars, key)
			return false
		}
		stringVars[key] = resolved
		return true
	}

	findAssignmentTarget := func(line string) string {
		assignIdx := strings.Index(line, ":=")
		if assignIdx == -1 {
			return ""
		}
		left := strings.TrimSpace(line[:assignIdx])
		if left == "" || strings.Contains(left, "[") {
			return ""
		}
		if strings.Contains(left, ".") {
			lower := strings.ToLower(left)
			if strings.HasSuffix(lower, ".sql") || strings.HasSuffix(lower, ".sql.text") || strings.HasSuffix(lower, ".query") {
				return left
			}
			return ""
		}
		for _, r := range left {
			if !(r == '_' || r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z') {
				return ""
			}
		}
		return left
	}

	hasExpressionTerminator := func(expr string) bool {
		inString := false
		depth := 0
		for i := 0; i < len(expr); i++ {
			ch := expr[i]
			if ch == '\'' {
				if inString && i+1 < len(expr) && expr[i+1] == '\'' {
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
			case ';':
				if depth == 0 {
					return true
				}
			}
		}
		return false
	}

	finalizePendingAssignment := func() {
		if pendingAssignVar == "" {
			return
		}
		expr := strings.TrimSpace(pendingAssignExpr.String())
		expr = strings.TrimSuffix(expr, ";")
		resolved := resolvePasStringExpr(expr)
		if stringVars == nil {
			stringVars = make(map[string]string)
		}
		key := strings.ToLower(pendingAssignVar)
		if pendingAssignIsSQL {
			if resolved != "" && isLikelySQL(resolved) {
				result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
					ClassName:  getClassContext(currentClass, currentMethod),
					MethodName: getMethodSafe(currentMethod),
					QueryText:  resolved,
					LineNumber: pendingAssignLine,
					Context:    "method",
				})
				p.extractTablesFromSQL(resolved, pendingAssignLine, result)
			}
		} else if resolved == "" {
			delete(stringVars, key)
		} else {
			stringVars[key] = resolved
			if isLikelySQL(resolved) {
				result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
					ClassName:  getClassContext(currentClass, currentMethod),
					MethodName: getMethodSafe(currentMethod),
					QueryText:  resolved,
					LineNumber: pendingAssignLine,
					Context:    "method",
				})
				p.extractTablesFromSQL(resolved, pendingAssignLine, result)
			}
		}
		pendingAssignVar = ""
		pendingAssignIsSQL = false
		pendingAssignLine = 0
		pendingAssignExpr.Reset()
	}

	beginPendingAssignment := func(line string) bool {
		if !inMethod && !inMethodBody {
			return false
		}
		target := findAssignmentTarget(line)
		if target == "" {
			return false
		}
		assignIdx := strings.Index(line, ":=")
		right := strings.TrimSpace(line[assignIdx+2:])
		pendingAssignVar = target
		lowerTarget := strings.ToLower(target)
		pendingAssignIsSQL = strings.HasSuffix(lowerTarget, ".sql") || strings.HasSuffix(lowerTarget, ".sql.text") || strings.HasSuffix(lowerTarget, ".query")
		pendingAssignLine = lineNum
		pendingAssignExpr.Reset()
		if right != "" {
			pendingAssignExpr.WriteString(right)
		}
		if right != "" && hasExpressionTerminator(right) {
			finalizePendingAssignment()
		}
		return true
	}

	appendPendingAssignmentLine := func(line string) bool {
		if pendingAssignVar == "" {
			return false
		}
		if pendingAssignExpr.Len() > 0 {
			pendingAssignExpr.WriteByte(' ')
		}
		pendingAssignExpr.WriteString(strings.TrimSpace(line))
		if hasExpressionTerminator(line) {
			finalizePendingAssignment()
		}
		return true
	}

	finalizePendingConst := func() {
		if pendingConstName == "" {
			return
		}
		if constVars == nil {
			constVars = make(map[string]string)
		}
		expr := strings.TrimSpace(pendingConstExpr.String())
		expr = strings.TrimSuffix(expr, ";")
		resolved := resolvePasStringExpr(expr)
		key := strings.ToLower(strings.TrimSpace(pendingConstName))
		if resolved == "" {
			delete(constVars, key)
		} else {
			constVars[key] = resolved
		}
		pendingConstName = ""
		pendingConstExpr.Reset()
	}

	beginPendingConst := func(line string) bool {
		if !inConstSection {
			return false
		}
		matches := regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)(?:\s*:\s*[^=]+)?\s*=\s*(.*)$`).FindStringSubmatch(line)
		if matches == nil {
			return false
		}
		pendingConstName = matches[1]
		pendingConstExpr.Reset()
		pendingConstExpr.WriteString(strings.TrimSpace(matches[2]))
		if strings.TrimSpace(matches[2]) != "" && hasExpressionTerminator(matches[2]) {
			finalizePendingConst()
		}
		return true
	}

	appendPendingConstLine := func(line string) bool {
		if pendingConstName == "" {
			return false
		}
		if pendingConstExpr.Len() > 0 {
			pendingConstExpr.WriteByte(' ')
		}
		pendingConstExpr.WriteString(strings.TrimSpace(line))
		if hasExpressionTerminator(line) {
			finalizePendingConst()
		}
		return true
	}

	flushSQLBuilder := func() {
		if len(sqlBuilderParts) == 0 {
			sqlBuilderName = ""
			sqlBuilderLine = 0
			return
		}
		assembled := strings.TrimSpace(strings.Join(sqlBuilderParts, "\n"))
		if assembled != "" && isLikelySQL(assembled) {
			result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
				ClassName:  getClassContext(currentClass, currentMethod),
				MethodName: getMethodSafe(currentMethod),
				QueryText:  assembled,
				LineNumber: sqlBuilderLine,
				Context:    "method",
			})
			p.extractTablesFromSQL(assembled, sqlBuilderLine, result)
		}
		sqlBuilderName = ""
		sqlBuilderParts = nil
		sqlBuilderLine = 0
	}

	flushSQLBuilderForOwner := func(owner string) {
		if owner == "" {
			return
		}
		if sqlBuilderName == strings.ToLower(owner) {
			flushSQLBuilder()
		}
	}

	isMethodDeclLine := func(text string) bool {
		lower := strings.ToLower(strings.TrimSpace(text))
		return strings.HasPrefix(lower, "function ") || strings.HasPrefix(lower, "procedure ") || strings.HasPrefix(lower, "constructor ") || strings.HasPrefix(lower, "destructor ")
	}

	// Helper для сброса состояния метода
	flushMethod := func() {
		flushSQLBuilder()
		if currentMethod != nil {
			result.Methods = append(result.Methods, currentMethod)
		}
		currentMethod = nil
		inMethod = false
		inMethodBody = false
		methodBlockDepth = 0
		nestedDepth = 0
		inNestedProcDecl = false
		stringVars = nil
		pendingAssignVar = ""
		pendingAssignIsSQL = false
		pendingAssignLine = 0
		pendingAssignExpr.Reset()
	}

	// Helper для сброса состояния класса
	flushClass := func() {
		if currentClass != nil {
			if currentClass.LineEnd == 0 {
				currentClass.LineEnd = lineNum
			}
			result.Classes = append(result.Classes, currentClass)
		}
		currentClass = nil
		inClass = false
	}

	startMethodFromDecl := func(decl string) bool {
		decl = strings.TrimSpace(decl)
		if decl == "" {
			return false
		}

		declarationOnly := inInterface || inClass
		// Forward declarations в interface (не внутри класса) — пропускаем,
		// они будут добавлены при парсинге implementation
		isForwardDecl := inInterface && !inClass

		if matches := p.functionDeclRe.FindStringSubmatch(decl); matches != nil {
			ownerMatches := p.qualifiedMethodRe.FindStringSubmatch(decl)
			if inClass && ownerMatches == nil {
				methodName := normalizeMethodName(matches[1])
				params := matches[2]
				returnType := matches[3]
				className := getClassSafe(currentClass)

				// Для implementation секции устанавливаем currentMethod для контекста переменных
				if inImplementation && !declarationOnly {
					flushMethod()
					currentMethod = &model.PASMethod{
						ClassName:  className,
						MethodName: methodName,
						LineNumber: methodLineStart,
						Visibility: currentVisibility,
					}
					currentMethod.Signature = buildSignature(methodName, params, returnType, "function")
					inMethod = true
					if strings.Contains(strings.ToLower(decl), "begin") {
						inMethodBody = true
						methodBlockDepth = 1
					}
				} else {
					// Для interface секции или declaration only - просто добавляем объявление
					declaredMethod := &model.PASMethod{
						ClassName:  className,
						MethodName: methodName,
						LineNumber: methodLineStart,
						Visibility: currentVisibility,
					}
					declaredMethod.Signature = buildSignature(methodName, params, returnType, "function")
					result.Methods = append(result.Methods, declaredMethod)
				}
				return true
			}
			methodName := normalizeMethodName(matches[1])
			params := matches[2]
			returnType := matches[3]
			className := getClassSafe(currentClass)
			if ownerMatches != nil {
				className = ownerMatches[2]
				methodName = ownerMatches[3]
			} else if fallbackClassName, fallbackMethodName, ok := resolveQualifiedOwnerFallback(matches[1], result.Classes); ok {
				className = fallbackClassName
				methodName = fallbackMethodName
			}
			if declarationOnly && !isForwardDecl {
				declaredMethod := &model.PASMethod{
					ClassName:  className,
					MethodName: methodName,
					LineNumber: methodLineStart,
					Visibility: currentVisibility,
				}
				declaredMethod.Signature = buildSignature(methodName, params, returnType, "function")
				result.Methods = append(result.Methods, declaredMethod)
				return true
			}
			if isForwardDecl {
				return true
			}
			flushMethod()
			currentMethod = &model.PASMethod{
				ClassName:  className,
				MethodName: methodName,
				LineNumber: methodLineStart,
				Visibility: currentVisibility,
			}
			currentMethod.Signature = buildSignature(methodName, params, returnType, "function")
			inMethod = true
			if strings.Contains(strings.ToLower(decl), "begin") {
				inMethodBody = true
				methodBlockDepth = 1
			}
			return true
		}

		if matches := p.procedureDeclRe.FindStringSubmatch(decl); matches != nil {
			ownerMatches := p.qualifiedMethodRe.FindStringSubmatch(decl)
			if inClass && ownerMatches == nil {
				methodName := normalizeMethodName(matches[1])
				params := matches[2]
				className := getClassSafe(currentClass)

				// Для implementation секции устанавливаем currentMethod для контекста переменных
				if inImplementation && !declarationOnly {
					flushMethod()
					currentMethod = &model.PASMethod{
						ClassName:  className,
						MethodName: methodName,
						LineNumber: methodLineStart,
						Visibility: currentVisibility,
					}
					currentMethod.Signature = buildSignature(methodName, params, "", "procedure")
					inMethod = true
					if strings.Contains(strings.ToLower(decl), "begin") {
						inMethodBody = true
						methodBlockDepth = 1
					}
				} else {
					// Для interface секции или declaration only - просто добавляем объявление
					declaredMethod := &model.PASMethod{
						ClassName:  className,
						MethodName: methodName,
						LineNumber: methodLineStart,
						Visibility: currentVisibility,
					}
					declaredMethod.Signature = buildSignature(methodName, params, "", "procedure")
					result.Methods = append(result.Methods, declaredMethod)
				}
				return true
			}
			methodName := normalizeMethodName(matches[1])
			params := matches[2]
			className := getClassSafe(currentClass)
			if ownerMatches != nil {
				className = ownerMatches[2]
				methodName = ownerMatches[3]
			} else if fallbackClassName, fallbackMethodName, ok := resolveQualifiedOwnerFallback(matches[1], result.Classes); ok {
				className = fallbackClassName
				methodName = fallbackMethodName
			}
			if declarationOnly && !isForwardDecl {
				declaredMethod := &model.PASMethod{
					ClassName:  className,
					MethodName: methodName,
					LineNumber: methodLineStart,
					Visibility: currentVisibility,
				}
				declaredMethod.Signature = buildSignature(methodName, params, "", "procedure")
				result.Methods = append(result.Methods, declaredMethod)
				return true
			}
			if isForwardDecl {
				return true
			}
			flushMethod()
			currentMethod = &model.PASMethod{
				ClassName:  className,
				MethodName: methodName,
				LineNumber: methodLineStart,
				Visibility: currentVisibility,
			}
			currentMethod.Signature = buildSignature(methodName, params, "", "procedure")
			inMethod = true
			if strings.Contains(strings.ToLower(decl), "begin") {
				inMethodBody = true
				methodBlockDepth = 1
			}
			return true
		}

		if matches := p.constructorRe.FindStringSubmatch(decl); matches != nil {
			ownerMatches := p.qualifiedMethodRe.FindStringSubmatch(decl)
			if inClass && ownerMatches == nil {
				methodName := normalizeMethodName(matches[1])
				className := getClassSafe(currentClass)

				// Для implementation секции устанавливаем currentMethod для контекста переменных
				if inImplementation && !declarationOnly {
					flushMethod()
					currentMethod = &model.PASMethod{
						ClassName:  className,
						MethodName: methodName,
						LineNumber: methodLineStart,
						Visibility: currentVisibility,
					}
					currentMethod.Signature = buildSignature(methodName, "", "", "constructor")
					inMethod = true
					if strings.Contains(strings.ToLower(decl), "begin") {
						inMethodBody = true
						methodBlockDepth = 1
					}
				} else {
					// Для interface секции или declaration only - просто добавляем объявление
					declaredMethod := &model.PASMethod{
						ClassName:  className,
						MethodName: methodName,
						LineNumber: methodLineStart,
						Visibility: currentVisibility,
					}
					declaredMethod.Signature = buildSignature(methodName, "", "", "constructor")
					result.Methods = append(result.Methods, declaredMethod)
				}
				return true
			}
			methodName := normalizeMethodName(matches[1])
			className := getClassSafe(currentClass)
			if ownerMatches != nil {
				className = ownerMatches[2]
				methodName = ownerMatches[3]
			} else if fallbackClassName, fallbackMethodName, ok := resolveQualifiedOwnerFallback(matches[1], result.Classes); ok {
				className = fallbackClassName
				methodName = fallbackMethodName
			}
			if declarationOnly && !isForwardDecl {
				declaredMethod := &model.PASMethod{
					ClassName:  className,
					MethodName: methodName,
					LineNumber: methodLineStart,
					Visibility: currentVisibility,
				}
				declaredMethod.Signature = buildSignature(methodName, "", "", "constructor")
				result.Methods = append(result.Methods, declaredMethod)
				return true
			}
			if isForwardDecl {
				return true
			}
			flushMethod()
			currentMethod = &model.PASMethod{
				ClassName:  className,
				MethodName: methodName,
				LineNumber: methodLineStart,
				Visibility: currentVisibility,
			}
			currentMethod.Signature = buildSignature(methodName, "", "", "constructor")
			inMethod = true
			if strings.Contains(strings.ToLower(decl), "begin") {
				inMethodBody = true
				methodBlockDepth = 1
			}
			return true
		}

		if matches := p.destructorRe.FindStringSubmatch(decl); matches != nil {
			ownerMatches := p.qualifiedMethodRe.FindStringSubmatch(decl)
			if inClass && ownerMatches == nil {
				methodName := normalizeMethodName(matches[1])
				className := getClassSafe(currentClass)

				// Для implementation секции устанавливаем currentMethod для контекста переменных
				if inImplementation && !declarationOnly {
					flushMethod()
					currentMethod = &model.PASMethod{
						ClassName:  className,
						MethodName: methodName,
						LineNumber: methodLineStart,
						Visibility: currentVisibility,
					}
					currentMethod.Signature = buildSignature(methodName, "", "", "destructor")
					inMethod = true
					if strings.Contains(strings.ToLower(decl), "begin") {
						inMethodBody = true
						methodBlockDepth = 1
					}
				} else {
					// Для interface секции или declaration only - просто добавляем объявление
					declaredMethod := &model.PASMethod{
						ClassName:  className,
						MethodName: methodName,
						LineNumber: methodLineStart,
						Visibility: currentVisibility,
					}
					declaredMethod.Signature = buildSignature(methodName, "", "", "destructor")
					result.Methods = append(result.Methods, declaredMethod)
				}
				return true
			}
			methodName := normalizeMethodName(matches[1])
			className := getClassSafe(currentClass)
			if ownerMatches != nil {
				className = ownerMatches[2]
				methodName = ownerMatches[3]
			} else if fallbackClassName, fallbackMethodName, ok := resolveQualifiedOwnerFallback(matches[1], result.Classes); ok {
				className = fallbackClassName
				methodName = fallbackMethodName
			}
			if declarationOnly && !isForwardDecl {
				declaredMethod := &model.PASMethod{
					ClassName:  className,
					MethodName: methodName,
					LineNumber: methodLineStart,
					Visibility: currentVisibility,
				}
				declaredMethod.Signature = buildSignature(methodName, "", "", "destructor")
				result.Methods = append(result.Methods, declaredMethod)
				return true
			}
			if isForwardDecl {
				return true
			}
			flushMethod()
			currentMethod = &model.PASMethod{
				ClassName:  className,
				MethodName: methodName,
				LineNumber: methodLineStart,
				Visibility: currentVisibility,
			}
			currentMethod.Signature = buildSignature(methodName, "", "", "destructor")
			inMethod = true
			if strings.Contains(strings.ToLower(decl), "begin") {
				inMethodBody = true
				methodBlockDepth = 1
			}
			return true
		}

		return false
	}

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		rawTrimmed := strings.TrimSpace(line)

		if inBraceCommentBlock {
			if p.blockCommentEndRe.MatchString(line) {
				inBraceCommentBlock = false
			}
			continue
		}
		if inParenCommentBlock {
			if p.parenCommentEndRe.MatchString(line) {
				inParenCommentBlock = false
			}
			continue
		}

		if p.commentLineRe.MatchString(rawTrimmed) {
			continue
		}
		if p.directiveRe.MatchString(rawTrimmed) {
			continue
		}
		if p.blockCommentStartRe.MatchString(line) && !p.blockCommentEndRe.MatchString(line) {
			inBraceCommentBlock = true
			continue
		}
		if p.parenCommentStartRe.MatchString(line) && !p.parenCommentEndRe.MatchString(line) {
			inParenCommentBlock = true
			continue
		}

		structuralLine := stripInlinePasComments(line)
		trimmed := strings.TrimSpace(structuralLine)
		lineHandledBySQLBuilder := false

		// Пропускаем пустые строки
		if trimmed == "" {
			continue
		}

		if pendingConstName != "" {
			appendPendingConstLine(structuralLine)
			continue
		}

		if pendingAssignVar != "" {
			appendPendingAssignmentLine(line)
			continue
		}

		if beginPendingAssignment(line) {
			if pendingAssignVar != "" {
				continue
			}
		}

		trackStringAssignment(line)

		if matches := p.sqlClearRe.FindStringSubmatch(line); matches != nil {
			builderName := strings.ToLower(matches[1])
			if sqlBuilderName == builderName {
				flushSQLBuilder()
			} else {
				sqlBuilderName = builderName
				sqlBuilderParts = nil
				sqlBuilderLine = 0
			}
		}
		if matches := p.sqlOpenCallRe.FindStringSubmatch(line); matches != nil {
			flushSQLBuilderForOwner(matches[1])
		}
		if matches := p.sqlExecCallRe.FindStringSubmatch(line); matches != nil {
			flushSQLBuilderForOwner(matches[1])
		}
		if matches := p.sqlPrepareCallRe.FindStringSubmatch(line); matches != nil {
			flushSQLBuilderForOwner(matches[1])
		}

		if inMethodDecl {
			if methodDeclBuffer.Len() > 0 {
				methodDeclBuffer.WriteString(" ")
			}
			methodDeclBuffer.WriteString(trimmed)
			if strings.Contains(trimmed, ";") {
				startMethodFromDecl(methodDeclBuffer.String())
				inMethodDecl = false
				methodDeclBuffer.Reset()
			}
			continue
		}

		// ========================================
		// Секции файла
		// ========================================

		// Interface секция
		if p.interfaceRe.MatchString(trimmed) {
			inInterface = true
			inImplementation = false
			inTypeSection = false
			inVarSection = false
			inConstSection = false
			usesTargetSection = ""
			usesBuffer.Reset()
			continue
		}

		// Implementation секция
		if p.implementationRe.MatchString(trimmed) {
			inInterface = false
			inImplementation = true
			inTypeSection = false
			inVarSection = false
			inConstSection = false
			usesTargetSection = ""
			usesBuffer.Reset()
			// Завершаем текущий класс если мы были в interface
			if inClass {
				flushClass()
			}
			continue
		}

		// Initialization секция
		if p.initializationRe.MatchString(trimmed) {
			inImplementation = false
			inUsesSection = false
			inConstSection = false
			usesTargetSection = ""
			usesBuffer.Reset()
			if inClass {
				flushClass()
			}
			continue
		}

		// Finalization секция
		if p.finalizationRe.MatchString(trimmed) {
			continue
		}

		// ========================================
		// Uses-клаузулы
		// ========================================

		if inInterface && !inTypeSection {
			if matches := p.interfaceUsesRe.FindStringSubmatch(line); matches != nil {
				if currentUnit == nil {
					currentUnit = &model.PASUnit{
						LineStart: lineNum,
					}
				}
				currentUnit.InterfaceUses = parseUsesList(matches[1])
				continue
			}
			if strings.EqualFold(trimmed, "uses") || strings.HasPrefix(strings.ToLower(trimmed), "uses ") {
				if currentUnit == nil {
					currentUnit = &model.PASUnit{
						LineStart: lineNum,
					}
				}
				inUsesSection = true
				usesTargetSection = "interface"
				usesBuffer.Reset()
				rest := strings.TrimSpace(trimmed[4:])
				if rest != "" {
					usesBuffer.WriteString(rest)
				}
				if strings.Contains(trimmed, ";") {
					currentUnit.InterfaceUses = parseUsesList(usesBuffer.String())
					inUsesSection = false
					usesTargetSection = ""
					usesBuffer.Reset()
				}
				continue
			}
		}

		if inImplementation && !inTypeSection && !inVarSection {
			if matches := p.implementationUsesRe.FindStringSubmatch(line); matches != nil {
				if currentUnit == nil {
					currentUnit = &model.PASUnit{
						LineStart: lineNum,
					}
				}
				currentUnit.ImplementationUses = parseUsesList(matches[1])
				continue
			}
			if strings.EqualFold(trimmed, "uses") || strings.HasPrefix(strings.ToLower(trimmed), "uses ") {
				if currentUnit == nil {
					currentUnit = &model.PASUnit{
						LineStart: lineNum,
					}
				}
				inUsesSection = true
				usesTargetSection = "implementation"
				usesBuffer.Reset()
				rest := strings.TrimSpace(trimmed[4:])
				if rest != "" {
					usesBuffer.WriteString(rest)
				}
				if strings.Contains(trimmed, ";") {
					currentUnit.ImplementationUses = parseUsesList(usesBuffer.String())
					inUsesSection = false
					usesTargetSection = ""
					usesBuffer.Reset()
				}
				continue
			}
		}

		if inUsesSection {
			if usesBuffer.Len() > 0 {
				usesBuffer.WriteString(" ")
			}
			usesBuffer.WriteString(trimmed)
			if strings.Contains(trimmed, ";") {
				if currentUnit == nil {
					currentUnit = &model.PASUnit{
						LineStart: lineNum,
					}
				}
				if usesTargetSection == "interface" {
					currentUnit.InterfaceUses = parseUsesList(usesBuffer.String())
				} else if usesTargetSection == "implementation" {
					currentUnit.ImplementationUses = parseUsesList(usesBuffer.String())
				}
				inUsesSection = false
				usesTargetSection = ""
				usesBuffer.Reset()
			}
			continue
		}

		// ========================================
		// Unit name (первая строка после interface)
		// ========================================

		if strings.HasPrefix(strings.ToLower(trimmed), "unit ") {
			parts := strings.Fields(trimmed)
			if len(parts) >= 2 {
				unitName := strings.TrimSuffix(parts[1], ";")
				currentUnit = &model.PASUnit{
					UnitName:  unitName,
					LineStart: lineNum,
				}
			}
			continue
		}

		// ========================================
		// Секции type/var/const
		// ========================================

		if p.typeSectionRe.MatchString(trimmed) {
			inTypeSection = true
			inVarSection = false
			inConstSection = false
			continue
		}

		if p.varSectionRe.MatchString(trimmed) {
			// Если в секции типа - это поля класса
			if inClass {
				// В классе var означает начало секции полей, продолжаем сборку
				inTypeSection = false
			} else {
				inVarSection = true
				inTypeSection = false
				inConstSection = false
			}
			continue
		}

		if p.constSectionRe.MatchString(trimmed) {
			inTypeSection = false
			inVarSection = false
			inConstSection = true
			continue
		}

		if inConstSection {
			if beginPendingConst(structuralLine) {
				if pendingConstName != "" {
					continue
				}
			}
			if isMethodDeclLine(trimmed) || p.typeSectionRe.MatchString(trimmed) || p.varSectionRe.MatchString(trimmed) || p.interfaceRe.MatchString(trimmed) || p.implementationRe.MatchString(trimmed) || p.initializationRe.MatchString(trimmed) || p.finalizationRe.MatchString(trimmed) {
				inConstSection = false
			} else {
				continue
			}
		}

		// ========================================
		// Объявление класса
		// ========================================

		if !inClass {
			normalizedTypeLine := normalizeTypeDeclarationLine(line)
			normalizedTypeTrimmed := strings.TrimSpace(normalizedTypeLine)
			// type TClassName = class - классы могут быть в разных секциях
			if matches := p.classDeclRe.FindStringSubmatch(normalizedTypeLine); matches != nil {
				flushClass() // Завершаем предыдущий класс
				className := matches[1]
				classLineStart = lineNum
				currentClass = &model.PASClass{
					ClassName: className,
					LineStart: classLineStart,
				}
				// Проверяем родителя
				if parentMatches := p.classParentRe.FindStringSubmatch(normalizedTypeLine); parentMatches != nil {
					currentClass.ParentClass = parentMatches[2]
				}
				if strings.HasSuffix(normalizedTypeTrimmed, ";") {
					currentClass.LineEnd = lineNum
					flushClass()
					continue
				}
				inClass = true
				currentVisibility = "public" // По умолчанию в Delphi
				continue
			}
		}

		// ========================================
		// Видимость в классе
		// ========================================

		if inClass {
			if p.visibilityPublicRe.MatchString(trimmed) {
				currentVisibility = "public"
				continue
			}
			if p.visibilityPrivateRe.MatchString(trimmed) {
				currentVisibility = "private"
				continue
			}
			if p.visibilityProtectedRe.MatchString(trimmed) {
				currentVisibility = "protected"
				continue
			}
			if p.visibilityPublishedRe.MatchString(trimmed) {
				currentVisibility = "published"
				continue
			}

			// Конец класса - более надежное определение
			if p.endRe.MatchString(trimmed) && inClass {
				// Проверяем, что это не конец метода или другой конструкции
				if !inMethod && !inMethodBody {
					flushClass()
				}
				continue
			}

			// ========================================
			// Поля класса
			// ========================================

			if !inMethod && !inMethodBody && inClass {
				if matches := p.fieldDeclRe.FindStringSubmatch(line); matches != nil {
					fieldName := matches[1]
					fieldType := strings.TrimSpace(matches[2])
					result.Fields = append(result.Fields, &model.PASField{
						ClassName:  getClassSafe(currentClass),
						FieldName:  fieldName,
						FieldType:  fieldType,
						Visibility: currentVisibility,
						LineNumber: lineNum,
					})

					// Особая обработка полей типа DsQuery - это ссылки на SQL-запросы в DFM
					if strings.Contains(strings.ToLower(fieldType), "dsquery") {
						result.DFMQueries = append(result.DFMQueries, &model.DFMQuery{
							ComponentName: fieldName,
							ComponentType: "DsQuery",
							LineNumber:    lineNum,
							MethodName:    currentMethodName(currentMethod),
							MethodLine:    currentMethodLine(currentMethod),
						})
					}
					continue
				}
			}
		}

		// ========================================
		// Методы и функции
		// ========================================

		if !inMethod && !inMethodBody {
			if isMethodDeclLine(trimmed) {
				hasOpenParen := strings.Contains(trimmed, "(")
				hasCloseParen := strings.Contains(trimmed, ")")
				// Многострочное объявление: есть '(' но нет ')' (параметры не закрыты),
				// либо строка не заканчивается на ';'
				isMultiline := (hasOpenParen && !hasCloseParen) || !strings.Contains(trimmed, ";")
				if isMultiline {
					methodLineStart = lineNum
					inVarSection = false
					inMethodDecl = true
					methodDeclBuffer.Reset()
					methodDeclBuffer.WriteString(trimmed)
					continue
				}
			}

			// Function declaration
			if matches := p.functionDeclRe.FindStringSubmatch(trimmed); matches != nil {
				methodLineStart = lineNum
				inVarSection = false
				startMethodFromDecl(trimmed)
				continue
			}

			// Procedure declaration
			if matches := p.procedureDeclRe.FindStringSubmatch(trimmed); matches != nil {
				methodLineStart = lineNum
				inVarSection = false
				startMethodFromDecl(trimmed)
				continue
			}

			// Constructor
			if matches := p.constructorRe.FindStringSubmatch(trimmed); matches != nil {
				methodLineStart = lineNum
				inVarSection = false
				startMethodFromDecl(trimmed)
				continue
			}

			// Destructor
			if matches := p.destructorRe.FindStringSubmatch(trimmed); matches != nil {
				methodLineStart = lineNum
				inVarSection = false
				startMethodFromDecl(trimmed)
				continue
			}

			// Property
			if matches := p.propertyRe.FindStringSubmatch(trimmed); matches != nil {
				// Properties могут иметь SQL в getter/setter
				p.extractSQLFromLine(line, lineNum, result)
				continue
			}
		}

		// ========================================
		// Тело метода
		// ========================================

		if inMethod || inMethodBody {
			// Внутри вложенной процедуры — считаем begin/end
			if nestedDepth > 0 {
				if p.beginRe.MatchString(trimmed) {
					nestedDepth++
					continue
				}
				if p.endRe.MatchString(trimmed) {
					nestedDepth--
					continue
				}
				continue
			}

			// Обнаружение объявления вложенной процедуры/функции (до begin)
			if inNestedProcDecl {
				// Пропускаем всё до begin вложенной процедуры
				if p.beginRe.MatchString(trimmed) {
					inNestedProcDecl = false
					nestedDepth = 1
					continue
				}
				continue
			}

			// Обнаружение вложенной процедуры/функции
			if isMethodDeclLine(trimmed) && nestedDepth == 0 && !inNestedProcDecl {
				inNestedProcDecl = true
				continue
			}

			// Начало тела метода
			if p.beginRe.MatchString(trimmed) {
				inMethodBody = true
				methodBlockDepth++
			}

			// Конец метода
			if p.endRe.MatchString(trimmed) && inMethodBody {
				if methodBlockDepth > 0 {
					methodBlockDepth--
				}
				if methodBlockDepth == 0 {
					flushMethod()
					continue
				}
			}

			// Асм-блок (пропускаем)
			if p.asmRe.MatchString(trimmed) {
				inAsm = true
				continue
			}
			if inAsm && p.endRe.MatchString(trimmed) {
				inAsm = false
				continue
			}
			if inAsm {
				continue
			}

			// Локальные переменные и продолжения объявлений (параметры на нескольких строках)
			if strings.Contains(trimmed, "):") || strings.HasSuffix(trimmed, ")") || strings.HasSuffix(trimmed, ");") {
				continue
			}
			if p.localVarDeclRe.FindStringSubmatch(line) != nil {
				continue
			}

			// ========================================
			// SQL-вызовы и извлечение SQL
			// ========================================

			// API_EXEC('procedure_name')
			if matches := p.apiExecRe.FindAllStringSubmatch(line, -1); matches != nil {
				for _, m := range matches {
					result.Calls = append(result.Calls, m[1])
				}
			}

			// ExecSQL('...')
			if matches := p.execSQLRe.FindAllStringSubmatch(line, -1); matches != nil {
				for _, m := range matches {
					sqlText := m[1]
					result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
						ClassName:  getClassContext(currentClass, currentMethod),
						MethodName: getMethodSafe(currentMethod),
						QueryText:  sqlText,
						LineNumber: lineNum,
						Context:    "method",
					})
					// Извлекаем таблицы из SQL
					p.extractTablesFromSQL(sqlText, lineNum, result)
				}
			}

			// Open('...')
			if matches := p.openSQLRe.FindAllStringSubmatch(line, -1); matches != nil {
				for _, m := range matches {
					sqlText := m[1]
					result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
						ClassName:  getClassContext(currentClass, currentMethod),
						MethodName: getMethodSafe(currentMethod),
						QueryText:  sqlText,
						LineNumber: lineNum,
						Context:    "method",
					})
					p.extractTablesFromSQL(sqlText, lineNum, result)
				}
			}

			// .SQL := '...'
			if matches := p.sqlPropertyRe.FindAllStringSubmatch(line, -1); matches != nil {
				flushSQLBuilder()
				for _, m := range matches {
					sqlText := m[1]
					result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
						ClassName:  getClassContext(currentClass, currentMethod),
						MethodName: getMethodSafe(currentMethod),
						QueryText:  sqlText,
						LineNumber: lineNum,
						Context:    "method",
					})
					p.extractTablesFromSQL(sqlText, lineNum, result)
				}
			}
			if sqlText, ok := extractResolvedSQLAssignment(line); ok {
				flushSQLBuilder()
				result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
					ClassName:  getClassContext(currentClass, currentMethod),
					MethodName: getMethodSafe(currentMethod),
					QueryText:  sqlText,
					LineNumber: lineNum,
					Context:    "method",
				})
				p.extractTablesFromSQL(sqlText, lineNum, result)
			}

			// .SQL.Text := '...'
			if matches := p.sqlTextRe.FindAllStringSubmatch(line, -1); matches != nil {
				flushSQLBuilder()
				for _, m := range matches {
					sqlText := m[1]
					result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
						ClassName:  getClassContext(currentClass, currentMethod),
						MethodName: getMethodSafe(currentMethod),
						QueryText:  sqlText,
						LineNumber: lineNum,
						Context:    "method",
					})
					p.extractTablesFromSQL(sqlText, lineNum, result)
				}
			}

			// .Query := '...'
			if matches := p.sqlQueryRe.FindAllStringSubmatch(line, -1); matches != nil {
				flushSQLBuilder()
				for _, m := range matches {
					sqlText := m[1]
					result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
						ClassName:  getClassContext(currentClass, currentMethod),
						MethodName: getMethodSafe(currentMethod),
						QueryText:  sqlText,
						LineNumber: lineNum,
						Context:    "method",
					})
					p.extractTablesFromSQL(sqlText, lineNum, result)
				}
			}

			// .Add('...') - для TQueryBuilder
			if builderOwnerMatches := p.sqlAddOwnerRe.FindStringSubmatch(line); builderOwnerMatches != nil {
				builderOwnerMatches := p.sqlAddOwnerRe.FindStringSubmatch(line)
				builderName := ""
				if builderOwnerMatches != nil {
					builderName = strings.ToLower(builderOwnerMatches[1])
					lineHandledBySQLBuilder = true
				}
				sqlText, ok := parseSQLAddExpression(line)
				if ok && builderName != "" {
					if sqlBuilderName != "" && sqlBuilderName != builderName {
						flushSQLBuilder()
					}
					sqlBuilderName = builderName
					if sqlBuilderLine == 0 {
						sqlBuilderLine = lineNum
					}
					sqlBuilderParts = append(sqlBuilderParts, sqlText)
					continue
				}
				if ok && isLikelySQL(sqlText) {
					result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
						ClassName:  getClassContext(currentClass, currentMethod),
						MethodName: getMethodSafe(currentMethod),
						QueryText:  sqlText,
						LineNumber: lineNum,
						Context:    "method",
					})
					p.extractTablesFromSQL(sqlText, lineNum, result)
				}
			}

			// Многострочные строковые литералы
			if !lineHandledBySQLBuilder {
				if matches := p.stringLiteralRe.FindAllStringSubmatch(line, -1); matches != nil {
					for _, m := range matches {
						strContent := m[1]
						// Проверяем, содержит ли строка SQL-ключевые слова
						if isLikelySQL(strContent) {
							result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
								ClassName:  getClassContext(currentClass, currentMethod),
								MethodName: getMethodSafe(currentMethod),
								QueryText:  strContent,
								LineNumber: lineNum,
								Context:    "method",
							})
							p.extractTablesFromSQL(strContent, lineNum, result)
						}
					}
				}
			}

			// ========================================
			// Вызовы GetQuery / GetQueryLines (Diasoft 5NT)
			// ========================================

			// tbSelect.GetQuery([params], [args]) - извлекаем имя компонента
			if matches := p.getQueryRe.FindAllStringSubmatch(line, -1); matches != nil {
				for _, m := range matches {
					componentName := m[1]
					// Сохраняем ссылку на DFM-компонент с запросом
					result.DFMQueries = append(result.DFMQueries, &model.DFMQuery{
						ComponentName: componentName,
						ComponentType: "GetQuery",
						LineNumber:    lineNum,
						MethodName:    currentMethodName(currentMethod),
						MethodLine:    currentMethodLine(currentMethod),
					})
				}
			}

			// GetQueryLines([params], hbText['name'], [args]) - извлекаем имя компонента
			if p.getQueryLinesRe.MatchString(line) {
				// Ищем ссылки на hbText['name'] или другие компоненты
				if matches := p.dfmQueryRefRe.FindAllStringSubmatch(line, -1); matches != nil {
					for _, m := range matches {
						componentType := m[1] // hbText, tbSelect, etc.
						componentName := m[2] // имя компонента
						result.DFMQueries = append(result.DFMQueries, &model.DFMQuery{
							ComponentName: componentName,
							ComponentType: componentType,
							LineNumber:    lineNum,
							MethodName:    currentMethodName(currentMethod),
							MethodLine:    currentMethodLine(currentMethod),
						})
					}
				}
			}
		}
	}

	// Финализация
	flushMethod()
	flushClass()

	// Завершаем unit
	if currentUnit != nil {
		currentUnit.LineEnd = lineNum
		result.Units = append(result.Units, currentUnit)
	}

	// Закрываем сканер
	if err := scanner.Err(); err != nil {
		result.Errors = append(result.Errors, ParseError{
			Line:     lineNum,
			Message:  err.Error(),
			Severity: "error",
		})
	}

	return result, nil
}

// extractSQLFromLine извлекает SQL из строки
func (p *Parser) extractSQLFromLine(line string, lineNum int, result *ParseResult) {
	// Ищем строковые литералы в property declaration
	if matches := p.stringLiteralRe.FindAllStringSubmatch(line, -1); matches != nil {
		for _, m := range matches {
			strContent := m[1]
			if isLikelySQL(strContent) {
				result.SQLFragments = append(result.SQLFragments, &model.SQLFragment{
					QueryText:  strContent,
					LineNumber: lineNum,
					Context:    "property",
				})
				p.extractTablesFromSQL(strContent, lineNum, result)
			}
		}
	}
}

// extractTablesFromSQL извлекает имена таблиц из SQL-строки
func (p *Parser) extractTablesFromSQL(sqlText string, lineNum int, result *ParseResult) {
	// Нормализуем SQL: убираем экранированные кавычки
	sqlText = strings.ReplaceAll(sqlText, "''", "'")

	// Ищем таблицы
	matchIndexes := p.tableInSQLRe.FindAllStringSubmatchIndex(sqlText, -1)
	if matches := p.tableInSQLRe.FindAllStringSubmatch(sqlText, -1); matches != nil {
		seenTables := make(map[string]bool)
		for i, m := range matches {
			tableName := m[1]
			// Пропускаем ключевые слова и плейсхолдеры
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
				Context:     "pas_embedded",
				IsTemporary: strings.HasPrefix(strings.ToLower(strings.TrimSpace(tableName)), "p") || strings.HasPrefix(strings.ToLower(strings.TrimSpace(tableName)), "#"),
				LineNumber:  lineNum,
				ColNumber:   colNumber,
			})
		}
	}
}

// buildSignature строит сигнатуру метода
func buildSignature(name, params, returnType, kind string) string {
	var sig strings.Builder
	sig.WriteString(kind)
	sig.WriteString(" ")
	sig.WriteString(name)
	sig.WriteString("(")
	sig.WriteString(params)
	sig.WriteString(")")
	if returnType != "" {
		sig.WriteString(": ")
		sig.WriteString(returnType)
	}
	return sig.String()
}

// parseUsesList парсит список uses-модулей
func parseUsesList(usesStr string) []string {
	// Разделяем по запятой
	parts := strings.Split(usesStr, ",")
	modules := make([]string, 0, len(parts))
	for _, part := range parts {
		module := strings.TrimSpace(part)
		// Убираем точки с запятой
		module = strings.TrimSuffix(module, ";")
		module = strings.TrimSpace(module)
		if module != "" {
			modules = append(modules, module)
		}
	}
	return modules
}

// isLikelySQL проверяет, похожа ли строка на SQL
func isLikelySQL(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	sqlKeywordDetectRe := regexp.MustCompile(`(?i)\b(SELECT\b.*\bFROM|INSERT\s+INTO|UPDATE\s+[A-Za-z_#][A-Za-z0-9_#]*\s+SET|DELETE\s+FROM|FROM\s+[A-Za-z_#][A-Za-z0-9_#]*|JOIN\s+[A-Za-z_#][A-Za-z0-9_#]*|WHERE\b|GROUP\s+BY|ORDER\s+BY|HAVING\b|UNION\b|CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE|EXEC(?:UTE)?\s+[A-Za-z_#][A-Za-z0-9_#]*)`)
	return sqlKeywordDetectRe.MatchString(s)
}

// isKeyword проверяет, является ли имя ключевым словом Pascal
func isKeyword(name string) bool {
	keywords := map[string]bool{
		"begin": true, "end": true, "var": true, "const": true,
		"type": true, "function": true, "procedure": true,
		"if": true, "then": true, "else": true, "for": true,
		"to": true, "do": true, "while": true, "repeat": true,
		"until": true, "case": true, "of": true, "with": true,
		"try": true, "except": true, "finally": true, "raise": true,
		"exit": true, "break": true, "continue": true,
		"nil": true, "true": true, "false": true,
		"and": true, "or": true, "not": true, "xor": true,
		"as": true, "is": true, "in": true,
		"self": true, "inherited": true,
	}
	return keywords[strings.ToLower(name)]
}

// isIgnoredTableName проверяет, нужно ли игнорировать имя таблицы
func isIgnoredTableName(name string) bool {
	ignored := map[string]bool{
		"select": true, "insert": true, "update": true, "delete": true,
		"from": true, "where": true, "join": true, "table": true,
		"values": true, "set": true, "into": true,
		"a": true, "b": true, "c": true, "d": true, "e": true,
		"t": true, "tmp": true, "temp": true,
	}
	return ignored[strings.ToLower(name)]
}
