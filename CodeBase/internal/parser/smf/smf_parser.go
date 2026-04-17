package smf

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/parser/js"
)

// Parser SMF-парсер
type Parser struct {
	// XML-элементы
	xmlDeclRe *regexp.Regexp

	// Модель Ф.О.
	instrumentNameRe   *regexp.Regexp
	instrumentBriefRe  *regexp.Regexp
	instrumentObjIDRe  *regexp.Regexp
	instrumentModuleRe *regexp.Regexp
	instrumentStartRe  *regexp.Regexp
	withInstrumentRe   *regexp.Regexp
	legacyNameRe       *regexp.Regexp
	legacyBriefRe      *regexp.Regexp
	legacyObjIDRe      *regexp.Regexp
	legacyModuleRe     *regexp.Regexp
	legacyStartRe      *regexp.Regexp

	// Переменные: var NAME = value; или const NAME = value; или просто NAME = value;
	smfVarDeclRe *regexp.Regexp

	// Константы: const NAME = VALUE; (где VALUE может быть идентификатором)
	smfConstDeclRe *regexp.Regexp

	// Числовые константы: const NAME = 123;
	smfNumericConstRe *regexp.Regexp

	// Альтернативные форматы констант
	smfAltConstRe1 *regexp.Regexp // var NAME = 123;
	smfAltConstRe2 *regexp.Regexp // NAME = 123; (без var/const)
	smfAltConstRe3 *regexp.Regexp // const NAME = SOME_OTHER_CONST
	smfAltConstRe4 *regexp.Regexp // var NAME = 123; (более гибкий)

	// Состояния
	createStateRe *regexp.Regexp
	legacyStateRe *regexp.Regexp
	stateTypeRe   *regexp.Regexp

	// Действия
	createTransitionRe         *regexp.Regexp
	legacyConsumerTransitionRe *regexp.Regexp
	legacyTransitionRe         *regexp.Regexp
	intoRe                     *regexp.Regexp
	propValRe                  *regexp.Regexp
	priorityRe                 *regexp.Regexp
	checkServiceRe             *regexp.Regexp

	// Счета
	typeAccLinkRe *regexp.Regexp

	// Сценарии
	createInstrumentRe *regexp.Regexp
	stepForwardRe      *regexp.Regexp
	massAccrualRe      *regexp.Regexp

	// JS-парсер
	jsParser *js.Parser
}

func (p *Parser) loadIncludeContents(includeFiles []string, basePath string) []string {
	contents := make([]string, 0)
	seen := make(map[string]bool)

	for _, includeFile := range includeFiles {
		for _, candidate := range p.buildIncludeCandidates(includeFile, basePath) {
			key := strings.ToLower(candidate)
			if seen[key] || !p.fileExists(candidate) {
				continue
			}
			seen[key] = true

			content, err := p.readIncludeFile(candidate)
			if err != nil {
				continue
			}
			contents = append(contents, content)
		}
	}

	return contents
}

// ParseResult результат парсинга SMF-файла
type ParseResult struct {
	Instrument  *model.SMFInstrument
	JSResult    *js.ParseResult
	PrequerySQL string
	Includes    []string
	Description string
	Errors      []ParseError
}

// ParseError ошибка парсинга
type ParseError struct {
	Line     int
	Column   int
	Message  string
	Severity string
}

// SMFJob структура XML
type SMFJob struct {
	XMLName     xml.Name `xml:"job"`
	Description string   `xml:"description"`
	Language    string   `xml:"language"`
	Include     struct {
		IncFiles []string `xml:"inc-file"`
	} `xml:"include"`
	PreQuery   string `xml:"prequery"`
	Script     string `xml:"script"`
	IncludeSvr struct {
		IncFiles []string `xml:"inc-file"`
	} `xml:"include-server"`
}

// NewParser создаёт новый SMF-парсер
func NewParser() *Parser {
	return &Parser{
		// XML declaration
		xmlDeclRe: regexp.MustCompile(`(?i)<\?xml.*\?>`),

		// Модель Ф.О.
		instrumentNameRe:   regexp.MustCompile(`(?:Instrument|MassAccrualInstrument)\.Name\s*=\s*"([^"]+)"`),
		instrumentBriefRe:  regexp.MustCompile(`(?:Instrument|MassAccrualInstrument)\.Brief\s*=\s*"([^"]+)"`),
		instrumentObjIDRe:  regexp.MustCompile(`(?:Instrument|MassAccrualInstrument)\.(?:InterfaceObjectID|DealObjectID)\s*=\s*(\d+|[A-Za-z_][A-Za-z0-9_]*)\s*;?`),
		instrumentModuleRe: regexp.MustCompile(`(?:Instrument|MassAccrualInstrument)\.(?:DsModuleID|ModuleID)\s*=\s*(\d+|[A-Za-z_][A-Za-z0-9_]*)\s*;?`),
		instrumentStartRe:  regexp.MustCompile(`Instrument\.StartState\s*=\s*(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))`),
		withInstrumentRe:   regexp.MustCompile(`with\s*\(\s*Instrument\s*\)\s*\{`),
		legacyNameRe:       regexp.MustCompile(`\bName\s*=\s*"([^"]+)"`),
		legacyBriefRe:      regexp.MustCompile(`\bBrief\s*=\s*"([^"]+)"`),
		legacyObjIDRe:      regexp.MustCompile(`\b(?:InterfaceObjectID|DealObjectID)\s*=\s*(\d+|[A-Za-z_][A-Za-z0-9_]*)\s*;?`),
		legacyModuleRe:     regexp.MustCompile(`\b(?:DsModuleID|ModuleID)\s*=\s*(\d+|[A-Za-z_][A-Za-z0-9_]*)\s*;?`),
		legacyStartRe:      regexp.MustCompile(`\bStartState\s*=\s*(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))`),

		// Переменные: var NAME = value; или const NAME = value; или просто NAME = value;
		smfVarDeclRe: regexp.MustCompile(`(?im)^\s*(?:var|const)?\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)`),

		// Константы: const NAME = VALUE; (где VALUE может быть идентификатором)
		smfConstDeclRe: regexp.MustCompile(`(?im)^\s*const\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)`),

		// Числовые константы: const NAME = 123;
		smfNumericConstRe: regexp.MustCompile(`(?im)^\s*const\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)`),

		// Альтернативные форматы констант
		smfAltConstRe1: regexp.MustCompile(`(?im)^\s*var\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)`),                      // var NAME = 123;
		smfAltConstRe2: regexp.MustCompile(`(?im)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)`),                            // NAME = 123;
		smfAltConstRe3: regexp.MustCompile(`(?im)^\s*const\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)`), // const NAME = SOME_OTHER_CONST
		smfAltConstRe4: regexp.MustCompile(`(?im)^\s*var\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)\s*;`),                  // var NAME = 123;

		// Состояния: Instrument.CreateStateWithSysName("name", "sys_name")
		createStateRe: regexp.MustCompile(`CreateStateWithSys[Nn]ame\(\s*(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))\s*,\s*(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))\s*\)`),
		legacyStateRe: regexp.MustCompile(`CreateState\(\s*"([^"]+)"\s*\)`),
		stateTypeRe:   regexp.MustCompile(`State\.StateType\s*=\s*(PROP_STATETYPE_\w+)`),

		// Действия: State.CreateConsumerTransitionWithSysName("name", "sys_name")
		createTransitionRe:         regexp.MustCompile(`CreateConsumerTransitionWithSysName\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)`),
		legacyConsumerTransitionRe: regexp.MustCompile(`CreateConsumerTransition\(\s*"([^"]+)"\s*\)`),
		legacyTransitionRe:         regexp.MustCompile(`CreateTransition\(\s*"([^"]+)"\s*\)`),
		intoRe:                     regexp.MustCompile(`Into\s*\(\s*"([^"]+)"\s*\)`),
		propValRe:                  regexp.MustCompile(`(?:Tran\.|Transition\.)?PropVal\s*=\s*(CONSUMER_ACTION_\w+|\d+)`),
		priorityRe:                 regexp.MustCompile(`(?:Tran\.|Transition\.)?Priority\s*=\s*(\d+)`),
		checkServiceRe:             regexp.MustCompile(`(?:Tran\.|Transition\.)?CheckService\s*=\s*(CHECKSERVICE_\w+|\d+)`),

		// Счета: TypeAccLinkCreate(TYPE, __ACCMASKN__)
		typeAccLinkRe: regexp.MustCompile(`TypeAccLinkCreate\s*\(\s*(RESDEP_\w+),\s*__ACCMASK(\w+)__`),

		// Сценарии
		createInstrumentRe: regexp.MustCompile(`function\s+CreateInstrument\s*\(`),
		stepForwardRe:      regexp.MustCompile(`function\s+StepForward\s*\(`),
		massAccrualRe:      regexp.MustCompile(`function\s+CreateMassAccrualInstrument\s*\(`),

		// JS-парсер
		jsParser: js.NewParser(),
	}
}

// ParseFile парсит SMF-файл
func (p *Parser) ParseFile(path string) (*ParseResult, error) {
	// Пробуем разные кодировки в порядке приоритета
	encodings := []encoding.Encoding{encoding.WIN1251, encoding.CP866, encoding.UTF8}

	var content string
	var err error

	for _, enc := range encodings {
		content, err = encoding.ReadFile(path, enc)
		if err == nil && p.isValidEncoding(content) {
			break // Файл успешно прочитан с корректной кодировкой
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to read file with any encoding: %w", err)
	}

	// Извлекаем базовый путь из полного пути файла
	basePath := ""
	if lastSlash := strings.LastIndex(path, "\\"); lastSlash != -1 {
		basePath = path[:lastSlash]
	} else if lastSlash := strings.LastIndex(path, "/"); lastSlash != -1 {
		basePath = path[:lastSlash]
	}

	return p.ParseContentWithBasePath(content, basePath)
}

// ParseContentWithBasePath парсит содержимое SMF-файла с базовым путем для include-файлов
func (p *Parser) ParseContentWithBasePath(content string, basePath string) (*ParseResult, error) {
	result := &ParseResult{
		Includes: make([]string, 0),
		Errors:   make([]ParseError, 0),
	}

	// Парсим XML
	var job SMFJob
	if err := xml.Unmarshal([]byte(content), &job); err != nil {
		// Если не удалось распарсить как XML, пробуем извлечь данные регуляками
		result.Description = p.extractDescription(content)
		result.PrequerySQL = p.extractPrequery(content)
		result.Includes = p.extractIncludes(content)
		result.Instrument = p.extractInstrumentWithBasePath(content, result.Includes, basePath)
		result.JSResult = p.parseJSScript(content)
		return result, nil
	}

	// Заполняем из XML
	result.Description = strings.TrimSpace(job.Description)
	result.PrequerySQL = strings.TrimSpace(job.PreQuery)

	result.Includes = append(result.Includes, job.Include.IncFiles...)

	// Извлекаем модель Ф.О. из скрипта (с учётом include-файлов)
	includePaths := append([]string{}, job.Include.IncFiles...)
	result.Instrument = p.extractInstrumentWithBasePath(job.Script, includePaths, basePath)

	// Парсим JavaScript из <script>
	result.JSResult = p.parseJSScript(job.Script)

	return result, nil
}

// ParseContent парсит содержимое SMF-файла
func (p *Parser) ParseContent(content string) (*ParseResult, error) {
	result := &ParseResult{
		Includes: make([]string, 0),
		Errors:   make([]ParseError, 0),
	}

	// Парсим XML
	var job SMFJob
	if err := xml.Unmarshal([]byte(content), &job); err != nil {
		// Если не удалось распарсить как XML, пробуем извлечь данные регуляками
		result.Description = p.extractDescription(content)
		result.PrequerySQL = p.extractPrequery(content)
		result.Includes = p.extractIncludes(content)
		result.Instrument = p.extractInstrumentWithBasePath(content, result.Includes, "")
		result.JSResult = p.parseJSScript(content)
		return result, nil
	}

	// Заполняем из XML
	result.Description = strings.TrimSpace(job.Description)
	result.PrequerySQL = strings.TrimSpace(job.PreQuery)

	result.Includes = append(result.Includes, job.Include.IncFiles...)

	// Извлекаем модель Ф.О. из скрипта (с учётом include-файлов)
	includePaths := append([]string{}, job.Include.IncFiles...)
	result.Instrument = p.extractInstrumentWithBasePath(job.Script, includePaths, "")

	// Парсим JavaScript из <script>
	result.JSResult = p.parseJSScript(job.Script)

	return result, nil
}

// extractDescription извлекает описание из не-XML контента
func (p *Parser) extractDescription(content string) string {
	re := regexp.MustCompile(`<description>([^<]+)</description>`)
	if matches := re.FindStringSubmatch(content); matches != nil {
		return strings.TrimSpace(matches[1])
	}
	return ""
}

// extractPrequery извлекает SQL из <prequery>
func (p *Parser) extractPrequery(content string) string {
	re := regexp.MustCompile(`<prequery>([\s\S]*?)</prequery>`)
	if matches := re.FindStringSubmatch(content); matches != nil {
		return strings.TrimSpace(matches[1])
	}
	return ""
}

// extractIncludes извлекает include-файлы
func (p *Parser) extractIncludes(content string) []string {
	includes := make([]string, 0)
	seen := make(map[string]bool)
	re := regexp.MustCompile(`<inc-file>([^<]+)</inc-file>`)
	matches := re.FindAllStringSubmatch(content, -1)
	for _, m := range matches {
		includeFile := strings.TrimSpace(m[1])
		// Извлекаем только имя файла без пути
		fileName := includeFile
		if lastSlash := strings.LastIndex(includeFile, "\\"); lastSlash != -1 {
			fileName = includeFile[lastSlash+1:]
		} else if lastSlash := strings.LastIndex(includeFile, "/"); lastSlash != -1 {
			fileName = includeFile[lastSlash+1:]
		}
		if !seen[fileName] {
			includes = append(includes, fileName)
			seen[fileName] = true
		}
	}
	return includes
}

// extractInstrumentWithBasePath извлекает модель Ф.О. с базовым путем для include-файлов
func (p *Parser) extractInstrumentWithBasePath(script string, includeFiles []string, basePath string) *model.SMFInstrument {
	instr := &model.SMFInstrument{
		States:   make([]map[string]interface{}, 0),
		Actions:  make([]map[string]interface{}, 0),
		Accounts: make([]map[string]interface{}, 0),
	}
	legacyInstrumentBlock := p.extractWithBlock(script, p.withInstrumentRe)

	// Извлекаем переменные из скрипта и include-файлов для разрешения имён
	smfVars := p.extractSMFVarsFromIncludesWithBasePath(script, includeFiles, basePath)

	// Имя
	if matches := p.instrumentNameRe.FindStringSubmatch(script); matches != nil {
		instr.InstrumentName = matches[1]
	} else if matches := p.legacyNameRe.FindStringSubmatch(legacyInstrumentBlock); matches != nil {
		instr.InstrumentName = matches[1]
	}

	// Brief
	if matches := p.instrumentBriefRe.FindStringSubmatch(script); matches != nil {
		instr.Brief = matches[1]
	} else if matches := p.legacyBriefRe.FindStringSubmatch(legacyInstrumentBlock); matches != nil {
		instr.Brief = matches[1]
	}

	// InterfaceObjectID (числовой литерал или переменная)
	if matches := p.instrumentObjIDRe.FindStringSubmatch(script); matches != nil {
		instr.DealObjectID = p.resolveSMFValue(matches[1], smfVars)
	} else if matches := p.legacyObjIDRe.FindStringSubmatch(legacyInstrumentBlock); matches != nil {
		instr.DealObjectID = p.resolveSMFValue(matches[1], smfVars)
	}

	// DsModuleID (числовой литерал или переменная)
	if matches := p.instrumentModuleRe.FindStringSubmatch(script); matches != nil {
		instr.DsModuleID = p.resolveSMFValue(matches[1], smfVars)
	} else if matches := p.legacyModuleRe.FindStringSubmatch(legacyInstrumentBlock); matches != nil {
		instr.DsModuleID = p.resolveSMFValue(matches[1], smfVars)
	}

	// StartState
	if matches := p.instrumentStartRe.FindStringSubmatch(script); matches != nil {
		instr.StartState = firstNonEmpty(matches[1:]...)
	} else if matches := p.legacyStartRe.FindStringSubmatch(legacyInstrumentBlock); matches != nil {
		instr.StartState = firstNonEmpty(matches[1:]...)
	}

	// Определяем тип сценария
	if p.createInstrumentRe.MatchString(script) {
		instr.ScenarioType = "instrument_model"
	} else if p.massAccrualRe.MatchString(script) {
		instr.ScenarioType = "mass_operation"
	}

	// Извлекаем состояния
	instr.States = p.extractStates(script)

	// Извлекаем действия
	instr.Actions = p.extractActions(script, includeFiles, basePath)

	// Извлекаем счета
	instr.Accounts = p.extractAccounts(script)

	hasInstrumentModel := instr.InstrumentName != "" ||
		instr.Brief != "" ||
		instr.DealObjectID != 0 ||
		instr.DsModuleID != 0 ||
		instr.StartState != "" ||
		instr.ScenarioType != "" ||
		len(instr.States) > 0 ||
		len(instr.Actions) > 0 ||
		len(instr.Accounts) > 0

	if !hasInstrumentModel {
		return nil
	}

	return instr
}

// extractSMFVarsFromIncludesWithBasePath извлекает переменные из SMF-скрипта и всех include-файлов с базовым путем
func (p *Parser) extractSMFVarsFromIncludesWithBasePath(script string, includeFiles []string, basePath string) map[string]int64 {
	vars := make(map[string]int64)

	// Извлекаем переменные из основного скрипта
	scriptVars := p.extractSMFVars(script)
	for name, value := range scriptVars {
		vars[name] = value
	}

	// Извлекаем переменные из каждого include-файла
	for _, includeFile := range includeFiles {
		for _, fullPath := range p.buildIncludeCandidates(includeFile, basePath) {
			includeVars := p.extractSMFVarsFromIncludeFile(fullPath)
			for name, value := range includeVars {
				vars[name] = value
			}
		}
	}

	return vars
}

func (p *Parser) buildIncludeCandidates(includeFile string, basePath string) []string {
	seen := make(map[string]bool)
	candidates := make([]string, 0)

	add := func(path string) {
		if strings.TrimSpace(path) == "" {
			return
		}
		normalized := filepath.Clean(strings.ReplaceAll(path, "/", string(os.PathSeparator)))
		key := strings.ToLower(normalized)
		if seen[key] {
			return
		}
		seen[key] = true
		candidates = append(candidates, normalized)
	}

	normalizedFile := strings.ReplaceAll(includeFile, "\\", string(os.PathSeparator))
	add(normalizedFile)

	if basePath == "" {
		return candidates
	}

	current := filepath.Clean(basePath)
	fileName := filepath.Base(normalizedFile)

	for {
		add(filepath.Join(current, normalizedFile))
		add(filepath.Join(current, "Include", normalizedFile))
		add(filepath.Join(current, fileName))
		add(filepath.Join(current, "Include", fileName))

		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}

	return candidates
}

// fileExists проверяет существование файла
func (p *Parser) fileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// isValidEncoding проверяет что текст прочитан с корректной кодировкой
func (p *Parser) isValidEncoding(content string) bool {
	// Если в тексте есть русские буквы, проверяем что они не превратились в кракозябры
	hasRussian := regexp.MustCompile(`[а-яА-Я]`).MatchString(content)
	if hasRussian {
		// Проверяем что нет характерных кракозябр из неправильной кодировки
		hasGarbage := regexp.MustCompile(`[ЎўЈ¤ҐЁЄЇІѕљњћќўџ]`).MatchString(content)
		if hasGarbage {
			return false
		}
	}
	return true
}

// extractSMFVarsFromIncludeFile извлекает переменные из одного include-файла
func (p *Parser) extractSMFVarsFromIncludeFile(includeFile string) map[string]int64 {
	vars := make(map[string]int64)
	if !p.fileExists(includeFile) {
		return vars
	}

	// Пытаемся прочитать файл с разными кодировками
	content, err := p.readIncludeFile(includeFile)
	if err != nil {
		return vars // Возвращаем пустую карту если файл не удалось прочитать
	}

	// Извлекаем переменные из содержимого файла
	fileVars := p.extractSMFVars(content)
	for name, value := range fileVars {
		vars[name] = value
	}

	return vars
}

// readIncludeFile читает include-файл, пробуя разные кодировки
func (p *Parser) readIncludeFile(includeFile string) (string, error) {
	// Пробуем прочитать файл с кодировкой WIN1251
	content, err := encoding.ReadFile(includeFile, encoding.WIN1251)
	if err == nil {
		return content, nil
	}

	// Пробуем прочитать файл с кодировкой UTF8
	content, err = encoding.ReadFile(includeFile, encoding.UTF8)
	if err == nil {
		return content, nil
	}

	return "", fmt.Errorf("failed to read include file: %w", err)
}

// extractSMFVars извлекает переменные из SMF-скрипта (var NAME = value; или const NAME = value;)
func (p *Parser) extractSMFVars(script string) map[string]int64 {
	vars := make(map[string]int64)

	// Извлекаем числовые переменные (основной формат)
	varMatches := p.smfVarDeclRe.FindAllStringSubmatch(script, -1)
	for _, matches := range varMatches {
		varName := strings.TrimSpace(matches[1])
		var val int64
		if _, err := fmt.Sscanf(matches[2], "%d", &val); err == nil {
			vars[varName] = val
		}
	}

	// Извлекаем var NAME = 123;
	varMatches1 := p.smfAltConstRe1.FindAllStringSubmatch(script, -1)
	for _, matches := range varMatches1 {
		varName := strings.TrimSpace(matches[1])
		var val int64
		if _, err := fmt.Sscanf(matches[2], "%d", &val); err == nil {
			vars[varName] = val
		}
	}

	// Извлекаем NAME = 123; (без var/const)
	varMatches2 := p.smfAltConstRe2.FindAllStringSubmatch(script, -1)
	for _, matches := range varMatches2 {
		varName := strings.TrimSpace(matches[1])
		var val int64
		if _, err := fmt.Sscanf(matches[2], "%d", &val); err == nil {
			vars[varName] = val
		}
	}

	// Извлекаем числовые константы const NAME = 123;
	numConstMatches := p.smfNumericConstRe.FindAllStringSubmatch(script, -1)
	for _, matches := range numConstMatches {
		constName := strings.TrimSpace(matches[1])
		var val int64
		if _, err := fmt.Sscanf(matches[2], "%d", &val); err == nil {
			vars[constName] = val
		}
	}

	// Извлекаем var NAME = 123; (более гибкий с точкой с запятой)
	varMatches4 := p.smfAltConstRe4.FindAllStringSubmatch(script, -1)
	for _, matches := range varMatches4 {
		varName := strings.TrimSpace(matches[1])
		var val int64
		if _, err := fmt.Sscanf(matches[2], "%d", &val); err == nil {
			vars[varName] = val
		}
	}

	// Извлекаем константы-идентификаторы const NAME = SOME_OTHER_CONST
	constMatches := p.smfAltConstRe3.FindAllStringSubmatch(script, -1)
	for _, matches := range constMatches {
		constName := strings.TrimSpace(matches[1])
		// Добавляем только если еще не добавлено как числовая константа
		if _, exists := vars[constName]; !exists {
			vars[constName] = 0 // Заглушка, реальное значение будет определено позже
		}
	}

	return vars
}

// resolveSMFValue разрешает значение: если это число — парсит напрямую,
// если переменная — ищет в карте переменных
func (p *Parser) resolveSMFValue(value string, vars map[string]int64) int64 {
	// Пробуем парсить как число
	var num int64
	if _, err := fmt.Sscanf(value, "%d", &num); err == nil {
		return num
	}
	// Ищем в переменных
	if val, ok := vars[value]; ok {
		return val
	}
	return 0
}

func (p *Parser) extractWithBlock(script string, re *regexp.Regexp) string {
	loc := re.FindStringIndex(script)
	if loc == nil {
		return ""
	}

	start := loc[1]
	depth := 1
	for i := start; i < len(script); i++ {
		switch script[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return script[start:i]
			}
		}
	}

	return script[start:]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// extractStates извлекает состояния
func (p *Parser) extractStates(script string) []map[string]interface{} {
	states := make([]map[string]interface{}, 0)
	seen := make(map[string]bool)
	appendState := func(state map[string]interface{}) {
		key := fmt.Sprintf("%v|%v", state["name"], state["sys_name"])
		if seen[key] {
			return
		}
		seen[key] = true
		states = append(states, state)
	}

	// Находим все CreateStateWithSysName
	stateMatches := p.createStateRe.FindAllStringSubmatch(script, -1)
	// Для каждого состояния ищем StateType
	lines := strings.Split(script, "\n")
	for _, m := range stateMatches {
		stateName := firstNonEmpty(m[1], m[2])
		stateSysName := firstNonEmpty(m[3], m[4])

		state := map[string]interface{}{
			"name":     stateName,
			"sys_name": stateSysName,
		}

		// Ищем StateType после создания состояния
		for i, line := range lines {
			if strings.Contains(line, m[0]) {
				// Ищем в следующих строках
				for j := i; j < len(lines) && j < i+10; j++ {
					if typeMatches := regexp.MustCompile(`(?:State\.)?StateType\s*=\s*(PROP_STATETYPE_\w+)`).FindStringSubmatch(lines[j]); typeMatches != nil {
						state["state_type"] = typeMatches[1]
						break
					}
				}
				break
			}
		}

		appendState(state)
	}

	legacyStateMatches := p.legacyStateRe.FindAllStringSubmatch(script, -1)
	for _, m := range legacyStateMatches {
		state := map[string]interface{}{
			"name":     m[1],
			"sys_name": m[1],
		}
		appendState(state)
	}

	return states
}

// extractActions извлекает действия
func (p *Parser) extractActions(script string, includeFiles []string, basePath string) []map[string]interface{} {
	actions := make([]map[string]interface{}, 0)
	seen := make(map[string]bool)
	appendAction := func(action map[string]interface{}) {
		key := fmt.Sprintf("%v|%v", action["name"], action["sys_name"])
		if seen[key] {
			return
		}
		seen[key] = true
		actions = append(actions, action)
	}

	// Находим все CreateConsumerTransitionWithSysName
	transMatches := p.createTransitionRe.FindAllStringSubmatch(script, -1)
	lines := strings.Split(script, "\n")
	for _, m := range transMatches {
		actionName := m[1]
		actionSysName := m[2]

		action := map[string]interface{}{
			"name":     actionName,
			"sys_name": actionSysName,
		}

		// Ищем параметры действия в следующих строках
		for i, line := range lines {
			if strings.Contains(line, m[0]) {
				// Ищем в следующих 15 строках
				for j := i; j < len(lines) && j < i+15; j++ {
					if propMatches := p.propValRe.FindStringSubmatch(lines[j]); propMatches != nil {
						action["prop_val"] = propMatches[1]
					}
					if prioMatches := p.priorityRe.FindStringSubmatch(lines[j]); prioMatches != nil {
						action["priority"] = prioMatches[1]
					}
					if csMatches := p.checkServiceRe.FindStringSubmatch(lines[j]); csMatches != nil {
						action["check_service"] = csMatches[1]
					}
				}
				break
			}
		}

		appendAction(action)
	}

	legacyTransitionPatterns := []*regexp.Regexp{p.legacyConsumerTransitionRe, p.legacyTransitionRe}
	for _, transitionRe := range legacyTransitionPatterns {
		legacyMatches := transitionRe.FindAllStringSubmatch(script, -1)
		for _, m := range legacyMatches {
			actionName := m[1]
			action := map[string]interface{}{
				"name":     actionName,
				"sys_name": actionName,
			}

			for i, line := range lines {
				if strings.Contains(line, m[0]) {
					for j := i; j < len(lines) && j < i+20; j++ {
						if intoMatches := p.intoRe.FindStringSubmatch(lines[j]); intoMatches != nil {
							action["into"] = intoMatches[1]
						}
						if propMatches := p.propValRe.FindStringSubmatch(lines[j]); propMatches != nil {
							action["prop_val"] = propMatches[1]
						}
						if prioMatches := p.priorityRe.FindStringSubmatch(lines[j]); prioMatches != nil {
							action["priority"] = prioMatches[1]
						}
						if csMatches := p.checkServiceRe.FindStringSubmatch(lines[j]); csMatches != nil {
							action["check_service"] = csMatches[1]
						}
					}
					break
				}
			}

			appendAction(action)
		}
	}

	helperRe := regexp.MustCompile(`(?m)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*,\s*(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))\s*\)\s*;`)
	helperMatches := helperRe.FindAllStringSubmatch(script, -1)
	if helperMatches == nil {
		return actions
	}

	helperSources := append([]string{script}, p.loadIncludeContents(includeFiles, basePath)...)
	for _, m := range helperMatches {
		helperName := m[1]
		targetState := firstNonEmpty(m[2], m[3])
		helperBody := p.findHelperFunctionBody(helperName, helperSources)

		action := map[string]interface{}{
			"name":     helperName,
			"sys_name": targetState,
		}

		if intoMatches := p.intoRe.FindStringSubmatch(helperBody); intoMatches != nil {
			action["into"] = intoMatches[1]
		}
		if propMatches := p.propValRe.FindStringSubmatch(helperBody); propMatches != nil {
			action["prop_val"] = propMatches[1]
		}
		if prioMatches := p.priorityRe.FindStringSubmatch(helperBody); prioMatches != nil {
			action["priority"] = prioMatches[1]
		}
		if csMatches := p.checkServiceRe.FindStringSubmatch(helperBody); csMatches != nil {
			action["check_service"] = csMatches[1]
		}

		appendAction(action)
	}

	return actions
}

func (p *Parser) findHelperFunctionBody(helperName string, sources []string) string {
	pattern := regexp.MustCompile(`function\s+` + regexp.QuoteMeta(helperName) + `\s*\([^)]*\)\s*\{`)

	for _, source := range sources {
		loc := pattern.FindStringIndex(source)
		if loc == nil {
			continue
		}

		start := loc[1]
		depth := 1
		for i := start; i < len(source); i++ {
			switch source[i] {
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					return source[start:i]
				}
			}
		}
	}

	return ""
}

// extractAccounts извлекает счета
func (p *Parser) extractAccounts(script string) []map[string]interface{} {
	accounts := make([]map[string]interface{}, 0)

	matches := p.typeAccLinkRe.FindAllStringSubmatch(script, -1)
	for _, m := range matches {
		account := map[string]interface{}{
			"type": m[1],
			"mask": m[2],
		}
		accounts = append(accounts, account)
	}

	return accounts
}

// parseJSScript парсит встроенный JavaScript
func (p *Parser) parseJSScript(script string) *js.ParseResult {
	if script == "" {
		return &js.ParseResult{
			Functions:      make([]*model.JSFunction, 0),
			ScriptObjects:  make([]*model.JSScriptObject, 0),
			ProcedureCalls: make([]*model.JSProcedureCall, 0),
			QueryCalls:     make([]*model.JSQueryCall, 0),
			Constants:      make([]*model.JSConstant, 0),
			Errors:         make([]js.ParseError, 0),
		}
	}

	result, err := p.jsParser.ParseContent(script)
	if err != nil {
		// Игнорируем ошибки JS парсинга, продолжаем с тем что есть
		return result
	}

	instrument := p.extractInstrumentWithBasePath(script, nil, "")
	if instrument != nil && instrument.ScenarioType != "" {
		for _, fn := range result.Functions {
			fn.ScenarioType = instrument.ScenarioType
		}
	}

	return result
}
