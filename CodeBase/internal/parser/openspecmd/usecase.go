package openspecmd

import (
	"regexp"
	"strconv"
	"strings"
)

// ParsedUsecase — результат парсинга файла usecase-слоя.
type ParsedUsecase struct {
	Name           string // имя файла без расширения (scenario-sms-disable / REQ-001-SC-001 / 1._Бизнес-процесс_...)
	Title          string // первый H1
	Description    string
	Actors         string
	Preconditions  string
	Postconditions string
	BusinessValue  string
	Architecture   string
	DataSchema     string
	SourceDir      string // scenarios | usecases | business-processes
	Kind           string // scenario | usecase | business-process
	PageID         int64  // Confluence pageId (0 = нет)
	Steps          []ParsedStep
	LineStart      int
	LineEnd        int
}

// ParsedStep — шаг потока usecase.
type ParsedStep struct {
	FlowKind   string // main | alternative
	StepOrder  int
	StepText   string
	LineNumber int
}

const backtick = "`"

var (
	reH1Header     = regexp.MustCompile(`^#\s+(.+)$`)
	reUsecaseH2    = regexp.MustCompile(`^##\s+(.+)$`)
	reUsecaseH3    = regexp.MustCompile(`^###\s+(.+)$`)
	reUsecaseH4    = regexp.MustCompile(`^####\s+(.+)$`)
	reConfluenceID = regexp.MustCompile(`Confluence\s+pageId\s*:?\s*(\d+)`)
	reURLPageID    = regexp.MustCompile(`[?&]pageId=(\d+)`)
	reInlineMeta   = regexp.MustCompile(`^\*\*(.+?)\*\*:\s*(.+)$`)
	reBoldStep     = regexp.MustCompile(`^\*\*Шаг\s+(\d+)\*\*[.)]?\s*(.+)$`)
	reNumberedStep = regexp.MustCompile(`^\s*(\d+(?:\.\d+)*)[.)]?\s+(.+)$`)
	reBulletStep   = regexp.MustCompile(`^\s*[-*]\s+(.+)$`)
	reIndexEntry   = regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)[^` + backtick + `]*` + backtick + `(\d+)` + backtick)
)

const (
	flowNone = iota
	flowMain
	flowAlt
)

// ParseUsecaseFile разбирает файл usecase-слоя. fileName — имя файла без каталога
// (для usecase_name). Формат определяется по classified.SourceDir:
// scenarios (fa-cards: актёры, потоки), usecases (fa-financialasset: REQ-NNN-SC-NNN),
// business-processes (fa-custody: Confluence-выгрузки с таблицей шагов).
func ParseUsecaseFile(classified Classified, fileName string, content string) *ParsedUsecase {
	lines := splitLines(content)
	uc := &ParsedUsecase{
		Name:      strings.TrimSuffix(fileName, ".md"),
		LineStart: 1,
		LineEnd:   len(lines),
		SourceDir: strings.ToLower(classified.SourceDir),
	}
	switch uc.SourceDir {
	case "scenarios":
		uc.Kind = "scenario"
	case "usecases":
		uc.Kind = "usecase"
	case "business-processes":
		uc.Kind = "business-process"
	}

	// pageId из цитаты "> Confluence pageId 403110443" (бизнес-процессы)
	// или из URL "pageId=467512912" (usecases-формат fa-financialasset)
	for _, line := range lines {
		if m := reConfluenceID.FindStringSubmatch(line); m != nil {
			if id, err := strconv.ParseInt(m[1], 10, 64); err == nil {
				uc.PageID = id
			}
			break
		}
		if m := reURLPageID.FindStringSubmatch(line); m != nil && uc.PageID == 0 {
			if id, err := strconv.ParseInt(m[1], 10, 64); err == nil {
				uc.PageID = id
			}
			break
		}
	}

	flow := flowNone
	stepOrder := 0

	var sectionTarget *string
	setTarget := func(t *string) {
		sectionTarget = t
		flow = flowNone
	}

	setSection := func(header string) {
		switch strings.ToLower(strings.TrimSpace(header)) {
		case "описание":
			setTarget(&uc.Description)
		case "пользователи", "акторы", "актёры", "пользователи и системы", "роли и системы":
			setTarget(&uc.Actors)
		case "предусловия", "пред-условия", "предпосылки":
			setTarget(&uc.Preconditions)
		case "постусловия", "пост-условия":
			setTarget(&uc.Postconditions)
		case "бизнес-ценность", "ценность", "бизнес ценность":
			setTarget(&uc.BusinessValue)
		case "архитектурные компоненты", "архитектурные компоненты + ограничения", "архитектурные компоненты и ограничения", "описание архитектуры и зависимостей", "архитектура", "точка старта":
			setTarget(&uc.Architecture)
		case "схема данных", "схема":
			setTarget(&uc.DataSchema)
		case "основной сценарий", "основной поток", "основные шаги", "шаги":
			setTarget(nil)
			flow = flowMain
			stepOrder = 0
		case "альтернативный сценарий", "альтернативный поток", "альтернативные шаги", "исключения", "ветвления":
			setTarget(nil)
			flow = flowAlt
			stepOrder = 0
		case "спеки-компоненты", "спеки-компоненты (запчасти)", "запчасти":
			setTarget(nil)
			flow = flowNone
		default:
			// прочая секция (Схема BPMN и т.п.) — не сохраняем
			setTarget(nil)
		}
	}

	inTable := false
	for i, line := range lines {
		lineNo := i + 1
		trimmed := strings.TrimSpace(line)

		if m := reH1Header.FindStringSubmatch(line); m != nil && uc.Title == "" {
			uc.Title = strings.TrimSpace(m[1])
			continue
		}
		if m := reUsecaseH2.FindStringSubmatch(line); m != nil {
			setSection(m[1])
			inTable = false
			continue
		}
		// H3-секции: "### Сценарий SC-001 - ..." → flow=flowMain
		if m := reUsecaseH3.FindStringSubmatch(line); m != nil {
			headerLower := strings.ToLower(strings.TrimSpace(m[1]))
			if strings.HasPrefix(headerLower, "сценарий") {
				setTarget(nil)
				flow = flowMain
				stepOrder = 0
			} else {
				setSection(m[1])
			}
			inTable = false
			continue
		}
		// H4-секции: "#### Предусловия", "#### Шаги", "#### Постусловия"
		if m := reUsecaseH4.FindStringSubmatch(line); m != nil {
			setSection(m[1])
			inTable = false
			continue
		}

		// Inline-метаданные: "**Ключ**: Значение" (вне текстовых секций)
		if sectionTarget == nil {
			if m := reInlineMeta.FindStringSubmatch(trimmed); m != nil {
				applyInlineMeta(uc, strings.TrimSpace(m[1]), strings.TrimSpace(m[2]))
				continue
			}
		}

		// Таблица шагов: бизнес-процессы (| Шаг | Система | Роль | Описание |)
		// и usecases (| Шаг | Пользователь/система | Действие | Ожидаемый результат |)
		if strings.HasPrefix(trimmed, "|") && (uc.Kind == "business-process" || uc.Kind == "usecase") {
			cols := strings.Split(strings.Trim(trimmed, "|"), "|")
			if isTableHeaderRow(cols) || isTableSeparatorRow(trimmed) {
				inTable = true
				continue
			}
			if inTable && flow != flowNone && len(cols) >= 2 {
				step := buildUsecaseTableStep(cols)
				if step != "" {
					stepOrder++
					uc.Steps = append(uc.Steps, ParsedStep{
						FlowKind:   flowKindOf(flow),
						StepOrder:  stepOrder,
						StepText:   step,
						LineNumber: lineNo,
					})
				}
				continue
			}
			if inTable && flow == flowNone && len(cols) >= 2 && uc.Kind == "business-process" {
				// Строка шага БП: шаг | система | роль | описание (legacy)
				step := buildBPStep(cols)
				if step != "" {
					stepOrder++
					uc.Steps = append(uc.Steps, ParsedStep{
						FlowKind:   "main",
						StepOrder:  stepOrder,
						StepText:   step,
						LineNumber: lineNo,
					})
				}
				continue
			}
			continue
		}
		if strings.HasPrefix(trimmed, "|") && uc.Kind != "business-process" && uc.Kind != "usecase" {
			// Таблица вне БП/usecase — часть текущей секции
			if sectionTarget != nil {
				*sectionTarget = appendLine(*sectionTarget, line)
			}
			continue
		}

		// Шаги **Шаг N**. текст (usecases-формат)
		if m := reBoldStep.FindStringSubmatch(trimmed); m != nil && flow != flowNone {
			order, _ := strconv.Atoi(m[1])
			if order == 0 {
				stepOrder++
			} else {
				stepOrder = order
			}
			uc.Steps = append(uc.Steps, ParsedStep{
				FlowKind:   flowKindOf(flow),
				StepOrder:  stepOrder,
				StepText:   strings.TrimSpace(m[2]),
				LineNumber: lineNo,
			})
			continue
		}

		// Нумерованные шаги потоков: "1. текст", "3.1. текст"
		if m := reNumberedStep.FindStringSubmatch(line); m != nil && flow != flowNone {
			stepOrder++
			uc.Steps = append(uc.Steps, ParsedStep{
				FlowKind:   flowKindOf(flow),
				StepOrder:  stepOrder,
				StepText:   strings.TrimSpace(m[2]),
				LineNumber: lineNo,
			})
			continue
		}
		// Маркированные шаги
		if m := reBulletStep.FindStringSubmatch(line); m != nil && flow != flowNone {
			stepOrder++
			uc.Steps = append(uc.Steps, ParsedStep{
				FlowKind:   flowKindOf(flow),
				StepOrder:  stepOrder,
				StepText:   strings.TrimSpace(m[1]),
				LineNumber: lineNo,
			})
			continue
		}

		// Обычная строка секции
		if sectionTarget != nil && trimmed != "" {
			*sectionTarget = appendLine(*sectionTarget, line)
		}
	}

	return uc
}

func flowKindOf(flow int) string {
	if flow == flowAlt {
		return "alternative"
	}
	return "main"
}

// applyInlineMeta разбирает inline-метаданные "**Ключ**: Значение" в поля ParsedUsecase.
func applyInlineMeta(uc *ParsedUsecase, key, value string) {
	keyLower := strings.ToLower(key)
	switch {
	case strings.Contains(keyLower, "описание"):
		uc.Description = appendLine(uc.Description, value)
	case strings.Contains(keyLower, "пользователи") || strings.Contains(keyLower, "роли") ||
		strings.Contains(keyLower, "актёр") || strings.Contains(keyLower, "актор"):
		uc.Actors = appendLine(uc.Actors, value)
	case strings.Contains(keyLower, "бизнес-ценность") || strings.Contains(keyLower, "ценность"):
		uc.BusinessValue = appendLine(uc.BusinessValue, value)
	case strings.Contains(keyLower, "точка старта"):
		uc.Architecture = appendLine(uc.Architecture, value)
	case strings.Contains(keyLower, "источник"):
		// Из источника извлекается pageId (уже обработан выше), но сохраняем текст
		// в description если оно пусто — не делаем, чтобы не засорять
	}
}

// buildUsecaseTableStep собирает текст шага из строки таблицы usecase-формата:
// | Шаг | Пользователь/система | Действие | Ожидаемый результат |
// Возвращает "<Действие> — <Ожидаемый результат>" или просто "<Действие>".
func buildUsecaseTableStep(cols []string) string {
	for i := range cols {
		cols[i] = strings.TrimSpace(cols[i])
	}
	switch {
	case len(cols) >= 4:
		// | Шаг | Пользователь/система | Действие | Ожидаемый результат |
		action, result := cols[2], cols[3]
		if action == "" && result == "" {
			return ""
		}
		if action == "" {
			return result
		}
		if result != "" {
			return action + " — " + result
		}
		return action
	case len(cols) == 3:
		// | Шаг | Пользователь | Действие |
		action := cols[2]
		if action == "" {
			return ""
		}
		return action
	case len(cols) == 2:
		if cols[0] == "" && cols[1] == "" {
			return ""
		}
		return cols[0] + ": " + cols[1]
	default:
		return ""
	}
}

func isTableHeaderRow(cols []string) bool {
	joined := strings.ToLower(strings.Join(cols, "|"))
	return strings.Contains(joined, "шаг") && (strings.Contains(joined, "роль") || strings.Contains(joined, "описание") || strings.Contains(joined, "система"))
}

func isTableSeparatorRow(trimmed string) bool {
	stripped := strings.ReplaceAll(strings.ReplaceAll(trimmed, "|", ""), " ", "")
	stripped = strings.ReplaceAll(stripped, "-", "")
	return stripped == "" && strings.Contains(trimmed, "-")
}

// buildBPStep собирает текст шага из строки таблицы БП:
// "<Название шага> — <Роль>: <Описание>" (система опускается: почти всегда Диасофт).
func buildBPStep(cols []string) string {
	for i := range cols {
		cols[i] = strings.TrimSpace(cols[i])
	}
	switch {
	case len(cols) >= 4:
		name, role, desc := cols[0], cols[2], cols[3]
		if name == "" && desc == "" {
			return ""
		}
		if name == "" {
			return desc
		}
		if role != "" && desc != "" {
			return name + " — " + role + ": " + desc
		}
		return name + ": " + desc
	case len(cols) == 3:
		name, role, desc := cols[0], cols[1], cols[2]
		if name == "" && desc == "" {
			return ""
		}
		if role != "" && desc != "" {
			return name + " — " + role + ": " + desc
		}
		return name + ": " + desc
	case len(cols) == 2:
		if cols[0] == "" && cols[1] == "" {
			return ""
		}
		return cols[0] + ": " + cols[1]
	default:
		// одно-колоночные строки-решения («"Вопрос": "Да"») — не шаги
		return ""
	}
}

// ParseUsecaseIndex разбирает INDEX.md usecase-слоя в карту
// имя файла → Confluence pageId.
func ParseUsecaseIndex(content string) map[string]int64 {
	result := map[string]int64{}
	for _, m := range reIndexEntry.FindAllStringSubmatch(content, -1) {
		pageID, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			continue
		}
		base := m[1]
		if idx := strings.LastIndexAny(base, "/\\"); idx >= 0 {
			base = base[idx+1:]
		}
		result[base] = pageID
	}
	return result
}
