package openspecmd

import (
	"regexp"
	"strings"
)

// ParsedSpec — результат парсинга specs/**/spec.md.
type ParsedSpec struct {
	Title        string
	Purpose      string
	Notes        string
	RelatedCode  string
	LineStart    int
	LineEnd      int
	Requirements []ParsedRequirement
}

// ParsedRequirement — блок "### Requirement:".
type ParsedRequirement struct {
	Name      string
	BodyText  string
	LineStart int
	LineEnd   int
	Order     int
	Scenarios []ParsedScenario
}

// ParsedScenario — блок "#### Scenario:" с раздельными given/when/then.
type ParsedScenario struct {
	Name      string
	Given     string
	When      string
	Then      string
	LineStart int
	LineEnd   int
	Order     int
}

var (
	reRequirementHeader = regexp.MustCompile(`^#{3}\s*Requirement:\s*(.+)$`)
	reScenarioHeader    = regexp.MustCompile(`^#{4}\s*Scenario:\s*(.+)$`)
	reH2Header          = regexp.MustCompile(`^#{2}\s+(.+)$`)
	reGWTLine           = regexp.MustCompile(`^\s*[-*]\s+(?:\*\*)?\s*(GIVEN|WHEN|THEN|AND)\s*(?:\*\*)?\s+(.+)$`)
)

// ParseSpecFile разбирает spec.md. Парсер не требует H1 и "## Requirements":
// якоря структуры — только "### Requirement:" / "#### Scenario:" и секции ##.
// line_start/line_end — 1-based границы блока в файле.
func ParseSpecFile(content string) *ParsedSpec {
	lines := splitLines(content)
	spec := &ParsedSpec{LineStart: 1, LineEnd: len(lines)}

	// Состояние: какая capability-секция (## ...) сейчас открыта.
	const (
		secNone = iota
		secPurpose
		secNotes
		secRelated
	)
	currentSection := secNone
	var sectionLines []string

	closeSection := func() {
		text := strings.TrimSpace(strings.Join(sectionLines, "\n"))
		switch currentSection {
		case secPurpose:
			spec.Purpose = text
		case secNotes:
			spec.Notes = text
		case secRelated:
			spec.RelatedCode = text
		}
		currentSection = secNone
		sectionLines = nil
	}

	var req *ParsedRequirement
	var scn *ParsedScenario
	lastGWTKind := "" // последний распознанный блок сценария: given|when|then

	closeScenario := func() {
		if scn != nil {
			scn.LineEnd = scn.LineStart + scnLineCount(scn) - 1
			if req != nil {
				req.Scenarios = append(req.Scenarios, *scn)
			}
			scn = nil
		}
	}

	finishRequirement := func(lineEnd int) {
		if req != nil {
			req.LineEnd = lineEnd
			req.BodyText = strings.TrimSpace(req.BodyText)
			if req.Name != "" {
				spec.Requirements = append(spec.Requirements, *req)
			}
			req = nil
		}
	}

	for i, line := range lines {
		lineNo := i + 1

		if m := reRequirementHeader.FindStringSubmatch(line); m != nil {
			closeScenario()
			finishRequirement(lineNo - 1)
			req = &ParsedRequirement{
				Name:      strings.TrimSpace(m[1]),
				LineStart: lineNo,
				Order:     len(spec.Requirements) + 1,
			}
			continue
		}
		if m := reScenarioHeader.FindStringSubmatch(line); m != nil {
			closeScenario()
			if req == nil {
				// Сценарий вне требования — структурная аномалия; пропускаем.
				continue
			}
			scn = &ParsedScenario{
				Name:      strings.TrimSpace(m[1]),
				LineStart: lineNo,
				Order:     len(req.Scenarios) + 1,
			}
			lastGWTKind = ""
			continue
		}
		if m := reH2Header.FindStringSubmatch(line); m != nil {
			// ## Purpose / ## Notes / ## Related code / ## Requirements / прочие
			closeScenario()
			finishRequirement(lineNo - 1)
			closeSection()
			switch strings.ToLower(strings.TrimSpace(m[1])) {
			case "purpose":
				currentSection = secPurpose
			case "notes":
				currentSection = secNotes
			case "related code", "related_code":
				currentSection = secRelated
			}
			continue
		}

		if scn != nil {
			if keyword, text, ok := parseGWTLine(line); ok {
				switch keyword {
				case "GIVEN":
					scn.Given = appendLine(scn.Given, text)
					lastGWTKind = "given"
				case "WHEN":
					scn.When = appendLine(scn.When, text)
					lastGWTKind = "when"
				case "THEN":
					scn.Then = appendLine(scn.Then, text)
					lastGWTKind = "then"
				case "AND":
					// AND присоединяется к последнему распознанному блоку
					switch lastGWTKind {
					case "given":
						scn.Given = appendLine(scn.Given, text)
					case "when":
						scn.When = appendLine(scn.When, text)
					case "then":
						scn.Then = appendLine(scn.Then, text)
					}
				}
				continue
			}
		}

		if req != nil && scn == nil {
			// Тело требования (до первого сценария): сохраняется как есть —
			// включая Markdown-таблицы.
			if strings.TrimSpace(req.BodyText) == "" && strings.TrimSpace(line) == "" {
				continue // не копим пустые строки в начале
			}
			req.BodyText += line + "\n"
			continue
		}

		if currentSection != secNone {
			sectionLines = append(sectionLines, line)
			continue
		}

		// H1 — заголовок capability (опционален)
		if spec.Title == "" && strings.HasPrefix(strings.TrimSpace(line), "# ") {
			spec.Title = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "# "))
		}
	}

	closeScenario()
	finishRequirement(len(lines))
	closeSection()

	return spec
}

// scnLineCount — грубая оценка числа строк сценария для line_end.
func scnLineCount(scn *ParsedScenario) int {
	count := 1
	for _, s := range []string{scn.Given, scn.When, scn.Then} {
		if s != "" {
			count += strings.Count(s, "\n") + 1
		}
	}
	if count < 1 {
		count = 1
	}
	return count
}

// parseGWTLine распознаёт строки сценариев "- **WHEN** текст" и "- WHEN текст"
// (регистр ключевого слова не значим).
func parseGWTLine(line string) (keyword string, text string, ok bool) {
	m := reGWTLine.FindStringSubmatch(strings.TrimSpace(line))
	if m == nil {
		return "", "", false
	}
	return strings.ToUpper(m[1]), strings.TrimSpace(m[2]), true
}

func appendLine(current string, line string) string {
	if current == "" {
		return line
	}
	return current + "\n" + line
}
