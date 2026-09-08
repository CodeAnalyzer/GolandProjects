package openspecmd

import (
	"regexp"
	"strings"
)

// ParsedChange — результат парсинга proposal.md change-директории.
type ParsedChange struct {
	Name        string // имя change-директории
	Archived    bool
	Description string // секция ## Описание / ## Description
}

var (
	reChangeH2     = regexp.MustCompile(`^##\s+(.+)$`)
	reSpecRef      = regexp.MustCompile(`openspec[/\\]specs[/\\]([A-Za-z0-9_.\-/\\]+?)(?:[/\\]spec\.md|[^A-Za-z0-9_.\-/\\]|$)`)
	reH3           = regexp.MustCompile(`^###\s+(.+)$`)
	reCapListItem  = regexp.MustCompile("^[-*]\\s+`([A-Za-z0-9_.\\-/]+)`")
	reCapBacktick  = regexp.MustCompile("`([A-Za-z0-9_.\\-/]+)`")
)

// ParseChangeProposal разбирает proposal.md: описание change (для контекста)
// не критично, основная ценность — извлечение затронутых capability.
func ParseChangeProposal(classified Classified, content string) *ParsedChange {
	lines := splitLines(content)
	pc := &ParsedChange{
		Name:     classified.ChangeName,
		Archived: classified.Archived,
	}

	var target *string
	for _, line := range lines {
		if m := reChangeH2.FindStringSubmatch(line); m != nil {
			switch strings.ToLower(strings.TrimSpace(m[1])) {
			case "описание", "description":
				target = &pc.Description
			default:
				target = nil
			}
			continue
		}
		if target != nil && strings.TrimSpace(line) != "" {
			*target = appendLine(*target, strings.TrimSpace(line))
		}
	}
	pc.Description = strings.TrimSpace(pc.Description)
	return pc
}

// ExtractSpecReferences извлекает slugs затронутых capability из текста
// (proposal.md при skip_specs, tasks.md, delta-спек). Распознаёт:
//   - полные пути "openspec/specs/<a>/<b>" и "openspec\specs\<a>\<b>"
//   - markdown-ссылки ../specs/<name>/spec.md
//   - относительные "specs/<a>/<b>/spec.md"
//   - секции "### Modified Capabilities" / "### New Capabilities" из proposal.md
//
// Возвращает набор slug-ов (полный путь от specs/).
func ExtractSpecReferences(content string) map[string]struct{} {
	result := map[string]struct{}{}

	for _, m := range reSpecRef.FindAllStringSubmatch(content, -1) {
		slug := normalizeSlug(m[1])
		if slug != "" {
			result[slug] = struct{}{}
		}
	}

	// markdown-ссылки ../specs/<name>/spec.md (включая формы без сегмента specs)
	for _, m := range reMDSpecLink.FindAllStringSubmatch(content, -1) {
		slug := normalizeSlug(m[1])
		if slug != "" {
			result[slug] = struct{}{}
		}
	}

	// Секции "### Modified Capabilities" / "### New Capabilities" из proposal.md
	// Формат: "- `slug`: описание" или "- `slug` — описание"
	for _, slug := range extractCapabilitySectionSlugs(content) {
		result[slug] = struct{}{}
	}

	return result
}

// extractCapabilitySectionSlugs парсит секции "### Modified Capabilities" и
// "### New Capabilities" (а также "### Added Capabilities") из proposal.md,
// извлекая slugs из строк вида "- `slug`: описание".
func extractCapabilitySectionSlugs(content string) []string {
	lines := splitLines(content)
	var slugs []string
	inCapSection := false

	for _, line := range lines {
		if m := reH3.FindStringSubmatch(line); m != nil {
			heading := strings.ToLower(strings.TrimSpace(m[1]))
			inCapSection = strings.Contains(heading, "modified capabilit") ||
				strings.Contains(heading, "new capabilit") ||
				strings.Contains(heading, "added capabilit")
			continue
		}
		// Любой другой H2/H3 заголовок заканчивает секцию
		if strings.HasPrefix(strings.TrimSpace(line), "##") {
			inCapSection = false
			continue
		}
		if !inCapSection {
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "<!--") {
			continue
		}
		// Извлекаем slug из backticks: - `slug`: описание
		if m := reCapListItem.FindStringSubmatch(line); m != nil {
			slug := normalizeSlug(m[1])
			if slug != "" {
				slugs = append(slugs, slug)
			}
			continue
		}
		// На случай если формат без буллета: `slug` — описание
		if m := reCapBacktick.FindStringSubmatch(trimmed); m != nil {
			slug := normalizeSlug(m[1])
			if slug != "" {
				slugs = append(slugs, slug)
			}
		}
	}
	return slugs
}

var reMDSpecLink = regexp.MustCompile(`\]\((?:\.\./)+(?:specs/)?([A-Za-z0-9_.\-/]+)/spec\.md\)`)

func normalizeSlug(raw string) string {
	slug := strings.ReplaceAll(strings.TrimSpace(raw), `\`, "/")
	slug = strings.TrimSuffix(slug, "/")
	slug = strings.TrimPrefix(slug, "specs/")
	if slug == "" || strings.Contains(slug, "..") {
		return ""
	}
	return slug
}

// ParsedDelta — delta-требование из секции ## ADDED/MODIFIED/REMOVED Requirements.
type ParsedDelta struct {
	Section         string // ADDED | MODIFIED | REMOVED
	RequirementName string
	BodyText        string
	LineStart       int
	LineEnd         int
}

var reDeltaSection = regexp.MustCompile(`^##\s*(ADDED|MODIFIED|REMOVED)\s+Requirements?\s*$`)

// ParseDeltaSpec разбирает delta-спеку change'а: секции
// "## ADDED/MODIFIED/REMOVED Requirements" с "### Requirement:" блоками.
// Тело требования — текст до первого "#### Scenario:" (сценарии delta
// не хранятся отдельными сущностями; тело включает их текст если сценариев нет).
func ParseDeltaSpec(content string) []ParsedDelta {
	lines := splitLines(content)
	var result []ParsedDelta

	section := ""
	var delta *ParsedDelta

	finish := func(lineEnd int) {
		if delta != nil {
			delta.LineEnd = lineEnd
			delta.BodyText = strings.TrimSpace(delta.BodyText)
			if delta.RequirementName != "" && section != "" {
				result = append(result, *delta)
			}
			delta = nil
		}
	}

	for i, line := range lines {
		lineNo := i + 1

		if m := reDeltaSection.FindStringSubmatch(line); m != nil {
			finish(lineNo - 1)
			section = strings.ToUpper(m[1])
			continue
		}
		if m := reRequirementHeader.FindStringSubmatch(line); m != nil {
			finish(lineNo - 1)
			if section != "" {
				delta = &ParsedDelta{
					Section:         section,
					RequirementName: strings.TrimSpace(m[1]),
					LineStart:       lineNo,
				}
			}
			continue
		}
		if m := reScenarioHeader.FindStringSubmatch(line); m != nil {
			// Начало сценария внутри delta-требования: тело закончилось
			if delta != nil && delta.BodyText == "" {
				// сценарий без тела требования — фиксируем пустое тело
				finish(lineNo - 1)
			} else if delta != nil {
				finish(delta.LineStart)
				// пропускаем строки сценарариев: они не попадают в body
				continue
			}
			continue
		}
		if delta != nil && strings.TrimSpace(line) != "" {
			delta.BodyText += strings.TrimSpace(line) + "\n"
		}
	}
	finish(len(lines))

	return result
}

// ParseSkipSpecsYAML определяет, помечен ли change маркером skip_specs.
func ParseSkipSpecsYAML(content string) bool {
	for _, line := range splitLines(content) {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "skip_specs:") {
			value := strings.TrimSpace(strings.TrimPrefix(trimmed, "skip_specs:"))
			return value == "true"
		}
	}
	return false
}
