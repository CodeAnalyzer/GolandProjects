package openspecmd

import (
	"regexp"
	"strings"
)

// Mention — сыров упоминание код-сущности в тексте спеки.
type Mention struct {
	Name string
	Kind string // procedure | api | table | form | smf | method | unknown
	Line int    // 1-based строка в исходном файле
}

// Регэкспы распознавания технических идентификаторов Diasoft.
var (
	reBacktickToken = regexp.MustCompile("`([^`]+)`")
	reAPIName       = regexp.MustCompile(`\bAPI_[A-Za-z0-9_/]+`)
	reFCDName       = regexp.MustCompile(`\bFCD_[A-Za-z0-9_]+`)
	reTableName     = regexp.MustCompile(`\b[pt][A-Z][A-Za-z0-9_]+`)
	rePathSQL       = regexp.MustCompile(`\b[A-Za-z0-9_/\\]+\.sql\b`)
	rePathSMF       = regexp.MustCompile(`\b[A-Za-z0-9_/\\]+\.smf\b`)
	rePathDFM       = regexp.MustCompile(`\b[A-Za-z0-9_/\\]+\.dfm\b`)
	rePathPAS       = regexp.MustCompile(`\b[A-Za-z0-9_/\\]+\.pas\b`)
	reUnderscoreIdent = regexp.MustCompile(`\b[A-Z][A-Za-z0-9]*(?:_[A-Za-z0-9]+)+\b`)
)

// Стоп-слова: обиходные термины, не являющиеся код-сущностями.
var mentionStopWords = map[string]struct{}{
	"confluence": {}, "diasoft": {}, "диасофт": {}, "shall": {}, "given": {},
	"when": {}, "then": {}, "and": {}, "bpmn": {}, "camunda": {}, "modeler": {},
	"req": {}, "scenario": {}, "spec": {}, "spec_md": {}, "readme": {},
	"openspec": {}, "git": {}, "github": {}, "req_nnn": {},
}

// ExtractMentionsFromRelatedCode извлекает упоминания из секции Related code
// (включая подсекции ###): буллеты с путями, бэктиками и bare-идентификаторами.
func ExtractMentionsFromRelatedCode(relatedCode string) []Mention {
	return extractMentions(relatedCode, true)
}

// ExtractMentionsInline извлекает упоминания из произвольного текста спеки
// (тела требований, сценарии, delta): только бэктики и однозначные маркеры
// (API_*/FCD_*/t-таблицы/пути файлов), чтобы не ловить шум.
func ExtractMentionsInline(text string) []Mention {
	return extractMentions(text, false)
}

func extractMentions(text string, bareIdentifiers bool) []Mention {
	lines := splitLines(text)
	var result []Mention
	seen := map[string]struct{}{}

	add := func(name string, kind string, line int) {
		if name == "" {
			return
		}
		if _, isStop := mentionStopWords[strings.ToLower(name)]; isStop {
			return
		}
		key := kind + "|" + name
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		result = append(result, Mention{Name: name, Kind: kind, Line: line})
	}

	for i, line := range lines {
		lineNo := i + 1

		// Бэктики: главный источник имён
		for _, m := range reBacktickToken.FindAllStringSubmatch(line, -1) {
			name, kind, ok := classifyMention(m[1])
			if ok {
				add(name, kind, lineNo)
			}
		}

		// Однозначные маркеры вне бэктиков (всегда безопасны)
		for _, m := range reAPIName.FindAllString(line, -1) {
			name, kind, ok := classifyMention(m)
			if ok {
				add(name, kind, lineNo)
			}
		}
		for _, m := range reFCDName.FindAllString(line, -1) {
			name, kind, ok := classifyMention(m)
			if ok {
				add(name, kind, lineNo)
			}
		}
		for _, m := range reTableName.FindAllString(line, -1) {
			name, kind, ok := classifyMention(m)
			if ok {
				add(name, kind, lineNo)
			}
		}
		for _, m := range rePathSQL.FindAllString(line, -1) {
			name, kind, ok := classifyMention(m)
			if ok {
				add(name, kind, lineNo)
			}
		}
		for _, m := range rePathSMF.FindAllString(line, -1) {
			name, kind, ok := classifyMention(m)
			if ok {
				add(name, kind, lineNo)
			}
		}
		for _, m := range rePathDFM.FindAllString(line, -1) {
			name, kind, ok := classifyMention(m)
			if ok {
				add(name, kind, lineNo)
			}
		}
		for _, m := range rePathPAS.FindAllString(line, -1) {
			name, kind, ok := classifyMention(m)
			if ok {
				add(name, kind, lineNo)
			}
		}

		// bare-идентификаторы с подчёркиваниями — только для Related code
		// (там буллеты без бэктиков: "CardLimit_proc.sql — обработка")
		if bareIdentifiers {
			for _, m := range reUnderscoreIdent.FindAllString(line, -1) {
				name, kind, ok := classifyMention(m)
				if ok {
					add(name, kind, lineNo)
				}
			}
		}
	}

	return result
}

// classifyMention определяет имя и kind цели по сырому токену.
// Пути (Cards/SERVER/Card/CardLimit_proc.sql) сводятся к последнему сегменту
// без расширения; расширения файлов определяют kind цели.
func classifyMention(raw string) (string, string, bool) {
	token := strings.TrimSpace(raw)
	if token == "" || strings.ContainsAny(token, " \t") && !strings.Contains(token, ".") {
		// многословные бэктики (кроме путей с расширением) — не идентификаторы
		return "", "", false
	}

	// Путь → последний сегмент
	if strings.ContainsAny(token, `/\`) {
		idx := strings.LastIndexAny(token, `/\`)
		token = token[idx+1:]
	}

	lower := strings.ToLower(token)

	switch {
	case strings.HasSuffix(lower, ".sql"):
		return strings.TrimSuffix(token, ".sql"), "procedure", true
	case strings.HasSuffix(lower, ".smf"):
		return strings.TrimSuffix(token, ".smf"), "smf", true
	case strings.HasSuffix(lower, ".dfm"):
		return strings.TrimSuffix(token, ".dfm"), "form", true
	case strings.HasSuffix(lower, ".pas"):
		return strings.TrimSuffix(token, ".pas"), "method", true
	case strings.HasPrefix(lower, "api_"):
		return token, "api", true
	case strings.HasPrefix(lower, "fcd_"):
		return token, "procedure", true
	}

	// bare идентификаторы после трима пути
	if len(token) < 4 {
		return "", "", false
	}

	// t-таблицы / p-таблицы (в т.ч. pCard_Buf_TmpTbl)
	if reTableName.MatchString(token) {
		return token, "table", true
	}

	// Слова с подчёркиваниями (CardLimit_proc, CON_STP_MassAccrual) → процедура
	if reUnderscoreIdent.MatchString(token) && !strings.ContainsAny(token, `-.:;()«»"'`) {
		return token, "procedure", true
	}

	if bareAllowed(token) {
		return token, "unknown", true
	}
	return "", "", false
}

// bareAllowed — правдоподобный идентификатор, не подошедший ни под один kind:
// сохраняем с kind unknown, только если это латинский токен с подчёркиваниями/верблюжьей нотацией.
func bareAllowed(token string) bool {
	for _, r := range token {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return false
	}
	return strings.Contains(token, "_") || (len(token) >= 6 && token == strings.ToUpper(token))
}
