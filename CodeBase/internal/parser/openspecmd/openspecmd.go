// Package openspecmd парсит OpenSpec-артефакты финпродуктов (FA): спецификации
// (specs/**/spec.md), usecase-слои (scenarios/, usecases/, business-processes/),
// changes (proposal/tasks/design/delta) и config.yaml openspec-корня.
//
// Классификация файла позиционная — по сегментам пути относительно openspec-корня
// (директория openspec|OpenSpec, регистр не значим). Парсер spec.md не опирается
// на H1 или заголовок "## Requirements": структурные якоря — только
// "### Requirement:" и "#### Scenario:".
package openspecmd

import (
	"path/filepath"
	"strings"
)

// Kind — класс артефакта внутри openspec-корня.
type Kind string

const (
	KindNone           Kind = ""                // вне openspec-корня
	KindConfig         Kind = "config"          // openspec/config.yaml
	KindSpec           Kind = "spec"            // specs/**/spec.md
	KindUsecase        Kind = "usecase"         // scenarios|usecases|business-processes/**.md
	KindUsecaseIndex   Kind = "usecase-index"   // scenarios|.../INDEX.md
	KindChangeProposal Kind = "change-proposal" // changes/<name>/proposal.md
	KindChangeMeta     Kind = "change-meta"     // changes/<name>/{tasks,design}.md|.openspec.yaml|прочее
	KindChangeDelta    Kind = "change-delta"    // changes/<name>/specs/**/spec.md
	KindOther          Kind = "other"           // прочее внутри корня (README, AGENTS.md, ...)
)

// Classified — результат позиционной классификации файла.
type Classified struct {
	Kind Kind
	// RootDir — openspec-корень (путь до директории openspec|OpenSpec включительно, slash);
	// пуст для KindNone.
	RootDir string
	// Slug — полный путь capability относительно specs/ (KindSpec, KindChangeDelta).
	Slug string
	// SourceDir — scenarios | usecases | business-processes (KindUsecase, KindUsecaseIndex).
	SourceDir string
	// ChangeName — имя change-директории (KindChange*).
	ChangeName string
	// Archived — change лежит в changes/archive/.
	Archived bool
}

// ClassifyPath классифицирует файл по абсолютному или относительному пути.
func ClassifyPath(path string) Classified {
	normalized := filepath.ToSlash(strings.TrimSpace(path))
	if normalized == "" {
		return Classified{Kind: KindNone}
	}
	segments := strings.Split(normalized, "/")

	// Ищем директорию openspec|OpenSpec (case-insensitive).
	rootIdx := -1
	for i, seg := range segments[:len(segments)-1] {
		if strings.EqualFold(seg, "openspec") {
			rootIdx = i
			break
		}
	}
	if rootIdx < 0 {
		return Classified{Kind: KindNone}
	}
	root := strings.Join(segments[:rootIdx+1], "/")
	rest := segments[rootIdx+1:]
	if len(rest) == 0 {
		return Classified{Kind: KindNone}
	}

	base := rest[len(rest)-1]
	baseLower := strings.ToLower(base)

	// openspec/config.yaml
	if len(rest) == 1 && baseLower == "config.yaml" {
		return Classified{Kind: KindConfig, RootDir: root}
	}

	switch {
	case strings.EqualFold(rest[0], "specs") && baseLower == "spec.md" && len(rest) >= 3:
		// specs/<a>/<b>/.../spec.md → slug = <a>/<b>/...
		slug := strings.Join(rest[1:len(rest)-1], "/")
		return Classified{Kind: KindSpec, RootDir: root, Slug: slug}

	case isUsecaseDir(rest[0]) && strings.HasSuffix(baseLower, ".md"):
		if baseLower == "index.md" {
			return Classified{Kind: KindUsecaseIndex, RootDir: root, SourceDir: rest[0]}
		}
		return Classified{Kind: KindUsecase, RootDir: root, SourceDir: rest[0]}

	case strings.EqualFold(rest[0], "changes") && len(rest) >= 2:
		return classifyChangePath(root, rest)

	default:
		return Classified{Kind: KindOther, RootDir: root}
	}
}

// CapabilityAncestors возвращает промежуточные slug от корня к листу.
func CapabilityAncestors(slug string) []string {
	normalized := strings.ReplaceAll(filepath.ToSlash(slug), `\`, "/")
	parts := strings.Split(strings.Trim(normalized, "/"), "/")
	if len(parts) < 2 {
		return nil
	}
	ancestors := make([]string, 0, len(parts)-1)
	for i := 1; i < len(parts); i++ {
		ancestors = append(ancestors, strings.Join(parts[:i], "/"))
	}
	return ancestors
}

func isUsecaseDir(seg string) bool {
	return strings.EqualFold(seg, "scenarios") ||
		strings.EqualFold(seg, "usecases") ||
		strings.EqualFold(seg, "business-processes")
}

// classifyChangePath разбирает путь внутри changes/: активные changes/<name>/...
// и архивные changes/archive/<name>/...
func classifyChangePath(root string, rest []string) Classified {
	name := ""
	file := []string{}
	archived := false
	if len(rest) >= 3 && strings.EqualFold(rest[1], "archive") {
		// changes/archive/<name>/<files...>
		if len(rest) < 4 {
			return Classified{Kind: KindOther, RootDir: root}
		}
		archived = true
		name = rest[2]
		file = rest[3:]
	} else {
		// changes/<name>/<files...>
		if len(rest) < 3 {
			// сама директория changes/ без имени — не артефакт
			return Classified{Kind: KindOther, RootDir: root}
		}
		name = rest[1]
		file = rest[2:]
	}

	fileLower := strings.ToLower(file[len(file)-1])
	switch {
	case fileLower == "proposal.md":
		return Classified{Kind: KindChangeProposal, RootDir: root, ChangeName: name, Archived: archived}

	case len(file) >= 3 && strings.EqualFold(file[0], "specs") && fileLower == "spec.md":
		// changes/<name>/specs/<cap>/spec.md → delta по capability
		slug := strings.Join(file[1:len(file)-1], "/")
		return Classified{Kind: KindChangeDelta, RootDir: root, ChangeName: name, Archived: archived, Slug: slug}

	case fileLower == "tasks.md" || fileLower == "design.md" || fileLower == ".openspec.yaml" ||
		fileLower == "readme.md" || strings.HasSuffix(fileLower, ".md") || strings.HasSuffix(fileLower, ".yaml"):
		return Classified{Kind: KindChangeMeta, RootDir: root, ChangeName: name, Archived: archived}

	default:
		return Classified{Kind: KindOther, RootDir: root, ChangeName: name, Archived: archived}
	}
}
