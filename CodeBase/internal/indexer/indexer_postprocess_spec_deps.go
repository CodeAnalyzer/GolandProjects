package indexer

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	codeencoding "github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/parser/openspecmd"
	"github.com/codebase/internal/store"
)

// postProcessSpecDependencies строит relations:
//   - depends_on_capability (5 маркеров: markdown-ссылка, "Связан с доменами",
//     inline "(см. `slug`)", cci:-хвост, notes-ссылка)
//   - change_modifies (из delta-файлов и proposal-извлечений)
func (idx *Indexer) postProcessSpecDependencies(ctx context.Context, collector *statsCollector) {
	// 1. Загружаем все capabilities с их текстами
	caps, err := idx.db.LoadAllSpecCapabilitiesForDeps(ctx)
	if err != nil {
		idx.logError("<post-processing>", "Error loading spec_capabilities for deps: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	if len(caps) == 0 {
		return
	}
	if err := idx.ensureSpecCapabilityHierarchy(ctx, caps); err != nil {
		idx.logError("<post-processing>", "Error building spec capability hierarchy: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	caps, err = idx.db.LoadAllSpecCapabilitiesForDeps(ctx)
	if err != nil {
		idx.logError("<post-processing>", "Error reloading spec_capabilities: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	// 2. Строим slug → capabilityID lookup
	slugToID := map[string]int64{}
	for _, c := range caps {
		slugToID[specSlugKey(c.SpecConfigID, c.CapabilityName)] = c.ID
	}

	// 3. Извлекаем depends_on_capability из текстов
	var depRelations []*model.Relation
	depSeen := map[string]struct{}{}
	for _, c := range caps {
		deps := extractCapabilityDeps(c, slugToID)
		for _, dep := range deps {
			dedupKey := fmt.Sprintf("spec_capability|%d|spec_capability|%d|depends_on_capability",
				c.ID, dep.TargetID)
			if _, exists := depSeen[dedupKey]; exists {
				continue
			}
			depSeen[dedupKey] = struct{}{}
			depRelations = append(depRelations, dep)
		}
	}

	usecaseRefs, err := idx.db.LoadSpecUsecaseRefs(ctx)
	if err != nil {
		idx.logError("<post-processing>", "Error loading usecase spec refs: %v", err)
	}
	usecaseTexts, err := idx.db.LoadSpecUsecaseTexts(ctx)
	if err != nil {
		idx.logError("<post-processing>", "Error loading usecase texts: %v", err)
	}
	for _, usecase := range usecaseTexts {
		text := usecase.Text
		data, readErr := codeencoding.ReadFileBytes(usecase.Path)
		if readErr == nil {
			decoded, decodeErr := codeencoding.DecodeBytes(data, codeencoding.DetectMarkdownEncoding(data))
			if decodeErr == nil {
				text = decoded
			}
		}
		for slug := range openspecmd.ExtractSpecReferences(text) {
			usecaseRefs = append(usecaseRefs, store.SpecUsecaseRefRow{UsecaseID: usecase.UsecaseID, SpecConfigID: usecase.SpecConfigID, Slug: slug})
		}
	}
	usecaseRelations := buildUsecaseInvolvesRelations(usecaseRefs, slugToID)

	// 4. Загружаем change deltas и proposal references для change_modifies
	changeRelations, err := idx.buildChangeModifiesRelations(ctx, slugToID)
	if err != nil {
		idx.logError("<post-processing>", "Error building change_modifies: %v", err)
	}

	allRelations := append(depRelations, usecaseRelations...)
	allRelations = append(allRelations, changeRelations...)

	// 5. Удаляем старые spec dependency relations
	if err := idx.db.DeleteSpecDependencyRelations(ctx); err != nil {
		idx.logError("<post-processing>", "Error deleting old spec dependency relations: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}

	localStats := &model.ScanStats{}
	if err := idx.saveRelations(ctx, allRelations, "<spec-deps>", localStats); err != nil {
		collector.Add(func(stats *model.ScanStats) { mergeScanStats(stats, localStats) })
		return
	}
	collector.Add(func(stats *model.ScanStats) { mergeScanStats(stats, localStats) })
}

func (idx *Indexer) ensureSpecCapabilityHierarchy(ctx context.Context, caps []*model.SpecCapability) error {
	for _, capability := range caps {
		var parentID int64
		for _, ancestor := range openspecmd.CapabilityAncestors(capability.CapabilityName) {
			id, err := idx.db.GetOrCreateSpecCapabilityContainer(ctx, capability.SpecConfigID, capability.DsProductID, parentID, ancestor)
			if err != nil {
				return err
			}
			parentID = id
		}
		if err := idx.db.SetSpecCapabilityParent(ctx, capability.ID, parentID); err != nil {
			return err
		}
	}
	return nil
}

// extractCapabilityDeps извлекает depends_on_capability из текстов capability.
// 5 маркеров:
// 1. markdown-ссылки на другие spec.md → slug
// 2. "Связан с доменами: `slug1`, `slug2`"
// 3. inline "(см. `calc-flow`)" — slug по префиксу
// 4. cci:-хвост в имени capability (cci:dependency-slug)
// 5. notes/purpose содержат `../specs/<slug>/spec.md`
func extractCapabilityDeps(cap *model.SpecCapability, slugToID map[string]int64) []*model.Relation {
	var relations []*model.Relation
	combinedText := cap.Purpose + "\n" + cap.Notes + "\n" + cap.RelatedCode

	// 1. markdown-ссылки: ](../specs/<slug>/spec.md)
	for _, m := range reMDSpecLinkDeps.FindAllStringSubmatch(combinedText, -1) {
		slug := normalizeSlugDep(m[1])
		targetID := resolveSlug(slug, cap.CapabilityName, cap.SpecConfigID, slugToID)
		if targetID > 0 {
			relations = append(relations, &model.Relation{
				SourceType:   "spec_capability",
				SourceID:     cap.ID,
				TargetType:   "spec_capability",
				TargetID:     targetID,
				RelationType: "depends_on_capability",
				Confidence:   "explicit",
			})
		}
	}

	// 2. "Связан с доменами: `slug1`, `slug2`"
	if m := reLinkedDomains.FindStringSubmatch(combinedText); m != nil {
		for _, slugRaw := range reBacktickSlugs.FindAllString(m[1], -1) {
			slug := strings.Trim(slugRaw, "`")
			targetID := resolveSlug(slug, cap.CapabilityName, cap.SpecConfigID, slugToID)
			if targetID > 0 {
				relations = append(relations, &model.Relation{
					SourceType:   "spec_capability",
					SourceID:     cap.ID,
					TargetType:   "spec_capability",
					TargetID:     targetID,
					RelationType: "depends_on_capability",
					Confidence:   "notes",
				})
			}
		}
	}

	// 3. inline "(см. `calc-flow`)" — slug по префиксу
	for _, m := range reSeeAlso.FindAllStringSubmatch(combinedText, -1) {
		slug := m[1]
		targetID := resolveSlug(slug, cap.CapabilityName, cap.SpecConfigID, slugToID)
		if targetID > 0 {
			relations = append(relations, &model.Relation{
				SourceType:   "spec_capability",
				SourceID:     cap.ID,
				TargetType:   "spec_capability",
				TargetID:     targetID,
				RelationType: "depends_on_capability",
				Confidence:   "inline",
			})
		}
	}

	// 4. cci:-хвост в имени capability
	if idx := strings.Index(strings.ToLower(cap.CapabilityName), "cci:"); idx >= 0 {
		cciPart := cap.CapabilityName[idx+4:]
		for _, slug := range strings.Split(cciPart, ",") {
			slug = strings.TrimSpace(slug)
			targetID := resolveSlug(slug, cap.CapabilityName, cap.SpecConfigID, slugToID)
			if targetID > 0 {
				relations = append(relations, &model.Relation{
					SourceType:   "spec_capability",
					SourceID:     cap.ID,
					TargetType:   "spec_capability",
					TargetID:     targetID,
					RelationType: "depends_on_capability",
					Confidence:   "explicit",
				})
			}
		}
	}

	// 5. Извлекаем из ExtractSpecReferences (переиспользуем парсер)
	for slug := range openspecmd.ExtractSpecReferences(combinedText) {
		targetID := resolveSlug(slug, cap.CapabilityName, cap.SpecConfigID, slugToID)
		if targetID > 0 {
			dedupKey := fmt.Sprintf("spec_capability|%d|spec_capability|%d|depends_on_capability",
				cap.ID, targetID)
			// Проверяем что ещё не добавили
			alreadyExists := false
			for _, r := range relations {
				if r.SourceID == cap.ID && r.TargetID == targetID {
					alreadyExists = true
					break
				}
			}
			if !alreadyExists {
				relations = append(relations, &model.Relation{
					SourceType:   "spec_capability",
					SourceID:     cap.ID,
					TargetType:   "spec_capability",
					TargetID:     targetID,
					RelationType: "depends_on_capability",
					Confidence:   "notes",
				})
			}
			_ = dedupKey
		}
	}

	return relations
}

// resolveSlug находит capability ID по slug, пытаясь резолвить по полному пути
// и по sibling-префиксу (если slug короткий, пробуем добавить префикс родителя).
func buildUsecaseInvolvesRelations(refs []store.SpecUsecaseRefRow, slugToID map[string]int64) []*model.Relation {
	var relations []*model.Relation
	seen := map[string]struct{}{}
	for _, ref := range refs {
		capID := resolveSlug(ref.Slug, "", ref.SpecConfigID, slugToID)
		if capID == 0 {
			continue
		}
		key := fmt.Sprintf("%d|%d", ref.UsecaseID, capID)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType: "spec_usecase", SourceID: ref.UsecaseID,
			TargetType: "spec_capability", TargetID: capID,
			RelationType: "usecase_involves", Confidence: "explicit",
		})
	}
	return relations
}

func specSlugKey(specConfigID int64, slug string) string {
	return fmt.Sprintf("%d|%s", specConfigID, strings.ToLower(slug))
}

func resolveSlug(slug string, sourceSlug string, specConfigID int64, slugToID map[string]int64) int64 {
	if slug == "" {
		return 0
	}
	slugLower := strings.ToLower(slug)
	// Прямой lookup
	if id, ok := slugToID[specSlugKey(specConfigID, slugLower)]; ok {
		if id == 0 {
			return 0
		}
		// Не ссылаемся на себя
		if strings.ToLower(sourceSlug) == slugLower {
			return 0
		}
		return id
	}
	// Sibling-резолв: если slug короткий (один сегмент), пробуем с префиксом родителя
	if !strings.Contains(slug, "/") {
		parts := strings.Split(sourceSlug, "/")
		if len(parts) > 1 {
			parentPrefix := strings.Join(parts[:len(parts)-1], "/")
			fullSlug := parentPrefix + "/" + slug
			if id, ok := slugToID[specSlugKey(specConfigID, fullSlug)]; ok && id != 0 {
				return id
			}
		}
	}
	return 0
}

// buildChangeModifiesRelations строит relations change_modifies из delta-файлов
// и proposal-извлечений.
func (idx *Indexer) buildChangeModifiesRelations(ctx context.Context, slugToID map[string]int64) ([]*model.Relation, error) {
	// Загружаем все change deltas с их change_id и capability_slug
	deltas, err := idx.db.LoadAllSpecChangeDeltasForModifies(ctx)
	if err != nil {
		return nil, err
	}

	// Загружаем proposal references (из tasks.md/design.md, извлечённые при индексации)
	proposals, err := idx.db.LoadSpecChangeProposalSlugs(ctx)
	if err != nil {
		idx.logError("<post-processing>", "Error loading change proposal refs: %v", err)
	}

	var relations []*model.Relation
	seen := map[string]struct{}{}

	// Из delta-файлов: change → capability (по capability_slug)
	for _, d := range deltas {
		capID := resolveSlug(d.CapabilitySlug, "", d.SpecConfigID, slugToID)
		if capID == 0 {
			continue
		}
		dedupKey := fmt.Sprintf("spec_change|%d|spec_capability|%d|change_modifies",
			d.ChangeID, capID)
		if _, exists := seen[dedupKey]; exists {
			continue
		}
		seen[dedupKey] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "spec_change",
			SourceID:     d.ChangeID,
			TargetType:   "spec_capability",
			TargetID:     capID,
			RelationType: "change_modifies",
			Confidence:   "delta",
		})
	}

	// Из proposal references (skip_specs changes)
	for _, p := range proposals {
		for slug := range p.References {
			capID := resolveSlug(slug, "", p.SpecConfigID, slugToID)
			if capID == 0 {
				continue
			}
			dedupKey := fmt.Sprintf("spec_change|%d|spec_capability|%d|change_modifies",
				p.ChangeID, capID)
			if _, exists := seen[dedupKey]; exists {
				continue
			}
			seen[dedupKey] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "spec_change",
				SourceID:     p.ChangeID,
				TargetType:   "spec_capability",
				TargetID:     capID,
				RelationType: "change_modifies",
				Confidence:   "proposal",
			})
		}
	}

	return relations, nil
}

// SpecChangeDeltaRefRow — минимальные данные delta для change_modifies.
type SpecChangeDeltaRefRow struct {
	ChangeID       int64
	CapabilitySlug string
}

// SpecChangeProposalRef — change_id + извлечённые references из proposal/meta.
type SpecChangeProposalRef struct {
	ChangeID   int64
	References map[string]struct{}
}

var (
	reMDSpecLinkDeps = regexp.MustCompile(`\]\((?:\.\./)+(?:specs/)?([A-Za-z0-9_.\-/]+)/spec\.md\)`)
	reLinkedDomains  = regexp.MustCompile(`(?i)связан\s+с\s+доменами?\s*:\s*(.+)`)
	reBacktickSlugs  = regexp.MustCompile("`[^`]+`")
	reSeeAlso        = regexp.MustCompile(`(?i)\(см\.\s*\x60([A-Za-z0-9_\-/]+)\x60\)`)
)

func normalizeSlugDep(raw string) string {
	slug := strings.ReplaceAll(strings.TrimSpace(raw), `\`, "/")
	slug = strings.TrimSuffix(slug, "/")
	slug = strings.TrimPrefix(slug, "specs/")
	if slug == "" || strings.Contains(slug, "..") {
		return ""
	}
	return slug
}
