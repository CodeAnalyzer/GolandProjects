package indexer

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	codeencoding "github.com/codebase/internal/encoding"
	"github.com/codebase/internal/fswalk"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/parser/openspecmd"
)

// sanitizeUTF8Lookup нормализует строку для lookup в map, ключи которого
// были сохранены через sanitizeUTF8String (strings.ToValidUTF8) в БД.
func sanitizeUTF8Lookup(s string) string {
	return strings.ToValidUTF8(s, "")
}

// parseMDFile парсит markdown-файл: если он внутри openspec-корня — извлекает
// spec-сущности; иначе — no-op (файл уже сохранён в files).
func (idx *Indexer) parseMDFile(ctx context.Context, file fswalk.FileInfo, fileID int64, stats *model.ScanStats) error {
	classified := openspecmd.ClassifyPath(file.RelPath)
	if classified.Kind == openspecmd.KindNone {
		// Не openspec — ничего не делаем, файл уже в files
		return nil
	}
	validRoot, err := validateArtifactRoot(file.Path)
	if err != nil {
		return err
	}
	if !validRoot {
		return nil
	}

	content, err := decodeIndexedFileContent(file)
	if err != nil {
		return fmt.Errorf("decode md: %w", err)
	}
	content = codeencoding.NormalizeMojibake(content)

	switch classified.Kind {
	case openspecmd.KindConfig:
		return idx.parseSpecConfigYAML(ctx, file, fileID, content, stats)
	case openspecmd.KindSpec:
		return idx.parseSpecMD(ctx, file, fileID, content, classified, stats)
	case openspecmd.KindUsecase, openspecmd.KindUsecaseIndex:
		return idx.parseUsecaseMD(ctx, file, fileID, content, classified, stats)
	case openspecmd.KindChangeProposal:
		return idx.parseChangeProposal(ctx, file, fileID, content, classified, stats)
	case openspecmd.KindChangeDelta:
		return idx.parseChangeDelta(ctx, file, fileID, content, classified, stats)
	case openspecmd.KindChangeMeta:
		return idx.parseChangeMeta(ctx, file, fileID, content, classified, stats)
	default:
		return nil
	}
}

// parseYAMLFile парсит YAML-файл: config.yaml openspec-корня или .openspec.yaml change'а.
func (idx *Indexer) parseYAMLFile(ctx context.Context, file fswalk.FileInfo, fileID int64, stats *model.ScanStats) error {
	classified := openspecmd.ClassifyPath(file.RelPath)
	if classified.Kind == openspecmd.KindNone || classified.Kind == openspecmd.KindOther {
		return nil
	}
	validRoot, err := validateArtifactRoot(file.Path)
	if err != nil {
		return err
	}
	if !validRoot {
		return nil
	}

	content, err := decodeIndexedFileContent(file)
	if err != nil {
		return fmt.Errorf("decode yaml: %w", err)
	}

	if classified.Kind == openspecmd.KindConfig {
		return idx.parseSpecConfigYAML(ctx, file, fileID, content, stats)
	}
	if classified.Kind == openspecmd.KindChangeMeta {
		return idx.parseChangeMeta(ctx, file, fileID, content, classified, stats)
	}
	return nil
}

func validateArtifactRoot(path string) (bool, error) {
	classified := openspecmd.ClassifyPath(path)
	if classified.RootDir == "" {
		return false, nil
	}
	ok, err := openspecmd.ValidateSpecDrivenRoot(classified.RootDir)
	if err != nil {
		return false, fmt.Errorf("validate openspec root %q: %w", classified.RootDir, err)
	}
	return ok, nil
}

// resolveSpecConfigID получает или создаёт spec_config для файла.
func (idx *Indexer) resolveSpecConfigID(ctx context.Context, file fswalk.FileInfo, fileID int64) (int64, error) {
	classified := openspecmd.ClassifyPath(file.RelPath)
	productName := extractCanonicalDSProductName(file.Path, file.RelPath)
	var dsProductID int64
	if productName != "" {
		pid, err := idx.resolveDSProductID(ctx, productName)
		if err != nil {
			return 0, err
		}
		dsProductID = pid
	}
	return idx.db.GetOrCreateSpecConfig(ctx, fileID, dsProductID, productName, classified.RootDir)
}

// parseSpecConfigYAML обрабатывает config.yaml openspec-корня.
func (idx *Indexer) parseSpecConfigYAML(ctx context.Context, file fswalk.FileInfo, fileID int64, content string, stats *model.ScanStats) error {
	schemaName, contextText, ok := openspecmd.ParseConfigYAML(content)
	if !ok {
		return nil // не spec-driven — пропускаем
	}

	productName := extractCanonicalDSProductName(file.Path, file.RelPath)
	configID, err := idx.resolveSpecConfigID(ctx, file, fileID)
	if err != nil {
		return err
	}

	if err := idx.db.UpdateSpecConfig(ctx, configID, fileID, schemaName, productName, contextText); err != nil {
		return err
	}
	stats.SpecConfigs++
	return nil
}

// parseSpecMD обрабатывает specs/**/spec.md.
func (idx *Indexer) parseSpecMD(ctx context.Context, file fswalk.FileInfo, fileID int64, content string, classified openspecmd.Classified, stats *model.ScanStats) error {
	parsed := openspecmd.ParseSpecFile(content)

	configID, err := idx.resolveSpecConfigID(ctx, file, fileID)
	if err != nil {
		return err
	}

	productName := extractCanonicalDSProductName(file.Path, file.RelPath)
	var dsProductID int64
	if productName != "" {
		dsProductID, _ = idx.resolveDSProductID(ctx, productName)
	}

	var parentID int64
	for _, ancestor := range openspecmd.CapabilityAncestors(classified.Slug) {
		parentID, err = idx.db.GetOrCreateSpecCapabilityContainer(ctx, configID, dsProductID, parentID, ancestor)
		if err != nil {
			return err
		}
	}

	cap := &model.SpecCapability{
		FileID:         fileID,
		SpecConfigID:   configID,
		DsProductID:    dsProductID,
		ParentID:       parentID,
		CapabilityName: classified.Slug,
		Title:          parsed.Title,
		Purpose:        parsed.Purpose,
		Notes:          parsed.Notes,
		RelatedCode:    parsed.RelatedCode,
		LineStart:      parsed.LineStart,
		LineEnd:        parsed.LineEnd,
	}

	capID, err := idx.db.UpsertSpecCapability(ctx, cap)
	if err != nil {
		return err
	}

	// Requirements + Scenarios
	var reqs []*model.SpecRequirement
	var scns []*model.SpecScenario
	var symbolsBatch []*model.Symbol
	for _, r := range parsed.Requirements {
		req := &model.SpecRequirement{
			FileID:          fileID,
			CapabilityID:    capID,
			RequirementName: r.Name,
			BodyText:        r.BodyText,
			LineStart:       r.LineStart,
			LineEnd:         r.LineEnd,
			ReqOrder:        r.Order,
		}
		reqs = append(reqs, req)
		symbolsBatch = append(symbolsBatch, &model.Symbol{
			FileID:     fileID,
			SymbolName: r.Name,
			SymbolType: "spec_requirement",
			EntityType: "spec",
			LineNumber: r.LineStart,
			Signature:  r.Name,
		})
		for _, s := range r.Scenarios {
			scns = append(scns, &model.SpecScenario{
				FileID:        fileID,
				RequirementID: 0, // будет заполнено после batch insert
				ScenarioName:  s.Name,
				GivenText:     s.Given,
				WhenText:      s.When,
				ThenText:      s.Then,
				LineStart:     s.LineStart,
				LineEnd:       s.LineEnd,
				ScnOrder:      s.Order,
			})
		}
	}

	reqIDs := map[string]int64{}
	if len(reqs) > 0 {
		if err := idx.db.BatchInsertSpecRequirements(ctx, reqs, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
		reqIDs, err = idx.db.FindSpecRequirementIDsByFile(ctx, fileID)
		if err != nil {
			return fmt.Errorf("find spec_requirement ids: %w", err)
		}
		// Заполняем requirement_id в сценариях
		scnOffset := 0
		for _, r := range parsed.Requirements {
			reqID, ok := reqIDs[sanitizeUTF8Lookup(r.Name)]
			if !ok {
				scnOffset += len(r.Scenarios)
				continue
			}
			for j := range r.Scenarios {
				idx := scnOffset + j
				if idx < len(scns) {
					scns[idx].RequirementID = reqID
				}
			}
			scnOffset += len(r.Scenarios)
		}
		// Обновляем entity_id в symbols для requirements
		for _, sym := range symbolsBatch {
			if sym.SymbolType == "spec_requirement" {
				sym.EntityID = reqIDs[sanitizeUTF8Lookup(sym.SymbolName)]
			}
		}
	}

	scenarioIDs := map[int64]map[int]int64{}
	if len(scns) > 0 {
		if err := idx.db.BatchInsertSpecScenarios(ctx, scns, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
		scenarioIDs, err = idx.db.FindSpecScenarioIDsByFile(ctx, fileID)
		if err != nil {
			return fmt.Errorf("find spec_scenario ids: %w", err)
		}
	}

	// Symbol for capability
	symbolsBatch = append(symbolsBatch, &model.Symbol{
		FileID:     fileID,
		SymbolName: classified.Slug,
		SymbolType: "spec_capability",
		EntityType: "spec",
		EntityID:   capID,
		LineNumber: parsed.LineStart,
		Signature:  parsed.Title,
	})

	if err := idx.db.BatchInsertSymbols(ctx, symbolsBatch, idx.config.Indexer.BatchSize); err != nil {
		return err
	}

	// Code mentions
	if err := idx.insertSpecMentions(ctx, fileID, capID, parsed.RelatedCode, parsed.Requirements, reqIDs, scenarioIDs, stats); err != nil {
		return err
	}

	// Search vectors
	if err := idx.db.EnsureSpecSearchVectors(ctx, fileID); err != nil {
		idx.logError(file.Path, "ensure spec search vectors: %v", err)
	}

	stats.SpecCapabilities++
	stats.SpecRequirements += len(reqs)
	stats.SpecScenarios += len(scns)
	return nil
}

// parseUsecaseMD обрабатывает файлы usecase-слоя.
func (idx *Indexer) parseUsecaseMD(ctx context.Context, file fswalk.FileInfo, fileID int64, content string, classified openspecmd.Classified, stats *model.ScanStats) error {
	if classified.Kind == openspecmd.KindUsecaseIndex {
		// INDEX.md — не отдельная сущность, pageId маппится при обработке файлов
		return nil
	}

	fileName := filepath.Base(file.RelPath)
	parsed := openspecmd.ParseUsecaseFile(classified, fileName, content)

	configID, err := idx.resolveSpecConfigID(ctx, file, fileID)
	if err != nil {
		return err
	}

	uc := &model.SpecUsecase{
		FileID:         fileID,
		SpecConfigID:   configID,
		UsecaseName:    parsed.Name,
		Title:          parsed.Title,
		Description:    parsed.Description,
		Actors:         parsed.Actors,
		Preconditions:  parsed.Preconditions,
		Postconditions: parsed.Postconditions,
		BusinessValue:  parsed.BusinessValue,
		Architecture:   parsed.Architecture,
		DataSchema:     parsed.DataSchema,
		SourceDir:      parsed.SourceDir,
		UsecaseKind:    parsed.Kind,
		PageID:         parsed.PageID,
		LineStart:      parsed.LineStart,
		LineEnd:        parsed.LineEnd,
	}

	if err := idx.db.BatchInsertSpecUsecases(ctx, []*model.SpecUsecase{uc}, nil, idx.config.Indexer.BatchSize); err != nil {
		return err
	}

	// Symbol for usecase
	ucID, err := idx.db.FindSpecUsecaseIDByFile(ctx, fileID)
	if err != nil {
		idx.logError(file.Path, "find spec_usecase: %v", err)
	} else {
		if err := idx.db.BatchInsertSymbols(ctx, []*model.Symbol{{
			FileID:     fileID,
			SymbolName: parsed.Name,
			SymbolType: "spec_usecase",
			EntityType: "spec",
			EntityID:   ucID,
			LineNumber: parsed.LineStart,
			Signature:  parsed.Title,
		}}, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
	}

	// Insert steps with resolved usecase_id
	if len(parsed.Steps) > 0 && ucID > 0 {
		var steps []*model.SpecUsecaseStep
		for _, s := range parsed.Steps {
			steps = append(steps, &model.SpecUsecaseStep{
				FileID:     fileID,
				UsecaseID:  ucID,
				FlowKind:   s.FlowKind,
				StepOrder:  s.StepOrder,
				StepText:   s.StepText,
				LineNumber: s.LineNumber,
			})
		}
		if err := idx.db.BatchInsertSpecUsecaseSteps(ctx, steps, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
	}

	if ucID > 0 {
		var mentions []*model.SpecCodeMention
		for slug := range openspecmd.ExtractSpecReferences(content) {
			mentions = append(mentions, &model.SpecCodeMention{
				FileID: fileID, SourceType: "spec_usecase", SourceID: ucID,
				MentionName: slug, MentionKind: "spec_ref",
			})
		}
		for _, mention := range openspecmd.ExtractMentionsInline(content) {
			mentions = append(mentions, &model.SpecCodeMention{
				FileID: fileID, SourceType: "spec_usecase", SourceID: ucID,
				MentionName: mention.Name, MentionKind: mention.Kind, LineNumber: mention.Line,
			})
		}
		if err := idx.db.BatchInsertSpecCodeMentions(ctx, mentions, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
		stats.SpecCodeMentions += len(mentions)
	}

	if err := idx.db.EnsureSpecSearchVectors(ctx, fileID); err != nil {
		idx.logError(file.Path, "ensure spec search vectors: %v", err)
	}

	stats.SpecUsecases++
	return nil
}

func (idx *Indexer) resolveChangeID(ctx context.Context, file fswalk.FileInfo, fileID int64, classified openspecmd.Classified, authoritative bool) (int64, error) {
	configID, err := idx.resolveSpecConfigID(ctx, file, fileID)
	if err != nil {
		return 0, fmt.Errorf("resolveSpecConfigID for change %q: %w", classified.ChangeName, err)
	}
	if configID == 0 {
		return 0, fmt.Errorf("resolveSpecConfigID returned 0 for change %q (path=%s)", classified.ChangeName, file.Path)
	}
	// Verify config exists before upserting change
	var exists bool
	var cfgFileID int64
	var cfgProduct string
	checkErr := idx.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM spec_configs WHERE id = $1), COALESCE((SELECT file_id FROM spec_configs WHERE id = $1), 0), COALESCE((SELECT product_name FROM spec_configs WHERE id = $1), '')`, configID).Scan(&exists, &cfgFileID, &cfgProduct)
	if checkErr != nil {
		return 0, fmt.Errorf("verify spec_config %d exists for change %q: %w", configID, classified.ChangeName, checkErr)
	}
	if !exists {
		// Config was deleted between GetOrCreateSpecConfig and now — log details
		idx.logError(file.Path, "spec_config %d does not exist for change %q (fileID=%d, cfgFileID=%d, product=%q)",
			configID, classified.ChangeName, fileID, cfgFileID, cfgProduct)
		return 0, fmt.Errorf("spec_config %d does not exist for change %q (path=%s, fileID=%d)", configID, classified.ChangeName, file.Path, fileID)
	}
	status := "active"
	parts := []string{filepath.FromSlash(classified.RootDir), "changes"}
	if classified.Archived {
		status = "archived"
		parts = append(parts, "archive")
	}
	parts = append(parts, classified.ChangeName)
	return idx.db.UpsertSpecChange(ctx, &model.SpecChange{
		FileID:       fileID,
		SpecConfigID: configID,
		ChangeName:   classified.ChangeName,
		Status:       status,
		DirPath:      filepath.Join(parts...),
	}, authoritative)
}

// parseChangeProposal обрабатывает changes/<name>/proposal.md.
func (idx *Indexer) parseChangeProposal(ctx context.Context, file fswalk.FileInfo, fileID int64, content string, classified openspecmd.Classified, stats *model.ScanStats) error {
	_ = openspecmd.ParseChangeProposal(classified, content)

	changeID, err := idx.resolveChangeID(ctx, file, fileID, classified, true)
	if err != nil {
		return err
	}

	if changeID > 0 {
		if err := idx.db.BatchInsertSymbols(ctx, []*model.Symbol{{
			FileID:     fileID,
			SymbolName: classified.ChangeName,
			SymbolType: "spec_change",
			EntityType: "spec",
			EntityID:   changeID,
			LineNumber: 1,
			Signature:  classified.ChangeName,
		}}, idx.config.Indexer.BatchSize); err != nil {
			return err
		}
	}

	if err := idx.db.EnsureSpecSearchVectors(ctx, fileID); err != nil {
		idx.logError(file.Path, "ensure spec search vectors: %v", err)
	}

	// Сохраняем spec references из proposal.md для change_modifies (постпроцессор)
	if changeID > 0 {
		refs := openspecmd.ExtractSpecReferences(content)
		if len(refs) > 0 {
			var refMentions []*model.SpecCodeMention
			for slug := range refs {
				refMentions = append(refMentions, &model.SpecCodeMention{
					FileID:      fileID,
					SourceType:  "spec_change",
					SourceID:    changeID,
					MentionName: slug,
					MentionKind: "spec_ref",
				})
			}
			if err := idx.db.BatchInsertSpecCodeMentions(ctx, refMentions, idx.config.Indexer.BatchSize); err != nil {
				idx.logError(file.Path, "insert spec_ref mentions: %v", err)
			}
			stats.SpecCodeMentions += len(refMentions)
		}
	}

	stats.SpecChanges++
	return nil
}

// parseChangeDelta обрабатывает changes/<name>/specs/<cap>/spec.md (delta-требования).
func (idx *Indexer) parseChangeDelta(ctx context.Context, file fswalk.FileInfo, fileID int64, content string, classified openspecmd.Classified, stats *model.ScanStats) error {
	deltas := openspecmd.ParseDeltaSpec(content)
	changeID, err := idx.resolveChangeID(ctx, file, fileID, classified, false)
	if err != nil {
		return err
	}
	if len(deltas) == 0 {
		return nil
	}

	var batchDeltas []*model.SpecChangeDelta
	for _, d := range deltas {
		batchDeltas = append(batchDeltas, &model.SpecChangeDelta{
			FileID:          fileID,
			ChangeID:        changeID,
			Section:         d.Section,
			CapabilitySlug:  classified.Slug,
			RequirementName: d.RequirementName,
			BodyText:        d.BodyText,
			LineStart:       d.LineStart,
			LineEnd:         d.LineEnd,
		})
	}

	if err := idx.db.BatchInsertSpecChanges(ctx, nil, batchDeltas, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	stats.SpecChangeDeltas += len(batchDeltas)
	return nil
}

// parseChangeMeta обрабатывает tasks.md, design.md, .openspec.yaml change'а.
func (idx *Indexer) parseChangeMeta(ctx context.Context, file fswalk.FileInfo, fileID int64, content string, classified openspecmd.Classified, stats *model.ScanStats) error {
	changeID, err := idx.resolveChangeID(ctx, file, fileID, classified, false)
	if err != nil {
		return err
	}

	// Извлекаем spec references для построения change_modifies (постпроцессор)
	refs := openspecmd.ExtractSpecReferences(content)
	if len(refs) == 0 {
		return nil
	}

	// Сохраняем как mentions с kind='spec_ref' для постпроцессора
	var mentions []*model.SpecCodeMention
	for slug := range refs {
		mentions = append(mentions, &model.SpecCodeMention{
			FileID:      fileID,
			SourceType:  "spec_change",
			SourceID:    changeID,
			MentionName: slug,
			MentionKind: "spec_ref",
		})
	}
	if err := idx.db.BatchInsertSpecCodeMentions(ctx, mentions, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	stats.SpecCodeMentions += len(mentions)
	return nil
}

// insertSpecMentions извлекает и вставляет упоминания кода из spec-данных.
func (idx *Indexer) insertSpecMentions(ctx context.Context, fileID int64, capID int64, relatedCode string, requirements []openspecmd.ParsedRequirement, reqIDs map[string]int64, scenarioIDs map[int64]map[int]int64, stats *model.ScanStats) error {
	var mentions []*model.SpecCodeMention

	// Related code секция
	for _, m := range openspecmd.ExtractMentionsFromRelatedCode(relatedCode) {
		mentions = append(mentions, &model.SpecCodeMention{
			FileID:      fileID,
			SourceType:  "spec_capability",
			SourceID:    capID,
			MentionName: m.Name,
			MentionKind: m.Kind,
			LineNumber:  m.Line,
		})
	}

	// Inline упоминания из тел требований и сценариев
	for _, r := range requirements {
		reqID := reqIDs[sanitizeUTF8Lookup(r.Name)]
		for _, m := range openspecmd.ExtractMentionsInline(r.BodyText) {
			mentions = append(mentions, &model.SpecCodeMention{
				FileID:      fileID,
				SourceType:  "spec_requirement",
				SourceID:    reqID,
				MentionName: m.Name,
				MentionKind: m.Kind,
				LineNumber:  r.LineStart + m.Line - 1,
			})
		}
		for _, s := range r.Scenarios {
			scenarioID := scenarioIDs[reqID][s.Order]
			for _, m := range openspecmd.ExtractMentionsInline(s.When + "\n" + s.Then) {
				mentions = append(mentions, &model.SpecCodeMention{
					FileID:      fileID,
					SourceType:  "spec_scenario",
					SourceID:    scenarioID,
					MentionName: m.Name,
					MentionKind: m.Kind,
					LineNumber:  s.LineStart + m.Line - 1,
				})
			}
		}
	}

	if len(mentions) == 0 {
		return nil
	}
	if err := idx.db.BatchInsertSpecCodeMentions(ctx, mentions, idx.config.Indexer.BatchSize); err != nil {
		return err
	}
	stats.SpecCodeMentions += len(mentions)
	return nil
}
