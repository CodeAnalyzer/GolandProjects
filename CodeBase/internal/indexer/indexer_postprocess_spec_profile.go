package indexer

import (
	"context"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
)

// postProcessSpecProductProfiles детектирует и обновляет профиль продукта
// для каждого spec_config: usecase_layout, id_style, cross_ref_style,
// normative_lang, traceability, has_changes, has_audit, has_adr.
// Запускается после завершения индексации всех файлов.
func (idx *Indexer) postProcessSpecProductProfiles(ctx context.Context, collector *statsCollector) {
	configs, err := idx.db.LoadAllSpecConfigsForProfile(ctx)
	if err != nil {
		idx.logError("<post-processing>", "Error loading spec_configs for profile: %v", err)
		collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		return
	}
	if len(configs) == 0 {
		return
	}

	for _, cfg := range configs {
		profile, err := idx.detectProductProfile(ctx, cfg)
		if err != nil {
			idx.logError("<post-processing>", "Error detecting profile for config %d: %v", cfg.ID, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
			continue
		}

		if err := idx.db.UpdateSpecConfigProfile(ctx, cfg.ID, profile); err != nil {
			idx.logError("<post-processing>", "Error updating spec_config profile %d: %v", cfg.ID, err)
			collector.Add(func(stats *model.ScanStats) { stats.Errors++ })
		}
	}
}

// detectProductProfile анализирует проиндексированные данные и определяет профиль.
func (idx *Indexer) detectProductProfile(ctx context.Context, cfg *store.SpecConfigProfileRow) (*store.ProductProfileUpdate, error) {
	profile := &store.ProductProfileUpdate{
		UsecaseLayout: "none",
		IDStyle:       "dir",
		CrossRefStyle: "mixed",
		NormativeLang: "en",
		Traceability:  "none",
	}

	// 1. UsecaseLayout: проверяем наличие usecase-сущностей
	usecaseLayout, err := idx.db.DetectUsecaseLayout(ctx, cfg.ID)
	if err == nil && usecaseLayout != "" {
		profile.UsecaseLayout = usecaseLayout
	}

	// 2. IDStyle: проверяем формат capability_name (slug)
	idStyle, err := idx.db.DetectIDStyle(ctx, cfg.ID)
	if err == nil && idStyle != "" {
		profile.IDStyle = idStyle
	}

	// 3. CrossRefStyle: анализируем depends_on_capability relations
	crossRefStyle, err := idx.db.DetectCrossRefStyle(ctx, cfg.ID)
	if err == nil && crossRefStyle != "" {
		profile.CrossRefStyle = crossRefStyle
	}

	// 4. NormativeLang: проверяем язык requirements/capabilities
	normativeLang, err := idx.db.DetectNormativeLang(ctx, cfg.ID)
	if err == nil && normativeLang != "" {
		profile.NormativeLang = normativeLang
	}

	// 5. Traceability: проверяем наличие pageId / html_comment
	traceability, err := idx.db.DetectTraceability(ctx, cfg.ID)
	if err == nil && traceability != "" {
		profile.Traceability = traceability
	}

	// 6. HasChanges: проверяем наличие spec_changes
	hasChanges, err := idx.db.HasSpecChanges(ctx, cfg.ID)
	if err == nil {
		profile.HasChanges = hasChanges
	}

	// 7. HasAudit: проверяем наличие audit-файлов
	hasAudit, err := idx.db.HasSpecAudit(ctx, cfg.ID)
	if err == nil {
		profile.HasAudit = hasAudit
	}

	// 8. HasADR: проверяем наличие ADR-файлов (architecture decision records)
	hasADR, err := idx.db.HasSpecADR(ctx, cfg.ID)
	if err == nil {
		profile.HasADR = hasADR
	}

	return profile, nil
}
