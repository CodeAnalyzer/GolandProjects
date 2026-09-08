package store

import (
	"context"
	"fmt"

	"github.com/codebase/internal/model"
)

// LoadAllSpecCapabilitiesForDeps загружает все capabilities с текстами для построения depends_on.
func (db *DB) LoadAllSpecCapabilitiesForDeps(ctx context.Context) ([]*model.SpecCapability, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, spec_config_id, COALESCE(ds_product_id, 0), capability_name, COALESCE(purpose, ''), COALESCE(notes, ''), COALESCE(related_code, '')
		FROM spec_capabilities
		ORDER BY id
	`)
	if err != nil {
		return nil, fmt.Errorf("load spec_capabilities for deps: %w", err)
	}
	defer rows.Close()

	var result []*model.SpecCapability
	for rows.Next() {
		var c model.SpecCapability
		if err := rows.Scan(&c.ID, &c.SpecConfigID, &c.DsProductID, &c.CapabilityName, &c.Purpose, &c.Notes, &c.RelatedCode); err != nil {
			return nil, err
		}
		result = append(result, &c)
	}
	return result, rows.Err()
}

// DeleteSpecDependencyRelations удаляет старые depends_on_capability и change_modifies relations.
func (db *DB) DeleteSpecDependencyRelations(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `
		DELETE FROM relations
		WHERE relation_type IN ('depends_on_capability', 'usecase_involves', 'change_modifies')
	`)
	if err != nil {
		return fmt.Errorf("delete spec dependency relations: %w", err)
	}
	return nil
}

// SpecChangeDeltaRow — строка delta для change_modifies.
type SpecChangeDeltaRow struct {
	ChangeID       int64
	SpecConfigID   int64
	CapabilitySlug string
}

// LoadAllSpecChangeDeltasForModifies загружает change_id + capability_slug из delta-таблицы.
func (db *DB) LoadAllSpecChangeDeltasForModifies(ctx context.Context) ([]*SpecChangeDeltaRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT d.change_id, c.spec_config_id, d.capability_slug
		FROM spec_change_delta d
		JOIN spec_changes c ON c.id = d.change_id
		ORDER BY d.change_id
	`)
	if err != nil {
		return nil, fmt.Errorf("load spec_change_delta for modifies: %w", err)
	}
	defer rows.Close()

	var result []*SpecChangeDeltaRow
	for rows.Next() {
		var r SpecChangeDeltaRow
		if err := rows.Scan(&r.ChangeID, &r.SpecConfigID, &r.CapabilitySlug); err != nil {
			return nil, err
		}
		result = append(result, &r)
	}
	return result, rows.Err()
}

// LoadSpecChangeProposalSlugs загружает change_id + slug из spec_code_mentions
// где mention_kind='spec_ref' — это references на capabilities из proposal/tasks/design.
func (db *DB) LoadSpecChangeProposalSlugs(ctx context.Context) ([]*SpecChangeProposalRefRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT sc.id, sc.spec_config_id, scm.mention_name
		FROM spec_changes sc
		JOIN spec_code_mentions scm ON scm.source_type = 'spec_change' AND scm.source_id = sc.id
		WHERE scm.mention_kind = 'spec_ref'
		ORDER BY sc.id
	`)
	if err != nil {
		return nil, fmt.Errorf("load change proposal slugs: %w", err)
	}
	defer rows.Close()

	changeMap := map[int64]map[string]struct{}{}
	configMap := map[int64]int64{}
	for rows.Next() {
		var changeID, configID int64
		var slug string
		if err := rows.Scan(&changeID, &configID, &slug); err != nil {
			return nil, err
		}
		if _, ok := changeMap[changeID]; !ok {
			changeMap[changeID] = map[string]struct{}{}
		}
		changeMap[changeID][slug] = struct{}{}
		configMap[changeID] = configID
	}

	var result []*SpecChangeProposalRefRow
	for changeID, refs := range changeMap {
		result = append(result, &SpecChangeProposalRefRow{
			ChangeID:     changeID,
			SpecConfigID: configMap[changeID],
			References:   refs,
		})
	}
	return result, rows.Err()
}

// SpecChangeProposalRefRow — change_id + извлечённые references из proposal/meta.
type SpecChangeProposalRefRow struct {
	ChangeID     int64
	SpecConfigID int64
	References   map[string]struct{}
}

type SpecUsecaseRefRow struct {
	UsecaseID    int64
	SpecConfigID int64
	Slug         string
}

type SpecUsecaseTextRow struct {
	UsecaseID    int64
	SpecConfigID int64
	Path         string
	Text         string
}

func (db *DB) LoadSpecUsecaseRefs(ctx context.Context) ([]SpecUsecaseRefRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT m.source_id, u.spec_config_id, m.mention_name
		FROM spec_code_mentions m
		JOIN spec_usecases u ON u.id = m.source_id
		WHERE m.source_type = 'spec_usecase' AND m.mention_kind = 'spec_ref'
		ORDER BY m.source_id, m.mention_name
	`)
	if err != nil {
		return nil, fmt.Errorf("load usecase spec refs: %w", err)
	}
	defer rows.Close()
	var result []SpecUsecaseRefRow
	for rows.Next() {
		var row SpecUsecaseRefRow
		if err := rows.Scan(&row.UsecaseID, &row.SpecConfigID, &row.Slug); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (db *DB) LoadSpecUsecaseTexts(ctx context.Context) ([]SpecUsecaseTextRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT uc.id, uc.spec_config_id, f.path, CONCAT_WS(' ', uc.description, uc.architecture, uc.data_schema)
		FROM spec_usecases uc
		JOIN files f ON f.id = uc.file_id
		ORDER BY uc.id
	`)
	if err != nil {
		return nil, fmt.Errorf("load usecase texts: %w", err)
	}
	defer rows.Close()
	var result []SpecUsecaseTextRow
	for rows.Next() {
		var row SpecUsecaseTextRow
		if err := rows.Scan(&row.UsecaseID, &row.SpecConfigID, &row.Path, &row.Text); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
