package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SpecConfigProfileRow — минимальные данные config для детекции профиля.
type SpecConfigProfileRow struct {
	ID      int64
	FileID  int64
	RootDir string
}

// SpecConfigFullProfileRow — полный профиль spec_config для MCP-инструмента spec_config.
type SpecConfigFullProfileRow struct {
	ID             int64
	ProductName    string
	SchemaName     string
	RootDir        string
	UsecaseLayout  string
	IDStyle        string
	CrossRefStyle  string
	NormativeLang  string
	Traceability   string
	HasChanges     bool
	HasAudit       bool
	HasADR          bool
	CoverageMetrics bool
	ContextText    string
}

// LoadSpecConfigProfileByProduct загружает полный профиль spec_config по имени продукта.
// Возвращает sql.ErrNoRows, если продукт не найден.
func (db *DB) LoadSpecConfigProfileByProduct(ctx context.Context, product string) (*SpecConfigFullProfileRow, error) {
	var r SpecConfigFullProfileRow
	var rootDir, schemaName, contextText sql.NullString
	err := db.QueryRowContext(ctx, `
		SELECT sc.id,
		       COALESCE(sc.product_name, ''),
		       sc.schema_name,
		       sc.root_dir,
		       sc.usecase_layout,
		       sc.id_style,
		       sc.cross_ref_style,
		       sc.normative_lang,
		       sc.traceability,
		       sc.has_changes,
		       sc.has_audit,
		       sc.has_adr,
		       sc.coverage_metrics,
		       sc.context_text
		FROM spec_configs sc
		JOIN ds_products dp ON dp.id = sc.ds_product_id
		WHERE dp.product_name = $1
		ORDER BY sc.id DESC
		LIMIT 1`, product).Scan(
		&r.ID, &r.ProductName, &schemaName, &rootDir,
		&r.UsecaseLayout, &r.IDStyle, &r.CrossRefStyle, &r.NormativeLang,
		&r.Traceability, &r.HasChanges, &r.HasAudit, &r.HasADR,
		&r.CoverageMetrics, &contextText)
	if err != nil {
		return nil, err
	}
	r.SchemaName = schemaName.String
	r.RootDir = strings.ReplaceAll(rootDir.String, `\`, "/")
	r.ContextText = contextText.String
	return &r, nil
}

// SpecConfigStatsRow — счётчики сущностей продукта.
type SpecConfigStatsRow struct {
	Capabilities int
	Requirements  int
	Scenarios    int
	Usecases     int
	Changes      int
}

// LoadSpecConfigStats возвращает счётчики сущностей по spec_config_id одним запросом.
func (db *DB) LoadSpecConfigStats(ctx context.Context, configID int64) (*SpecConfigStatsRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT 'capabilities' AS kind, COUNT(*)::bigint AS cnt FROM spec_capabilities WHERE spec_config_id = $1
		UNION ALL
		SELECT 'requirements', COUNT(*)::bigint FROM spec_requirements r
			JOIN spec_capabilities c ON c.id = r.capability_id WHERE c.spec_config_id = $1
		UNION ALL
		SELECT 'scenarios', COUNT(*)::bigint FROM spec_scenarios s
			JOIN spec_requirements r ON r.id = s.requirement_id
			JOIN spec_capabilities c ON c.id = r.capability_id WHERE c.spec_config_id = $1
		UNION ALL
		SELECT 'usecases', COUNT(*)::bigint FROM spec_usecases WHERE spec_config_id = $1
		UNION ALL
		SELECT 'changes', COUNT(*)::bigint FROM spec_changes WHERE spec_config_id = $1
	`, configID)
	if err != nil {
		return nil, fmt.Errorf("load spec_config stats: %w", err)
	}
	defer rows.Close()

	var stats SpecConfigStatsRow
	for rows.Next() {
		var kind string
		var cnt int64
		if err := rows.Scan(&kind, &cnt); err != nil {
			return nil, err
		}
		switch kind {
		case "capabilities":
			stats.Capabilities = int(cnt)
		case "requirements":
			stats.Requirements = int(cnt)
		case "scenarios":
			stats.Scenarios = int(cnt)
		case "usecases":
			stats.Usecases = int(cnt)
		case "changes":
			stats.Changes = int(cnt)
		}
	}
	return &stats, rows.Err()
}

// SpecConfigHierarchyRow — узел дерева capabilities.
type SpecConfigHierarchyRow struct {
	ID             int64
	ParentID       sql.NullInt64
	CapabilityName string
	Title          string
	Purpose        sql.NullString
	Notes          sql.NullString
}

// LoadSpecConfigHierarchy загружает все capabilities продукта для построения дерева.
func (db *DB) LoadSpecConfigHierarchy(ctx context.Context, configID int64) ([]SpecConfigHierarchyRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, parent_id, capability_name, title, purpose, notes
		FROM spec_capabilities
		WHERE spec_config_id = $1
		ORDER BY capability_name
	`, configID)
	if err != nil {
		return nil, fmt.Errorf("load spec_config hierarchy: %w", err)
	}
	defer rows.Close()

	var result []SpecConfigHierarchyRow
	for rows.Next() {
		var r SpecConfigHierarchyRow
		if err := rows.Scan(&r.ID, &r.ParentID, &r.CapabilityName, &r.Title, &r.Purpose, &r.Notes); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// LoadAllSpecConfigsForProfile загружает все spec_configs для детекции профиля.
func (db *DB) LoadAllSpecConfigsForProfile(ctx context.Context) ([]*SpecConfigProfileRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT sc.id, sc.file_id, COALESCE(sc.root_dir, ''), f.rel_path
		FROM spec_configs sc
		JOIN files f ON f.id = sc.file_id
		ORDER BY sc.id
	`)
	if err != nil {
		return nil, fmt.Errorf("load spec_configs for profile: %w", err)
	}
	defer rows.Close()

	var result []*SpecConfigProfileRow
	for rows.Next() {
		var r SpecConfigProfileRow
		var storedRootDir string
		var configPath string
		if err := rows.Scan(&r.ID, &r.FileID, &storedRootDir, &configPath); err != nil {
			return nil, err
		}
		r.RootDir = strings.ReplaceAll(storedRootDir, `\`, "/")
		if r.RootDir != "" {
			result = append(result, &r)
			continue
		}
		configPath = strings.ReplaceAll(configPath, `\`, "/")
		if slash := strings.LastIndex(configPath, "/"); slash >= 0 {
			r.RootDir = configPath[:slash]
		}
		result = append(result, &r)
	}
	return result, rows.Err()
}

// ProductProfileUpdate — данные для обновления профиля spec_config.
type ProductProfileUpdate struct {
	UsecaseLayout string
	IDStyle       string
	CrossRefStyle string
	NormativeLang string
	Traceability  string
	HasChanges    bool
	HasAudit      bool
	HasADR        bool
}

// UpdateSpecConfigProfile обновляет поля профиля spec_config.
func (db *DB) UpdateSpecConfigProfile(ctx context.Context, configID int64, profile *ProductProfileUpdate) error {
	_, err := db.ExecContext(ctx, `
		UPDATE spec_configs
		SET usecase_layout = $1,
		    id_style = $2,
		    cross_ref_style = $3,
		    normative_lang = $4,
		    traceability = $5,
		    has_changes = $6,
		    has_audit = $7,
		    has_adr = $8
		WHERE id = $9
	`,
		profile.UsecaseLayout,
		profile.IDStyle,
		profile.CrossRefStyle,
		profile.NormativeLang,
		profile.Traceability,
		profile.HasChanges,
		profile.HasAudit,
		profile.HasADR,
		configID,
	)
	if err != nil {
		return fmt.Errorf("update spec_config profile: %w", err)
	}
	return nil
}

// DetectUsecaseLayout определяет layout usecase-слоя по наличию usecase-сущностей.
func (db *DB) DetectUsecaseLayout(ctx context.Context, configID int64) (string, error) {
	var layout string
	err := db.QueryRowContext(ctx, `
		SELECT source_dir FROM spec_usecases
		WHERE spec_config_id = $1 AND source_dir <> ''
		LIMIT 1
	`, configID).Scan(&layout)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return "none", nil
		}
		return "", err
	}
	return layout, nil
}

// DetectIDStyle определяет стиль ID по slug'ам capabilities.
func (db *DB) DetectIDStyle(ctx context.Context, configID int64) (string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT capability_name FROM spec_capabilities WHERE spec_config_id = $1
	`, configID)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	multiSegment := 0
	total := 0
	for rows.Next() {
		var slug string
		if err := rows.Scan(&slug); err != nil {
			return "", err
		}
		total++
		if strings.Contains(slug, "/") {
			multiSegment++
		}
	}
	if total == 0 {
		return "dir", nil
	}
	if multiSegment > total/2 {
		return "full_path", nil
	}
	return "dir", nil
}

// DetectCrossRefStyle определяет стиль cross-references по confidence relations.
func (db *DB) DetectCrossRefStyle(ctx context.Context, configID int64) (string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT r.confidence
		FROM relations r
		JOIN spec_capabilities sc ON sc.id = r.source_id
		WHERE r.relation_type = 'depends_on_capability'
		AND r.source_type = 'spec_capability'
		AND sc.spec_config_id = $1
	`, configID)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	counts := map[string]int{}
	total := 0
	for rows.Next() {
		var confidence string
		if err := rows.Scan(&confidence); err != nil {
			return "", err
		}
		counts[confidence]++
		total++
	}
	if total == 0 {
		return "mixed", nil
	}
	threshold := total * 6 / 10
	if counts["explicit"] > threshold {
		return "explicit", nil
	}
	if counts["notes"] > threshold {
		return "notes", nil
	}
	if counts["inline"] > threshold {
		return "inline", nil
	}
	return "mixed", nil
}

// DetectNormativeLang определяет язык спецификаций по содержанию requirements.
func (db *DB) DetectNormativeLang(ctx context.Context, configID int64) (string, error) {
	// Проверяем наличие кириллицы в titles/purpose capabilities
	var text string
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(string_agg(COALESCE(title, '') || ' ' || COALESCE(purpose, ''), ' '), '')
		FROM spec_capabilities
		WHERE spec_config_id = $1
		LIMIT 50
	`, configID).Scan(&text)
	if err != nil {
		return "", err
	}
	if text == "" {
		return "en", nil
	}
	cyrillic := 0
	latin := 0
	for _, r := range text {
		if (r >= 'а' && r <= 'я') || (r >= 'А' && r <= 'Я') || r == 'ё' || r == 'Ё' {
			cyrillic++
		} else if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			latin++
		}
	}
	if cyrillic > latin {
		return "ru", nil
	}
	return "en", nil
}

// DetectTraceability определяет стиль traceability по наличию pageId в usecases.
func (db *DB) DetectTraceability(ctx context.Context, configID int64) (string, error) {
	var hasPageID bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM spec_usecases
			WHERE spec_config_id = $1 AND page_id IS NOT NULL AND page_id <> 0
		)
	`, configID).Scan(&hasPageID)
	if err != nil {
		return "", err
	}
	if hasPageID {
		return "pageid", nil
	}
	return "none", nil
}

// HasSpecChanges проверяет наличие changes для config.
func (db *DB) HasSpecChanges(ctx context.Context, configID int64) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM spec_changes WHERE spec_config_id = $1
		)
	`, configID).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (db *DB) hasSpecDirectory(ctx context.Context, configID int64, directory string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `
		WITH config_root AS (
			SELECT cf.scan_run_id,
			       REGEXP_REPLACE(REPLACE(cf.rel_path, CHR(92), '/'), '/[^/]+$', '') AS root_path
			FROM spec_configs sc
			JOIN files cf ON cf.id = sc.file_id
			WHERE sc.id = $1
		), scoped_files AS (
			SELECT SUBSTRING(REPLACE(f.rel_path, CHR(92), '/') FROM CHAR_LENGTH(cr.root_path) + 2) AS root_rel_path
			FROM config_root cr
			JOIN files f ON f.scan_run_id = cr.scan_run_id
			WHERE LEFT(REPLACE(f.rel_path, CHR(92), '/'), CHAR_LENGTH(cr.root_path) + 1) = cr.root_path || '/'
		)
		SELECT EXISTS(
			SELECT 1 FROM scoped_files
			WHERE root_rel_path ~* ('(^|/)' || $2 || '(/|$)')
		)
	`, configID, directory).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// HasSpecAudit проверяет наличие audit-файлов внутри openspec-корня config.
func (db *DB) HasSpecAudit(ctx context.Context, configID int64) (bool, error) {
	return db.hasSpecDirectory(ctx, configID, "audit")
}

// HasSpecADR проверяет наличие ADR-файлов (architecture decision records).
func (db *DB) HasSpecADR(ctx context.Context, configID int64) (bool, error) {
	return db.hasSpecDirectory(ctx, configID, "adr")
}
