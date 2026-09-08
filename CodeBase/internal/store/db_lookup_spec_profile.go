package store

import (
	"context"
	"fmt"
	"strings"
)

// SpecConfigProfileRow — минимальные данные config для детекции профиля.
type SpecConfigProfileRow struct {
	ID      int64
	FileID  int64
	RootDir string
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
