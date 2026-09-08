package store

import (
	"context"
	"database/sql"
	"fmt"
)

// FindSpecCapabilityIDByFile возвращает id последней spec_capability для file_id.
func (db *DB) FindSpecCapabilityIDByFile(ctx context.Context, fileID int64) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		SELECT id FROM spec_capabilities WHERE file_id = $1 ORDER BY id DESC LIMIT 1
	`, fileID).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("find spec_capability by file: %w", err)
	}
	return id, nil
}

// FindSpecRequirementIDsByFile возвращает map[requirement_name]id для file_id.
func (db *DB) FindSpecRequirementIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, requirement_name FROM spec_requirements WHERE file_id = $1
	`, fileID)
	if err != nil {
		return nil, fmt.Errorf("query spec_requirements by file: %w", err)
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		result[name] = id
	}
	return result, rows.Err()
}

func (db *DB) FindSpecScenarioIDsByFile(ctx context.Context, fileID int64) (map[int64]map[int]int64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, requirement_id, scn_order FROM spec_scenarios WHERE file_id = $1
	`, fileID)
	if err != nil {
		return nil, fmt.Errorf("query spec_scenarios by file: %w", err)
	}
	defer rows.Close()
	result := map[int64]map[int]int64{}
	for rows.Next() {
		var id, requirementID int64
		var order int
		if err := rows.Scan(&id, &requirementID, &order); err != nil {
			return nil, err
		}
		if result[requirementID] == nil {
			result[requirementID] = map[int]int64{}
		}
		result[requirementID][order] = id
	}
	return result, rows.Err()
}

// FindSpecUsecaseIDByFile возвращает id последней spec_usecase для file_id.
func (db *DB) FindSpecUsecaseIDByFile(ctx context.Context, fileID int64) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		SELECT id FROM spec_usecases WHERE file_id = $1 ORDER BY id DESC LIMIT 1
	`, fileID).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("find spec_usecase by file: %w", err)
	}
	return id, nil
}

// FindSpecChangeIDByFile возвращает id последней spec_change для file_id (proposal.md).
func (db *DB) FindSpecChangeIDByFile(ctx context.Context, fileID int64) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		SELECT id FROM spec_changes WHERE file_id = $1 ORDER BY id DESC LIMIT 1
	`, fileID).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("find spec_change by file: %w", err)
	}
	return id, nil
}

// FindSpecChangeIDByName возвращает id spec_change по имени и config_id.
func (db *DB) FindSpecChangeIDByName(ctx context.Context, configID int64, changeName string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `
		SELECT id FROM spec_changes WHERE spec_config_id = $1 AND change_name = $2 ORDER BY id DESC LIMIT 1
	`, configID, changeName).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("spec_change not found: %s", changeName)
	}
	if err != nil {
		return 0, fmt.Errorf("find spec_change by name: %w", err)
	}
	return id, nil
}
