package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/codebase/internal/model"
)

func (db *DB) GetOrCreateSpecCapabilityContainer(ctx context.Context, configID, productID, parentID int64, name string) (int64, error) {
	// Serialize spec writes for a config without taking a row lock that can
	// conflict with foreign-key/index locks acquired by concurrent inserts.
	if _, err := db.ExecContext(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
	`, fmt.Sprintf("spec-config/%d", configID)); err != nil {
		return 0, fmt.Errorf("lock spec config for capability %q: %w", name, err)
	}
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO spec_capabilities
			(file_id, spec_config_id, ds_product_id, parent_id, capability_name)
		VALUES ((SELECT file_id FROM spec_configs WHERE id = $1), $1, $2, $3, $4)
		ON CONFLICT (spec_config_id, capability_name) DO UPDATE SET
			parent_id = COALESCE(spec_capabilities.parent_id, EXCLUDED.parent_id),
			ds_product_id = COALESCE(spec_capabilities.ds_product_id, EXCLUDED.ds_product_id)
		RETURNING id
	`, configID, NullableInt64(productID), NullableInt64(parentID), sanitizeUTF8String(name)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("get or create spec capability container %q: %w", name, err)
	}
	return id, nil
}

func (db *DB) SetSpecCapabilityParent(ctx context.Context, capabilityID, parentID int64) error {
	if _, err := db.ExecContext(ctx, `UPDATE spec_capabilities SET parent_id = $2 WHERE id = $1`, capabilityID, NullableInt64(parentID)); err != nil {
		return fmt.Errorf("set spec capability parent: %w", err)
	}
	return nil
}

func (db *DB) UpsertSpecCapability(ctx context.Context, capability *model.SpecCapability) (int64, error) {
	if _, err := db.ExecContext(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
	`, fmt.Sprintf("spec-config/%d", capability.SpecConfigID)); err != nil {
		return 0, fmt.Errorf("lock spec config for capability upsert %q: %w", capability.CapabilityName, err)
	}
	var id int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO spec_capabilities
			(file_id, spec_config_id, ds_product_id, parent_id, capability_name,
			 title, purpose, notes, related_code, line_start, line_end,
			 api_total, api_covered, code_total, code_listed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT (spec_config_id, capability_name) DO UPDATE SET
			file_id = EXCLUDED.file_id,
			ds_product_id = EXCLUDED.ds_product_id,
			parent_id = EXCLUDED.parent_id,
			title = EXCLUDED.title,
			purpose = EXCLUDED.purpose,
			notes = EXCLUDED.notes,
			related_code = EXCLUDED.related_code,
			line_start = EXCLUDED.line_start,
			line_end = EXCLUDED.line_end,
			api_total = EXCLUDED.api_total,
			api_covered = EXCLUDED.api_covered,
			code_total = EXCLUDED.code_total,
			code_listed = EXCLUDED.code_listed,
			search_vector = NULL
		RETURNING id
	`, capability.FileID, capability.SpecConfigID, NullableInt64(capability.DsProductID), NullableInt64(capability.ParentID),
		sanitizeUTF8String(capability.CapabilityName), sanitizeUTF8String(capability.Title), NullableString(capability.Purpose),
		NullableString(capability.Notes), NullableString(capability.RelatedCode), capability.LineStart, capability.LineEnd,
		nullableIntPtr(capability.ApiTotal), nullableIntPtr(capability.ApiCovered), nullableIntPtr(capability.CodeTotal), nullableIntPtr(capability.CodeListed)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert spec capability %q: %w", capability.CapabilityName, err)
	}
	return id, nil
}

func (db *DB) UpsertSpecChange(ctx context.Context, change *model.SpecChange, authoritative bool) (int64, error) {
	var id int64
	err := db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		// Use the same config-level advisory lock as capability writes. Row locks
		// here can deadlock with foreign-key/index locks from concurrent inserts.
		var cfgExists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM spec_configs WHERE id = $1)`, change.SpecConfigID).Scan(&cfgExists); err != nil {
			return fmt.Errorf("verify spec_config in tx: %w", err)
		}
		if !cfgExists {
			return fmt.Errorf("spec_config %d does not exist in tx (fileID=%d, change=%q)", change.SpecConfigID, change.FileID, change.ChangeName)
		}
		if _, err := tx.ExecContext(ctx, `
			SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
		`, fmt.Sprintf("spec-config/%d", change.SpecConfigID)); err != nil {
			return fmt.Errorf("lock spec config in tx: %w", err)
		}

		lockKey := fmt.Sprintf("%d/%s", change.SpecConfigID, sanitizeUTF8String(change.ChangeName))
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
			return fmt.Errorf("lock spec change: %w", err)
		}

		err := tx.QueryRowContext(ctx, `
			SELECT id FROM spec_changes
			WHERE spec_config_id = $1 AND change_name = $2
			ORDER BY id DESC LIMIT 1
		`, change.SpecConfigID, sanitizeUTF8String(change.ChangeName)).Scan(&id)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("find spec change for upsert: %w", err)
		}
		if err == sql.ErrNoRows {
			if err := tx.QueryRowContext(ctx, `
				INSERT INTO spec_changes (file_id, spec_config_id, change_name, status, dir_path)
				VALUES ($1, $2, $3, $4, $5)
				RETURNING id
			`, change.FileID, change.SpecConfigID, sanitizeUTF8String(change.ChangeName),
				sanitizeUTF8String(change.Status), sanitizeUTF8String(change.DirPath)).Scan(&id); err != nil {
				return fmt.Errorf("create spec change: %w", err)
			}
		} else if authoritative {
			if _, err := tx.ExecContext(ctx, `
				UPDATE spec_changes
				SET file_id = $2, status = $3, dir_path = $4, search_vector = NULL
				WHERE id = $1
			`, id, change.FileID, sanitizeUTF8String(change.Status), sanitizeUTF8String(change.DirPath)); err != nil {
				return fmt.Errorf("update spec change: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}
