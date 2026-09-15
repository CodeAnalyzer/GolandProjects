package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
)

var ErrSchemaNotInitialized = errors.New("schema not initialized")
var ErrSchemaUpdateRequired = errors.New("schema update required")

func (db *DB) CheckSchemaVersion(ctx context.Context) error {
	var current bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)
	`, CurrentSchemaVersion).Scan(&current); err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "42P01" {
			return fmt.Errorf("%w: expected schema marker %q", ErrSchemaNotInitialized, CurrentSchemaVersion)
		}
		return fmt.Errorf("schema compatibility check failed: %w", err)
	}
	if current {
		return nil
	}

	var latest string
	err := db.QueryRowContext(ctx, `
		SELECT version
		FROM schema_migrations
		WHERE version LIKE 'codebase_schema_%'
		ORDER BY applied_at DESC, version DESC
		LIMIT 1
	`).Scan(&latest)
	if errors.Is(err, sql.ErrNoRows) {
		latest = "none"
	} else if err != nil {
		return fmt.Errorf("schema compatibility check failed: %w", err)
	}
	return fmt.Errorf("%w: expected schema marker %q, found %q", ErrSchemaUpdateRequired, CurrentSchemaVersion, latest)
}
