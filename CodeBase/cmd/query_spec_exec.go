package cmd

import (
	"context"
	"fmt"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/store"
)

// specsvcExecute открывает БД и выполняет spec-запрос.
func specsvcExecute(ctx context.Context, run func(db *store.DB) (interface{}, error)) (interface{}, error) {
	cfg := config.Get()
	if cfg == nil {
		return nil, errs.ErrConfigNotLoaded
	}

	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrDBConnect, err)
	}
	defer db.Close()

	// Read-only политика: схема не создаётся, только проверяется готовность
	// (readonly-schema-policy). Ошибка версии пробрасывается как есть.
	if err := db.CheckSchemaVersion(ctx); err != nil {
		return nil, err
	}

	result, err := run(db)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrQueryFailed, err)
	}

	return result, nil
}
