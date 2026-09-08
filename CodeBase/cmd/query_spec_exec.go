package cmd

import (
	"fmt"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/store"
)

// specsvcExecute открывает БД и выполняет spec-запрос.
func specsvcExecute(run func(db *store.DB) (interface{}, error)) (interface{}, error) {
	cfg := config.Get()
	if cfg == nil {
		return nil, errs.ErrConfigNotLoaded
	}

	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrDBConnect, err)
	}
	defer db.Close()

	if err := db.InitSchema(); err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrSchemaInit, err)
	}

	result, err := run(db)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrQueryFailed, err)
	}

	return result, nil
}
