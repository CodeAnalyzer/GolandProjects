package reviewsvc

import (
	"fmt"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/review"
	"github.com/codebase/internal/store"
)

func ExecuteWith(db *store.DB, path string, opts review.Options, onProgress func(completed, total int)) (*review.Result, error) {
	runner := review.NewRunner(db)
	if onProgress != nil {
		runner.SetOnProgress(onProgress)
	}
	result, err := runner.RunSQLFile(path, opts)
	if err != nil {
		return nil, fmt.Errorf("review failed: %w", err)
	}
	return result, nil
}

func Execute(path string, opts review.Options, onProgress func(completed, total int)) (*review.Result, error) {
	cfg := config.Get()
	if cfg == nil {
		return nil, fmt.Errorf("config not loaded")
	}

	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	if err := db.InitSchema(); err != nil {
		return nil, fmt.Errorf("failed to init schema: %w", err)
	}

	return ExecuteWith(db, path, opts, onProgress)
}
