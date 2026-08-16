package reviewsvc

import (
	"context"
	"fmt"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/review"
	"github.com/codebase/internal/store"
)

func ExecuteWithCtx(ctx context.Context, db *store.DB, path string, opts review.Options, onProgress func(completed, total int)) (*review.Result, error) {
	runner := review.NewRunner(db)
	if onProgress != nil {
		runner.SetOnProgress(onProgress)
	}
	result, err := runner.RunSQLFileCtx(ctx, path, opts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrReviewFailed, err)
	}
	return result, nil
}

func ExecuteWith(db *store.DB, path string, opts review.Options, onProgress func(completed, total int)) (*review.Result, error) {
	return ExecuteWithCtx(context.Background(), db, path, opts, onProgress)
}

func ExecuteCtx(ctx context.Context, path string, opts review.Options, onProgress func(completed, total int)) (*review.Result, error) {
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

	return ExecuteWithCtx(ctx, db, path, opts, onProgress)
}

func Execute(path string, opts review.Options, onProgress func(completed, total int)) (*review.Result, error) {
	return ExecuteCtx(context.Background(), path, opts, onProgress)
}
