package reviewsvc

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

// ExecuteBatchWithCtx выполняет пакетный review на готовом соединении с БД.
func ExecuteBatchWithCtx(ctx context.Context, db *store.DB, paths []string, opts review.Options, hooks review.BatchHooks) (*review.BatchResult, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is not initialized", errs.ErrReviewFailed)
	}
	runner := review.NewRunner(db)
	if hooks.OnRuleProgress != nil {
		runner.SetOnProgress(hooks.OnRuleProgress)
	}
	result, err := runner.RunSQLFilesCtx(ctx, paths, opts, hooks)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrReviewFailed, err)
	}
	return result, nil
}

// ExecuteBatchCtx выполняет пакетный review с собственным соединением к БД.
func ExecuteBatchCtx(ctx context.Context, paths []string, opts review.Options, hooks review.BatchHooks) (*review.BatchResult, error) {
	cfg := config.Get()
	if cfg == nil {
		return nil, errs.ErrConfigNotLoaded
	}

	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrDBConnect, err)
	}
	defer db.Close()

	if err := db.CheckSchemaVersion(ctx); err != nil {
		return nil, err
	}

	return ExecuteBatchWithCtx(ctx, db, paths, opts, hooks)
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

	// Read-only политика: схема не создаётся, только проверяется готовность
	// (readonly-schema-policy). Ошибка версии пробрасывается как есть.
	if err := db.CheckSchemaVersion(ctx); err != nil {
		return nil, err
	}

	return ExecuteWithCtx(ctx, db, path, opts, onProgress)
}

func Execute(path string, opts review.Options, onProgress func(completed, total int)) (*review.Result, error) {
	return ExecuteCtx(context.Background(), path, opts, onProgress)
}

// ClassifyError маппит ошибку review в стабильный machine-readable код.
func ClassifyError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errs.ErrConfigNotLoaded):
		return "config_error"
	case errors.Is(err, errs.ErrDBConnect):
		return "database_unavailable"
	case errors.Is(err, store.ErrSchemaNotInitialized):
		return "schema_not_initialized"
	case errors.Is(err, store.ErrSchemaUpdateRequired):
		return "schema_update_required"
	case errors.Is(err, errs.ErrReviewFailed):
		return "review_failed"
	case errors.Is(err, errs.ErrInvalidArguments):
		return "invalid_arguments"
	case containsAny(err.Error(), "required flag", "accepts", "unknown review rule", "unknown flag", "invalid argument"):
		return "invalid_arguments"
	default:
		return "internal_error"
	}
}

func containsAny(value string, parts ...string) bool {
	for _, part := range parts {
		if part != "" && strings.Contains(value, part) {
			return true
		}
	}
	return false
}
