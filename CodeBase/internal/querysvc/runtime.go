package querysvc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/dsinfosvc"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/query"
	"github.com/codebase/internal/store"
)

// ExecuteWith выполняет query-runner на уже открытом соединении.
// InitSchema не вызывается — предполагается, что схема уже инициализирована.
func ExecuteWith(db *store.DB, run func(q *query.Query) (interface{}, error)) (interface{}, error) {
	q := query.New(db)
	results, err := run(q)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrQueryFailed, err)
	}
	return results, nil
}

// ProfileResolver разрешает контекст профиля dsinfo на открытом соединении.
// nil-контекст означает «профиль не задан» (поведение default-ветки).
type ProfileResolver func(ctx context.Context, db *store.DB) (*query.ProfileContext, error)

// ResolveQueryProfile разрешает контекст профиля dsinfo по цепочке
// --dsinfo > CODEBASE_DSINFO_ID > активный профиль > отсутствие профиля и
// переводит его в profile-контекст query-слоя. nil-контекст означает
// «профиль не задан».
func ResolveQueryProfile(ctx context.Context, db *store.DB, explicitID int64) (*query.ProfileContext, error) {
	ac, err := dsinfosvc.ResolveAnalysisContext(ctx, db, explicitID)
	if err != nil {
		return nil, err
	}
	if ac == nil || ac.ProfileID == 0 {
		return nil, nil
	}
	pc := &query.ProfileContext{
		ProfileID: ac.ProfileID,
		Branches:  ac.Branches,
		Requested: make(map[string][]string),
	}
	if ac.Matching != nil {
		for _, item := range ac.Matching.Items {
			if item.MatchedProductID == 0 || item.RequestedBranchKey == "" {
				continue
			}
			if !slices.Contains(pc.Requested[item.ProductName], item.RequestedBranchKey) {
				pc.Requested[item.ProductName] = append(pc.Requested[item.ProductName], item.RequestedBranchKey)
			}
		}
	}
	for name := range pc.Requested {
		sort.Strings(pc.Requested[name])
	}
	return pc, nil
}

// ExecuteWithDB выполняет query-runner на открытом соединении с контекстом
// профиля (если resolver вернул его).
func ExecuteWithDB(ctx context.Context, db *store.DB, resolve ProfileResolver, run func(q *query.Query) (interface{}, error)) (interface{}, error) {
	var opts []query.Option
	if resolve != nil {
		pc, err := resolve(ctx, db)
		if err != nil {
			return nil, err
		}
		if pc != nil {
			opts = append(opts, query.WithProfile(pc))
		}
	}
	q := query.New(db, opts...)
	results, err := run(q)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrQueryFailed, err)
	}
	return results, nil
}

// ExecuteWithProfile открывает БД, разрешает контекст профиля dsinfo и
// выполняет query-runner с осью profile_match.
func ExecuteWithProfile(ctx context.Context, resolve ProfileResolver, run func(q *query.Query) (interface{}, error)) (interface{}, error) {
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

	return ExecuteWithDB(ctx, db, resolve, run)
}

// ProcedureResultItems нормализует результат поиска процедуры для query-команд:
// отсутствие процедуры (sql.ErrNoRows) — пустой результат, а не ошибка.
func ProcedureResultItems(proc *query.SQLProcedureResult, err error) (interface{}, error) {
	if errors.Is(err, sql.ErrNoRows) {
		return []query.SQLProcedureResult{}, nil
	}
	if err != nil {
		return nil, err
	}
	return []query.SQLProcedureResult{*proc}, nil
}

// Execute открывает БД, инициализирует схему и выполняет переданный query-runner.
func Execute(run func(q *query.Query) (interface{}, error)) (interface{}, error) {
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

	q := query.New(db)
	results, err := run(q)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrQueryFailed, err)
	}

	return results, nil
}
