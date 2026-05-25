package querysvc

import (
	"fmt"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/query"
	"github.com/codebase/internal/store"
)

// ExecuteWith выполняет query-runner на уже открытом соединении.
// InitSchema не вызывается — предполагается, что схема уже инициализирована.
func ExecuteWith(db *store.DB, run func(q *query.Query) (interface{}, error)) (interface{}, error) {
	q := query.New(db)
	results, err := run(q)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	return results, nil
}

// Execute открывает БД, инициализирует схему и выполняет переданный query-runner.
func Execute(run func(q *query.Query) (interface{}, error)) (interface{}, error) {
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

	q := query.New(db)
	results, err := run(q)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return results, nil
}
