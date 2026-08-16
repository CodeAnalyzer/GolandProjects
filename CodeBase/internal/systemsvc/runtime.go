package systemsvc

import (
	"context"
	"fmt"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/store"
)

type HealthCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type HealthResult struct {
	Status string        `json:"status"`
	Checks []HealthCheck `json:"checks"`
}

func ExecuteHealth(db *store.DB) (HealthResult, error) {
	result := HealthResult{
		Status: "ok",
		Checks: make([]HealthCheck, 0, 4),
	}

	cfg := config.Get()
	if cfg == nil {
		return result, errs.ErrConfigNotLoaded
	}
	result.Checks = append(result.Checks, HealthCheck{Name: "config", Status: "ok"})
	result.Checks = append(result.Checks, HealthCheck{Name: "database", Status: "ok"})
	result.Checks = append(result.Checks, HealthCheck{Name: "schema", Status: "ok"})

	hasIndex, err := db.HasCompletedInit(context.Background())
	if err != nil {
		return result, fmt.Errorf("%w: %w", errs.ErrHealthCheckFailed, err)
	}
	if hasIndex {
		result.Checks = append(result.Checks, HealthCheck{Name: "index", Status: "ok"})
		return result, nil
	}

	result.Status = "degraded"
	result.Checks = append(result.Checks, HealthCheck{
		Name:    "index",
		Status:  "missing",
		Message: "no completed scan run found",
	})
	return result, nil
}

func ExecuteStats(db *store.DB) (*store.Stats, error) {
	cfg := config.Get()
	if cfg == nil {
		return nil, errs.ErrConfigNotLoaded
	}

	stats, err := db.GetStats(context.Background())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrStatsFailed, err)
	}
	return stats, nil
}
