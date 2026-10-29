package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/systemsvc"
	"github.com/spf13/cobra"
)

type healthCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type healthResponse struct {
	Success       bool          `json:"success"`
	FormatVersion string        `json:"format_version"`
	Command       string        `json:"command"`
	Status        string        `json:"status"`
	Checks        []healthCheck `json:"checks"`
}

type healthErrorResponse struct {
	Success       bool           `json:"success"`
	FormatVersion string         `json:"format_version"`
	Command       string         `json:"command"`
	Error         queryErrorBody `json:"error"`
}

var healthOutputJSON bool

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check CLI and index readiness",
	Long:  `Runs basic health checks for configuration, database connectivity, schema availability and index readiness.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		response, err := executeHealth(cmd.Context())
		if err != nil {
			return err
		}
		if healthOutputJSON {
			return writeJSON(os.Stdout, response)
		}
		fmt.Printf("Health status: %s\n\n", response.Status)
		for _, check := range response.Checks {
			fmt.Printf("- %s: %s", check.Name, check.Status)
			if check.Message != "" {
				fmt.Printf(" (%s)", check.Message)
			}
			fmt.Printf("\n")
		}
		return nil
	},
}

func executeHealth(ctx context.Context) (healthResponse, error) {
	cfg := config.Get()
	if cfg == nil {
		return healthResponse{}, errs.ErrConfigNotLoaded
	}
	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return healthResponse{}, fmt.Errorf("%w: %w", errs.ErrDBConnect, err)
	}
	defer db.Close()
	// Read-only политика: схема не создаётся, только проверяется готовность
	// (readonly-schema-policy). Ошибка версии пробрасывается как есть.
	if err := db.CheckSchemaVersion(ctx); err != nil {
		return healthResponse{}, err
	}
	result, err := systemsvc.ExecuteHealth(db)
	if err != nil {
		return healthResponse{}, err
	}

	response := healthResponse{
		Success:       true,
		FormatVersion: "1.0",
		Command:       "health",
		Status:        result.Status,
		Checks:        make([]healthCheck, 0, len(result.Checks)),
	}
	for _, check := range result.Checks {
		response.Checks = append(response.Checks, healthCheck{
			Name:    check.Name,
			Status:  check.Status,
			Message: check.Message,
		})
	}
	return response, nil
}

func writeHealthErrorResponse(err error) error {
	response := healthErrorResponse{
		Success:       false,
		FormatVersion: "1.0",
		Command:       "health",
		Error: queryErrorBody{
			Code:    classifyHealthError(err),
			Message: err.Error(),
		},
	}
	return writeJSON(os.Stdout, response)
}

func classifyHealthError(err error) string {
	switch {
	case errors.Is(err, errs.ErrConfigNotLoaded):
		return "config_error"
	case errors.Is(err, errs.ErrDBConnect):
		return "database_unavailable"
	case errors.Is(err, store.ErrSchemaNotInitialized):
		return "schema_not_initialized"
	case errors.Is(err, store.ErrSchemaUpdateRequired):
		return "schema_update_required"
	case errors.Is(err, errs.ErrHealthCheckFailed):
		return "health_check_failed"
	case containsAny(err.Error(), "failed to ping default database", "failed to ping database", "connection refused", "dial tcp"):
		return "database_unavailable"
	default:
		return "internal_error"
	}
}

func init() {
	healthCmd.Flags().BoolVar(&healthOutputJSON, "json", false, "output as JSON")
	rootCmd.AddCommand(healthCmd)
}
