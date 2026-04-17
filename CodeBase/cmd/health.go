package cmd

import (
	"fmt"
	"os"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store"
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
		response, err := executeHealth()
		if err != nil {
			return handleHealthError(err)
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

func executeHealth() (healthResponse, error) {
	response := healthResponse{
		Success:       true,
		FormatVersion: "1.0",
		Command:       "health",
		Status:        "ok",
		Checks:        make([]healthCheck, 0, 4),
	}

	cfg := config.Get()
	if cfg == nil {
		return response, fmt.Errorf("config not loaded")
	}
	response.Checks = append(response.Checks, healthCheck{
		Name:   "config",
		Status: "ok",
	})

	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return response, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()
	response.Checks = append(response.Checks, healthCheck{
		Name:   "database",
		Status: "ok",
	})

	if err := db.InitSchema(); err != nil {
		return response, fmt.Errorf("failed to init schema: %w", err)
	}
	response.Checks = append(response.Checks, healthCheck{
		Name:   "schema",
		Status: "ok",
	})

	hasIndex, err := db.HasCompletedInit()
	if err != nil {
		return response, fmt.Errorf("failed to inspect index readiness: %w", err)
	}
	if hasIndex {
		response.Checks = append(response.Checks, healthCheck{
			Name:   "index",
			Status: "ok",
		})
		return response, nil
	}

	response.Status = "degraded"
	response.Checks = append(response.Checks, healthCheck{
		Name:    "index",
		Status:  "missing",
		Message: "no completed scan run found",
	})
	return response, nil
}

func handleHealthError(err error) error {
	if !healthOutputJSON {
		return err
	}
	return writeHealthErrorResponse(err)
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
	message := err.Error()
	switch {
	case message == "config not loaded":
		return "config_error"
	case containsAny(message, "failed to connect to database", "failed to ping default database", "failed to ping database", "connection refused", "dial tcp"):
		return "database_unavailable"
	case containsAny(message, "failed to init schema"):
		return "schema_init_failed"
	case containsAny(message, "failed to inspect index readiness"):
		return "health_check_failed"
	default:
		return "internal_error"
	}
}

func init() {
	healthCmd.Flags().BoolVar(&healthOutputJSON, "json", false, "output as JSON")
	rootCmd.AddCommand(healthCmd)
}
