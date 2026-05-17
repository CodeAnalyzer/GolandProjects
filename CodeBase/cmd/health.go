package cmd

import (
	"fmt"
	"os"

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
	result, err := systemsvc.ExecuteHealth()
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
