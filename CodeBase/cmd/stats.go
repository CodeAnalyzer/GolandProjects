package cmd

import (
	"fmt"
	"os"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store"
	"github.com/spf13/cobra"
)

type statsFilesSummary struct {
	Total int `json:"total"`
	SQL   int `json:"sql"`
	H     int `json:"h"`
	PAS   int `json:"pas"`
	INC   int `json:"inc"`
	JS    int `json:"js"`
	XML   int `json:"xml"`
	SMF   int `json:"smf"`
	DFM   int `json:"dfm"`
	TPR   int `json:"tpr"`
	RPT   int `json:"rpt"`
}

type statsEntitiesSummary struct {
	Procedures     int `json:"procedures"`
	Tables         int `json:"tables"`
	Columns        int `json:"columns"`
	Units          int `json:"units"`
	Classes        int `json:"classes"`
	Methods        int `json:"methods"`
	PASFields      int `json:"pas_fields"`
	JSFunctions    int `json:"js_functions"`
	SMFInstruments int `json:"smf_instruments"`
	Forms          int `json:"forms"`
	Defines        int `json:"defines"`
	ReportForms    int `json:"report_forms"`
	ReportFields   int `json:"report_fields"`
	ReportParams   int `json:"report_params"`
	VBFunctions    int `json:"vb_functions"`
	APIBusinessObjects int `json:"api_business_objects"`
	APIContracts       int `json:"api_contracts"`
	APIContractParams  int `json:"api_contract_params"`
	APIContractTables  int `json:"api_contract_tables"`
	APIContractFields  int `json:"api_contract_fields"`
	APIBusinessParams  int `json:"api_business_params"`
	APIBusinessTables  int `json:"api_business_tables"`
	QueryFragments int `json:"query_fragments"`
	Relations      int `json:"relations"`
	SQLTableIndexes int `json:"sql_table_indexes"`
	APITableIndexes int `json:"api_table_indexes"`
}

type statsLastScanSummary struct {
	RunID    int64  `json:"run_id"`
	Started  string `json:"started,omitempty"`
	Finished string `json:"finished,omitempty"`
	Status   string `json:"status,omitempty"`
	Errors   int    `json:"errors"`
}

type statsResponse struct {
	Success       bool                 `json:"success"`
	FormatVersion string               `json:"format_version"`
	Command       string               `json:"command"`
	Files         statsFilesSummary    `json:"files"`
	Entities      statsEntitiesSummary `json:"entities"`
	LastScan      statsLastScanSummary `json:"last_scan"`
}

type statsErrorResponse struct {
	Success       bool           `json:"success"`
	FormatVersion string         `json:"format_version"`
	Command       string         `json:"command"`
	Error         queryErrorBody `json:"error"`
}

var statsOutputJSON bool

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Index summary",
	Long:  `Displays statistics on indexed files and entities.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		stats, err := executeStats()
		if err != nil {
			return handleStatsError(err)
		}

		if statsOutputJSON {
			return writeJSON(os.Stdout, buildStatsResponse(stats))
		}

		fmt.Printf("CodeBase Statistics\n")
		fmt.Printf("===================\n\n")
		fmt.Printf("Files:\n")
		fmt.Printf("  Total files:     %d\n", stats.TotalFiles)
		fmt.Printf("  SQL files:       %d\n", stats.SQLFiles)
		fmt.Printf("  H files:         %d\n", stats.HFiles)
		fmt.Printf("  PAS files:       %d\n", stats.PASFiles)
		fmt.Printf("  INC files:       %d\n", stats.INCFiles)
		fmt.Printf("  JS files:        %d\n", stats.JSFiles)
		fmt.Printf("  XML files:       %d\n", stats.XMLFiles)
		fmt.Printf("  SMF files:       %d\n", stats.SMFFiles)
		fmt.Printf("  DFM files:       %d\n", stats.DFMFiles)
		fmt.Printf("  TPR files:       %d\n", stats.TPRFiles)
		fmt.Printf("  RPT files:       %d\n", stats.RPTFiles)
		fmt.Printf("\n")
		fmt.Printf("SQL Entities:\n")
		fmt.Printf("  Procedures:      %d\n", stats.Procedures)
		fmt.Printf("  Tables:          %d\n", stats.Tables)
		fmt.Printf("  Columns:         %d\n", stats.Columns)
		fmt.Printf("\n")
		fmt.Printf("Pascal Entities:\n")
		fmt.Printf("  Units:           %d\n", stats.Units)
		fmt.Printf("  Classes:         %d\n", stats.Classes)
		fmt.Printf("  Methods:         %d\n", stats.Methods)
		fmt.Printf("  Fields:          %d\n", stats.PASFields)
		fmt.Printf("\n")
		fmt.Printf("JavaScript Entities:\n")
		fmt.Printf("  Functions:       %d\n", stats.JSFunctions)
		fmt.Printf("\n")
		fmt.Printf("SMF Entities:\n")
		fmt.Printf("  Instruments:     %d\n", stats.SMFInstruments)
		fmt.Printf("\n")
		fmt.Printf("DFM Entities:\n")
		fmt.Printf("  Forms:           %d\n", stats.Forms)
		fmt.Printf("\n")
		fmt.Printf("Report Entities:\n")
		fmt.Printf("  Report forms:    %d\n", stats.ReportForms)
		fmt.Printf("  Report fields:   %d\n", stats.ReportFields)
		fmt.Printf("  Report params:   %d\n", stats.ReportParams)
		fmt.Printf("  VB functions:    %d\n", stats.VBFunctions)
		fmt.Printf("\n")
		fmt.Printf("API XML Entities:\n")
		fmt.Printf("  Business objects:%d\n", stats.APIBusinessObjects)
		fmt.Printf("  Contracts:       %d\n", stats.APIContracts)
		fmt.Printf("  Contract params: %d\n", stats.APIContractParams)
		fmt.Printf("  Contract tables: %d\n", stats.APIContractTables)
		fmt.Printf("  Contract fields: %d\n", stats.APIContractFields)
		fmt.Printf("  Business params: %d\n", stats.APIBusinessParams)
		fmt.Printf("  Business tables: %d\n", stats.APIBusinessTables)
		fmt.Printf("\n")
		fmt.Printf("SQL Indexes:\n")
		fmt.Printf("  SQL table idx:   %d\n", stats.SQLTableIndexes)
		fmt.Printf("  API table idx:   %d\n", stats.APITableIndexes)
		fmt.Printf("\n")
		fmt.Printf("Embedded SQL:\n")
		fmt.Printf("  Query fragments: %d\n", stats.QueryFragments)
		fmt.Printf("\n")
		fmt.Printf("Relations:\n")
		fmt.Printf("  Total relations: %d\n", stats.Relations)
		fmt.Printf("\n")
		fmt.Printf("Errors:\n")
		fmt.Printf("  Parse errors:    %d\n", stats.Errors)
		fmt.Printf("\n")
		fmt.Printf("Last scan:\n")
		fmt.Printf("  Run ID:          %d\n", stats.LastScanID)
		fmt.Printf("  Started:         %s\n", stats.LastScanStarted)
		fmt.Printf("  Finished:        %s\n", stats.LastScanFinished)
		fmt.Printf("  Status:          %s\n", stats.LastScanStatus)

		return nil
	},
}

func executeStats() (*store.Stats, error) {
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

	stats, err := db.GetStats()
	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}

	return stats, nil
}

func buildStatsResponse(stats *store.Stats) statsResponse {
	response := statsResponse{
		Success:       true,
		FormatVersion: "1.0",
		Command:       "stats",
		Files: statsFilesSummary{
			Total: stats.TotalFiles,
			SQL:   stats.SQLFiles,
			H:     stats.HFiles,
			PAS:   stats.PASFiles,
			INC:   stats.INCFiles,
			JS:    stats.JSFiles,
			XML:   stats.XMLFiles,
			SMF:   stats.SMFFiles,
			DFM:   stats.DFMFiles,
			TPR:   stats.TPRFiles,
			RPT:   stats.RPTFiles,
		},
		Entities: statsEntitiesSummary{
			Procedures:     stats.Procedures,
			Tables:         stats.Tables,
			Columns:        stats.Columns,
			Units:          stats.Units,
			Classes:        stats.Classes,
			Methods:        stats.Methods,
			PASFields:      stats.PASFields,
			JSFunctions:    stats.JSFunctions,
			SMFInstruments: stats.SMFInstruments,
			Forms:          stats.Forms,
			Defines:        stats.Defines,
			ReportForms:    stats.ReportForms,
			ReportFields:   stats.ReportFields,
			ReportParams:   stats.ReportParams,
			VBFunctions:    stats.VBFunctions,
			APIBusinessObjects: stats.APIBusinessObjects,
			APIContracts:       stats.APIContracts,
			APIContractParams:  stats.APIContractParams,
			APIContractTables:  stats.APIContractTables,
			APIContractFields:  stats.APIContractFields,
			APIBusinessParams:  stats.APIBusinessParams,
			APIBusinessTables:  stats.APIBusinessTables,
			QueryFragments: stats.QueryFragments,
			Relations:      stats.Relations,
			SQLTableIndexes: stats.SQLTableIndexes,
			APITableIndexes: stats.APITableIndexes,
		},
		LastScan: statsLastScanSummary{
			RunID:  stats.LastScanID,
			Status: stats.LastScanStatus,
			Errors: stats.Errors,
		},
	}
	if !stats.LastScanStarted.IsZero() {
		response.LastScan.Started = stats.LastScanStarted.Format("2006-01-02T15:04:05Z07:00")
	}
	if !stats.LastScanFinished.IsZero() {
		response.LastScan.Finished = stats.LastScanFinished.Format("2006-01-02T15:04:05Z07:00")
	}
	return response
}

func handleStatsError(err error) error {
	if !statsOutputJSON {
		return err
	}
	return writeStatsErrorResponse(err)
}

func writeStatsErrorResponse(err error) error {
	response := statsErrorResponse{
		Success:       false,
		FormatVersion: "1.0",
		Command:       "stats",
		Error: queryErrorBody{
			Code:    classifyStatsError(err),
			Message: err.Error(),
		},
	}
	return writeJSON(os.Stdout, response)
}

func classifyStatsError(err error) string {
	message := err.Error()
	switch {
	case message == "config not loaded":
		return "config_error"
	case containsAny(message, "failed to connect to database", "connection refused", "dial tcp"):
		return "database_unavailable"
	case containsAny(message, "failed to init schema"):
		return "schema_init_failed"
	case containsAny(message, "failed to get stats"):
		return "stats_query_failed"
	default:
		return "internal_error"
	}
}

func init() {
	statsCmd.Flags().BoolVar(&statsOutputJSON, "json", false, "output as JSON")
	rootCmd.AddCommand(statsCmd)
}
