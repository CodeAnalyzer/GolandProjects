package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/systemsvc"
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
	MD    int `json:"md"`
	YAML  int `json:"yaml"`
}

type statsEntitiesSummary struct {
	Procedures         int `json:"procedures"`
	Tables             int `json:"tables"`
	Columns            int `json:"columns"`
	Units              int `json:"units"`
	Classes            int `json:"classes"`
	Methods            int `json:"methods"`
	PASFields          int `json:"pas_fields"`
	JSFunctions        int `json:"js_functions"`
	SMFInstruments     int `json:"smf_instruments"`
	Forms              int `json:"forms"`
	Defines            int `json:"defines"`
	ReportForms        int `json:"report_forms"`
	ReportFields       int `json:"report_fields"`
	ReportParams       int `json:"report_params"`
	VBFunctions        int `json:"vb_functions"`
	APIBusinessObjects int `json:"api_business_objects"`
	APIContracts       int `json:"api_contracts"`
	APIContractParams  int `json:"api_contract_params"`
	APIContractTables  int `json:"api_contract_tables"`
	APIContractFields  int `json:"api_contract_fields"`
	APIBusinessParams  int `json:"api_business_params"`
	APIBusinessTables  int `json:"api_business_tables"`
	QueryFragments     int `json:"query_fragments"`
	Relations          int `json:"relations"`
	SQLTableIndexes    int `json:"sql_table_indexes"`
	APITableIndexes    int `json:"api_table_indexes"`
	SpecConfigs        int `json:"spec_configs"`
	SpecCapabilities   int `json:"spec_capabilities"`
	SpecRequirements   int `json:"spec_requirements"`
	SpecScenarios      int `json:"spec_scenarios"`
	SpecUsecases       int `json:"spec_usecases"`
	SpecChanges        int `json:"spec_changes"`
	SpecChangeDeltas   int `json:"spec_change_deltas"`
	SpecCodeMentions   int `json:"spec_code_mentions"`
	SpecVocabTerms     int `json:"spec_vocab_terms"`
	SpecEmbeddings     int `json:"spec_embeddings"`
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
		fmt.Printf("  MD files:        %d\n", stats.MDFiles)
		fmt.Printf("  YAML files:      %d\n", stats.YAMLFiles)
		fmt.Printf("\n")
		fmt.Printf("SQL Entities:\n")
		fmt.Printf("  Procedures:      %d\n", stats.Procedures)
		fmt.Printf("  Tables:          %d\n", stats.Tables)
		fmt.Printf("  Columns:         %d\n", stats.Columns)
		fmt.Printf("  Defines:         %d\n", stats.Defines)
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
		fmt.Printf("OpenSpec Entities:\n")
		fmt.Printf("  Configs:         %d\n", stats.SpecConfigs)
		fmt.Printf("  Capabilities:    %d\n", stats.SpecCapabilities)
		fmt.Printf("  Requirements:    %d\n", stats.SpecRequirements)
		fmt.Printf("  Scenarios:       %d\n", stats.SpecScenarios)
		fmt.Printf("  Usecases:        %d\n", stats.SpecUsecases)
		fmt.Printf("  Changes:         %d\n", stats.SpecChanges)
		fmt.Printf("  Change deltas:   %d\n", stats.SpecChangeDeltas)
		fmt.Printf("  Code mentions:   %d\n", stats.SpecCodeMentions)
		fmt.Printf("  Vocab terms:     %d\n", stats.SpecVocabTerms)
		fmt.Printf("  Embeddings:      %d\n", stats.SpecEmbeddings)
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
	return systemsvc.ExecuteStats(db)
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
			MD:    stats.MDFiles,
			YAML:  stats.YAMLFiles,
		},
		Entities: statsEntitiesSummary{
			Procedures:         stats.Procedures,
			Tables:             stats.Tables,
			Columns:            stats.Columns,
			Units:              stats.Units,
			Classes:            stats.Classes,
			Methods:            stats.Methods,
			PASFields:          stats.PASFields,
			JSFunctions:        stats.JSFunctions,
			SMFInstruments:     stats.SMFInstruments,
			Forms:              stats.Forms,
			Defines:            stats.Defines,
			ReportForms:        stats.ReportForms,
			ReportFields:       stats.ReportFields,
			ReportParams:       stats.ReportParams,
			VBFunctions:        stats.VBFunctions,
			APIBusinessObjects: stats.APIBusinessObjects,
			APIContracts:       stats.APIContracts,
			APIContractParams:  stats.APIContractParams,
			APIContractTables:  stats.APIContractTables,
			APIContractFields:  stats.APIContractFields,
			APIBusinessParams:  stats.APIBusinessParams,
			APIBusinessTables:  stats.APIBusinessTables,
			QueryFragments:     stats.QueryFragments,
			Relations:          stats.Relations,
			SQLTableIndexes:    stats.SQLTableIndexes,
			APITableIndexes:    stats.APITableIndexes,
			SpecConfigs:        stats.SpecConfigs,
			SpecCapabilities:   stats.SpecCapabilities,
			SpecRequirements:   stats.SpecRequirements,
			SpecScenarios:      stats.SpecScenarios,
			SpecUsecases:       stats.SpecUsecases,
			SpecChanges:        stats.SpecChanges,
			SpecChangeDeltas:   stats.SpecChangeDeltas,
			SpecCodeMentions:   stats.SpecCodeMentions,
			SpecVocabTerms:     stats.SpecVocabTerms,
			SpecEmbeddings:     stats.SpecEmbeddings,
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
	switch {
	case errors.Is(err, errs.ErrConfigNotLoaded):
		return "config_error"
	case errors.Is(err, errs.ErrDBConnect):
		return "database_unavailable"
	case errors.Is(err, errs.ErrSchemaInit):
		return "schema_init_failed"
	case errors.Is(err, errs.ErrStatsFailed):
		return "stats_query_failed"
	case containsAny(err.Error(), "connection refused", "dial tcp"):
		return "database_unavailable"
	default:
		return "internal_error"
	}
}

func init() {
	statsCmd.Flags().BoolVar(&statsOutputJSON, "json", false, "output as JSON")
	rootCmd.AddCommand(statsCmd)
}
