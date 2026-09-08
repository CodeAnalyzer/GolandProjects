package cmd

import (
	"fmt"

	"github.com/codebase/internal/specsvc"
	"github.com/codebase/internal/store"
	"github.com/spf13/cobra"
)

var (
	specSearchQuery     string
	specSearchProduct   string
	specSearchLevel     string
	specSearchLayer     string
	specByCodeName      string
	specDepsName        string
	specDepsProduct     string
	specDepsDirection   string
	specDepsMaxDepth    int
	specUsecaseName     string
	specCoverageName    string
	specCoverageProduct string
	specCoverageMode    string
	specHistoryName     string
	specHistoryChange   string
	specHistoryProduct  string
)

var querySpecCmd = &cobra.Command{
	Use:   "spec",
	Short: "Query OpenSpec artifacts",
	Long:  `Search and inspect OpenSpec specifications, capabilities, usecases, and changes.`,
}

var querySpecSearchCmd = &cobra.Command{
	Use:   "search --query <text> [--product <name>] [--level <level>]",
	Short: "Two-layer spec search (exact tsvector/trgm + semantic LSA)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runSpecCommand("query spec search", map[string]string{
			"query":   specSearchQuery,
			"product": specSearchProduct,
			"level":   specSearchLevel,
			"layer":   specSearchLayer,
		}, func(db *store.DB) (interface{}, error) {
			return specsvc.ExecuteSpecSearch(ctx, db, specSearchQuery, specSearchProduct, specSearchLevel, specSearchLayer, limit)
		})
	},
}

var querySpecByCodeCmd = &cobra.Command{
	Use:   "by-code --name <entity-name>",
	Short: "Find specs referencing a code entity by name",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runSpecCommand("query spec by-code", map[string]string{
			"name": specByCodeName,
		}, func(db *store.DB) (interface{}, error) {
			return specsvc.ExecuteSpecByCode(ctx, db, specByCodeName, limit)
		})
	},
}

var querySpecDepsCmd = &cobra.Command{
	Use:   "deps --name <capability> [--direction outgoing|incoming] [--max-depth N]",
	Short: "Capability dependency tree (depends_on_capability)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runSpecCommand("query spec deps", map[string]string{
			"name":      specDepsName,
			"product":   specDepsProduct,
			"direction": specDepsDirection,
			"max_depth": fmt.Sprintf("%d", specDepsMaxDepth),
		}, func(db *store.DB) (interface{}, error) {
			return specsvc.ExecuteSpecDeps(ctx, db, specDepsName, specDepsDirection, specDepsMaxDepth, specDepsProduct)
		})
	},
}

var querySpecUsecaseCmd = &cobra.Command{
	Use:   "usecase --name <usecase-name>",
	Short: "Usecase with steps and involved entities",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runSpecCommand("query spec usecase", map[string]string{
			"name": specUsecaseName,
		}, func(db *store.DB) (interface{}, error) {
			return specsvc.ExecuteSpecUsecase(ctx, db, specUsecaseName)
		})
	},
}

var querySpecCoverageCmd = &cobra.Command{
	Use:   "coverage --name <capability> [--mode saved|gaps]",
	Short: "Capability coverage metrics or coverage gaps",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runSpecCommand("query spec coverage", map[string]string{
			"name":    specCoverageName,
			"product": specCoverageProduct,
			"mode":    specCoverageMode,
		}, func(db *store.DB) (interface{}, error) {
			return specsvc.ExecuteSpecCoverage(ctx, db, specCoverageName, specCoverageMode, specCoverageProduct)
		})
	},
}

var querySpecHistoryCmd = &cobra.Command{
	Use:   "history (--name <capability> | --change <change>)",
	Short: "Change history for a capability or capabilities modified by a change",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runSpecCommand("query spec history", map[string]string{
			"name":    specHistoryName,
			"change":  specHistoryChange,
			"product": specHistoryProduct,
		}, func(db *store.DB) (interface{}, error) {
			return specsvc.ExecuteSpecHistory(ctx, db, specHistoryName, specHistoryChange, specHistoryProduct)
		})
	},
}

func init() {
	querySpecSearchCmd.Flags().StringVar(&specSearchQuery, "query", "", "search text")
	querySpecSearchCmd.Flags().StringVar(&specSearchProduct, "product", "", "filter by product name")
	querySpecSearchCmd.Flags().StringVar(&specSearchLevel, "level", "", "filter by spec level (capability, requirement, scenario, usecase)")
	querySpecSearchCmd.Flags().StringVar(&specSearchLayer, "layer", "both", "search layer (exact, semantic, both)")
	cobra.CheckErr(querySpecSearchCmd.MarkFlagRequired("query"))

	querySpecByCodeCmd.Flags().StringVar(&specByCodeName, "name", "", "code entity name to find in specs")
	cobra.CheckErr(querySpecByCodeCmd.MarkFlagRequired("name"))

	querySpecDepsCmd.Flags().StringVar(&specDepsName, "name", "", "capability name (slug)")
	querySpecDepsCmd.Flags().StringVar(&specDepsProduct, "product", "", "filter by product name")
	querySpecDepsCmd.Flags().StringVar(&specDepsDirection, "direction", "depends_on", "depends_on or depended_by")
	querySpecDepsCmd.Flags().IntVar(&specDepsMaxDepth, "max-depth", 2, "max traversal depth")
	cobra.CheckErr(querySpecDepsCmd.MarkFlagRequired("name"))

	querySpecUsecaseCmd.Flags().StringVar(&specUsecaseName, "name", "", "usecase name")
	cobra.CheckErr(querySpecUsecaseCmd.MarkFlagRequired("name"))

	querySpecCoverageCmd.Flags().StringVar(&specCoverageName, "name", "", "capability name (slug)")
	querySpecCoverageCmd.Flags().StringVar(&specCoverageProduct, "product", "", "filter by product name")
	querySpecCoverageCmd.Flags().StringVar(&specCoverageMode, "mode", "saved", "saved (stored metrics) or gaps (unresolved mentions)")
	cobra.CheckErr(querySpecCoverageCmd.MarkFlagRequired("name"))

	querySpecHistoryCmd.Flags().StringVar(&specHistoryName, "name", "", "capability name (slug)")
	querySpecHistoryCmd.Flags().StringVar(&specHistoryChange, "change", "", "change name")
	querySpecHistoryCmd.Flags().StringVar(&specHistoryProduct, "product", "", "filter by product name")
	querySpecHistoryCmd.MarkFlagsOneRequired("name", "change")
	querySpecHistoryCmd.MarkFlagsMutuallyExclusive("name", "change")

	querySpecCmd.AddCommand(querySpecSearchCmd)
	querySpecCmd.AddCommand(querySpecByCodeCmd)
	querySpecCmd.AddCommand(querySpecDepsCmd)
	querySpecCmd.AddCommand(querySpecUsecaseCmd)
	querySpecCmd.AddCommand(querySpecCoverageCmd)
	querySpecCmd.AddCommand(querySpecHistoryCmd)

	queryCmd.AddCommand(querySpecCmd)
}

// runSpecCommand — аналог runQueryCommand, но для spec-запросов через store.DB напрямую.
func runSpecCommand(commandName string, filters map[string]string, run func(db *store.DB) (interface{}, error)) error {
	result, err := executeSpecQuery(run)
	if err != nil {
		return handleSpecError(commandName, err)
	}
	return printResults(commandName, filters, result)
}

func executeSpecQuery(run func(db *store.DB) (interface{}, error)) (interface{}, error) {
	return specsvcExecute(run)
}

func handleSpecError(commandName string, err error) error {
	if !outputJSON {
		return err
	}
	return writeQueryErrorResponse(commandName, err)
}
