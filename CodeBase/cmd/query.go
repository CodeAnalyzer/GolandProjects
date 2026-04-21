package cmd

import (
	"github.com/spf13/cobra"
)

func init() {
	queryCmd.PersistentFlags().BoolVar(&outputJSON, "json", false, "output as JSON")
	queryCmd.PersistentFlags().BoolVar(&outputNDJSON, "ndjson", false, "output as NDJSON")
	queryCmd.PersistentFlags().BoolVar(&outputSummary, "summary", false, "output summary only")
	queryCmd.PersistentFlags().IntVar(&limit, "limit", 100, "max results to return")

	querySymbolCmd.Flags().StringVar(&symbolName, "name", "", "symbol name to search")
	querySymbolCmd.Flags().StringVar(&symbolType, "type", "", "symbol type (procedure, function, class, etc.)")
	cobra.CheckErr(querySymbolCmd.MarkFlagRequired("name"))

	queryTableCmd.Flags().StringVar(&tableName, "name", "", "table name to search")
	queryTableCmd.Flags().BoolVar(&tableLikeSearch, "like", false, "use partial match search for table name")
	cobra.CheckErr(queryTableCmd.MarkFlagRequired("name"))

	queryTableSchemaCmd.Flags().StringVar(&tableName, "name", "", "table name to inspect schema")
	cobra.CheckErr(queryTableSchemaCmd.MarkFlagRequired("name"))

	queryTableIndexCmd.Flags().StringVar(&tableIndexName, "name", "", "table index or table name to search")
	cobra.CheckErr(queryTableIndexCmd.MarkFlagRequired("name"))

	queryProcedureCmd.Flags().StringVar(&procedureName, "name", "", "procedure name to inspect")
	cobra.CheckErr(queryProcedureCmd.MarkFlagRequired("name"))

	queryCallersCmd.Flags().StringVar(&procedureName, "procedure", "", "procedure name")
	cobra.CheckErr(queryCallersCmd.MarkFlagRequired("procedure"))

	queryMethodsCmd.Flags().StringVar(&tableName, "table", "", "table name")
	cobra.CheckErr(queryMethodsCmd.MarkFlagRequired("table"))

	queryFormCmd.Flags().StringVar(&formName, "name", "", "DFM form name/class/caption to search")
	cobra.CheckErr(queryFormCmd.MarkFlagRequired("name"))

	queryFormComponentCmd.Flags().StringVar(&formComponentName, "name", "", "DFM component name/type/caption to search")
	cobra.CheckErr(queryFormComponentCmd.MarkFlagRequired("name"))

	querySQLFragmentCmd.Flags().StringVar(&queryFragmentText, "text", "", "SQL text fragment to search")
	cobra.CheckErr(querySQLFragmentCmd.MarkFlagRequired("text"))

	queryRelationsCmd.Flags().StringVar(&relationSourceType, "source-type", "", "source entity type")
	queryRelationsCmd.Flags().StringVar(&relationSourceName, "source-name", "", "source entity name pattern")
	queryRelationsCmd.Flags().StringVar(&relationTargetType, "target-type", "", "target entity type")
	queryRelationsCmd.Flags().StringVar(&relationTargetName, "target-name", "", "target entity name pattern")
	queryRelationsCmd.Flags().StringVar(&relationType, "relation-type", "", "relation type")

	queryInspectCmd.Flags().StringVar(&inspectName, "name", "", "entity name to inspect")
	queryInspectCmd.Flags().StringVar(&inspectType, "type", "", "symbol type to inspect")
	cobra.CheckErr(queryInspectCmd.MarkFlagRequired("name"))

	queryJSFunctionCmd.Flags().StringVar(&jsFuncName, "name", "", "JS function name to search")
	cobra.CheckErr(queryJSFunctionCmd.MarkFlagRequired("name"))

	querySMFInstrumentCmd.Flags().StringVar(&smfInstrName, "name", "", "SMF instrument name, brief, or file name to search")
	cobra.CheckErr(querySMFInstrumentCmd.MarkFlagRequired("name"))

	querySMFTypeCmd.Flags().StringVar(&smfType, "type", "", "SMF scenario type (instrument_model, mass_operation)")
	cobra.CheckErr(querySMFTypeCmd.MarkFlagRequired("type"))

	queryReportFormCmd.Flags().StringVar(&reportFormName, "name", "", "report form name to search")
	cobra.CheckErr(queryReportFormCmd.MarkFlagRequired("name"))

	queryReportFieldCmd.Flags().StringVar(&reportFieldName, "name", "", "report field name to search")
	cobra.CheckErr(queryReportFieldCmd.MarkFlagRequired("name"))

	queryReportParamCmd.Flags().StringVar(&reportParamName, "name", "", "report param name/text to search")
	cobra.CheckErr(queryReportParamCmd.MarkFlagRequired("name"))

	queryVBFunctionCmd.Flags().StringVar(&vbFuncName, "name", "", "VB function name to search")
	cobra.CheckErr(queryVBFunctionCmd.MarkFlagRequired("name"))

	queryCmd.AddCommand(querySymbolCmd)
	queryCmd.AddCommand(queryTableCmd)
	queryCmd.AddCommand(queryTableSchemaCmd)
	queryCmd.AddCommand(queryTableIndexCmd)
	queryCmd.AddCommand(queryProcedureCmd)
	queryCmd.AddCommand(queryCallersCmd)
	queryCmd.AddCommand(queryMethodsCmd)
	queryCmd.AddCommand(queryFormCmd)
	queryCmd.AddCommand(queryFormComponentCmd)
	queryCmd.AddCommand(querySQLFragmentCmd)
	queryCmd.AddCommand(queryRelationsCmd)
	queryCmd.AddCommand(queryInspectCmd)
	queryCmd.AddCommand(queryJSFunctionCmd)
	queryCmd.AddCommand(querySMFInstrumentCmd)
	queryCmd.AddCommand(querySMFTypeCmd)
	queryCmd.AddCommand(queryReportFormCmd)
	queryCmd.AddCommand(queryReportFieldCmd)
	queryCmd.AddCommand(queryReportParamCmd)
	queryCmd.AddCommand(queryVBFunctionCmd)

	rootCmd.AddCommand(queryCmd)
}
