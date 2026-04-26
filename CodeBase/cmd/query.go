package cmd

import (
	"github.com/spf13/cobra"
)

func init() {
	queryCmd.PersistentFlags().BoolVar(&outputJSON, "json", false, "output as JSON")
	queryCmd.PersistentFlags().BoolVar(&outputNDJSON, "ndjson", false, "output as NDJSON")
	queryCmd.PersistentFlags().BoolVar(&outputSummary, "summary", false, "output summary only")
	queryCmd.PersistentFlags().IntVar(&limit, "limit", 100, "max results to return")

	querySymbolCmd.Flags().StringVar(&symbolName, "name", "", "symbol name to search (exact by default)")
	querySymbolCmd.Flags().StringVar(&symbolType, "type", "", "symbol type (procedure, function, class, etc.)")
	querySymbolCmd.Flags().BoolVar(&symbolLikeSearch, "like", false, "use partial match search for symbol name")
	cobra.CheckErr(querySymbolCmd.MarkFlagRequired("name"))

	queryTableCmd.Flags().StringVar(&tableName, "name", "", "table name to search")
	queryTableCmd.Flags().BoolVar(&tableLikeSearch, "like", false, "use partial match search for table name")
	cobra.CheckErr(queryTableCmd.MarkFlagRequired("name"))

	queryTableSchemaCmd.Flags().StringVar(&tableName, "name", "", "table name to inspect schema")
	cobra.CheckErr(queryTableSchemaCmd.MarkFlagRequired("name"))

	queryTableIndexCmd.Flags().StringVar(&tableIndexName, "name", "", "table index or table name to search")
	queryTableIndexCmd.Flags().BoolVar(&tableIndexLikeSearch, "like", false, "use partial match search for table index or table name")
	cobra.CheckErr(queryTableIndexCmd.MarkFlagRequired("name"))

	queryProcedureCmd.Flags().StringVar(&procedureName, "name", "", "procedure name to inspect")
	cobra.CheckErr(queryProcedureCmd.MarkFlagRequired("name"))

	queryCallersCmd.Flags().StringVar(&procedureName, "procedure", "", "procedure name")
	cobra.CheckErr(queryCallersCmd.MarkFlagRequired("procedure"))

	queryMethodsCmd.Flags().StringVar(&tableName, "table", "", "table name")
	cobra.CheckErr(queryMethodsCmd.MarkFlagRequired("table"))

	queryFormCmd.Flags().StringVar(&formName, "name", "", "DFM form name/class/caption to search (exact by default)")
	queryFormCmd.Flags().BoolVar(&formLikeSearch, "like", false, "use partial match search for DFM form fields")
	cobra.CheckErr(queryFormCmd.MarkFlagRequired("name"))

	queryFormComponentCmd.Flags().StringVar(&formComponentName, "name", "", "DFM component name/type/caption to search (exact by default)")
	queryFormComponentCmd.Flags().BoolVar(&formComponentLikeSearch, "like", false, "use partial match search for DFM component fields")
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

	queryJSFunctionCmd.Flags().StringVar(&jsFuncName, "name", "", "JS function name to search (exact by default)")
	queryJSFunctionCmd.Flags().BoolVar(&jsFunctionLikeSearch, "like", false, "use partial match search for JS function name")
	cobra.CheckErr(queryJSFunctionCmd.MarkFlagRequired("name"))

	querySMFInstrumentCmd.Flags().StringVar(&smfInstrName, "name", "", "SMF instrument name, brief, or file name to search (exact by default)")
	querySMFInstrumentCmd.Flags().BoolVar(&smfInstrumentLikeSearch, "like", false, "use partial match search for SMF instrument attributes")
	cobra.CheckErr(querySMFInstrumentCmd.MarkFlagRequired("name"))

	querySMFTypeCmd.Flags().StringVar(&smfType, "type", "", "SMF scenario type (instrument_model, mass_operation)")
	cobra.CheckErr(querySMFTypeCmd.MarkFlagRequired("type"))

	queryReportFormCmd.Flags().StringVar(&reportFormName, "name", "", "report form name to search (exact by default)")
	queryReportFormCmd.Flags().BoolVar(&reportFormLikeSearch, "like", false, "use partial match search for report form fields")
	cobra.CheckErr(queryReportFormCmd.MarkFlagRequired("name"))

	queryReportFieldCmd.Flags().StringVar(&reportFieldName, "name", "", "report field name to search (exact by default)")
	queryReportFieldCmd.Flags().BoolVar(&reportFieldLikeSearch, "like", false, "use partial match search for report field attributes")
	cobra.CheckErr(queryReportFieldCmd.MarkFlagRequired("name"))

	queryReportParamCmd.Flags().StringVar(&reportParamName, "name", "", "report param name/text to search (exact by default)")
	queryReportParamCmd.Flags().BoolVar(&reportParamLikeSearch, "like", false, "use partial match search for report param attributes")
	cobra.CheckErr(queryReportParamCmd.MarkFlagRequired("name"))

	queryVBFunctionCmd.Flags().StringVar(&vbFuncName, "name", "", "VB function name to search (exact by default)")
	queryVBFunctionCmd.Flags().BoolVar(&vbFunctionLikeSearch, "like", false, "use partial match search for VB function name")
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
