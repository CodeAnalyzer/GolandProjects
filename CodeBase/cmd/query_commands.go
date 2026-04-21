package cmd

import (
	"github.com/codebase/internal/query"
	"github.com/spf13/cobra"
)

var querySymbolCmd = &cobra.Command{
	Use:   "symbol --name <name>",
	Short: "Search entity by name",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query symbol",
			filters: map[string]string{
				"name": symbolName,
				"type": symbolType,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchSymbol(symbolName, symbolType, limit)
			},
		})
	},
}

var queryInspectCmd = &cobra.Command{
	Use:   "inspect --name <name>",
	Short: "Inspect entity with related graph context",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query inspect",
			filters: map[string]string{
				"name": inspectName,
				"type": inspectType,
			},
			run: func(q *query.Query) (interface{}, error) {
				return runInspectQuery(q, inspectName, inspectType, limit)
			},
		})
	},
}

var queryRelationsCmd = &cobra.Command{
	Use:   "relations",
	Short: "Search relations between indexed entities",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query relations",
			filters: map[string]string{
				"source_type":   relationSourceType,
				"source_name":   relationSourceName,
				"target_type":   relationTargetType,
				"target_name":   relationTargetName,
				"relation_type": relationType,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchRelations(relationSourceType, relationSourceName, relationTargetType, relationTargetName, relationType, limit)
			},
		})
	},
}

var queryTableCmd = &cobra.Command{
	Use:   "table --name <name>",
	Short: "Search table information",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query table",
			filters: map[string]string{
				"name": tableName,
				"like": boolFilterValue(tableLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchTable(tableName, tableLikeSearch, limit)
			},
		})
	},
}

var queryTableSchemaCmd = &cobra.Command{
	Use:   "table-schema --name <name>",
	Short: "Search table schema definitions",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query table-schema",
			filters: map[string]string{
				"name": tableName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchTableSchema(tableName, limit)
			},
		})
	},
}

var queryTableIndexCmd = &cobra.Command{
	Use:   "table-index --name <name>",
	Short: "Search SQL table index definitions",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query table-index",
			filters: map[string]string{
				"name": tableIndexName,
				"like": boolFilterValue(tableIndexLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchSQLTableIndex(tableIndexName, tableIndexLikeSearch, limit)
			},
		})
	},
}

var queryProcedureCmd = &cobra.Command{
	Use:   "procedure --name <name>",
	Short: "Show SQL procedure details",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query procedure",
			filters: map[string]string{
				"name": procedureName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.GetProcedureResult(procedureName)
			},
		})
	},
}

var queryCallersCmd = &cobra.Command{
	Use:   "callers --procedure <name>",
	Short: "Search procedure callers",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query callers",
			filters: map[string]string{
				"procedure": procedureName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.FindCallers(procedureName, limit)
			},
		})
	},
}

var queryMethodsCmd = &cobra.Command{
	Use:   "methods --table <name>",
	Short: "Search methods working with table",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query methods",
			filters: map[string]string{
				"table": tableName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.FindMethodsByTable(tableName, limit)
			},
		})
	},
}

var queryFormCmd = &cobra.Command{
	Use:   "form --name <name>",
	Short: "Search DFM forms",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query form",
			filters: map[string]string{
				"name": formName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchDFMForm(formName, limit)
			},
		})
	},
}

var queryFormComponentCmd = &cobra.Command{
	Use:   "form-component --name <name>",
	Short: "Search DFM form components",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query form-component",
			filters: map[string]string{
				"name": formComponentName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchDFMComponent(formComponentName, limit)
			},
		})
	},
}

var querySQLFragmentCmd = &cobra.Command{
	Use:   "sql-fragment --text <text>",
	Short: "Search SQL query fragments by text",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query sql-fragment",
			filters: map[string]string{
				"text": queryFragmentText,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchQueryFragment(queryFragmentText, limit)
			},
		})
	},
}

var queryReportFormCmd = &cobra.Command{
	Use:   "report-form --name <name>",
	Short: "Search report forms",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query report-form",
			filters: map[string]string{
				"name": reportFormName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchReportForm(reportFormName, limit)
			},
		})
	},
}

var queryReportFieldCmd = &cobra.Command{
	Use:   "report-field --name <name>",
	Short: "Search TPR report fields",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query report-field",
			filters: map[string]string{
				"name": reportFieldName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchReportField(reportFieldName, limit)
			},
		})
	},
}

var queryReportParamCmd = &cobra.Command{
	Use:   "report-param --name <name>",
	Short: "Search report params and report controls",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query report-param",
			filters: map[string]string{
				"name": reportParamName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchReportParam(reportParamName, limit)
			},
		})
	},
}

var queryVBFunctionCmd = &cobra.Command{
	Use:   "vb-function --name <name>",
	Short: "Search VBScript function",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query vb-function",
			filters: map[string]string{
				"name": vbFuncName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchVBFunction(vbFuncName, limit)
			},
		})
	},
}

var queryJSFunctionCmd = &cobra.Command{
	Use:   "js-function --name <name>",
	Short: "Search JS function",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query js-function",
			filters: map[string]string{
				"name": jsFuncName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchJSFunction(jsFuncName, limit)
			},
		})
	},
}

var querySMFInstrumentCmd = &cobra.Command{
	Use:   "smf-instrument --name <name>",
	Short: "Search SMF instrument",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query smf-instrument",
			filters: map[string]string{
				"name": smfInstrName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchSMFInstrument(smfInstrName, limit)
			},
		})
	},
}

var querySMFTypeCmd = &cobra.Command{
	Use:   "smf-type --type <type>",
	Short: "Search SMF by scenario type",
	Long:  `Search SMF instruments by type: instrument_model or mass_operation`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query smf-type",
			filters: map[string]string{
				"type": smfType,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchSMFByType(smfType, limit)
			},
		})
	},
}
