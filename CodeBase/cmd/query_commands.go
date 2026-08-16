package cmd

import (
	"fmt"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/querysvc"
	"github.com/spf13/cobra"
)

var querySymbolCmd = &cobra.Command{
	Use:   "symbol --name <name> [--type <type>] [--like]",
	Short: "Search entity by name (exact by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query symbol",
			filters: map[string]string{
				"name": symbolName,
				"type": symbolType,
				"like": boolFilterValue(symbolLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchSymbol(ctx, symbolName, symbolType, symbolLikeSearch, limit)
			},
		})
	},
}

var queryInspectCmd = &cobra.Command{
	Use:   "inspect --name <name>",
	Short: "Inspect entity with related graph context",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query inspect",
			filters: map[string]string{
				"name": inspectName,
				"type": inspectType,
			},
			run: func(q *query.Query) (interface{}, error) {
				return querysvc.RunInspectQuery(ctx, q, inspectName, inspectType, limit)
			},
		})
	},
}

var queryRelationsCmd = &cobra.Command{
	Use:   "relations",
	Short: "Search relations between indexed entities",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
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
				return q.SearchRelations(ctx, relationSourceType, relationSourceName, relationTargetType, relationTargetName, relationType, limit)
			},
		})
	},
}

var queryTableCmd = &cobra.Command{
	Use:   "table --name <name>",
	Short: "Search table information",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query table",
			filters: map[string]string{
				"name": tableName,
				"like": boolFilterValue(tableLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchTable(ctx, tableName, tableLikeSearch, limit)
			},
		})
	},
}

var queryTableSchemaCmd = &cobra.Command{
	Use:   "table-schema --name <name>",
	Short: "Search table schema definitions",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query table-schema",
			filters: map[string]string{
				"name": tableName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchTableSchema(ctx, tableName, limit)
			},
		})
	},
}

var queryTableIndexCmd = &cobra.Command{
	Use:   "table-index --name <name>",
	Short: "Search SQL table index definitions",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query table-index",
			filters: map[string]string{
				"name": tableIndexName,
				"like": boolFilterValue(tableIndexLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchSQLTableIndex(ctx, tableIndexName, tableIndexLikeSearch, limit)
			},
		})
	},
}

var queryProcedureCmd = &cobra.Command{
	Use:   "procedure --name <name>",
	Short: "Show SQL procedure details",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query procedure",
			filters: map[string]string{
				"name": procedureName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.GetProcedureResult(ctx, procedureName)
			},
		})
	},
}

var queryCallersCmd = &cobra.Command{
	Use:   "callers --procedure <name>",
	Short: "Search procedure callers",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query callers",
			filters: map[string]string{
				"procedure": procedureName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.FindCallers(ctx, procedureName, limit)
			},
		})
	},
}

var queryMethodsCmd = &cobra.Command{
	Use:   "methods --table <name>",
	Short: "Search methods working with table",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query methods",
			filters: map[string]string{
				"table": tableName,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.FindMethodsByTable(ctx, tableName, limit)
			},
		})
	},
}

var queryMethodCmd = &cobra.Command{
	Use:   "method --name <name> [--like]",
	Short: "Search PAS methods by name",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query method",
			filters: map[string]string{
				"name": methodName,
				"like": boolFilterValue(methodLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.FindPASMethodsByName(ctx, methodName, methodLikeSearch, limit)
			},
		})
	},
}

var queryFormCmd = &cobra.Command{
	Use:   "form --name <name> [--like]",
	Short: "Search DFM forms (exact by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query form",
			filters: map[string]string{
				"name": formName,
				"like": boolFilterValue(formLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchDFMForm(ctx, formName, formLikeSearch, limit)
			},
		})
	},
}

var queryFormComponentCmd = &cobra.Command{
	Use:   "form-component --name <name> [--like]",
	Short: "Search DFM form components (exact by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query form-component",
			filters: map[string]string{
				"name": formComponentName,
				"like": boolFilterValue(formComponentLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchDFMComponent(ctx, formComponentName, formComponentLikeSearch, limit)
			},
		})
	},
}

var querySQLFragmentCmd = &cobra.Command{
	Use:   "sql-fragment --text <text>",
	Short: "Search SQL query fragments by text",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query sql-fragment",
			filters: map[string]string{
				"text": queryFragmentText,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchQueryFragment(ctx, queryFragmentText, limit)
			},
		})
	},
}

var queryReportFormCmd = &cobra.Command{
	Use:   "report-form --name <name> [--like]",
	Short: "Search report forms (exact by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query report-form",
			filters: map[string]string{
				"name": reportFormName,
				"like": boolFilterValue(reportFormLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchReportForm(ctx, reportFormName, reportFormLikeSearch, limit)
			},
		})
	},
}

var queryReportFieldCmd = &cobra.Command{
	Use:   "report-field --name <name> [--like]",
	Short: "Search TPR report fields (exact by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query report-field",
			filters: map[string]string{
				"name": reportFieldName,
				"like": boolFilterValue(reportFieldLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchReportField(ctx, reportFieldName, reportFieldLikeSearch, limit)
			},
		})
	},
}

var queryReportParamCmd = &cobra.Command{
	Use:   "report-param --name <name> [--like]",
	Short: "Search report params and report controls (exact by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query report-param",
			filters: map[string]string{
				"name": reportParamName,
				"like": boolFilterValue(reportParamLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchReportParam(ctx, reportParamName, reportParamLikeSearch, limit)
			},
		})
	},
}

var queryVBFunctionCmd = &cobra.Command{
	Use:   "vb-function --name <name> [--like]",
	Short: "Search VBScript function (exact by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query vb-function",
			filters: map[string]string{
				"name": vbFuncName,
				"like": boolFilterValue(vbFunctionLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchVBFunction(ctx, vbFuncName, vbFunctionLikeSearch, limit)
			},
		})
	},
}

var queryJSFunctionCmd = &cobra.Command{
	Use:   "js-function --name <name> [--like]",
	Short: "Search JS function (exact by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query js-function",
			filters: map[string]string{
				"name": jsFuncName,
				"like": boolFilterValue(jsFunctionLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchJSFunction(ctx, jsFuncName, jsFunctionLikeSearch, limit)
			},
		})
	},
}

var querySMFInstrumentCmd = &cobra.Command{
	Use:   "smf-instrument --name <name> [--like]",
	Short: "Search SMF instrument (exact by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query smf-instrument",
			filters: map[string]string{
				"name": smfInstrName,
				"like": boolFilterValue(smfInstrumentLikeSearch),
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchSMFInstrument(ctx, smfInstrName, smfInstrumentLikeSearch, limit)
			},
		})
	},
}

var querySMFTypeCmd = &cobra.Command{
	Use:   "smf-type --type <type>",
	Short: "Search SMF by scenario type",
	Long:  `Search SMF instruments by type: instrument_model or mass_operation`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query smf-type",
			filters: map[string]string{
				"type": smfType,
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchSMFByType(ctx, smfType, limit)
			},
		})
	},
}

var queryRetCodeCmd = &cobra.Command{
	Use:   "retcode [--code <N>] [--message <text>]",
	Short: "Look up return code descriptions from ds_return_codes",
	Long:  `Search ds_return_codes by numeric ret_code (exact) or by message text fragment (case-insensitive partial match).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		return runQueryCommand(queryCommandSpec{
			commandName: "query retcode",
			filters: map[string]string{
				"code":    fmt.Sprintf("%d", retCode),
				"message": retCodeMessage,
			},
			run: func(q *query.Query) (interface{}, error) {
				if retCode != 0 {
					return q.LookupRetCode(ctx, retCode)
				}
				if retCodeMessage != "" {
					return q.LookupRetCodeByMessage(ctx, retCodeMessage, limit)
				}
				return nil, fmt.Errorf("either --code or --message is required")
			},
		})
	},
}
