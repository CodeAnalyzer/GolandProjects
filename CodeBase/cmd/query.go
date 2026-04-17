package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/query"
	"github.com/codebase/internal/store"
	"github.com/spf13/cobra"
)

var (
	outputJSON    bool
	outputNDJSON  bool
	outputSummary bool
	limit         int
)

type queryResponseMeta struct {
	Limit   int               `json:"limit"`
	Filters map[string]string `json:"filters,omitempty"`
	Output  string            `json:"output,omitempty"`
}

type querySuccessResponse struct {
	Success       bool              `json:"success"`
	FormatVersion string            `json:"format_version"`
	Command       string            `json:"command"`
	Count         int               `json:"count"`
	Items         interface{}       `json:"items"`
	Summary       interface{}       `json:"summary,omitempty"`
	Meta          queryResponseMeta `json:"meta"`
}

type queryErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type queryErrorResponse struct {
	Success       bool           `json:"success"`
	FormatVersion string         `json:"format_version"`
	Command       string         `json:"command"`
	Error         queryErrorBody `json:"error"`
}

type queryCommandSpec struct {
	commandName string
	filters     map[string]string
	run         func(q *query.Query) (interface{}, error)
}

type querySummary struct {
	Count           int            `json:"count"`
	Kinds           map[string]int `json:"kinds,omitempty"`
	Files           int            `json:"files,omitempty"`
	RelationTypes   map[string]int `json:"relation_types,omitempty"`
	SourceTypes     map[string]int `json:"source_types,omitempty"`
	TargetTypes     map[string]int `json:"target_types,omitempty"`
	DistinctTargets int            `json:"distinct_targets,omitempty"`
}

type inspectResult struct {
	Symbol    query.SymbolResult     `json:"symbol"`
	Incoming  []query.RelationResult `json:"incoming"`
	Outgoing  []query.RelationResult `json:"outgoing"`
	Neighbors []query.SymbolResult   `json:"neighbors,omitempty"`
}

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Point-in-time index queries",
	Long:  `Searches for entities in the index and outputs results.`,
}

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
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchTable(tableName, limit)
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
			},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchSQLTableIndex(tableIndexName, limit)
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

var (
	symbolName         string
	symbolType         string
	tableName          string
	tableIndexName     string
	procedureName      string
	jsFuncName         string
	smfInstrName       string
	smfType            string
	formName           string
	formComponentName  string
	queryFragmentText  string
	reportFormName     string
	reportFieldName    string
	reportParamName    string
	vbFuncName         string
	relationSourceType string
	relationSourceName string
	relationTargetType string
	relationTargetName string
	relationType       string
	inspectName        string
	inspectType        string
)

func runQueryCommand(spec queryCommandSpec) error {
	results, err := executeQuery(spec.run)
	if err != nil {
		return handleQueryError(spec.commandName, err)
	}
	return printResults(spec.commandName, spec.filters, results)
}

func executeQuery(run func(q *query.Query) (interface{}, error)) (interface{}, error) {
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

	q := query.New(db)
	results, err := run(q)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return results, nil
}

func printResults(commandName string, filters map[string]string, results interface{}) error {
	if outputNDJSON {
		return writeNDJSON(os.Stdout, normalizeNilResults(results))
	}

	if outputJSON {
		meta := queryResponseMeta{
			Limit:   limit,
			Filters: filterEmptyValues(filters),
			Output:  detectQueryOutputMode(),
		}
		response := querySuccessResponse{
			Success:       true,
			FormatVersion: "1.0",
			Command:       commandName,
			Count:         resultCount(results),
			Items:         normalizeNilResults(results),
			Meta:          meta,
		}
		if outputSummary {
			response.Summary = buildQuerySummary(results)
		}
		return writeJSON(os.Stdout, response)
	}

	if outputSummary {
		return writeJSON(os.Stdout, buildQuerySummary(results))
	}

	fmt.Printf("%+v\n", normalizeNilResults(results))
	return nil
}

func handleQueryError(commandName string, err error) error {
	if !outputJSON {
		return err
	}
	return writeQueryErrorResponse(commandName, err)
}

func writeQueryErrorResponse(commandName string, err error) error {
	response := queryErrorResponse{
		Success:       false,
		FormatVersion: "1.0",
		Command:       commandName,
		Error: queryErrorBody{
			Code:    classifyQueryError(err),
			Message: err.Error(),
		},
	}
	if writeErr := writeJSON(os.Stdout, response); writeErr != nil {
		return writeErr
	}
	return nil
}

func writeJSON(out *os.File, value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(data))
	return err
}

func writeNDJSON(out *os.File, value interface{}) error {
	items := normalizeNilResults(value)
	rv := reflect.ValueOf(items)
	if !rv.IsValid() {
		return nil
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		data, err := json.Marshal(items)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(data))
		return err
	}
	for i := 0; i < rv.Len(); i++ {
		data, err := json.Marshal(rv.Index(i).Interface())
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(out, string(data)); err != nil {
			return err
		}
	}
	return nil
}

func resultCount(results interface{}) int {
	value := reflect.ValueOf(results)
	if !value.IsValid() {
		return 0
	}
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
		return value.Len()
	}
	return 1
}

func normalizeNilResults(results interface{}) interface{} {
	if results == nil {
		return []interface{}{}
	}
	value := reflect.ValueOf(results)
	if !value.IsValid() {
		return []interface{}{}
	}
	if value.Kind() == reflect.Slice && value.IsNil() {
		return reflect.MakeSlice(value.Type(), 0, 0).Interface()
	}
	return results
}

func filterEmptyValues(filters map[string]string) map[string]string {
	if len(filters) == 0 {
		return nil
	}
	result := make(map[string]string)
	for key, value := range filters {
		if value != "" {
			result[key] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func detectQueryOutputMode() string {
	switch {
	case outputNDJSON:
		return "ndjson"
	case outputJSON:
		return "json"
	case outputSummary:
		return "summary"
	default:
		return "text"
	}
}

func buildQuerySummary(results interface{}) querySummary {
	items := normalizeNilResults(results)
	summary := querySummary{
		Count:         resultCount(items),
		Kinds:         map[string]int{},
		RelationTypes: map[string]int{},
		SourceTypes:   map[string]int{},
		TargetTypes:   map[string]int{},
	}
	files := map[string]struct{}{}
	targets := map[string]struct{}{}
	rv := reflect.ValueOf(items)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
		return summary
	}
	for i := 0; i < rv.Len(); i++ {
		item := rv.Index(i)
		if item.Kind() == reflect.Pointer && !item.IsNil() {
			item = item.Elem()
		}
		if item.Kind() != reflect.Struct {
			continue
		}
		incrementSummaryField(summary.Kinds, item, "Type")
		incrementSummaryField(summary.Kinds, item, "EntityType")
		incrementSummaryField(summary.Kinds, item, "CallerType")
		incrementSummaryField(summary.Kinds, item, "ReportType")
		incrementSummaryField(summary.Kinds, item, "ScenarioType")
		incrementSummaryField(summary.RelationTypes, item, "RelationType")
		if source := item.FieldByName("Source"); source.IsValid() {
			incrementSummaryField(summary.SourceTypes, source, "Type")
			collectSummaryField(files, source, "File")
			collectSummaryField(targets, source, "Name")
		}
		if target := item.FieldByName("Target"); target.IsValid() {
			incrementSummaryField(summary.TargetTypes, target, "Type")
			collectSummaryField(files, target, "File")
			collectSummaryField(targets, target, "Name")
		}
		collectSummaryField(files, item, "File")
		collectSummarySliceField(files, item, "Files")
	}
	if len(files) > 0 {
		summary.Files = len(files)
	}
	if len(targets) > 0 {
		summary.DistinctTargets = len(targets)
	}
	if len(summary.Kinds) == 0 {
		summary.Kinds = nil
	}
	if len(summary.RelationTypes) == 0 {
		summary.RelationTypes = nil
	}
	if len(summary.SourceTypes) == 0 {
		summary.SourceTypes = nil
	}
	if len(summary.TargetTypes) == 0 {
		summary.TargetTypes = nil
	}
	return summary
}

func incrementSummaryField(counter map[string]int, value reflect.Value, fieldName string) {
	field := value.FieldByName(fieldName)
	if !field.IsValid() || field.Kind() != reflect.String {
		return
	}
	name := strings.TrimSpace(field.String())
	if name == "" {
		return
	}
	counter[name]++
}

func collectSummaryField(set map[string]struct{}, value reflect.Value, fieldName string) {
	field := value.FieldByName(fieldName)
	if !field.IsValid() || field.Kind() != reflect.String {
		return
	}
	name := strings.TrimSpace(field.String())
	if name == "" {
		return
	}
	set[name] = struct{}{}
}

func collectSummarySliceField(set map[string]struct{}, value reflect.Value, fieldName string) {
	field := value.FieldByName(fieldName)
	if !field.IsValid() || field.Kind() != reflect.Slice {
		return
	}
	for i := 0; i < field.Len(); i++ {
		item := field.Index(i)
		if item.Kind() == reflect.String {
			name := strings.TrimSpace(item.String())
			if name != "" {
				set[name] = struct{}{}
			}
		}
	}
}

func runInspectQuery(q *query.Query, name string, symbolType string, limit int) ([]inspectResult, error) {
	symbols, err := q.SearchSymbol(name, symbolType, limit)
	if err != nil {
		return nil, err
	}
	ordered := prioritizeExactSymbolMatches(symbols, name, symbolType)
	results := make([]inspectResult, 0, len(ordered))
	for _, symbol := range ordered {
		relationType := inspectRelationType(symbol)
		outgoing, err := q.SearchRelationsByEntity(relationType, symbol.EntityID, "", 0, "", limit)
		if err != nil && !strings.Contains(err.Error(), "at least one relation filter must be provided") {
			return nil, err
		}
		incoming, err := q.SearchRelationsByEntity("", 0, relationType, symbol.EntityID, "", limit)
		if err != nil && !strings.Contains(err.Error(), "at least one relation filter must be provided") {
			return nil, err
		}
		neighbors := collectInspectNeighbors(symbol, incoming, outgoing)
		results = append(results, inspectResult{
			Symbol:    symbol,
			Incoming:  incoming,
			Outgoing:  outgoing,
			Neighbors: neighbors,
		})
	}
	return results, nil
}

func inspectRelationType(symbol query.SymbolResult) string {
	if strings.TrimSpace(symbol.Type) == "" {
		return strings.TrimSpace(symbol.EntityType)
	}
	switch strings.ToLower(strings.TrimSpace(symbol.EntityType)) {
	case "sql":
		if strings.EqualFold(symbol.Type, "procedure") {
			return "sql_procedure"
		}
		if strings.EqualFold(symbol.Type, "table") {
			return "sql_table"
		}
	case "dfm":
		if strings.EqualFold(symbol.Type, "form") {
			return "dfm_form"
		}
		if strings.EqualFold(symbol.Type, "component") {
			return "dfm_component"
		}
	}
	return strings.TrimSpace(symbol.Type)
}

func prioritizeExactSymbolMatches(symbols []query.SymbolResult, name string, symbolType string) []query.SymbolResult {
	ordered := append([]query.SymbolResult(nil), symbols...)
	needleName := strings.ToLower(strings.TrimSpace(name))
	needleType := strings.ToLower(strings.TrimSpace(symbolType))
	sort.SliceStable(ordered, func(i, j int) bool {
		left := inspectScore(ordered[i], needleName, needleType)
		right := inspectScore(ordered[j], needleName, needleType)
		if left != right {
			return left > right
		}
		return ordered[i].Name < ordered[j].Name
	})
	return ordered
}

func inspectScore(item query.SymbolResult, needleName string, needleType string) int {
	score := 0
	if strings.EqualFold(item.Name, needleName) {
		score += 10
	}
	if needleType != "" && strings.EqualFold(item.Type, needleType) {
		score += 5
	}
	if strings.EqualFold(item.EntityType, needleType) {
		score += 3
	}
	return score
}

func collectInspectNeighbors(symbol query.SymbolResult, incoming []query.RelationResult, outgoing []query.RelationResult) []query.SymbolResult {
	neighbors := make([]query.SymbolResult, 0)
	seen := map[string]struct{}{}
	appendNeighbor := func(ref query.RelationEntityRef) {
		key := fmt.Sprintf("%s:%d", ref.Type, ref.ID)
		if ref.ID == 0 || key == fmt.Sprintf("%s:%d", symbol.EntityType, symbol.ID) {
			return
		}
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		neighbors = append(neighbors, query.SymbolResult{
			ID:         ref.ID,
			FileID:     ref.FileID,
			Name:       ref.Name,
			Type:       ref.Type,
			EntityType: ref.Type,
			File:       ref.File,
			LineNumber: ref.LineNumber,
		})
	}
	for _, relation := range incoming {
		appendNeighbor(relation.Source)
	}
	for _, relation := range outgoing {
		appendNeighbor(relation.Target)
	}
	return neighbors
}

func classifyQueryError(err error) string {
	message := err.Error()
	switch {
	case message == "config not loaded":
		return "config_error"
	case containsAny(message, "required flag(s)", "unknown flag:", "accepts 0 arg(s)", "unknown command", "at least one relation filter must be provided"):
		return "invalid_arguments"
	case containsAny(message, "failed to connect to database", "connection refused", "dial tcp"):
		return "database_unavailable"
	case containsAny(message, "failed to init schema"):
		return "schema_init_failed"
	case containsAny(message, "query failed"):
		return "query_failed"
	default:
		return "internal_error"
	}
}

func containsAny(value string, parts ...string) bool {
	for _, part := range parts {
		if part != "" && strings.Contains(value, part) {
			return true
		}
	}
	return false
}

func init() {
	queryCmd.PersistentFlags().BoolVar(&outputJSON, "json", false, "output as JSON")
	queryCmd.PersistentFlags().BoolVar(&outputNDJSON, "ndjson", false, "output as NDJSON")
	queryCmd.PersistentFlags().BoolVar(&outputSummary, "summary", false, "output summary only")
	queryCmd.PersistentFlags().IntVar(&limit, "limit", 100, "max results to return")

	querySymbolCmd.Flags().StringVar(&symbolName, "name", "", "symbol name to search")
	querySymbolCmd.Flags().StringVar(&symbolType, "type", "", "symbol type (procedure, function, class, etc.)")
	cobra.CheckErr(querySymbolCmd.MarkFlagRequired("name"))

	queryTableCmd.Flags().StringVar(&tableName, "name", "", "table name to search")
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

	querySMFInstrumentCmd.Flags().StringVar(&smfInstrName, "name", "", "SMF instrument name to search")
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
