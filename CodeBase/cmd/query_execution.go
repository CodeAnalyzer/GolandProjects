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

func boolFilterValue(enabled bool) string {
	if !enabled {
		return ""
	}
	return "true"
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
	symbols, err := q.SearchSymbol(name, symbolType, true, limit)
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
