package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/query"
	"github.com/codebase/internal/querysvc"
)

func runQueryCommand(spec queryCommandSpec) error {
	results, err := executeQuery(spec.run)
	if err != nil {
		return handleQueryError(spec.commandName, err)
	}
	return printResults(spec.commandName, spec.filters, results)
}

func executeQuery(run func(q *query.Query) (interface{}, error)) (interface{}, error) {
	return querysvc.Execute(run)
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

func prioritizeExactSymbolMatches(symbols []query.SymbolResult, name string, symbolType string) []query.SymbolResult {
	return querysvc.PrioritizeExactSymbolMatches(symbols, name, symbolType)
}

func inspectRelationType(symbol query.SymbolResult) string {
	return querysvc.InspectRelationType(symbol)
}

func limitInspectSymbols(symbols []query.SymbolResult, limit int) []query.SymbolResult {
	return querysvc.LimitInspectSymbols(symbols, limit)
}

func collectInspectNeighbors(symbol query.SymbolResult, incoming []query.RelationResult, outgoing []query.RelationResult) []query.SymbolResult {
	return querysvc.CollectInspectNeighbors(symbol, incoming, outgoing)
}

func classifyQueryError(err error) string {
	switch {
	case errors.Is(err, errs.ErrConfigNotLoaded):
		return "config_error"
	case errors.Is(err, errs.ErrNoRelationFilters):
		return "invalid_arguments"
	case errors.Is(err, errs.ErrDBConnect):
		return "database_unavailable"
	case errors.Is(err, errs.ErrSchemaInit):
		return "schema_init_failed"
	case errors.Is(err, errs.ErrQueryFailed):
		return "query_failed"
	case containsAny(err.Error(), "required flag(s)", "unknown flag:", "accepts 0 arg(s)", "unknown command"):
		return "invalid_arguments"
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
