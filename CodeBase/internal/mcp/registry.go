package mcp

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/querysvc"
	"github.com/codebase/internal/review"
	"github.com/codebase/internal/reviewsvc"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/systemsvc"
)

type toolHandler func(args map[string]interface{}) (interface{}, error)

type registeredTool struct {
	Definition toolDefinition
	Handler    toolHandler
}

func optionalStringSlice(args map[string]interface{}, key string) ([]string, error) {
	value, ok := args[key]
	if !ok || value == nil {
		return nil, nil
	}
	items, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("argument %s must be string array", key)
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("argument %s must be string array", key)
		}
		if text != "" {
			result = append(result, text)
		}
	}
	return result, nil
}

func optionalInt(args map[string]interface{}, key string) (int, error) {
	value, ok := args[key]
	if !ok || value == nil {
		return 0, nil
	}
	switch v := value.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("argument %s must be integer", key)
	}
}

var toolRegistry = buildToolRegistry(nil)

func buildToolRegistry(db *store.DB) map[string]registeredTool {
	return map[string]registeredTool{
		"codebase_ping": {
			Definition: toolDefinition{
				Name:        "codebase_ping",
				Description: "Health check tool for MCP transport",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				return map[string]interface{}{"ok": true, "service": "codebase-mcp"}, nil
			},
		},
		"codebase_health": {
			Definition: toolDefinition{
				Name:        "codebase_health",
				Description: "Check CodeBase readiness",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				return systemsvc.ExecuteHealth()
			},
		},
		"codebase_stats": {
			Definition: toolDefinition{
				Name:        "codebase_stats",
				Description: "Get CodeBase index statistics",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				return systemsvc.ExecuteStats()
			},
		},
		"codebase_review_sql": {
			Definition: toolDefinition{Name: "codebase_review_sql", Description: "Run SQL review checks for one file", InputSchema: objectSchema(map[string]interface{}{"file_path": stringProp("Full SQL file path"), "rules": map[string]interface{}{"type": "array", "description": "Optional rule ids", "items": map[string]interface{}{"type": "string"}}, "min_severity": intProp("Minimum severity")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				filePath, err := requiredString(args, "file_path")
				if err != nil {
					return nil, err
				}
				rulesRaw, err := optionalStringSlice(args, "rules")
				if err != nil {
					return nil, err
				}
				minSeverity, err := optionalInt(args, "min_severity")
				if err != nil {
					return nil, err
				}
				rules := make([]review.RuleID, 0, len(rulesRaw))
				for _, rule := range rulesRaw {
					ruleID := review.RuleID(rule)
					switch ruleID {
					case review.RuleForeignTablesUsing, review.RuleForeignPTablesUsing, review.RuleForeignProcedureUsing, review.RuleExecNotExistsProc, review.RuleProcDuplicate, review.RuleProcParamDefValue, review.RuleProcElseCase, review.RuleUseSelectAll, review.RuleTruncTbl, review.RuleDatatype:
						rules = append(rules, ruleID)
					default:
						return nil, fmt.Errorf("unknown review rule: %s", rule)
					}
				}
				opts := review.Options{Rules: rules, MinSeverity: minSeverity}
				if db != nil {
					return reviewsvc.ExecuteWith(db, filePath, opts)
				}
				return reviewsvc.Execute(filePath, opts)
			},
		},
		"codebase_query_symbol": {
			Definition: toolDefinition{Name: "codebase_query_symbol", Description: "Search symbol", InputSchema: querySchema("name", stringProp("Symbol name"), map[string]interface{}{"type": stringProp("Symbol type"), "like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				typeName, _ := optionalString(args, "type")
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchSymbol(name, typeName, like, limit)
				})
			},
		},
		"codebase_query_table": {
			Definition: toolDefinition{Name: "codebase_query_table", Description: "Search table", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchTable(name, like, limit)
				})
			},
		},
		"codebase_query_table_schema": {
			Definition: toolDefinition{Name: "codebase_query_table_schema", Description: "Search table schema", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchTableSchema(name, limit)
				})
			},
		},
		"codebase_query_table_index": {
			Definition: toolDefinition{Name: "codebase_query_table_index", Description: "Search SQL table indexes", InputSchema: querySchema("name", stringProp("Table/index name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchSQLTableIndex(name, like, limit)
				})
			},
		},
		"codebase_query_procedure": {
			Definition: toolDefinition{Name: "codebase_query_procedure", Description: "Get SQL procedure details", InputSchema: querySchema("name", stringProp("Procedure name"), map[string]interface{}{})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.GetProcedureResult(name)
				})
			},
		},
		"codebase_query_callers": {
			Definition: toolDefinition{Name: "codebase_query_callers", Description: "Find procedure callers", InputSchema: querySchema("procedure", stringProp("Procedure name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "procedure")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.FindCallers(name, limit)
				})
			},
		},
		"codebase_query_methods": {
			Definition: toolDefinition{Name: "codebase_query_methods", Description: "Find methods by table", InputSchema: querySchema("table", stringProp("Table name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				table, err := requiredString(args, "table")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.FindMethodsByTable(table, limit)
				})
			},
		},
		"codebase_query_method": {
			Definition: toolDefinition{Name: "codebase_query_method", Description: "Find PAS methods", InputSchema: querySchema("name", stringProp("Method name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.FindPASMethodsByName(name, like, limit)
				})
			},
		},
		"codebase_query_form": {
			Definition: toolDefinition{Name: "codebase_query_form", Description: "Find DFM forms", InputSchema: querySchema("name", stringProp("Form name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchDFMForm(name, like, limit)
				})
			},
		},
		"codebase_query_form_component": {
			Definition: toolDefinition{Name: "codebase_query_form_component", Description: "Find DFM form components", InputSchema: querySchema("name", stringProp("Component name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchDFMComponent(name, like, limit)
				})
			},
		},
		"codebase_query_sql_fragment": {
			Definition: toolDefinition{Name: "codebase_query_sql_fragment", Description: "Search SQL fragments", InputSchema: querySchema("text", stringProp("Text fragment"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				text, err := requiredString(args, "text")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchQueryFragment(text, limit)
				})
			},
		},
		"codebase_query_relations": {
			Definition: toolDefinition{Name: "codebase_query_relations", Description: "Search relations", InputSchema: objectSchema(map[string]interface{}{"source_type": stringProp("Source entity type"), "source_name": stringProp("Source entity name"), "target_type": stringProp("Target entity type"), "target_name": stringProp("Target entity name"), "relation_type": stringProp("Relation type"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				sourceType, _ := optionalString(args, "source_type")
				sourceName, _ := optionalString(args, "source_name")
				targetType, _ := optionalString(args, "target_type")
				targetName, _ := optionalString(args, "target_name")
				relationType, _ := optionalString(args, "relation_type")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchRelations(sourceType, sourceName, targetType, targetName, relationType, limit)
				})
			},
		},
		"codebase_query_inspect": {
			Definition: toolDefinition{Name: "codebase_query_inspect", Description: "Inspect symbol and graph neighborhood", InputSchema: querySchema("name", stringProp("Symbol name"), map[string]interface{}{"type": stringProp("Symbol type"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				typeName, _ := optionalString(args, "type")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return querysvc.RunInspectQuery(q, name, typeName, limit)
				})
			},
		},
		"codebase_query_js_function": {
			Definition: toolDefinition{Name: "codebase_query_js_function", Description: "Search JS functions", InputSchema: querySchema("name", stringProp("Function name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchJSFunction(name, like, limit)
				})
			},
		},
		"codebase_query_smf_instrument": {
			Definition: toolDefinition{Name: "codebase_query_smf_instrument", Description: "Search SMF instruments", InputSchema: querySchema("name", stringProp("Instrument name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchSMFInstrument(name, like, limit)
				})
			},
		},
		"codebase_query_smf_type": {
			Definition: toolDefinition{Name: "codebase_query_smf_type", Description: "Search SMF by scenario type", InputSchema: querySchema("type", stringProp("Scenario type"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				smfType, err := requiredString(args, "type")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchSMFByType(smfType, limit)
				})
			},
		},
		"codebase_query_report_form": {
			Definition: toolDefinition{Name: "codebase_query_report_form", Description: "Search report forms", InputSchema: querySchema("name", stringProp("Report form name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchReportForm(name, like, limit)
				})
			},
		},
		"codebase_query_report_field": {
			Definition: toolDefinition{Name: "codebase_query_report_field", Description: "Search report fields", InputSchema: querySchema("name", stringProp("Report field name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchReportField(name, like, limit)
				})
			},
		},
		"codebase_query_report_param": {
			Definition: toolDefinition{Name: "codebase_query_report_param", Description: "Search report params", InputSchema: querySchema("name", stringProp("Report param name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchReportParam(name, like, limit)
				})
			},
		},
		"codebase_query_vb_function": {
			Definition: toolDefinition{Name: "codebase_query_vb_function", Description: "Search VB functions", InputSchema: querySchema("name", stringProp("Function name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchVBFunction(name, like, limit)
				})
			},
		},
		"codebase_query_api_contract": {
			Definition: toolDefinition{Name: "codebase_query_api_contract", Description: "Search API contracts", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIContract(name, like, limit)
				})
			},
		},
		"codebase_query_api_table": {
			Definition: toolDefinition{Name: "codebase_query_api_table", Description: "Search API tables", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPITable(name, like, limit)
				})
			},
		},
		"codebase_query_api_table_index": {
			Definition: toolDefinition{Name: "codebase_query_api_table_index", Description: "Search API table indexes", InputSchema: querySchema("name", stringProp("Table/index name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPITableIndex(name, like, limit)
				})
			},
		},
		"codebase_query_api_param": {
			Definition: toolDefinition{Name: "codebase_query_api_param", Description: "Search API params", InputSchema: querySchema("name", stringProp("Param name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIParam(name, like, limit)
				})
			},
		},
		"codebase_query_api_impl": {
			Definition: toolDefinition{Name: "codebase_query_api_impl", Description: "Find SQL implementations for API contract", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIImplementations(name, limit)
				})
			},
		},
		"codebase_query_api_publishers": {
			Definition: toolDefinition{Name: "codebase_query_api_publishers", Description: "Find API event publishers", InputSchema: querySchema("event", stringProp("Event name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				eventName, err := requiredString(args, "event")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIPublishers(eventName, limit)
				})
			},
		},
		"codebase_query_api_consumers": {
			Definition: toolDefinition{Name: "codebase_query_api_consumers", Description: "Find API contract consumers", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIConsumers(name, limit)
				})
			},
		},
	}
}

func runQueryOpt(db *store.DB, run func(q *query.Query) (interface{}, error)) (interface{}, error) {
	var items interface{}
	var err error
	if db != nil {
		items, err = querysvc.ExecuteWith(db, run)
	} else {
		items, err = querysvc.Execute(run)
	}
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"count": resultCount(items),
		"items": normalizeNilResults(items),
	}, nil
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

func objectSchema(properties map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"type":       "object",
		"properties": properties,
	}
}

func querySchema(requiredField string, requiredProp map[string]interface{}, optional map[string]interface{}) map[string]interface{} {
	props := map[string]interface{}{
		requiredField: requiredProp,
	}
	for k, v := range optional {
		props[k] = v
	}
	schema := objectSchema(props)
	schema["required"] = []string{requiredField}
	return schema
}

func stringProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "string", "description": description}
}

func boolProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "boolean", "description": description}
}

func intProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "integer", "description": description}
}

func requiredString(args map[string]interface{}, key string) (string, error) {
	value, ok := args[key]
	if !ok {
		return "", fmt.Errorf("missing required argument: %s", key)
	}
	str, ok := value.(string)
	if !ok || str == "" {
		return "", fmt.Errorf("argument %s must be non-empty string", key)
	}
	return str, nil
}

func optionalString(args map[string]interface{}, key string) (string, error) {
	value, ok := args[key]
	if !ok || value == nil {
		return "", nil
	}
	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("argument %s must be string", key)
	}
	return str, nil
}

func optionalBool(args map[string]interface{}, key string) (bool, error) {
	value, ok := args[key]
	if !ok || value == nil {
		return false, nil
	}
	boolean, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("argument %s must be boolean", key)
	}
	return boolean, nil
}

func optionalLimit(args map[string]interface{}) int {
	const defaultLimit = 100
	value, ok := args["limit"]
	if !ok || value == nil {
		return defaultLimit
	}
	switch v := value.(type) {
	case float64:
		if int(v) <= 0 {
			return defaultLimit
		}
		return int(v)
	case int:
		if v <= 0 {
			return defaultLimit
		}
		return v
	default:
		return defaultLimit
	}
}

func toJSONText(value interface{}) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
