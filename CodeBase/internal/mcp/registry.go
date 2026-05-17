package mcp

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/querysvc"
	"github.com/codebase/internal/systemsvc"
)

type toolHandler func(args map[string]interface{}) (interface{}, error)

type registeredTool struct {
	Definition toolDefinition
	Handler    toolHandler
}

var toolRegistry = map[string]registeredTool{
	"codebase.ping": {
		Definition: toolDefinition{
			Name:        "codebase.ping",
			Description: "Health check tool for MCP transport",
			InputSchema: objectSchema(map[string]interface{}{}),
		},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			return map[string]interface{}{"ok": true, "service": "codebase-mcp"}, nil
		},
	},
	"codebase.health": {
		Definition: toolDefinition{
			Name:        "codebase.health",
			Description: "Check CodeBase readiness",
			InputSchema: objectSchema(map[string]interface{}{}),
		},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			return systemsvc.ExecuteHealth()
		},
	},
	"codebase.stats": {
		Definition: toolDefinition{
			Name:        "codebase.stats",
			Description: "Get CodeBase index statistics",
			InputSchema: objectSchema(map[string]interface{}{}),
		},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			return systemsvc.ExecuteStats()
		},
	},
	"codebase.query.symbol": {
		Definition: toolDefinition{Name: "codebase.query.symbol", Description: "Search symbol", InputSchema: querySchema("name", stringProp("Symbol name"), map[string]interface{}{"type": stringProp("Symbol type"), "like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			typeName, _ := optionalString(args, "type")
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchSymbol(name, typeName, like, limit)
			})
		},
	},
	"codebase.query.table": {
		Definition: toolDefinition{Name: "codebase.query.table", Description: "Search table", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchTable(name, like, limit)
			})
		},
	},
	"codebase.query.table-schema": {
		Definition: toolDefinition{Name: "codebase.query.table-schema", Description: "Search table schema", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchTableSchema(name, limit)
			})
		},
	},
	"codebase.query.table-index": {
		Definition: toolDefinition{Name: "codebase.query.table-index", Description: "Search SQL table indexes", InputSchema: querySchema("name", stringProp("Table/index name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchSQLTableIndex(name, like, limit)
			})
		},
	},
	"codebase.query.procedure": {
		Definition: toolDefinition{Name: "codebase.query.procedure", Description: "Get SQL procedure details", InputSchema: querySchema("name", stringProp("Procedure name"), map[string]interface{}{})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.GetProcedureResult(name)
			})
		},
	},
	"codebase.query.callers": {
		Definition: toolDefinition{Name: "codebase.query.callers", Description: "Find procedure callers", InputSchema: querySchema("procedure", stringProp("Procedure name"), map[string]interface{}{"limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "procedure")
			if err != nil {
				return nil, err
			}
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.FindCallers(name, limit)
			})
		},
	},
	"codebase.query.methods": {
		Definition: toolDefinition{Name: "codebase.query.methods", Description: "Find methods by table", InputSchema: querySchema("table", stringProp("Table name"), map[string]interface{}{"limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			table, err := requiredString(args, "table")
			if err != nil {
				return nil, err
			}
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.FindMethodsByTable(table, limit)
			})
		},
	},
	"codebase.query.method": {
		Definition: toolDefinition{Name: "codebase.query.method", Description: "Find PAS methods", InputSchema: querySchema("name", stringProp("Method name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.FindPASMethodsByName(name, like, limit)
			})
		},
	},
	"codebase.query.form": {
		Definition: toolDefinition{Name: "codebase.query.form", Description: "Find DFM forms", InputSchema: querySchema("name", stringProp("Form name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchDFMForm(name, like, limit)
			})
		},
	},
	"codebase.query.form-component": {
		Definition: toolDefinition{Name: "codebase.query.form-component", Description: "Find DFM form components", InputSchema: querySchema("name", stringProp("Component name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchDFMComponent(name, like, limit)
			})
		},
	},
	"codebase.query.sql-fragment": {
		Definition: toolDefinition{Name: "codebase.query.sql-fragment", Description: "Search SQL fragments", InputSchema: querySchema("text", stringProp("Text fragment"), map[string]interface{}{"limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			text, err := requiredString(args, "text")
			if err != nil {
				return nil, err
			}
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchQueryFragment(text, limit)
			})
		},
	},
	"codebase.query.relations": {
		Definition: toolDefinition{Name: "codebase.query.relations", Description: "Search relations", InputSchema: objectSchema(map[string]interface{}{"source_type": stringProp("Source entity type"), "source_name": stringProp("Source entity name"), "target_type": stringProp("Target entity type"), "target_name": stringProp("Target entity name"), "relation_type": stringProp("Relation type"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			sourceType, _ := optionalString(args, "source_type")
			sourceName, _ := optionalString(args, "source_name")
			targetType, _ := optionalString(args, "target_type")
			targetName, _ := optionalString(args, "target_name")
			relationType, _ := optionalString(args, "relation_type")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchRelations(sourceType, sourceName, targetType, targetName, relationType, limit)
			})
		},
	},
	"codebase.query.inspect": {
		Definition: toolDefinition{Name: "codebase.query.inspect", Description: "Inspect symbol and graph neighborhood", InputSchema: querySchema("name", stringProp("Symbol name"), map[string]interface{}{"type": stringProp("Symbol type"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			typeName, _ := optionalString(args, "type")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return querysvc.RunInspectQuery(q, name, typeName, limit)
			})
		},
	},
	"codebase.query.js-function": {
		Definition: toolDefinition{Name: "codebase.query.js-function", Description: "Search JS functions", InputSchema: querySchema("name", stringProp("Function name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchJSFunction(name, like, limit)
			})
		},
	},
	"codebase.query.smf-instrument": {
		Definition: toolDefinition{Name: "codebase.query.smf-instrument", Description: "Search SMF instruments", InputSchema: querySchema("name", stringProp("Instrument name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchSMFInstrument(name, like, limit)
			})
		},
	},
	"codebase.query.smf-type": {
		Definition: toolDefinition{Name: "codebase.query.smf-type", Description: "Search SMF by scenario type", InputSchema: querySchema("type", stringProp("Scenario type"), map[string]interface{}{"limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			smfType, err := requiredString(args, "type")
			if err != nil {
				return nil, err
			}
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchSMFByType(smfType, limit)
			})
		},
	},
	"codebase.query.report-form": {
		Definition: toolDefinition{Name: "codebase.query.report-form", Description: "Search report forms", InputSchema: querySchema("name", stringProp("Report form name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchReportForm(name, like, limit)
			})
		},
	},
	"codebase.query.report-field": {
		Definition: toolDefinition{Name: "codebase.query.report-field", Description: "Search report fields", InputSchema: querySchema("name", stringProp("Report field name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchReportField(name, like, limit)
			})
		},
	},
	"codebase.query.report-param": {
		Definition: toolDefinition{Name: "codebase.query.report-param", Description: "Search report params", InputSchema: querySchema("name", stringProp("Report param name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchReportParam(name, like, limit)
			})
		},
	},
	"codebase.query.vb-function": {
		Definition: toolDefinition{Name: "codebase.query.vb-function", Description: "Search VB functions", InputSchema: querySchema("name", stringProp("Function name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchVBFunction(name, like, limit)
			})
		},
	},
	"codebase.query.api-contract": {
		Definition: toolDefinition{Name: "codebase.query.api-contract", Description: "Search API contracts", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchAPIContract(name, like, limit)
			})
		},
	},
	"codebase.query.api-table": {
		Definition: toolDefinition{Name: "codebase.query.api-table", Description: "Search API tables", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchAPITable(name, like, limit)
			})
		},
	},
	"codebase.query.api-table-index": {
		Definition: toolDefinition{Name: "codebase.query.api-table-index", Description: "Search API table indexes", InputSchema: querySchema("name", stringProp("Table/index name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchAPITableIndex(name, like, limit)
			})
		},
	},
	"codebase.query.api-param": {
		Definition: toolDefinition{Name: "codebase.query.api-param", Description: "Search API params", InputSchema: querySchema("name", stringProp("Param name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			like, _ := optionalBool(args, "like")
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchAPIParam(name, like, limit)
			})
		},
	},
	"codebase.query.api-impl": {
		Definition: toolDefinition{Name: "codebase.query.api-impl", Description: "Find SQL implementations for API contract", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchAPIImplementations(name, limit)
			})
		},
	},
	"codebase.query.api-publishers": {
		Definition: toolDefinition{Name: "codebase.query.api-publishers", Description: "Find API event publishers", InputSchema: querySchema("event", stringProp("Event name"), map[string]interface{}{"limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			eventName, err := requiredString(args, "event")
			if err != nil {
				return nil, err
			}
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchAPIPublishers(eventName, limit)
			})
		},
	},
	"codebase.query.api-consumers": {
		Definition: toolDefinition{Name: "codebase.query.api-consumers", Description: "Find API contract consumers", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"limit": intProp("Max results")})},
		Handler: func(args map[string]interface{}) (interface{}, error) {
			name, err := requiredString(args, "name")
			if err != nil {
				return nil, err
			}
			limit := optionalLimit(args)
			return runQuery(func(q *query.Query) (interface{}, error) {
				return q.SearchAPIConsumers(name, limit)
			})
		},
	},
}

func runQuery(run func(q *query.Query) (interface{}, error)) (interface{}, error) {
	items, err := querysvc.Execute(run)
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
