package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/querysvc"
	"github.com/codebase/internal/review"
	"github.com/codebase/internal/reviewsvc"
	"github.com/codebase/internal/rtisvc"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/systemsvc"
	"github.com/codebase/internal/trcsvc"
)

type toolHandler func(ctx context.Context, args map[string]interface{}) (interface{}, error)

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

func requiredInt(args map[string]interface{}, key string) (int, error) {
	value, ok := args[key]
	if !ok || value == nil {
		return 0, fmt.Errorf("missing required argument: %s", key)
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

// baseTools — инструменты, доступные во всех профилях.
var baseTools = map[string]bool{
	"codebase_ping":      true,
	"codebase_health":    true,
	"codebase_stats":     true,
	"codebase_read_more": true,
}

// queryTools — codebase_query_* инструменты.
var queryTools = map[string]bool{
	"codebase_query_symbol":          true,
	"codebase_query_table":           true,
	"codebase_query_table_schema":    true,
	"codebase_query_table_index":     true,
	"codebase_query_procedure":       true,
	"codebase_query_callers":         true,
	"codebase_query_methods":         true,
	"codebase_query_method":          true,
	"codebase_query_form":            true,
	"codebase_query_form_component":  true,
	"codebase_query_sql_fragment":    true,
	"codebase_query_relations":       true,
	"codebase_query_inspect":         true,
	"codebase_query_js_function":     true,
	"codebase_query_smf_instrument":  true,
	"codebase_query_smf_type":        true,
	"codebase_query_report_form":     true,
	"codebase_query_report_field":    true,
	"codebase_query_report_param":    true,
	"codebase_query_vb_function":     true,
	"codebase_query_api_contract":    true,
	"codebase_query_api_table":       true,
	"codebase_query_api_table_index": true,
	"codebase_query_api_param":       true,
	"codebase_query_api_impl":        true,
	"codebase_query_api_publishers":  true,
	"codebase_query_api_consumers":   true,
	"codebase_query_retcode":         true,
}

// rtiTools — codebase_rti_* инструменты.
var rtiTools = map[string]bool{
	"codebase_rti_parse":       true,
	"codebase_rti_list":        true,
	"codebase_rti_summary":     true,
	"codebase_rti_tree":        true,
	"codebase_rti_errors":      true,
	"codebase_rti_slow":        true,
	"codebase_rti_details":     true,
	"codebase_rti_delete":      true,
	"codebase_rti_prune":       true,
	"codebase_rti_blog":        true,
	"codebase_rti_client_tree": true,
	"codebase_rti_timeline":    true,
}

// trcTools — codebase_trc_* инструменты.
var trcTools = map[string]bool{
	"codebase_trc_parse":      true,
	"codebase_trc_list":       true,
	"codebase_trc_summary":    true,
	"codebase_trc_events":     true,
	"codebase_trc_procedures": true,
	"codebase_trc_tree":       true,
	"codebase_trc_errors":     true,
	"codebase_trc_slow":       true,
	"codebase_trc_delete":     true,
	"codebase_trc_prune":      true,
}

// reviewTools — codebase_review_sql инструмент.
var reviewTools = map[string]bool{
	"codebase_review_sql": true,
}

// profileToolSets — маппинг профиль → whitelist имён инструментов.
var profileToolSets = map[string]map[string]bool{
	"query":  mergeMaps(baseTools, queryTools),
	"rti":    mergeMaps(baseTools, rtiTools),
	"trc":    mergeMaps(baseTools, trcTools),
	"review": mergeMaps(baseTools, reviewTools),
}

// ValidProfiles возвращает список имён валидных профилей.
func ValidProfiles() []string {
profiles := make([]string, 0, len(profileToolSets))
for name := range profileToolSets {
	profiles = append(profiles, name)
}
return profiles
}

// mergeMaps объединяет две map[string]bool в новую.
func mergeMaps(a, b map[string]bool) map[string]bool {
result := make(map[string]bool, len(a)+len(b))
for k, v := range a {
	result[k] = v
}
for k, v := range b {
	result[k] = v
}
return result
}

// buildToolRegistryForProfile возвращает registry, отфильтрованный по профилю.
// При пустом profile — все инструменты (buildToolRegistry).
// При известном profile — только инструменты из whitelist.
// При неизвестном profile — ошибка.
func buildToolRegistryForProfile(db *store.DB, profile string) (map[string]registeredTool, error) {
full := buildToolRegistry(db)
if profile == "" {
	return full, nil
}
whitelist, ok := profileToolSets[profile]
if !ok {
	return nil, fmt.Errorf("unknown profile %q, valid profiles: %v", profile, ValidProfiles())
}
filtered := make(map[string]registeredTool, len(whitelist))
for name, rt := range full {
	if whitelist[name] {
		filtered[name] = rt
	}
}
return filtered, nil
}

func buildToolRegistry(db *store.DB) map[string]registeredTool {
	return map[string]registeredTool{
		"codebase_read_more": {
			Definition: toolDefinition{
				Name: "codebase_read_more",
				Description: "Read the next chunk of a paginated MCP response. " +
					"Use this when a previous tool response starts with '⚠️ PAGINATED RESPONSE'. " +
					"Copy the continuation_id and chunk number from the '👉 Call' hint. " +
					"Repeat until you see '✅ FINAL CHUNK'.",
				InputSchema: func() map[string]interface{} {
					s := objectSchema(map[string]interface{}{
						"continuation_id": stringProp("Continuation ID from the paginated response header"),
						"chunk":           intProp("Chunk number to read (as shown in the 👉 hint)"),
					})
					s["required"] = []string{"continuation_id", "chunk"}
					return s
				}(),
			},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				id, err := requiredString(args, "continuation_id")
				if err != nil {
					return nil, err
				}
				chunk, err := requiredInt(args, "chunk")
				if err != nil {
					return nil, err
				}
				return globalPages.readChunk(id, chunk)
			},
		},
		"codebase_ping": {
			Definition: toolDefinition{
				Name:        "codebase_ping",
				Description: "Verify that the MCP transport and codebase server process are alive. Use this as a first step to confirm the server is reachable before calling any other tool.",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				return map[string]interface{}{"ok": true, "service": "codebase-mcp"}, nil
			},
		},
		"codebase_health": {
			Definition: toolDefinition{
				Name:        "codebase_health",
				Description: "Check whether the CodeBase index database is connected and ready to serve queries. Returns status and DB connection info. Use before running queries if you are unsure whether the index is available.",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				return systemsvc.ExecuteHealth(db)
			},
		},
		"codebase_stats": {
			Definition: toolDefinition{
				Name:        "codebase_stats",
				Description: "Return index statistics: total counts of indexed files, SQL procedures, tables, PAS methods, JS/VB functions, DFM forms, SMF instruments, API contracts, report forms, and relations. Use to understand the scope of the indexed codebase.",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				return systemsvc.ExecuteStats(db)
			},
		},
		"codebase_review_sql": {
			Definition: toolDefinition{Name: "codebase_review_sql", Description: "Run static analysis (lint) checks on a single SQL file and return a list of findings with rule ID, severity, line number and message. Requires absolute file path. Available rule IDs: " + review.RuleListString() + ". Omit rules to run all enabled rules.", InputSchema: objectSchema(map[string]interface{}{"file_path": stringProp("Full SQL file path"), "rules": map[string]interface{}{"type": "array", "description": "Optional rule ids", "items": map[string]interface{}{"type": "string"}}, "min_severity": intProp("Minimum severity")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
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
				rules, err := review.ValidateRuleIDs(rulesRaw)
				if err != nil {
					return nil, err
				}
				opts := review.Options{Rules: rules, MinSeverity: minSeverity}
				if db != nil {
					return reviewsvc.ExecuteWithCtx(ctx, db, filePath, opts, nil)
				}
				return reviewsvc.Execute(filePath, opts, nil)
			},
		},
		"codebase_query_symbol": {
			Definition: toolDefinition{Name: "codebase_query_symbol", Description: "Search across all indexed entity types by name: SQL procedures, SQL tables, PAS methods, JS functions, VB functions, DFM forms, DFM components, SMF instruments, API contracts, report forms. Use 'type' to narrow to a specific entity type (e.g. 'sql_procedure', 'sql_table', 'pas_method', 'js_function', 'vb_function', 'dfm_form', 'dfm_component', 'smf_instrument', 'api_contract', 'report_form'). Set like=true for partial name match. Prefer this tool for initial discovery when you do not know the exact entity type.", InputSchema: querySchema("name", stringProp("Symbol name"), map[string]interface{}{"type": stringProp("Symbol type"), "like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				typeName, _ := optionalString(args, "type")
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchSymbol(ctx, name, typeName, like, limit)
				})
			},
		},
		"codebase_query_table": {
			Definition: toolDefinition{Name: "codebase_query_table", Description: "Search SQL tables by table name. Returns table name, file path, line number and definition metadata. Use when you need to find a specific DB table definition or check if a table exists in the index.", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchTable(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_table_schema": {
			Definition: toolDefinition{Name: "codebase_query_table_schema", Description: "Return the column definitions (schema) for a SQL table: column name, data type, nullability, default value, line number. Use when you need to inspect table structure or check column data types for impact analysis or type compatibility review.", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchTableSchema(ctx, name, limit)
				})
			},
		},
		"codebase_query_table_index": {
			Definition: toolDefinition{Name: "codebase_query_table_index", Description: "Search SQL index definitions by index name or table name. Returns index name, table name, index fields, index type (clustered/nonclustered), uniqueness flag and file location. Use when analysing query performance hints or verifying that expected indexes exist.", InputSchema: querySchema("name", stringProp("Table/index name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchSQLTableIndex(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_procedure": {
			Definition: toolDefinition{Name: "codebase_query_procedure", Description: "Get detailed information about a SQL stored procedure by exact name: file path, line range, parameter list with data types and default values, and the full body text. Use when you need to inspect a specific procedure's signature or source code. For fuzzy search use codebase_query_symbol with type='sql_procedure'.", InputSchema: querySchema("name", stringProp("Procedure name"), map[string]interface{}{})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.GetProcedureResult(ctx, name)
				})
			},
		},
		"codebase_query_callers": {
			Definition: toolDefinition{Name: "codebase_query_callers", Description: "Find all callers of a SQL stored procedure. Returns direct callers (sql_procedure, pas_method, js_function, report_form, vb_function, query_fragment) and indirect callers up to 2 hops via calls_procedure/dispatches_to/dispatches_to_subscriber chains. Use for impact analysis: who calls this procedure and what code will be affected by its change.", InputSchema: querySchema("procedure", stringProp("Procedure name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "procedure")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.FindCallers(ctx, name, limit)
				})
			},
		},
		"codebase_query_methods": {
			Definition: toolDefinition{Name: "codebase_query_methods", Description: "Find all PAS (Delphi/Pascal) methods that reference a specific SQL table through their query fragments. Returns method name, class name, unit name, file path and line number. Use for DB impact analysis: which Object Pascal code reads/writes this table.", InputSchema: querySchema("table", stringProp("Table name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				table, err := requiredString(args, "table")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.FindMethodsByTable(ctx, table, limit)
				})
			},
		},
		"codebase_query_method": {
			Definition: toolDefinition{Name: "codebase_query_method", Description: "Search PAS (Delphi/Pascal) methods by method name. Returns method name, class name, unit name, file path and line number. Set like=true for partial match. Use when navigating Object Pascal source code or finding all implementations of a method name pattern.", InputSchema: querySchema("name", stringProp("Method name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.FindPASMethodsByName(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_form": {
			Definition: toolDefinition{Name: "codebase_query_form", Description: "Search DFM (Delphi Form) definitions by form name. Returns form name, class name, caption, file path and line range. Use when looking for a UI form definition in a Delphi codebase.", InputSchema: querySchema("name", stringProp("Form name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchDFMForm(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_form_component": {
			Definition: toolDefinition{Name: "codebase_query_form_component", Description: "Search DFM form components (buttons, grids, datasources, etc.) by component name. Returns component name, component type, parent form name, file path and line number. Use when locating a specific UI control within Delphi forms.", InputSchema: querySchema("name", stringProp("Component name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchDFMComponent(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_sql_fragment": {
			Definition: toolDefinition{Name: "codebase_query_sql_fragment", Description: "Search indexed SQL query fragments (inline SQL embedded in PAS/JS/VB/SMF/DFM source files) by text content. Returns the fragment text, parent entity (method/function), file path and line number. Use when looking for ad-hoc queries that reference a table or procedure name but are not standalone SQL procedures.", InputSchema: querySchema("text", stringProp("Text fragment"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				text, err := requiredString(args, "text")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchQueryFragment(ctx, text, limit)
				})
			},
		},
		"codebase_query_relations": {
			Definition: toolDefinition{Name: "codebase_query_relations", Description: "Search the dependency/relation graph by source entity, target entity, and/or relation type. All parameters are optional but at least one must be provided. Entity types: sql_procedure, sql_table, pas_method, js_function, vb_function, api_contract, report_form, report_field, report_param, smf_instrument, query_fragment. Relation types: calls_procedure, dispatches_to, dispatches_to_subscriber, selects_from, inserts_into, updates, deletes_from, references_table, executes_query, builds_query, implements_contract, executes_contract, publishes_event, subscribes_to_event, has_field, has_param, uses_param. Use for arbitrary graph traversal when the dedicated tools (callers, methods, api_*) do not cover the needed relation type.", InputSchema: objectSchema(map[string]interface{}{"source_type": stringProp("Source entity type"), "source_name": stringProp("Source entity name"), "target_type": stringProp("Target entity type"), "target_name": stringProp("Target entity name"), "relation_type": stringProp("Relation type"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				sourceType, _ := optionalString(args, "source_type")
				sourceName, _ := optionalString(args, "source_name")
				targetType, _ := optionalString(args, "target_type")
				targetName, _ := optionalString(args, "target_name")
				relationType, _ := optionalString(args, "relation_type")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchRelations(ctx, sourceType, sourceName, targetType, targetName, relationType, limit)
				})
			},
		},
		"codebase_query_inspect": {
			Definition: toolDefinition{Name: "codebase_query_inspect", Description: "Deep-inspect a symbol by name: find the symbol, then fetch all incoming and outgoing relations in the dependency graph, and collect neighboring symbols. Returns symbol metadata plus full relation context (incoming, outgoing, neighbors). Use this as an all-in-one exploration starting point when you need to understand both what a symbol does and how it is connected. Differs from codebase_query_symbol (returns only symbol metadata) and codebase_query_relations (requires explicit entity IDs/types).", InputSchema: querySchema("name", stringProp("Symbol name"), map[string]interface{}{"type": stringProp("Symbol type"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				typeName, _ := optionalString(args, "type")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return querysvc.RunInspectQuery(ctx, q, name, typeName, limit)
				})
			},
		},
		"codebase_query_js_function": {
			Definition: toolDefinition{Name: "codebase_query_js_function", Description: "Search JavaScript functions indexed from JS/SMF source files by function name. Returns function name, file path and line number. Set like=true for partial match. Use when tracing client-side or scenario (SMF) logic.", InputSchema: querySchema("name", stringProp("Function name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchJSFunction(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_smf_instrument": {
			Definition: toolDefinition{Name: "codebase_query_smf_instrument", Description: "Search SMF (Scenario/Workflow) instruments by instrument name. Returns instrument name, scenario type, file path and line number. Set like=true for partial match. Use when exploring business process automation scenarios in Diasoft 5NT.", InputSchema: querySchema("name", stringProp("Instrument name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchSMFInstrument(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_smf_type": {
			Definition: toolDefinition{Name: "codebase_query_smf_type", Description: "Search SMF (Scenario/Workflow) instruments filtered by scenario type string. Returns all instruments matching the given type. Use when you need to enumerate all scenarios of a given process category rather than searching by name.", InputSchema: querySchema("type", stringProp("Scenario type"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				smfType, err := requiredString(args, "type")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchSMFByType(ctx, smfType, limit)
				})
			},
		},
		"codebase_query_report_form": {
			Definition: toolDefinition{Name: "codebase_query_report_form", Description: "Search report definitions (TPR/RPT report forms) by report name. Returns report name, report type, file path and line range. Set like=true for partial match. Use when locating a report template or analysing report dependencies.", InputSchema: querySchema("name", stringProp("Report form name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchReportForm(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_report_field": {
			Definition: toolDefinition{Name: "codebase_query_report_field", Description: "Search report field definitions by field name across all indexed report forms. Returns field name, data type, parent report name, file path and line number. Use when checking what fields a report exposes or when tracing a column name across reports.", InputSchema: querySchema("name", stringProp("Report field name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchReportField(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_report_param": {
			Definition: toolDefinition{Name: "codebase_query_report_param", Description: "Search report parameter definitions by parameter name across all indexed report forms. Returns parameter name, data type, parent report name, file path and line number. Use when verifying which parameters a report accepts or finding reports that use a specific parameter.", InputSchema: querySchema("name", stringProp("Report param name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchReportParam(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_vb_function": {
			Definition: toolDefinition{Name: "codebase_query_vb_function", Description: "Search VBA/VBScript functions indexed from report source files by function name. Returns function name, parent report form, file path and line number. Set like=true for partial match. Use when tracing report calculation logic written in Visual Basic.", InputSchema: querySchema("name", stringProp("Function name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchVBFunction(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_api_contract": {
			Definition: toolDefinition{Name: "codebase_query_api_contract", Description: "Search Diasoft API contracts by contract name. Returns contract name, contract kind (service, event, callback_event, used_service), file path and line number. Set like=true for partial match. Use as the entry point when exploring inter-module API surface.", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIContract(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_api_table": {
			Definition: toolDefinition{Name: "codebase_query_api_table", Description: "Search tables declared inside Diasoft API contracts by table name. Returns table name, column list, parent contract name, file path and line number. Use when you need to inspect the data structure that an API contract operates on.", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPITable(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_api_table_index": {
			Definition: toolDefinition{Name: "codebase_query_api_table_index", Description: "Search index definitions declared inside Diasoft API contracts by index or table name. Returns index name, table name, fields and parent contract. Use when verifying API-level index expectations versus actual DB indexes.", InputSchema: querySchema("name", stringProp("Table/index name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPITableIndex(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_api_param": {
			Definition: toolDefinition{Name: "codebase_query_api_param", Description: "Search parameters declared in Diasoft API contracts by parameter name. Returns parameter name, data type, direction, parent contract name, file path and line number. Use when checking the API signature or finding contracts that expose a specific parameter.", InputSchema: querySchema("name", stringProp("Param name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				like, _ := optionalBool(args, "like")
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIParam(ctx, name, like, limit)
				})
			},
		},
		"codebase_query_api_impl": {
			Definition: toolDefinition{Name: "codebase_query_api_impl", Description: "Find SQL stored procedures that implement a given API contract (relation type implements_contract). Returns direct and indirect (via calls_procedure/dispatches_to) implementing procedures with file paths. Use when you need to find the server-side SQL implementation behind an API contract name.", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIImplementations(ctx, name, limit)
				})
			},
		},
		"codebase_query_api_publishers": {
			Definition: toolDefinition{Name: "codebase_query_api_publishers", Description: "Find SQL stored procedures that publish (raise) a given API event contract (relation type publishes_event) and callback contracts that subscribe to it. Returns direct and indirect publishers with file paths. Use when tracing the origin of an event in the Diasoft API event bus.", InputSchema: querySchema("event", stringProp("Event name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				eventName, err := requiredString(args, "event")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIPublishers(ctx, eventName, limit)
				})
			},
		},
		"codebase_query_api_consumers": {
			Definition: toolDefinition{Name: "codebase_query_api_consumers", Description: "Find SQL stored procedures that consume (call via exec_contract) a given API service contract (relation type executes_contract). Returns direct and indirect consumers with file paths. Use when analysing which modules depend on a given API service and would be affected by its change.", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"limit": intProp("Max results")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				name, err := requiredString(args, "name")
				if err != nil {
					return nil, err
				}
				limit := optionalLimit(args)
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					return q.SearchAPIConsumers(ctx, name, limit)
				})
			},
		},
		"codebase_query_retcode": {
			Definition: toolDefinition{Name: "codebase_query_retcode", Description: "Look up return code descriptions from ds_return_codes. Supports two modes: (1) search by ret_code (integer) — returns a single match with message, proc_name, module_id; (2) search by message text fragment (string, case-insensitive ILIKE) — returns all matching codes. Use when you need to decode a numeric return value from a stored procedure or find which error code corresponds to an error message.", InputSchema: objectSchema(map[string]interface{}{"ret_code": intProp("Return code to look up (exact match)"), "message": stringProp("Message text fragment to search (case-insensitive partial match)"), "limit": intProp("Max results for message search (default 50)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				return runQueryOpt(db, func(q *query.Query) (interface{}, error) {
					retCode, err := optionalInt64(args, "ret_code")
					if err != nil {
						return nil, err
					}
					if retCode != 0 {
						r, err := q.LookupRetCode(ctx, retCode)
						if err != nil {
							return nil, err
						}
						if r == nil {
							return []interface{}{}, nil
						}
						return r, nil
					}
					msgPattern, err := optionalString(args, "message")
					if err != nil {
						return nil, err
					}
					if msgPattern == "" {
						return nil, fmt.Errorf("either ret_code or message is required")
					}
					limit := optionalLimit(args)
					return q.LookupRetCodeByMessage(ctx, msgPattern, limit)
				})
			},
		},
		"codebase_rti_parse": {
			Definition: toolDefinition{Name: "codebase_rti_parse", Description: "Parse an RTI trace log file and save the session to the database. Returns summary statistics (total calls, errors, max nest level, top slowest calls) and the saved session ID. Use this as the first step before querying RTI data via other rti tools.", InputSchema: objectSchema(map[string]interface{}{"file_path": stringProp("Absolute path to .rti file")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				filePath, err := requiredString(args, "file_path")
				if err != nil {
					return nil, err
				}
				return rtisvc.ExecuteParse(ctx, db, filePath)
			},
		},
		"codebase_rti_list": {
			Definition: toolDefinition{Name: "codebase_rti_list", Description: "List saved RTI parsing sessions from the database, ordered by most recent first. Returns session ID, file path, call counts, error counts, file size, and parse timestamp.", InputSchema: objectSchema(map[string]interface{}{"limit": intProp("Max sessions to return (default 20)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				limit := optionalLimit(args)
				return rtisvc.ExecuteList(ctx, db, limit)
			},
		},
		"codebase_rti_summary": {
			Definition: toolDefinition{Name: "codebase_rti_summary", Description: "Get summary statistics for an RTI session: total calls, errors, max nest level, unparsed lines, top 10 slowest calls. Requires either a saved session ID or a file path to parse on the fly.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .rti file to parse on the fly")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return rtisvc.ExecuteSummary(ctx, db, rtisvc.SessionSource{SessionID: sessionID, FilePath: filePath})
			},
		},
		"codebase_rti_tree": {
			Definition: toolDefinition{Name: "codebase_rti_tree", Description: "Build and return a call tree from an RTI session. The tree shows nested procedure calls with elapsed time, return values, module info, and source file locations (enriched from CodeBase index). If procedure is omitted, auto-selects the root call with the most descendants.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .rti file"), "procedure": stringProp("Root procedure name (default: auto-select)"), "max_depth": intProp("Max tree depth (0 = unlimited)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				procName, _ := optionalString(args, "procedure")
				maxDepth, _ := optionalInt(args, "max_depth")
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				result, err := rtisvc.ExecuteTree(ctx, db, rtisvc.TreeParams{
					Source:    rtisvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					Procedure: procName,
					MaxDepth:  maxDepth,
				})
				if err != nil {
					return nil, err
				}
				if result.Tree == nil {
					return nil, fmt.Errorf("procedure %q not found in RTI log", procName)
				}
				return result, nil
			},
		},
		"codebase_rti_errors": {
			Definition: toolDefinition{Name: "codebase_rti_errors", Description: "Find all calls with non-zero RetVal in an RTI session. Returns server errors (procedure name, line number, return value, error context, elapsed time, nest level, module info, error code description, source file) and client errors (ClassName.MethodName, error text, source file from CodeBase enrichment).", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .rti file"), "limit": intProp("Maximum number of errors to return (default 100, max 1000)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				limit, _ := optionalInt(args, "limit")
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return rtisvc.ExecuteErrors(ctx, db, rtisvc.ErrorsParams{
					Source: rtisvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					Limit:  limit,
				})
			},
		},
		"codebase_rti_slow": {
			Definition: toolDefinition{Name: "codebase_rti_slow", Description: "Find the slowest calls in an RTI session above a threshold. Returns server calls sorted by elapsed time descending, and client SQL blocks sorted by duration. Includes enrichment data (source files, SQL origin) from CodeBase index.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .rti file"), "threshold_ms": intProp("Minimum elapsed milliseconds (default 100)"), "limit": intProp("Maximum number of calls to return (default 100, max 1000)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				threshold, _ := optionalInt(args, "threshold_ms")
				limit, _ := optionalInt(args, "limit")
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return rtisvc.ExecuteSlow(ctx, db, rtisvc.SlowParams{
					Source:      rtisvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					ThresholdMs: threshold,
					Limit:       limit,
				})
			},
		},
		"codebase_rti_details": {
			Definition: toolDefinition{Name: "codebase_rti_details", Description: "Get enriched details for a specific procedure in an RTI session: source file path, line range, parameter definitions (name, type, direction) from CodeBase index, all call instances with timing, return values, error descriptions, and context.", InputSchema: objectSchema(map[string]interface{}{"procedure": stringProp("Procedure name"), "session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .rti file"), "limit": intProp("Maximum number of call instances to return (default 100, max 1000)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				procName, err := requiredString(args, "procedure")
				if err != nil {
					return nil, err
				}
				limit, _ := optionalInt(args, "limit")
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				result, err := rtisvc.ExecuteDetails(ctx, db, rtisvc.DetailsParams{
					Source:    rtisvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					Procedure: procName,
					Limit:     limit,
				})
				if err != nil {
					return nil, err
				}
				if result.Count == 0 {
					return nil, fmt.Errorf("procedure %q not found in RTI log", procName)
				}
				return result, nil
			},
		},
		"codebase_rti_delete": {
			Definition: toolDefinition{Name: "codebase_rti_delete", Description: "Delete a saved RTI session by ID. Cascades to delete all associated calls, parameters, and checkpoints.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Session ID to delete")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				sessionID, err := optionalInt64(args, "session_id")
				if err != nil {
					return nil, err
				}
				if sessionID <= 0 {
					return nil, fmt.Errorf("session_id is required")
				}
				return rtisvc.ExecuteDelete(ctx, db, sessionID)
			},
		},
		"codebase_rti_prune": {
			Definition: toolDefinition{Name: "codebase_rti_prune", Description: "Delete old RTI sessions, keeping only the most recent N. Use keep_last=0 to delete all sessions (uses TRUNCATE for instant cleanup). Returns the number of deleted sessions.", InputSchema: objectSchema(map[string]interface{}{"keep_last": intProp("Number of most recent sessions to keep")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				keepLast, err := optionalInt(args, "keep_last")
				if err != nil {
					return nil, err
				}
				return rtisvc.ExecutePrune(ctx, db, keepLast)
			},
		},
		"codebase_rti_blog": {
			Definition: toolDefinition{Name: "codebase_rti_blog", Description: "Get business log data for a specific procedure in an RTI session: business log blocks (BLOCK_BEGIN/END with names and timing), checkpoints with timestamps, and table dumps (M_LOG_TABLE/M_LOG_TABLE_LISTID). Requires either a saved session ID or a file path.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .rti file"), "procedure": stringProp("Procedure name"), "limit": intProp("Maximum number of call instances to return (default 100, max 1000)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				procName, err := requiredString(args, "procedure")
				if err != nil {
					return nil, err
				}
				limit, _ := optionalInt(args, "limit")
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				result, err := rtisvc.ExecuteBlog(ctx, db, rtisvc.BlogParams{
					Source:    rtisvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					Procedure: procName,
					Limit:     limit,
				})
				if err != nil {
					return nil, err
				}
				if result.Count == 0 {
					return nil, fmt.Errorf("procedure %q not found in RTI log", procName)
				}
				return result, nil
			},
		},
		"codebase_rti_client_tree": {
			Definition: toolDefinition{Name: "codebase_rti_client_tree", Description: "Build and return a tree of client-side (thick client d5nt) events from an RTI session, grouped by PID. Each event includes kind (sql_block, recordset_open, connection, bpl_list, error, memory, generic), timestamp, class/method, and enrichment data (PAS source file, DFM form, SQL query fragment origin) from CodeBase index. Supports optional filters: time_from/time_to (RFC3339), pid, class_name, method_name (case-insensitive), and format=short to omit heavy fields (BPL, Connection, SQL, Memory, ErrorText, RawBody).", InputSchema: objectSchema(map[string]interface{}{
				"session_id":  intProp("Saved session ID"),
				"file_path":   stringProp("Or: path to .rti file"),
				"pid":         intProp("Filter by client PID (0 = all)"),
				"time_from":   stringProp("Optional RFC3339 lower bound (e.g. 2026-02-20T12:40:00+03:00)"),
				"time_to":     stringProp("Optional RFC3339 upper bound"),
				"class_name":  stringProp("Optional client event class name filter (case-insensitive)"),
				"method_name": stringProp("Optional client event method name filter (case-insensitive)"),
				"format":      stringProp("Output format: full (default) or short. Short omits BPL, Connection, SQL, Memory, ErrorText, RawBody."),
				"limit":       intProp("Maximum number of events to return (default 100, max 1000)"),
			})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				limit, _ := optionalInt(args, "limit")
				filter, err := buildTimelineFilter(args)
				if err != nil {
					return nil, err
				}
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return rtisvc.ExecuteClientTree(ctx, db, rtisvc.ClientTreeParams{
					Source: rtisvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					Filter: filter,
					Limit:  limit,
				})
			},
		},
		"codebase_rti_timeline": {
			Definition: toolDefinition{Name: "codebase_rti_timeline", Description: "Get a unified chronological timeline of server calls and client events from an RTI session. Entries are sorted by timestamp and tagged [server] or [client]. Client events include enrichment data (source file locations) from CodeBase index. Useful for correlating client-side SQL execution with server-side procedure calls. Supports optional filters: time_from/time_to (RFC3339), pid, procedure (case-insensitive exact match), class_name, method_name, and format=short to omit heavy fields (params, checkpoints, blog_*, SQL text).", InputSchema: objectSchema(map[string]interface{}{
				"session_id":  intProp("Saved session ID"),
				"file_path":   stringProp("Or: path to .rti file"),
				"time_from":   stringProp("Optional RFC3339 lower bound (e.g. 2026-02-20T12:40:00+03:00)"),
				"time_to":     stringProp("Optional RFC3339 upper bound"),
				"pid":         intProp("Optional client PID filter"),
				"procedure":   stringProp("Optional server procedure name filter (case-insensitive exact match)"),
				"class_name":  stringProp("Optional client event class name filter (case-insensitive)"),
				"method_name": stringProp("Optional client event method name filter (case-insensitive)"),
				"format":      stringProp("Output format: full (default) or short. Short omits params, checkpoints, blog_*, SQL text."),
				"limit":       intProp("Maximum number of items to return per type (default 100, max 1000)"),
			})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				limit, _ := optionalInt(args, "limit")
				filter, err := buildTimelineFilter(args)
				if err != nil {
					return nil, err
				}
				procName, _ := optionalString(args, "procedure")
				filter.Procedure = procName
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return rtisvc.ExecuteTimeline(ctx, db, rtisvc.TimelineParams{
					Source: rtisvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					Filter: filter,
					Limit:  limit,
				})
			},
		},
		"codebase_trc_parse": {
			Definition: toolDefinition{Name: "codebase_trc_parse", Description: "Parse a SQL Server trace file (.trc binary, .trc XML export, or .xel Extended Events) and save the session to the database. Returns total event count and the saved session ID. Use this as the first step before querying trc data via other trc tools.", InputSchema: objectSchema(map[string]interface{}{"file_path": stringProp("Absolute path to .trc or .xel file")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				filePath, err := requiredString(args, "file_path")
				if err != nil {
					return nil, err
				}
				return trcsvc.ExecuteParse(ctx, db, filePath)
			},
		},
		"codebase_trc_list": {
			Definition: toolDefinition{Name: "codebase_trc_list", Description: "List saved trc parsing sessions from the database, ordered by most recent first. Returns session ID, file path, total events, file size, and parse timestamp.", InputSchema: objectSchema(map[string]interface{}{"limit": intProp("Max sessions to return (default 20)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				limit := optionalLimit(args)
				return trcsvc.ExecuteList(ctx, db, limit)
			},
		},
		"codebase_trc_summary": {
			Definition: toolDefinition{Name: "codebase_trc_summary", Description: "Get summary info for a trc session: total events and session metadata (provider/server/version). Requires either a saved session ID or a file path to parse on the fly.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .trc file to parse on the fly")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return trcsvc.ExecuteSummary(ctx, db, trcsvc.SessionSource{SessionID: sessionID, FilePath: filePath})
			},
		},
		"codebase_trc_events": {
			Definition: toolDefinition{Name: "codebase_trc_events", Description: "List decoded events from a trc session, with optional filters. Returns event class, name, procedure, params, duration, and full decoded columns. Supports server-side filtering by SPID, procedure, and event_name with limit.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .trc file"), "spid": intProp("Optional SPID filter"), "procedure": stringProp("Optional procedure name filter (exact match)"), "event_name": stringProp("Optional event name filter (e.g. RPC:Completed)"), "limit": intProp("Max events to return (default 100, max 1000)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				limit, _ := optionalInt(args, "limit")
				spidFilter, _ := optionalInt(args, "spid")
				procFilter, _ := optionalString(args, "procedure")
				eventNameFilter, _ := optionalString(args, "event_name")
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return trcsvc.ExecuteEvents(ctx, db, trcsvc.EventsParams{
					Source:    trcsvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					SPID:      spidFilter,
					Procedure: procFilter,
					EventName: eventNameFilter,
					Limit:     limit,
				})
			},
		},
		"codebase_trc_procedures": {
			Definition: toolDefinition{Name: "codebase_trc_procedures", Description: "Aggregate trc session events by procedure name (extracted from exec-statements in TextData): call count, min/max/avg/total duration. Enriched with source file location from CodeBase index. Sorted by total duration descending. Uses server-side SQL aggregation when session_id is provided.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .trc file")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return trcsvc.ExecuteProcedures(ctx, db, trcsvc.SessionSource{SessionID: sessionID, FilePath: filePath})
			},
		},
		"codebase_trc_tree": {
			Definition: toolDefinition{Name: "codebase_trc_tree", Description: "Build call trees from a trc session, grouped by SPID, restoring nesting via Starting/Completed event pairs (RPC, SQL:Batch, SQL:Stmt, SP, SP:Stmt). Uses server-side recursive CTE when session_id is provided. If spid is given, returns only that SPID's tree. max_depth limits tree depth (0 = unlimited). limit caps the number of root nodes and children per node (0 = unlimited). procedure filters the tree to show only subtrees rooted at events with the matching procedure name.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .trc file"), "spid": intProp("Optional SPID filter (0 = auto-select busiest SPID)"), "max_depth": intProp("Maximum tree depth (0 = unlimited)"), "limit": intProp("Maximum root nodes and children per node (0 = unlimited)"), "procedure": stringProp("Optional procedure name filter — show only subtrees rooted at events with this procedure")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				maxDepth, _ := optionalInt(args, "max_depth")
				limit, _ := optionalInt(args, "limit")
				spidFilter, _ := optionalInt(args, "spid")
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				procedure, _ := optionalString(args, "procedure")
				return trcsvc.ExecuteTree(ctx, db, trcsvc.TreeParams{
					Source:    trcsvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					SPID:      spidFilter,
					MaxDepth:  maxDepth,
					Limit:     limit,
					Procedure: procedure,
				})
			},
		},
		"codebase_trc_slow": {
			Definition: toolDefinition{Name: "codebase_trc_slow", Description: "Find the slowest events in a trc session above a duration threshold (DurationMs). Sorted by duration descending. Uses server-side SQL when session_id is provided.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .trc file"), "threshold_ms": intProp("Minimum duration in milliseconds (default 100)"), "limit": intProp("Max events to return (default 100, max 1000)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				threshold, _ := optionalInt(args, "threshold_ms")
				limit, _ := optionalInt(args, "limit")
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return trcsvc.ExecuteSlow(ctx, db, trcsvc.SlowParams{
					Source:      trcsvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					ThresholdMs: threshold,
					Limit:       limit,
				})
			},
		},
		"codebase_trc_errors": {
			Definition: toolDefinition{Name: "codebase_trc_errors", Description: "Find events with a non-zero Error(31) column in a trc session. Uses server-side SQL when session_id is provided.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Saved session ID"), "file_path": stringProp("Or: path to .trc file"), "limit": intProp("Max events to return (default 100, max 1000)")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				limit, _ := optionalInt(args, "limit")
				sessionID, _ := optionalInt64(args, "session_id")
				filePath, _ := optionalString(args, "file_path")
				return trcsvc.ExecuteErrors(ctx, db, trcsvc.ErrorsParams{
					Source: trcsvc.SessionSource{SessionID: sessionID, FilePath: filePath},
					Limit:  limit,
				})
			},
		},
		"codebase_trc_delete": {
			Definition: toolDefinition{Name: "codebase_trc_delete", Description: "Delete a saved trc session by ID. Batch-deletes all associated events first, then removes the session.", InputSchema: objectSchema(map[string]interface{}{"session_id": intProp("Session ID to delete")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				sessionID, err := optionalInt64(args, "session_id")
				if err != nil {
					return nil, err
				}
				if sessionID <= 0 {
					return nil, fmt.Errorf("session_id is required")
				}
				return trcsvc.ExecuteDelete(ctx, db, sessionID)
			},
		},
		"codebase_trc_prune": {
			Definition: toolDefinition{Name: "codebase_trc_prune", Description: "Delete old trc sessions, keeping only the most recent N. Use keep_last=0 to delete all sessions (uses TRUNCATE for instant cleanup). Returns the number of deleted sessions.", InputSchema: objectSchema(map[string]interface{}{"keep_last": intProp("Number of most recent sessions to keep")})},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				keepLast, err := optionalInt(args, "keep_last")
				if err != nil {
					return nil, err
				}
				return trcsvc.ExecutePrune(ctx, db, keepLast)
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

var (
	queryDefaultLimit = 100
	queryMaxLimit     = 1000
)

// SetQueryLimits устанавливает лимиты для query-вызовов из конфига.
func SetQueryLimits(defaultLimit, maxLimit int) {
	if defaultLimit > 0 {
		queryDefaultLimit = defaultLimit
	}
	if maxLimit > 0 {
		queryMaxLimit = maxLimit
	}
}

func optionalLimit(args map[string]interface{}) int {
	value, ok := args["limit"]
	if !ok || value == nil {
		return queryDefaultLimit
	}
	var result int
	switch v := value.(type) {
	case float64:
		result = int(v)
	case int:
		result = v
	default:
		return queryDefaultLimit
	}
	if result <= 0 {
		return queryDefaultLimit
	}
	if result > queryMaxLimit {
		return queryMaxLimit
	}
	return result
}

func optionalInt64(args map[string]interface{}, key string) (int64, error) {
	value, ok := args[key]
	if !ok || value == nil {
		return 0, nil
	}
	switch v := value.(type) {
	case float64:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	default:
		return 0, fmt.Errorf("argument %s must be integer", key)
	}
}

// buildTimelineFilter constructs a TimelineFilter from common MCP args.
func buildTimelineFilter(args map[string]interface{}) (rtisvc.TimelineFilter, error) {
	var filter rtisvc.TimelineFilter

	className, err := optionalString(args, "class_name")
	if err != nil {
		return filter, err
	}
	filter.ClassName = className

	methodName, err := optionalString(args, "method_name")
	if err != nil {
		return filter, err
	}
	filter.MethodName = methodName

	formatVal, err := optionalString(args, "format")
	if err != nil {
		return filter, err
	}
	filter.Format = formatVal

	if v, err := optionalString(args, "time_from"); err != nil {
		return filter, err
	} else if v != "" {
		t, perr := time.Parse(time.RFC3339, v)
		if perr != nil {
			return filter, fmt.Errorf("invalid time_from (expected RFC3339): %w", perr)
		}
		filter.TimeFrom = &t
	}

	if v, err := optionalString(args, "time_to"); err != nil {
		return filter, err
	} else if v != "" {
		t, perr := time.Parse(time.RFC3339, v)
		if perr != nil {
			return filter, fmt.Errorf("invalid time_to (expected RFC3339): %w", perr)
		}
		filter.TimeTo = &t
	}

	pidVal, err := optionalInt(args, "pid")
	if err != nil {
		return filter, err
	}
	if pidVal > 0 {
		filter.PID = &pidVal
	}

	return filter, nil
}

func toJSONText(value interface{}) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
