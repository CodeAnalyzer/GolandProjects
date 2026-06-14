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
				Description: "Verify that the MCP transport and codebase server process are alive. Use this as a first step to confirm the server is reachable before calling any other tool.",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				return map[string]interface{}{"ok": true, "service": "codebase-mcp"}, nil
			},
		},
		"codebase_health": {
			Definition: toolDefinition{
				Name:        "codebase_health",
				Description: "Check whether the CodeBase index database is connected and ready to serve queries. Returns status and DB connection info. Use before running queries if you are unsure whether the index is available.",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				return systemsvc.ExecuteHealth()
			},
		},
		"codebase_stats": {
			Definition: toolDefinition{
				Name:        "codebase_stats",
				Description: "Return index statistics: total counts of indexed files, SQL procedures, tables, PAS methods, JS/VB functions, DFM forms, SMF instruments, API contracts, report forms, and relations. Use to understand the scope of the indexed codebase.",
				InputSchema: objectSchema(map[string]interface{}{}),
			},
			Handler: func(args map[string]interface{}) (interface{}, error) {
				return systemsvc.ExecuteStats()
			},
		},
		"codebase_review_sql": {
			Definition: toolDefinition{Name: "codebase_review_sql", Description: "Run static analysis (lint) checks on a single SQL file and return a list of findings with rule ID, severity, line number and message. Requires absolute file path. Available rule IDs: foreignTablesUsing, foreignPTablesUsing, foreignProcedureUsing, execNotExistsProc, procDuplicate, procParamDefValue, procElseCase, useSelectAll, truncTbl, datatype, ansiInJoin, insertRowLock, useEqColumn, tableFullScan, tableHintExists, tableHintIsRight, indexExistsInDB, indexWrong, updateOnlyVar, pTableSpid, forceOrder2Tbl, saveTran, useDrop, mathOperations, existsWithAndInIf, nullComparison, shouldBeCP866, tooManyJoins, maxProcParam, modifyOutProc, emptyReturn, rawTransactionControl, deferredUpdate, inSubQuery, varcharSize, columnInsert, postgreLabelGotoLevel, dateIntoString, emptyStringDate. Omit rules to run all enabled rules.", InputSchema: objectSchema(map[string]interface{}{"file_path": stringProp("Full SQL file path"), "rules": map[string]interface{}{"type": "array", "description": "Optional rule ids", "items": map[string]interface{}{"type": "string"}}, "min_severity": intProp("Minimum severity")})},
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
					case review.RuleForeignTablesUsing, review.RuleForeignPTablesUsing, review.RuleForeignProcedureUsing, review.RuleExecNotExistsProc, review.RuleProcDuplicate, review.RuleProcParamDefValue, review.RuleProcElseCase, review.RuleUseSelectAll, review.RuleTruncTbl, review.RuleDatatype, review.RuleAnsiInJoin, review.RuleInsertRowLock, review.RuleUseEqColumn, review.RuleTableFullScan, review.RuleTableHintExists, review.RuleTableHintIsRight, review.RuleIndexExistsInDB, review.RuleIndexWrong, review.RuleUpdateOnlyVar, review.RulePTableSpid, review.RuleForceOrder2Tbl, review.RuleSaveTran, review.RuleUseDrop, review.RuleMathOperations, review.RuleExistsWithAndInIf, review.RuleNullComparison, review.RuleShouldBeCP866, review.RuleTooManyJoins, review.RuleMaxProcParam, review.RuleModifyOutProc, review.RuleEmptyReturn, review.RuleRawTransactionControl, review.RuleDeferredUpdate, review.RuleInSubQuery, review.RuleVarcharSize, review.RuleColumnInsert, review.RulePostgreLabelGotoLevel, review.RuleDateIntoString, review.RuleEmptyStringDate:
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
			Definition: toolDefinition{Name: "codebase_query_symbol", Description: "Search across all indexed entity types by name: SQL procedures, SQL tables, PAS methods, JS functions, VB functions, DFM forms, DFM components, SMF instruments, API contracts, report forms. Use 'type' to narrow to a specific entity type (e.g. 'sql_procedure', 'sql_table', 'pas_method', 'js_function', 'vb_function', 'dfm_form', 'dfm_component', 'smf_instrument', 'api_contract', 'report_form'). Set like=true for partial name match. Prefer this tool for initial discovery when you do not know the exact entity type.", InputSchema: querySchema("name", stringProp("Symbol name"), map[string]interface{}{"type": stringProp("Symbol type"), "like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_table", Description: "Search SQL tables by table name. Returns table name, file path, line number and definition metadata. Use when you need to find a specific DB table definition or check if a table exists in the index.", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_table_schema", Description: "Return the column definitions (schema) for a SQL table: column name, data type, nullability, default value, line number. Use when you need to inspect table structure or check column data types for impact analysis or type compatibility review.", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_table_index", Description: "Search SQL index definitions by index name or table name. Returns index name, table name, index fields, index type (clustered/nonclustered), uniqueness flag and file location. Use when analysing query performance hints or verifying that expected indexes exist.", InputSchema: querySchema("name", stringProp("Table/index name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_procedure", Description: "Get detailed information about a SQL stored procedure by exact name: file path, line range, parameter list with data types and default values, and the full body text. Use when you need to inspect a specific procedure's signature or source code. For fuzzy search use codebase_query_symbol with type='sql_procedure'.", InputSchema: querySchema("name", stringProp("Procedure name"), map[string]interface{}{})},
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
			Definition: toolDefinition{Name: "codebase_query_callers", Description: "Find all callers of a SQL stored procedure. Returns direct callers (sql_procedure, pas_method, js_function, report_form, vb_function, query_fragment) and indirect callers up to 2 hops via calls_procedure/dispatches_to/dispatches_to_subscriber chains. Use for impact analysis: who calls this procedure and what code will be affected by its change.", InputSchema: querySchema("procedure", stringProp("Procedure name"), map[string]interface{}{"limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_methods", Description: "Find all PAS (Delphi/Pascal) methods that reference a specific SQL table through their query fragments. Returns method name, class name, unit name, file path and line number. Use for DB impact analysis: which Object Pascal code reads/writes this table.", InputSchema: querySchema("table", stringProp("Table name"), map[string]interface{}{"limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_method", Description: "Search PAS (Delphi/Pascal) methods by method name. Returns method name, class name, unit name, file path and line number. Set like=true for partial match. Use when navigating Object Pascal source code or finding all implementations of a method name pattern.", InputSchema: querySchema("name", stringProp("Method name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_form", Description: "Search DFM (Delphi Form) definitions by form name. Returns form name, class name, caption, file path and line range. Use when looking for a UI form definition in a Delphi codebase.", InputSchema: querySchema("name", stringProp("Form name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_form_component", Description: "Search DFM form components (buttons, grids, datasources, etc.) by component name. Returns component name, component type, parent form name, file path and line number. Use when locating a specific UI control within Delphi forms.", InputSchema: querySchema("name", stringProp("Component name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_sql_fragment", Description: "Search indexed SQL query fragments (inline SQL embedded in PAS/JS/VB/SMF/DFM source files) by text content. Returns the fragment text, parent entity (method/function), file path and line number. Use when looking for ad-hoc queries that reference a table or procedure name but are not standalone SQL procedures.", InputSchema: querySchema("text", stringProp("Text fragment"), map[string]interface{}{"limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_relations", Description: "Search the dependency/relation graph by source entity, target entity, and/or relation type. All parameters are optional but at least one must be provided. Entity types: sql_procedure, sql_table, pas_method, js_function, vb_function, api_contract, report_form, report_field, report_param, smf_instrument, query_fragment. Relation types: calls_procedure, dispatches_to, dispatches_to_subscriber, selects_from, inserts_into, updates, deletes_from, references_table, executes_query, builds_query, implements_contract, executes_contract, publishes_event, subscribes_to_event, has_field, has_param, uses_param. Use for arbitrary graph traversal when the dedicated tools (callers, methods, api_*) do not cover the needed relation type.", InputSchema: objectSchema(map[string]interface{}{"source_type": stringProp("Source entity type"), "source_name": stringProp("Source entity name"), "target_type": stringProp("Target entity type"), "target_name": stringProp("Target entity name"), "relation_type": stringProp("Relation type"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_inspect", Description: "Deep-inspect a symbol by name: find the symbol, then fetch all incoming and outgoing relations in the dependency graph, and collect neighboring symbols. Returns symbol metadata plus full relation context (incoming, outgoing, neighbors). Use this as an all-in-one exploration starting point when you need to understand both what a symbol does and how it is connected. Differs from codebase_query_symbol (returns only symbol metadata) and codebase_query_relations (requires explicit entity IDs/types).", InputSchema: querySchema("name", stringProp("Symbol name"), map[string]interface{}{"type": stringProp("Symbol type"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_js_function", Description: "Search JavaScript functions indexed from JS/SMF source files by function name. Returns function name, file path and line number. Set like=true for partial match. Use when tracing client-side or scenario (SMF) logic.", InputSchema: querySchema("name", stringProp("Function name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_smf_instrument", Description: "Search SMF (Scenario/Workflow) instruments by instrument name. Returns instrument name, scenario type, file path and line number. Set like=true for partial match. Use when exploring business process automation scenarios in Diasoft 5NT.", InputSchema: querySchema("name", stringProp("Instrument name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_smf_type", Description: "Search SMF (Scenario/Workflow) instruments filtered by scenario type string. Returns all instruments matching the given type. Use when you need to enumerate all scenarios of a given process category rather than searching by name.", InputSchema: querySchema("type", stringProp("Scenario type"), map[string]interface{}{"limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_report_form", Description: "Search report definitions (TPR/RPT report forms) by report name. Returns report name, report type, file path and line range. Set like=true for partial match. Use when locating a report template or analysing report dependencies.", InputSchema: querySchema("name", stringProp("Report form name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_report_field", Description: "Search report field definitions by field name across all indexed report forms. Returns field name, data type, parent report name, file path and line number. Use when checking what fields a report exposes or when tracing a column name across reports.", InputSchema: querySchema("name", stringProp("Report field name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_report_param", Description: "Search report parameter definitions by parameter name across all indexed report forms. Returns parameter name, data type, parent report name, file path and line number. Use when verifying which parameters a report accepts or finding reports that use a specific parameter.", InputSchema: querySchema("name", stringProp("Report param name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_vb_function", Description: "Search VBA/VBScript functions indexed from report source files by function name. Returns function name, parent report form, file path and line number. Set like=true for partial match. Use when tracing report calculation logic written in Visual Basic.", InputSchema: querySchema("name", stringProp("Function name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_api_contract", Description: "Search Diasoft API contracts by contract name. Returns contract name, contract kind (service, event, callback_event, used_service), file path and line number. Set like=true for partial match. Use as the entry point when exploring inter-module API surface.", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_api_table", Description: "Search tables declared inside Diasoft API contracts by table name. Returns table name, column list, parent contract name, file path and line number. Use when you need to inspect the data structure that an API contract operates on.", InputSchema: querySchema("name", stringProp("Table name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_api_table_index", Description: "Search index definitions declared inside Diasoft API contracts by index or table name. Returns index name, table name, fields and parent contract. Use when verifying API-level index expectations versus actual DB indexes.", InputSchema: querySchema("name", stringProp("Table/index name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_api_param", Description: "Search parameters declared in Diasoft API contracts by parameter name. Returns parameter name, data type, direction, parent contract name, file path and line number. Use when checking the API signature or finding contracts that expose a specific parameter.", InputSchema: querySchema("name", stringProp("Param name"), map[string]interface{}{"like": boolProp("Use partial match"), "limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_api_impl", Description: "Find SQL stored procedures that implement a given API contract (relation type implements_contract). Returns direct and indirect (via calls_procedure/dispatches_to) implementing procedures with file paths. Use when you need to find the server-side SQL implementation behind an API contract name.", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_api_publishers", Description: "Find SQL stored procedures that publish (raise) a given API event contract (relation type publishes_event) and callback contracts that subscribe to it. Returns direct and indirect publishers with file paths. Use when tracing the origin of an event in the Diasoft API event bus.", InputSchema: querySchema("event", stringProp("Event name"), map[string]interface{}{"limit": intProp("Max results")})},
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
			Definition: toolDefinition{Name: "codebase_query_api_consumers", Description: "Find SQL stored procedures that consume (call via exec_contract) a given API service contract (relation type executes_contract). Returns direct and indirect consumers with file paths. Use when analysing which modules depend on a given API service and would be affected by its change.", InputSchema: querySchema("name", stringProp("Contract name"), map[string]interface{}{"limit": intProp("Max results")})},
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
