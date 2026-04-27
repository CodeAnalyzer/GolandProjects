package indexer

import (
	"crypto/sha256"
	dbsql "database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store"
)

var execProcNameRe = regexp.MustCompile(`(?i)\bexec(?:ute)?\s+(?:@\w+\s*=\s*)?(?:\[?([^\]]+)\]?\.)?(\[?([A-Za-z_][A-Za-z0-9_]*)\]?)`)

func mapTableRelationType(context string) string {
	switch strings.ToLower(strings.TrimSpace(context)) {
	case "select":
		return "selects_from"
	case "insert":
		return "inserts_into"
	case "update":
		return "updates"
	case "delete":
		return "deletes_from"
	default:
		return "references_table"
	}
}

func computeQueryHash(query string) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(query)))
	return hex.EncodeToString(h[:])
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, value)
	}
	return result
}

func extractProcedureCallsFromQuery(queryText string) []string {
	matches := execProcNameRe.FindAllStringSubmatch(queryText, -1)
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 4 {
			continue
		}
		// match[3] - имя процедуры без квадратных скобок
		// match[2] - имя процедуры с квадратными скобками (если есть)
		procName := match[3]
		if procName == "" {
			procName = match[2]
		}
		// Убираем квадратные скобки, если они есть
		procName = strings.Trim(procName, "[]")
		if procName != "" {
			result = append(result, procName)
		}
	}
	return uniqueStrings(result)
}

func mapQueryFragmentParentRelationType(parentType string, fragment *model.QueryFragment) string {
	switch parentType {
	case "pas_method":
		if fragment != nil && strings.EqualFold(strings.TrimSpace(fragment.Context), "method") {
			return "builds_query"
		}
		return "executes_query"
	case "sql_procedure":
		return "executes_query"
	case "js_function", "smf_instrument", "dfm_form":
		return "executes_query"
	case "report_form", "vb_function":
		return "executes_query"
	default:
		return ""
	}
}

func (idx *Indexer) enrichQueryFragmentsWithSQL(fileID int64, fragments []*model.QueryFragment) ([]*model.SQLTable, []*model.SQLColumn, error) {
	parser := sqlparser.NewParser()
	tablesBatch := make([]*model.SQLTable, 0)
	columnsBatch := make([]*model.SQLColumn, 0)

	for _, fragment := range fragments {
		if fragment == nil || strings.TrimSpace(fragment.QueryText) == "" {
			continue
		}
		parsed, err := parser.ParseContent(fragment.QueryText)
		if err != nil {
			return nil, nil, err
		}
		for _, table := range parsed.Tables {
			if table == nil {
				continue
			}
			table.FileID = fileID
			if table.LineNumber <= 0 {
				table.LineNumber = fragment.LineNumber
			} else {
				table.LineNumber += fragment.LineNumber - 1
			}
			tablesBatch = append(tablesBatch, table)
			fragment.TablesReferenced = append(fragment.TablesReferenced, table.TableName)
		}
		for _, column := range parsed.Columns {
			if column == nil {
				continue
			}
			column.FileID = fileID
			if column.LineNumber <= 0 {
				column.LineNumber = fragment.LineNumber
			} else {
				column.LineNumber += fragment.LineNumber - 1
			}
			columnsBatch = append(columnsBatch, column)
		}
		fragment.TablesReferenced = uniqueStrings(fragment.TablesReferenced)
	}

	return tablesBatch, columnsBatch, nil
}

func (idx *Indexer) saveRelations(relations []*model.Relation, path string, stats *model.ScanStats) error {
	if len(relations) == 0 {
		return nil
	}
	if err := idx.db.BatchInsertRelations(relations, idx.config.Indexer.BatchSize); err != nil {
		idx.logError(path, "Error batch inserting relations: %v", err)
		stats.Errors += len(relations)
		return err
	}
	stats.Relations += len(relations)
	return nil
}

func (idx *Indexer) buildCallbackEventRelations(contracts []*model.APIContract, contractIDs map[string]int64) ([]*model.Relation, error) {
	if len(contracts) == 0 || len(contractIDs) == 0 {
		return nil, nil
	}
	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})
	for _, contract := range contracts {
		if contract == nil || !strings.EqualFold(strings.TrimSpace(contract.ContractKind), "callback_event") {
			continue
		}
		usedObjectName := strings.TrimSpace(contract.UsedObjectName)
		if usedObjectName == "" {
			continue
		}
		sourceID := contractIDs[store.BuildAPIContractLookupKey(contract.ContractName, contract.ContractKind)]
		if sourceID == 0 {
			continue
		}
		targetID, err := idx.db.FindLatestAPIContractIDByNameKindAndOwnerModule(usedObjectName, "event", strings.TrimSpace(contract.UsedModuleSysName))
		if err != nil {
			if err == dbsql.ErrNoRows {
				continue
			}
			return nil, err
		}
		if targetID == 0 {
			continue
		}
		key := fmt.Sprintf("api_contract|%d|api_contract|%d|subscribes_to_event", sourceID, targetID)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "api_contract",
			SourceID:     sourceID,
			TargetType:   "api_contract",
			TargetID:     targetID,
			RelationType: "subscribes_to_event",
			Confidence:   "xml",
			LineNumber:   1,
		})
	}
	return relations, nil
}

func (idx *Indexer) buildSQLProcedureRelations(fileID int64, procedures []*model.SQLProcedure, tables []*model.SQLTable, calls []*model.SQLProcedureCall) ([]*model.Relation, error) {
	procedureIDs, err := idx.db.FindSQLProcedureIDsByFile(fileID)
	if err != nil {
		return nil, err
	}
	tableIDs, err := idx.db.FindSQLTableIDsByFileAndLine(fileID)
	if err != nil {
		return nil, err
	}

	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})

	for _, call := range calls {
		if call == nil {
			continue
		}
		sourceID := procedureIDs[strings.ToLower(strings.TrimSpace(call.CallerName))]
		if sourceID == 0 {
			continue
		}
		targetID, err := idx.db.FindLatestSQLProcedureIDByName(call.CalleeName)
		if err != nil {
			if err == dbsql.ErrNoRows {
				continue
			}
			return nil, err
		}
		key := fmt.Sprintf("sql_procedure|%d|sql_procedure|%d|calls_procedure|%d", sourceID, targetID, call.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "sql_procedure",
			SourceID:     sourceID,
			TargetType:   "sql_procedure",
			TargetID:     targetID,
			RelationType: "calls_procedure",
			Confidence:   "regex",
			LineNumber:   call.LineNumber,
		})
	}

	for _, proc := range procedures {
		if proc == nil {
			continue
		}
		sourceID := procedureIDs[strings.ToLower(strings.TrimSpace(proc.ProcName))]
		if sourceID == 0 {
			continue
		}
		for _, table := range tables {
			if table == nil {
				continue
			}
			if table.LineNumber < proc.LineStart || (proc.LineEnd > 0 && table.LineNumber > proc.LineEnd) {
				continue
			}
			targetID := tableIDs[store.BuildSQLTableLookupKey(table.TableName, table.Context, table.LineNumber)]
			if targetID == 0 {
				continue
			}
			relationType := mapTableRelationType(table.Context)
			key := fmt.Sprintf("sql_procedure|%d|sql_table|%d|%s|%d", sourceID, targetID, relationType, table.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "sql_procedure",
				SourceID:     sourceID,
				TargetType:   "sql_table",
				TargetID:     targetID,
				RelationType: relationType,
				Confidence:   "regex",
				LineNumber:   table.LineNumber,
			})
		}
	}

	return relations, nil
}

func (idx *Indexer) buildReportStructureRelations(reportFormID int64, fields []*model.ReportField, params []*model.ReportParam) ([]*model.Relation, error) {
	relations := make([]*model.Relation, 0, len(fields)+len(params))
	seen := make(map[string]struct{})

	fieldIDs, err := idx.db.FindReportFieldIDsByForm(reportFormID)
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		if field == nil {
			continue
		}
		targetID := fieldIDs[store.BuildReportFieldLookupKey(field.FieldName, field.LineNumber)]
		if targetID == 0 {
			continue
		}
		key := fmt.Sprintf("report_form|%d|report_field|%d|has_field|%d", reportFormID, targetID, field.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "report_form",
			SourceID:     reportFormID,
			TargetType:   "report_field",
			TargetID:     targetID,
			RelationType: "has_field",
			Confidence:   "regex",
			LineNumber:   field.LineNumber,
		})
	}

	paramIDs, err := idx.db.FindReportParamIDsByForm(reportFormID)
	if err != nil {
		return nil, err
	}
	for _, param := range params {
		if param == nil {
			continue
		}
		targetID := paramIDs[store.BuildReportParamLookupKey(param.ParamName, param.LineNumber)]
		if targetID == 0 {
			continue
		}
		key := fmt.Sprintf("report_form|%d|report_param|%d|has_param|%d", reportFormID, targetID, param.LineNumber)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		relations = append(relations, &model.Relation{
			SourceType:   "report_form",
			SourceID:     reportFormID,
			TargetType:   "report_param",
			TargetID:     targetID,
			RelationType: "has_param",
			Confidence:   "regex",
			LineNumber:   param.LineNumber,
		})
	}

	return relations, nil
}

func extractReportParamRefs(text string, params []*model.ReportParam) []string {
	if strings.TrimSpace(text) == "" || len(params) == 0 {
		return nil
	}
	textLower := strings.ToLower(text)
	result := make([]string, 0)
	seen := make(map[string]struct{})
	for _, param := range params {
		if param == nil {
			continue
		}
		name := strings.TrimSpace(param.ParamName)
		if name == "" {
			continue
		}
		nameLower := strings.ToLower(name)
		patterns := []string{
			"%" + nameLower,
			":" + nameLower,
			"@" + nameLower,
			"[" + nameLower + "]",
			nameLower,
		}
		for _, p := range patterns {
			if strings.Contains(textLower, p) {
				if _, exists := seen[nameLower]; !exists {
					seen[nameLower] = struct{}{}
					result = append(result, name)
				}
				break
			}
		}
	}
	return result
}

func (idx *Indexer) buildReportParamUsageRelations(fileID int64, reportFormID int64, fragments []*model.QueryFragment, params []*model.ReportParam) ([]*model.Relation, error) {
	fragmentIDs, err := idx.db.FindQueryFragmentIDsByFileAndHash(fileID)
	if err != nil {
		return nil, err
	}
	paramIDs, err := idx.db.FindReportParamIDsByForm(reportFormID)
	if err != nil {
		return nil, err
	}

	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})
	for _, fragment := range fragments {
		if fragment == nil {
			continue
		}
		fragmentID := fragmentIDs[store.BuildQueryFragmentLookupKey(fragment.QueryHash, fragment.Context, fragment.LineNumber)]
		if fragmentID == 0 {
			continue
		}
		for _, paramName := range extractReportParamRefs(fragment.QueryText, params) {
			param := findReportParamByName(params, paramName)
			if param == nil {
				continue
			}
			targetID := paramIDs[store.BuildReportParamLookupKey(param.ParamName, param.LineNumber)]
			if targetID == 0 {
				continue
			}
			key := fmt.Sprintf("query_fragment|%d|report_param|%d|uses_param|%d", fragmentID, targetID, fragment.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "query_fragment",
				SourceID:     fragmentID,
				TargetType:   "report_param",
				TargetID:     targetID,
				RelationType: "uses_param",
				Confidence:   "regex",
				LineNumber:   fragment.LineNumber,
			})
		}
	}
	return relations, nil
}

func findReportParamByName(params []*model.ReportParam, name string) *model.ReportParam {
	for _, param := range params {
		if param != nil && strings.EqualFold(strings.TrimSpace(param.ParamName), strings.TrimSpace(name)) {
			return param
		}
	}
	return nil
}

func (idx *Indexer) buildVBFunctionQueryRelations(fileID int64, reportFormID int64, functions []*model.VBFunction, fragments []*model.QueryFragment) ([]*model.Relation, error) {
	fragmentIDs, err := idx.db.FindQueryFragmentIDsByFileAndHash(fileID)
	if err != nil {
		return nil, err
	}
	functionIDs, err := idx.db.FindVBFunctionIDsByForm(reportFormID)
	if err != nil {
		return nil, err
	}

	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})
	for _, fn := range functions {
		if fn == nil {
			continue
		}
		fnID := functionIDs[store.BuildVBFunctionLookupKey(fn.FunctionName, fn.LineStart)]
		if fnID == 0 {
			continue
		}
		bodyLower := strings.ToLower(fn.BodyText)
		for _, fragment := range fragments {
			if fragment == nil {
				continue
			}
			fragmentID := fragmentIDs[store.BuildQueryFragmentLookupKey(fragment.QueryHash, fragment.Context, fragment.LineNumber)]
			if fragmentID == 0 {
				continue
			}
			componentName := strings.ToLower(strings.TrimSpace(fragment.ComponentName))
			if componentName == "" || !strings.Contains(bodyLower, componentName) {
				continue
			}
			key := fmt.Sprintf("vb_function|%d|query_fragment|%d|executes_query|%d", fnID, fragmentID, fragment.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "vb_function",
				SourceID:     fnID,
				TargetType:   "query_fragment",
				TargetID:     fragmentID,
				RelationType: "executes_query",
				Confidence:   "regex",
				LineNumber:   fragment.LineNumber,
			})
		}
	}
	return relations, nil
}

func (idx *Indexer) buildQueryFragmentRelations(fileID int64, fragments []*model.QueryFragment) ([]*model.Relation, error) {
	fragmentIDs, err := idx.db.FindQueryFragmentIDsByFileAndHash(fileID)
	if err != nil {
		return nil, err
	}
	relations := make([]*model.Relation, 0)
	seen := make(map[string]struct{})

	for _, fragment := range fragments {
		if fragment == nil {
			continue
		}
		fragmentID := fragmentIDs[store.BuildQueryFragmentLookupKey(fragment.QueryHash, fragment.Context, fragment.LineNumber)]
		if fragmentID == 0 {
			continue
		}
		parentRelationType := mapQueryFragmentParentRelationType(fragment.ParentType, fragment)
		if fragment.ParentID > 0 && parentRelationType != "" {
			key := fmt.Sprintf("%s|%d|query_fragment|%d|%s|%d", fragment.ParentType, fragment.ParentID, fragmentID, parentRelationType, fragment.LineNumber)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				relations = append(relations, &model.Relation{
					SourceType:   fragment.ParentType,
					SourceID:     fragment.ParentID,
					TargetType:   "query_fragment",
					TargetID:     fragmentID,
					RelationType: parentRelationType,
					Confidence:   "regex",
					LineNumber:   fragment.LineNumber,
				})
			}
		}
		for _, tableName := range uniqueStrings(fragment.TablesReferenced) {
			targetID, err := idx.db.FindLatestSQLTableIDByName(tableName)
			if err != nil {
				if err == dbsql.ErrNoRows {
					continue
				}
				return nil, err
			}
			key := fmt.Sprintf("query_fragment|%d|sql_table|%d|references_table|%d", fragmentID, targetID, fragment.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "query_fragment",
				SourceID:     fragmentID,
				TargetType:   "sql_table",
				TargetID:     targetID,
				RelationType: "references_table",
				Confidence:   "regex",
				LineNumber:   fragment.LineNumber,
			})
		}
		for _, procName := range extractProcedureCallsFromQuery(fragment.QueryText) {
			targetID, err := idx.db.FindLatestSQLProcedureIDByName(procName)
			if err != nil {
				if err == dbsql.ErrNoRows {
					continue
				}
				return nil, err
			}
			key := fmt.Sprintf("query_fragment|%d|sql_procedure|%d|calls_procedure|%d", fragmentID, targetID, fragment.LineNumber)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relations = append(relations, &model.Relation{
				SourceType:   "query_fragment",
				SourceID:     fragmentID,
				TargetType:   "sql_procedure",
				TargetID:     targetID,
				RelationType: "calls_procedure",
				Confidence:   "regex",
				LineNumber:   fragment.LineNumber,
			})
		}
	}

	return relations, nil
}
