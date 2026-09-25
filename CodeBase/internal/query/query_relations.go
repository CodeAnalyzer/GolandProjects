package query

import (
	"context"
	"fmt"
	"strings"

	"github.com/codebase/internal/errs"
)

type relationEntityMatch struct {
	Type string
	ID   int64
}

func (q *Query) SearchRelationsByEntity(ctx context.Context, sourceType string, sourceID int64, targetType string, targetID int64, relationType string, limit int) ([]RelationResult, error) {
	conditions := make([]string, 0, 5)
	args := make([]interface{}, 0, 6)
	argPos := 1

	if sourceType != "" {
		conditions = append(conditions, fmt.Sprintf("r.source_type = $%d", argPos))
		args = append(args, sourceType)
		argPos++
	}
	if sourceID > 0 {
		conditions = append(conditions, fmt.Sprintf("r.source_id = $%d", argPos))
		args = append(args, sourceID)
		argPos++
	}
	if targetType != "" {
		conditions = append(conditions, fmt.Sprintf("r.target_type = $%d", argPos))
		args = append(args, targetType)
		argPos++
	}
	if targetID > 0 {
		conditions = append(conditions, fmt.Sprintf("r.target_id = $%d", argPos))
		args = append(args, targetID)
		argPos++
	}
	if relationType != "" {
		conditions = append(conditions, fmt.Sprintf("r.relation_type = $%d", argPos))
		args = append(args, relationType)
		argPos++
	}

	if len(conditions) == 0 {
		return nil, errs.ErrNoRelationFilters
	}
	relationIDs, err := q.selectRelationIDs(ctx, conditions, args, argPos, limit)
	if err != nil {
		return nil, err
	}
	if len(relationIDs) == 0 {
		return []RelationResult{}, nil
	}

	return q.loadRelationDetails(ctx, relationIDs)
}

func (q *Query) selectRelationIDs(ctx context.Context, conditions []string, args []interface{}, argPos int, limit int) ([]int64, error) {
	queryText := "SELECT r.id FROM relations r WHERE " + strings.Join(conditions, " AND ")
	queryText += fmt.Sprintf(" ORDER BY r.id DESC LIMIT $%d", argPos)
	queryArgs := make([]interface{}, 0, len(args)+1)
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, limit)

	return q.fetchRelationIDs(ctx, queryText, queryArgs)
}

func buildRelationDetailsQueryByIDs(ids []int64) (string, []interface{}) {
	placeholders := make([]string, 0, len(ids))
	args := make([]interface{}, 0, len(ids))
	for i, id := range ids {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, id)
	}
	queryText := relationSearchBaseQuery()
	queryText += " WHERE r.id IN (" + strings.Join(placeholders, ",") + ")"
	queryText += " ORDER BY r.id DESC"
	return queryText, args
}

func (q *Query) loadRelationDetails(ctx context.Context, relationIDs []int64) ([]RelationResult, error) {
	queryText, queryArgs := buildRelationDetailsQueryByIDs(relationIDs)
	rows, err := q.db.QueryContext(ctx, queryText, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]RelationResult, 0, len(relationIDs))
	for rows.Next() {
		var r RelationResult
		if err := rows.Scan(
			&r.ID,
			&r.RelationType,
			&r.Confidence,
			&r.LineNumber,
			&r.Source.ID,
			&r.Source.Type,
			&r.Source.Name,
			&r.Source.FileID,
			&r.Source.File,
			&r.Source.LineNumber,
			&r.Target.ID,
			&r.Target.Type,
			&r.Target.Name,
			&r.Target.FileID,
			&r.Target.File,
			&r.Target.LineNumber,
		); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchRelations ищет связи по именам и/или типам сущностей.
//
// Используется единый двухпроходный алгоритм: имена резолвятся в пары
// (entity_type, entity_id) через типизированные индексированные lookup'ы
// (exact-first, затем подстрочный фолбэк), после чего relations выбираются
// по составному ключу (source_type/source_id, target_type/target_id).
// При двух именах результат — пересечение: source по первому имени AND
// target по второму.
func (q *Query) SearchRelations(ctx context.Context, sourceType string, sourceName string, targetType string, targetName string, relationType string, limit int) ([]RelationResult, error) {
	sourceName = strings.TrimSpace(sourceName)
	targetName = strings.TrimSpace(targetName)
	if sourceType == "" && sourceName == "" && targetType == "" && targetName == "" && relationType == "" {
		return nil, errs.ErrNoRelationFilters
	}

	matchLimit := relationEntityMatchLimit(limit)

	var sourceMatches []relationEntityMatch
	if sourceName != "" {
		matches, err := q.findRelationEntityMatches(ctx, sourceName, sourceType, matchLimit)
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return []RelationResult{}, nil
		}
		sourceMatches = matches
	}

	var targetMatches []relationEntityMatch
	if targetName != "" {
		matches, err := q.findRelationEntityMatches(ctx, targetName, targetType, matchLimit)
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return []RelationResult{}, nil
		}
		targetMatches = matches
	}

	queryText, queryArgs := buildRelationIDsQuery(sourceMatches, targetMatches, sourceType, targetType, relationType, limit)
	relationIDs, err := q.fetchRelationIDs(ctx, queryText, queryArgs)
	if err != nil {
		return nil, err
	}
	if len(relationIDs) == 0 {
		return []RelationResult{}, nil
	}

	return q.loadRelationDetails(ctx, relationIDs)
}

// buildRelationIDsQuery собирает второй проход: выборку id связей по уже
// найденным наборам совпадений. Если для стороны задано только имя — набор
// приходит из первого прохода и накладывается JOIN'ом к VALUES; если задан
// только тип — накладывается условие на колонку типа. relationType и limit
// применяются в конце. Функция чистая и покрыта unit-тестами.
func buildRelationIDsQuery(sourceMatches, targetMatches []relationEntityMatch, sourceType string, targetType string, relationType string, limit int) (string, []interface{}) {
	args := make([]interface{}, 0, len(sourceMatches)*2+len(targetMatches)*2+4)
	argPos := 1
	clauses := make([]string, 0, 3)
	joins := make([]string, 0, 2)

	matchesValues := func(matches []relationEntityMatch) string {
		values := make([]string, 0, len(matches))
		for _, match := range matches {
			values = append(values, fmt.Sprintf("($%d, $%d)", argPos, argPos+1))
			args = append(args, match.Type, match.ID)
			argPos += 2
		}
		return strings.Join(values, ",")
	}

	if len(sourceMatches) > 0 {
		joins = append(joins, fmt.Sprintf(
			"JOIN (VALUES %s) AS matched_source(entity_type, entity_id) ON r.source_type = matched_source.entity_type AND r.source_id = matched_source.entity_id::BIGINT",
			matchesValues(sourceMatches),
		))
	} else if strings.TrimSpace(sourceType) != "" {
		clauses = append(clauses, fmt.Sprintf("r.source_type = $%d", argPos))
		args = append(args, sourceType)
		argPos++
	}

	if len(targetMatches) > 0 {
		joins = append(joins, fmt.Sprintf(
			"JOIN (VALUES %s) AS matched_target(entity_type, entity_id) ON r.target_type = matched_target.entity_type AND r.target_id = matched_target.entity_id::BIGINT",
			matchesValues(targetMatches),
		))
	} else if strings.TrimSpace(targetType) != "" {
		clauses = append(clauses, fmt.Sprintf("r.target_type = $%d", argPos))
		args = append(args, targetType)
		argPos++
	}

	if strings.TrimSpace(relationType) != "" {
		clauses = append(clauses, fmt.Sprintf("r.relation_type = $%d", argPos))
		args = append(args, relationType)
		argPos++
	}

	queryText := "SELECT r.id FROM relations r"
	if len(joins) > 0 {
		queryText += " " + strings.Join(joins, " ")
	}
	if len(clauses) > 0 {
		queryText += " WHERE " + strings.Join(clauses, " AND ")
	}
	queryText += fmt.Sprintf(" ORDER BY r.id DESC LIMIT $%d", argPos)
	args = append(args, limit)
	return queryText, args
}

func (q *Query) fetchRelationIDs(ctx context.Context, queryText string, queryArgs []interface{}) ([]int64, error) {
	rows, err := q.db.QueryContext(ctx, queryText, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

func relationEntityMatchLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	matchLimit := limit * 4
	if matchLimit < 50 {
		return 50
	}
	return matchLimit
}

func (q *Query) findRelationEntityMatches(ctx context.Context, name string, entityType string, limit int) ([]relationEntityMatch, error) {
	matches, err := q.findRelationEntityMatchesWithCondition(ctx, name, entityType, limit, true)
	if err != nil {
		return nil, err
	}
	if len(matches) > 0 {
		return matches, nil
	}
	return q.findRelationEntityMatchesWithCondition(ctx, "%"+name+"%", entityType, limit, false)
}

func (q *Query) findRelationEntityMatchesWithCondition(ctx context.Context, value string, entityType string, limit int, exact bool) ([]relationEntityMatch, error) {
	queryParts, ok := relationEntityMatchQueryParts(entityType, exact)
	if !ok {
		return nil, fmt.Errorf("unsupported relation entity type for name filter: %s", entityType)
	}
	queryText := strings.Join(queryParts, "\nUNION ALL\n")
	queryText += "\nLIMIT $2"

	rows, err := q.db.QueryContext(ctx, queryText, value, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	matches := make([]relationEntityMatch, 0, limit)
	for rows.Next() {
		var match relationEntityMatch
		if err := rows.Scan(&match.Type, &match.ID); err != nil {
			return nil, err
		}
		matches = append(matches, match)
	}
	return matches, rows.Err()
}

func relationEntityMatchQueryParts(entityType string, exact bool) ([]string, bool) {
	type entitySource struct {
		entityType string
		tableName  string
		nameColumn string
	}
	sources := []entitySource{
		{"sql_procedure", "sql_procedures", "proc_name"},
		{"sql_table", "sql_tables", "table_name"},
		{"pas_method", "pas_methods", "method_name"},
		{"js_function", "js_functions", "function_name"},
		{"api_contract", "api_contracts", "contract_name"},
		{"report_form", "report_forms", "report_name"},
		{"report_field", "report_fields", "field_name"},
		{"report_param", "report_params", "param_name"},
		{"vb_function", "vb_functions", "function_name"},
		{"query_fragment", "query_fragments", "component_name"},
		{"smf_instrument", "smf_instruments", "instrument_name"},
	}

	normalizedType := strings.ToLower(strings.TrimSpace(entityType))
	parts := make([]string, 0, len(sources))
	for _, source := range sources {
		if normalizedType != "" && normalizedType != source.entityType {
			continue
		}
		condition := fmt.Sprintf("LOWER(%s) = LOWER($1)", source.nameColumn)
		if !exact {
			condition = fmt.Sprintf("%s ILIKE $1", source.nameColumn)
		}
		parts = append(parts, fmt.Sprintf("SELECT '%s' AS entity_type, id FROM %s WHERE %s", source.entityType, source.tableName, condition))
	}
	if len(parts) == 0 {
		return nil, false
	}
	return parts, true
}

func relationSearchBaseQuery() string {
	return `
		SELECT
			r.id,
			r.relation_type,
			COALESCE(r.confidence, '') as confidence,
			r.line_number,
			r.source_id,
			r.source_type,
			COALESCE(
				sp_src.proc_name,
				st_src.table_name,
				pm_src.method_name,
				jf_src.function_name,
				ac_src.contract_name,
				rf_src.report_name,
				rfield_src.field_name,
				rparam_src.param_name,
				vf_src.function_name,
				qf_src.component_name,
				smf_src.instrument_name,
				r.source_type || ':' || r.source_id::text
			) as source_name,
			COALESCE(f_sp_src.id, f_st_src.id, f_pm_src.id, f_jf_src.id, f_ac_src.id, f_rf_src.id, f_rfield_src.id, f_rparam_src.id, f_vf_src.id, f_qf_src.id, f_smf_src.id, 0) as source_file_id,
			COALESCE(f_sp_src.rel_path, f_st_src.rel_path, f_pm_src.rel_path, f_jf_src.rel_path, f_ac_src.rel_path, f_rf_src.rel_path, f_rfield_src.rel_path, f_rparam_src.rel_path, f_vf_src.rel_path, f_qf_src.rel_path, f_smf_src.rel_path, '') as source_file,
			COALESCE(sp_src.line_start, st_src.line_number, pm_src.line_number, jf_src.line_start, ac_src.line_start, rf_src.line_start, rfield_src.line_number, rparam_src.line_number, vf_src.line_start, qf_src.line_number, 0) as source_line_number,
			r.target_id,
			r.target_type,
			COALESCE(
				sp_tgt.proc_name,
				st_tgt.table_name,
				pm_tgt.method_name,
				jf_tgt.function_name,
				ac_tgt.contract_name,
				rf_tgt.report_name,
				rfield_tgt.field_name,
				rparam_tgt.param_name,
				vf_tgt.function_name,
				qf_tgt.component_name,
				smf_tgt.instrument_name,
				r.target_type || ':' || r.target_id::text
			) as target_name,
			COALESCE(f_sp_tgt.id, f_st_tgt.id, f_pm_tgt.id, f_jf_tgt.id, f_ac_tgt.id, f_rf_tgt.id, f_rfield_tgt.id, f_rparam_tgt.id, f_vf_tgt.id, f_qf_tgt.id, f_smf_tgt.id, 0) as target_file_id,
			COALESCE(f_sp_tgt.rel_path, f_st_tgt.rel_path, f_pm_tgt.rel_path, f_jf_tgt.rel_path, f_ac_tgt.rel_path, f_rf_tgt.rel_path, f_rfield_tgt.rel_path, f_rparam_tgt.rel_path, f_vf_tgt.rel_path, f_qf_tgt.rel_path, f_smf_tgt.rel_path, '') as target_file,
			COALESCE(sp_tgt.line_start, st_tgt.line_number, pm_tgt.line_number, jf_tgt.line_start, ac_tgt.line_start, rf_tgt.line_start, rfield_tgt.line_number, rparam_tgt.line_number, vf_tgt.line_start, qf_tgt.line_number, 0) as target_line_number
		FROM relations r
		LEFT JOIN sql_procedures sp_src ON r.source_type = 'sql_procedure' AND r.source_id = sp_src.id
		LEFT JOIN files f_sp_src ON sp_src.file_id = f_sp_src.id
		LEFT JOIN sql_tables st_src ON r.source_type = 'sql_table' AND r.source_id = st_src.id
		LEFT JOIN files f_st_src ON st_src.file_id = f_st_src.id
		LEFT JOIN pas_methods pm_src ON r.source_type = 'pas_method' AND r.source_id = pm_src.id
		LEFT JOIN pas_units pu_src ON pm_src.unit_id = pu_src.id
		LEFT JOIN files f_pm_src ON pu_src.file_id = f_pm_src.id
		LEFT JOIN js_functions jf_src ON r.source_type = 'js_function' AND r.source_id = jf_src.id
		LEFT JOIN files f_jf_src ON jf_src.file_id = f_jf_src.id
		LEFT JOIN api_contracts ac_src ON r.source_type = 'api_contract' AND r.source_id = ac_src.id
		LEFT JOIN files f_ac_src ON ac_src.file_id = f_ac_src.id
		LEFT JOIN report_forms rf_src ON r.source_type = 'report_form' AND r.source_id = rf_src.id
		LEFT JOIN files f_rf_src ON rf_src.file_id = f_rf_src.id
		LEFT JOIN report_fields rfield_src ON r.source_type = 'report_field' AND r.source_id = rfield_src.id
		LEFT JOIN report_forms rf_src_field ON rfield_src.report_form_id = rf_src_field.id
		LEFT JOIN files f_rfield_src ON rf_src_field.file_id = f_rfield_src.id
		LEFT JOIN report_params rparam_src ON r.source_type = 'report_param' AND r.source_id = rparam_src.id
		LEFT JOIN report_forms rf_src_param ON rparam_src.report_form_id = rf_src_param.id
		LEFT JOIN files f_rparam_src ON rf_src_param.file_id = f_rparam_src.id
		LEFT JOIN vb_functions vf_src ON r.source_type = 'vb_function' AND r.source_id = vf_src.id
		LEFT JOIN report_forms rf_src_vf ON vf_src.report_form_id = rf_src_vf.id
		LEFT JOIN files f_vf_src ON rf_src_vf.file_id = f_vf_src.id
		LEFT JOIN query_fragments qf_src ON r.source_type = 'query_fragment' AND r.source_id = qf_src.id
		LEFT JOIN files f_qf_src ON qf_src.file_id = f_qf_src.id
		LEFT JOIN smf_instruments smf_src ON r.source_type = 'smf_instrument' AND r.source_id = smf_src.id
		LEFT JOIN files f_smf_src ON smf_src.file_id = f_smf_src.id
		LEFT JOIN sql_procedures sp_tgt ON r.target_type = 'sql_procedure' AND r.target_id = sp_tgt.id
		LEFT JOIN files f_sp_tgt ON sp_tgt.file_id = f_sp_tgt.id
		LEFT JOIN sql_tables st_tgt ON r.target_type = 'sql_table' AND r.target_id = st_tgt.id
		LEFT JOIN files f_st_tgt ON st_tgt.file_id = f_st_tgt.id
		LEFT JOIN pas_methods pm_tgt ON r.target_type = 'pas_method' AND r.target_id = pm_tgt.id
		LEFT JOIN pas_units pu_tgt ON pm_tgt.unit_id = pu_tgt.id
		LEFT JOIN files f_pm_tgt ON pu_tgt.file_id = f_pm_tgt.id
		LEFT JOIN js_functions jf_tgt ON r.target_type = 'js_function' AND r.target_id = jf_tgt.id
		LEFT JOIN files f_jf_tgt ON jf_tgt.file_id = f_jf_tgt.id
		LEFT JOIN api_contracts ac_tgt ON r.target_type = 'api_contract' AND r.target_id = ac_tgt.id
		LEFT JOIN files f_ac_tgt ON ac_tgt.file_id = f_ac_tgt.id
		LEFT JOIN report_forms rf_tgt ON r.target_type = 'report_form' AND r.target_id = rf_tgt.id
		LEFT JOIN files f_rf_tgt ON rf_tgt.file_id = f_rf_tgt.id
		LEFT JOIN report_fields rfield_tgt ON r.target_type = 'report_field' AND r.target_id = rfield_tgt.id
		LEFT JOIN report_forms rf_tgt_field ON rfield_tgt.report_form_id = rf_tgt_field.id
		LEFT JOIN files f_rfield_tgt ON rf_tgt_field.file_id = f_rfield_tgt.id
		LEFT JOIN report_params rparam_tgt ON r.target_type = 'report_param' AND r.target_id = rparam_tgt.id
		LEFT JOIN report_forms rf_tgt_param ON rparam_tgt.report_form_id = rf_tgt_param.id
		LEFT JOIN files f_rparam_tgt ON rf_tgt_param.file_id = f_rparam_tgt.id
		LEFT JOIN vb_functions vf_tgt ON r.target_type = 'vb_function' AND r.target_id = vf_tgt.id
		LEFT JOIN report_forms rf_tgt_vf ON vf_tgt.report_form_id = rf_tgt_vf.id
		LEFT JOIN files f_vf_tgt ON rf_tgt_vf.file_id = f_vf_tgt.id
		LEFT JOIN query_fragments qf_tgt ON r.target_type = 'query_fragment' AND r.target_id = qf_tgt.id
		LEFT JOIN files f_qf_tgt ON qf_tgt.file_id = f_qf_tgt.id
		LEFT JOIN smf_instruments smf_tgt ON r.target_type = 'smf_instrument' AND r.target_id = smf_tgt.id
		LEFT JOIN files f_smf_tgt ON smf_tgt.file_id = f_smf_tgt.id
	`
}
