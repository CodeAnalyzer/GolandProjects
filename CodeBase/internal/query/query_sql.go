package query

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
)

func (q *Query) SearchTable(name string, likeSearch bool, limit int) ([]TableResult, error) {
	queryText := `
		SELECT 
			MIN(st.id) as table_id,
			st.table_name,
			st.context,
			COUNT(*) as usages
		FROM sql_tables st
		WHERE %s
		GROUP BY st.table_name, st.context
		ORDER BY usages DESC
		LIMIT $2
	`

	searchValue := strings.TrimSpace(name)
	whereClause := "LOWER(st.table_name) = LOWER($1)"
	if likeSearch {
		whereClause = "st.table_name ILIKE $1"
		searchValue = "%" + searchValue + "%"
	}
	queryText = fmt.Sprintf(queryText, whereClause)

	rows, err := q.db.Query(queryText, searchValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TableResult
	for rows.Next() {
		var r TableResult
		if err := rows.Scan(&r.ID, &r.TableName, &r.Context, &r.Usages); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	for i := range results {
		filesQuery := `
			SELECT DISTINCT f.id, f.rel_path
			FROM sql_tables st
			JOIN files f ON st.file_id = f.id
			WHERE st.table_name = $1
			LIMIT 20
		`
		fileRows, err := q.db.Query(filesQuery, results[i].TableName)
		if err == nil {
			for fileRows.Next() {
				var fileID int64
				var file string
				if err := fileRows.Scan(&fileID, &file); err == nil {
					results[i].FileIDs = append(results[i].FileIDs, fileID)
					results[i].Files = append(results[i].Files, file)
				}
			}
			fileRows.Close()
		}

		columnsQuery := `
			SELECT DISTINCT column_name
			FROM sql_columns
			WHERE table_name = $1
			LIMIT 50
		`
		colRows, err := q.db.Query(columnsQuery, results[i].TableName)
		if err == nil {
			for colRows.Next() {
				var col string
				if err := colRows.Scan(&col); err == nil {
					results[i].Columns = append(results[i].Columns, col)
				}
			}
			colRows.Close()
		}

		procsQuery := `
			SELECT DISTINCT sp.proc_name
			FROM relations r
			JOIN sql_procedures sp ON r.source_id = sp.id
			JOIN sql_tables st ON r.target_id = st.id
			WHERE st.table_name = $1
			  AND r.source_type = 'sql_procedure'
			  AND r.target_type = 'sql_table'
			  AND r.relation_type IN ('selects_from', 'inserts_into', 'updates', 'deletes_from', 'references_table')
			LIMIT 20
		`
		procRows, err := q.db.Query(procsQuery, results[i].TableName)
		if err == nil {
			for procRows.Next() {
				var proc string
				if err := procRows.Scan(&proc); err == nil {
					results[i].Procedures = append(results[i].Procedures, proc)
				}
			}
			procRows.Close()
		}
	}

	return results, nil
}

func (q *Query) SearchTableSchema(name string, limit int) ([]TableSchemaColumnResult, error) {
	query := `
		SELECT
			scd.table_name,
			scd.column_name,
			scd.data_type,
			scd.definition_kind,
			f.rel_path,
			scd.file_id,
			scd.line_number,
			scd.column_order
		FROM sql_column_definitions scd
		JOIN files f ON scd.file_id = f.id
		WHERE scd.table_name = $1
		ORDER BY scd.table_name, scd.column_order, scd.line_number
		LIMIT $2
	`

	rows, err := q.db.Query(query, name, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]TableSchemaColumnResult, 0)
	for rows.Next() {
		var item TableSchemaColumnResult
		var file sql.NullString
		if err := rows.Scan(&item.TableName, &item.ColumnName, &item.DataType, &item.DefinitionKind, &file, &item.FileID, &item.LineNumber, &item.ColumnOrder); err != nil {
			return nil, err
		}
		if file.Valid {
			item.File = file.String
		}
		results = append(results, item)
	}

	return results, rows.Err()
}

func (q *Query) SearchSQLTableIndex(name string, likeSearch bool, limit int) ([]SQLTableIndexResult, error) {
	queryText := `
		SELECT i.id, i.table_name, i.index_name, COALESCE(i.index_fields,''), COALESCE(i.index_type,''), i.is_unique,
		       i.definition_kind, COALESCE(f.rel_path,''), i.file_id, i.line_number
		FROM sql_index_definitions i
		JOIN files f ON f.id = i.file_id
		WHERE %s
		ORDER BY i.table_name, i.index_name, i.id DESC
		LIMIT $2
	`

	searchValue := strings.TrimSpace(name)
	whereClause := "LOWER(i.index_name) = LOWER($1) OR LOWER(i.table_name) = LOWER($1)"
	if likeSearch {
		whereClause = "i.index_name ILIKE $1 OR i.table_name ILIKE $1"
		searchValue = "%" + searchValue + "%"
	}
	queryText = fmt.Sprintf(queryText, whereClause)

	rows, err := q.db.Query(queryText, searchValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]SQLTableIndexResult, 0)
	for rows.Next() {
		var item SQLTableIndexResult
		if err := rows.Scan(&item.ID, &item.TableName, &item.IndexName, &item.IndexFields, &item.IndexType, &item.IsUnique, &item.DefinitionKind, &item.File, &item.FileID, &item.LineNumber); err != nil {
			return nil, err
		}
		fieldRows, err := q.db.Query(`SELECT field_name, field_order, line_number FROM sql_index_definition_fields WHERE table_index_id = $1 ORDER BY field_order, id`, item.ID)
		if err != nil {
			return nil, err
		}
		for fieldRows.Next() {
			var field SQLTableIndexFieldResult
			if err := fieldRows.Scan(&field.FieldName, &field.FieldOrder, &field.LineNumber); err != nil {
				fieldRows.Close()
				return nil, err
			}
			item.Fields = append(item.Fields, field)
		}
		fieldRows.Close()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) FindCallers(procedureName string, limit int) ([]CallerResult, error) {
	query := `
		SELECT *
		FROM (
			SELECT
				r.source_id,
				CASE
					WHEN r.source_type = 'sql_procedure' THEN sp.proc_name
					WHEN r.source_type = 'pas_method' THEN pm.method_name
					WHEN r.source_type = 'js_function' THEN jf.function_name
					WHEN r.source_type = 'report_form' THEN rf.report_name
					WHEN r.source_type = 'vb_function' THEN vf.function_name
					WHEN r.source_type = 'query_fragment' THEN COALESCE(NULLIF(qf.component_name, ''), NULLIF(qf.component_type, ''), 'query_fragment')
					ELSE 'unknown'
				 END as caller_name,
				 r.source_type as caller_type,
				 COALESCE(f_sql.rel_path, f_pas.rel_path, f_js.rel_path, f_rf.rel_path, f_vf.rel_path, f_qf.rel_path) as file,
				 r.line_number,
				 CASE
					WHEN r.source_type = 'query_fragment' THEN qf.context
					ELSE NULL
				 END as call_context,
				 FALSE AS is_indirect,
				 '' AS via_procedure,
				 r.relation_type
			FROM relations r
			JOIN sql_procedures sp_target ON r.target_id = sp_target.id
			LEFT JOIN sql_procedures sp ON r.source_type = 'sql_procedure' AND r.source_id = sp.id
			LEFT JOIN pas_methods pm ON r.source_type = 'pas_method' AND r.source_id = pm.id
			LEFT JOIN js_functions jf ON r.source_type = 'js_function' AND r.source_id = jf.id
			LEFT JOIN report_forms rf ON r.source_type = 'report_form' AND r.source_id = rf.id
			LEFT JOIN vb_functions vf ON r.source_type = 'vb_function' AND r.source_id = vf.id
			LEFT JOIN query_fragments qf ON r.source_type = 'query_fragment' AND r.source_id = qf.id
			LEFT JOIN files f_sql ON sp.file_id = f_sql.id
			LEFT JOIN pas_units pu ON pm.unit_id = pu.id
			LEFT JOIN files f_pas ON pu.file_id = f_pas.id
			LEFT JOIN files f_js ON jf.file_id = f_js.id
			LEFT JOIN files f_rf ON rf.file_id = f_rf.id
			LEFT JOIN report_forms rf_vf ON vf.report_form_id = rf_vf.id
			LEFT JOIN files f_vf ON rf_vf.file_id = f_vf.id
			LEFT JOIN files f_qf ON qf.file_id = f_qf.id
			WHERE sp_target.proc_name ILIKE $1
			  AND r.target_type = 'sql_procedure'
			  AND r.relation_type IN ('calls_procedure', 'dispatches_to_subscriber')
			  AND r.source_type IN ('sql_procedure', 'pas_method', 'js_function', 'report_form', 'vb_function', 'query_fragment')

			UNION

			SELECT
				r_up.source_id,
				sp_up.proc_name as caller_name,
				'sql_procedure' as caller_type,
				f_up.rel_path as file,
				r_up.line_number,
				NULL as call_context,
				TRUE AS is_indirect,
				sp_mid.proc_name AS via_procedure,
				CONCAT(r_up.relation_type, '->', r_down.relation_type) AS relation_type
			FROM sql_procedures sp_target
			JOIN relations r_down ON r_down.target_type = 'sql_procedure' AND r_down.target_id = sp_target.id
			JOIN sql_procedures sp_mid ON r_down.source_type = 'sql_procedure' AND r_down.source_id = sp_mid.id
			JOIN relations r_up ON r_up.target_type = 'sql_procedure' AND r_up.target_id = sp_mid.id
			JOIN sql_procedures sp_up ON r_up.source_type = 'sql_procedure' AND r_up.source_id = sp_up.id
			JOIN files f_up ON sp_up.file_id = f_up.id
			WHERE sp_target.proc_name ILIKE $1
			  AND r_down.relation_type IN ('calls_procedure', 'dispatches_to_subscriber')
			  AND r_up.relation_type IN ('dispatches_to', 'dispatches_to_subscriber', 'calls_procedure')
		) callers
		ORDER BY is_indirect, caller_type, caller_name
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+procedureName+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []CallerResult
	for rows.Next() {
		var r CallerResult
		var file sql.NullString
		var context sql.NullString
		if err := rows.Scan(&r.CallerID, &r.CallerName, &r.CallerType, &file, &r.LineNumber, &context, &r.IsIndirect, &r.ViaProcedure, &r.RelationType); err != nil {
			return nil, err
		}
		if file.Valid {
			r.File = file.String
		}
		if context.Valid {
			r.CallContext = context.String
		}
		results = append(results, r)
	}

	return results, nil
}

func (q *Query) FindMethodsByTable(tableName string, limit int) ([]MethodResult, error) {
	query := `
		SELECT 
			pm.id,
			pm.unit_id,
			pm.method_name,
			pc.class_name,
			pu.unit_name,
			f.rel_path,
			pm.line_number,
			st.table_name
		FROM pas_methods pm
		LEFT JOIN pas_classes pc ON pm.class_id = pc.id
		JOIN pas_units pu ON pm.unit_id = pu.id
		JOIN files f ON pu.file_id = f.id
		JOIN relations r_query ON r_query.source_type = 'pas_method' AND r_query.source_id = pm.id
		JOIN query_fragments qf ON r_query.target_type = 'query_fragment' AND r_query.target_id = qf.id
		JOIN relations r_table ON r_table.source_type = 'query_fragment' AND r_table.source_id = qf.id
		JOIN sql_tables st ON r_table.target_type = 'sql_table' AND r_table.target_id = st.id
		WHERE LOWER(st.table_name) = LOWER($1)
		  AND r_query.relation_type IN ('executes_query', 'builds_query')
		  AND r_table.relation_type = 'references_table'
		ORDER BY pu.unit_name, pm.method_name, pm.line_number
		LIMIT $2
	`

	rows, err := q.db.Query(query, tableName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []MethodResult
	for rows.Next() {
		var r MethodResult
		if err := rows.Scan(&r.ID, &r.UnitID, &r.MethodName, &r.ClassName, &r.UnitName, &r.File, &r.LineNumber, &r.TableUsage); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, nil
}

type SQLParamResult struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Direction string `json:"direction"`
}

type SQLProcedureResult struct {
	ID        int64            `json:"id"`
	FileID    int64            `json:"file_id"`
	ProcName  string           `json:"proc_name"`
	Params    []SQLParamResult `json:"params"`
	File      string           `json:"file,omitempty"`
	LineStart int              `json:"line_start"`
	LineEnd   int              `json:"line_end"`
	BodyHash  string           `json:"body_hash,omitempty"`
}

func (q *Query) GetProcedureDetails(name string) (*model.SQLProcedure, error) {
	query := `
		SELECT 
			sp.id, sp.file_id, sp.proc_name,
			sp.parameters, sp.line_start, sp.line_end, sp.body_hash
		FROM sql_procedures sp
		WHERE sp.proc_name = $1
	`

	row := q.db.QueryRow(query, name)

	var proc model.SQLProcedure
	var paramsJSON sql.NullString

	err := row.Scan(&proc.ID, &proc.FileID, &proc.ProcName,
		&paramsJSON, &proc.LineStart, &proc.LineEnd, &proc.BodyHash)
	if err != nil {
		return nil, err
	}

	if paramsJSON.Valid {
		if err := json.Unmarshal([]byte(paramsJSON.String), &proc.Params); err != nil {
			proc.Params = make([]model.SQLParam, 0)
		}
	}

	return &proc, nil
}

func (q *Query) GetProcedureResult(name string) (*SQLProcedureResult, error) {
	query := `
		SELECT 
			sp.id, sp.file_id, sp.proc_name,
			sp.parameters, f.rel_path, sp.line_start, sp.line_end, sp.body_hash
		FROM sql_procedures sp
		LEFT JOIN files f ON sp.file_id = f.id
		WHERE sp.proc_name = $1
	`

	row := q.db.QueryRow(query, name)

	var proc SQLProcedureResult
	var paramsJSON sql.NullString
	var file sql.NullString
	var bodyHash sql.NullString

	err := row.Scan(&proc.ID, &proc.FileID, &proc.ProcName,
		&paramsJSON, &file, &proc.LineStart, &proc.LineEnd, &bodyHash)
	if err != nil {
		return nil, err
	}

	proc.Params = make([]SQLParamResult, 0)
	if paramsJSON.Valid {
		var params []model.SQLParam
		if err := json.Unmarshal([]byte(paramsJSON.String), &params); err == nil {
			proc.Params = make([]SQLParamResult, 0, len(params))
			for _, param := range params {
				proc.Params = append(proc.Params, SQLParamResult{
					Name:      param.Name,
					Type:      param.Type,
					Direction: param.Direction,
				})
			}
		}
	}
	if file.Valid {
		proc.File = file.String
	}
	if bodyHash.Valid {
		proc.BodyHash = bodyHash.String
	}

	return &proc, nil
}
