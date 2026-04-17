package query

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
)

// Query API для запросов к индексу
type Query struct {
	db *store.DB
}

// SymbolResult результат поиска сущности
type SymbolResult struct {
	ID         int64  `json:"id"`
	FileID     int64  `json:"file_id"`
	EntityID   int64  `json:"entity_id,omitempty"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	EntityType string `json:"entity_type"`
	File       string `json:"file"`
	LineNumber int    `json:"line_number"`
	Signature  string `json:"signature,omitempty"`
}

// TableResult результат поиска таблицы
type TableResult struct {
	ID         int64    `json:"id"`
	FileIDs    []int64  `json:"file_ids,omitempty"`
	TableName  string   `json:"table_name"`
	Context    string   `json:"context"`
	Files      []string `json:"files"`
	Usages     int      `json:"usages"`
	Columns    []string `json:"columns"`
	Procedures []string `json:"procedures"`
}

// TableSchemaColumnResult результат поиска определений колонок таблицы
type TableSchemaColumnResult struct {
	TableName   string `json:"table_name"`
	ColumnName  string `json:"column_name"`
	DataType    string `json:"data_type"`
	DefinitionKind string `json:"definition_kind,omitempty"`
	File        string `json:"file,omitempty"`
	FileID      int64  `json:"file_id"`
	LineNumber  int    `json:"line_number"`
	ColumnOrder int    `json:"column_order"`
}

// SQLTableIndexFieldResult результат поиска определений полей индекса таблицы
type SQLTableIndexFieldResult struct {
	FieldName  string `json:"field_name"`
	FieldOrder int    `json:"field_order,omitempty"`
	LineNumber int    `json:"line_number,omitempty"`
}

// SQLTableIndexResult результат поиска определений индексов таблицы
type SQLTableIndexResult struct {
	ID             int64                      `json:"id"`
	TableName      string                     `json:"table_name"`
	IndexName      string                     `json:"index_name"`
	IndexFields    string                     `json:"index_fields,omitempty"`
	IndexType      string                     `json:"index_type,omitempty"`
	IsUnique       bool                       `json:"is_unique,omitempty"`
	DefinitionKind string                     `json:"definition_kind,omitempty"`
	File           string                     `json:"file,omitempty"`
	FileID         int64                      `json:"file_id,omitempty"`
	LineNumber     int                        `json:"line_number,omitempty"`
	Fields         []SQLTableIndexFieldResult `json:"fields,omitempty"`
}

// CallerResult результат поиска вызовов
type CallerResult struct {
	CallerID    int64  `json:"caller_id,omitempty"`
	CallerName  string `json:"caller_name"`
	CallerType  string `json:"caller_type"`
	File        string `json:"file"`
	LineNumber  int    `json:"line_number"`
	CallContext string `json:"call_context,omitempty"`
}

type RelationEntityRef struct {
	ID         int64  `json:"id"`
	Type       string `json:"type"`
	Name       string `json:"name"`
	File       string `json:"file,omitempty"`
	FileID     int64  `json:"file_id,omitempty"`
	LineNumber int    `json:"line_number,omitempty"`
}

type RelationResult struct {
	ID           int64             `json:"id"`
	RelationType string            `json:"relation_type"`
	Confidence   string            `json:"confidence,omitempty"`
	LineNumber   int               `json:"line_number,omitempty"`
	Source       RelationEntityRef `json:"source"`
	Target       RelationEntityRef `json:"target"`
}

type DFMFormResult struct {
	ID        int64  `json:"id"`
	FileID    int64  `json:"file_id"`
	FormName  string `json:"form_name"`
	FormClass string `json:"form_class"`
	Caption   string `json:"caption,omitempty"`
	File      string `json:"file"`
	LineStart int    `json:"line_start"`
	LineEnd   int    `json:"line_end"`
}

type DFMComponentResult struct {
	ID            int64  `json:"id"`
	FileID        int64  `json:"file_id"`
	FormID        int64  `json:"form_id"`
	FormName      string `json:"form_name"`
	FormClass     string `json:"form_class"`
	ComponentName string `json:"component_name"`
	ComponentType string `json:"component_type"`
	ParentName    string `json:"parent_name,omitempty"`
	Caption       string `json:"caption,omitempty"`
	PASFieldName  string `json:"pas_field_name,omitempty"`
	PASFieldType  string `json:"pas_field_type,omitempty"`
	File          string `json:"file"`
	LineStart     int    `json:"line_start"`
	LineEnd       int    `json:"line_end"`
}

type QueryFragmentResult struct {
	ID               int64    `json:"id"`
	FileID           int64    `json:"file_id"`
	ParentType       string   `json:"parent_type"`
	ParentID         int64    `json:"parent_id"`
	ComponentName    string   `json:"component_name"`
	ComponentType    string   `json:"component_type"`
	QueryText        string   `json:"query_text"`
	TablesReferenced []string `json:"tables_referenced,omitempty"`
	Context          string   `json:"context"`
	File             string   `json:"file"`
	LineNumber       int      `json:"line_number"`
}

// ReportFormResult результат поиска report form
type ReportFormResult struct {
	ID         int64  `json:"id"`
	FileID     int64  `json:"file_id"`
	ReportName string `json:"report_name"`
	ReportType string `json:"report_type"`
	FormName   string `json:"form_name,omitempty"`
	FormClass  string `json:"form_class,omitempty"`
	File       string `json:"file"`
	LineStart  int    `json:"line_start"`
	LineEnd    int    `json:"line_end"`
}

// ReportFieldResult результат поиска report field
type ReportFieldResult struct {
	ID           int64    `json:"id"`
	ReportFormID int64    `json:"report_form_id"`
	FieldName    string   `json:"field_name"`
	SourceName   string   `json:"source_name,omitempty"`
	FormatMask   string   `json:"format_mask,omitempty"`
	Options      []string `json:"options,omitempty"`
	ReportName   string   `json:"report_name"`
	ReportType   string   `json:"report_type"`
	File         string   `json:"file"`
	LineNumber   int      `json:"line_number"`
	RawText      string   `json:"raw_text,omitempty"`
}

// ReportParamResult результат поиска report param
type ReportParamResult struct {
	ID            int64  `json:"id"`
	ReportFormID  int64  `json:"report_form_id"`
	ParamName     string `json:"param_name"`
	ParamKind     string `json:"param_kind"`
	ComponentType string `json:"component_type,omitempty"`
	DataType      string `json:"data_type,omitempty"`
	LookupForm    string `json:"lookup_form,omitempty"`
	LookupTable   string `json:"lookup_table,omitempty"`
	LookupColumn  string `json:"lookup_column,omitempty"`
	KeyColumn     string `json:"key_column,omitempty"`
	DefaultValue  string `json:"default_value,omitempty"`
	Required      bool   `json:"required"`
	ReportName    string `json:"report_name"`
	ReportType    string `json:"report_type"`
	File          string `json:"file"`
	LineNumber    int    `json:"line_number"`
	RawText       string `json:"raw_text,omitempty"`
}

// VBFunctionResult результат поиска VB функции
type VBFunctionResult struct {
	ID           int64  `json:"id"`
	ReportFormID int64  `json:"report_form_id"`
	FunctionName string `json:"function_name"`
	FunctionType string `json:"function_type"`
	Signature    string `json:"signature,omitempty"`
	ReportName   string `json:"report_name"`
	File         string `json:"file"`
	LineStart    int    `json:"line_start"`
	LineEnd      int    `json:"line_end"`
}

// MethodResult результат поиска методов
type MethodResult struct {
	ID         int64  `json:"id"`
	UnitID     int64  `json:"unit_id"`
	MethodName string `json:"method_name"`
	ClassName  string `json:"class_name"`
	UnitName   string `json:"unit_name"`
	File       string `json:"file"`
	LineNumber int    `json:"line_number"`
	TableUsage string `json:"table_usage"`
}

// New создаёт новый Query API
func New(db *store.DB) *Query {
	return &Query{db: db}
}

// SearchSymbol ищет сущность по имени
func (q *Query) SearchSymbol(name string, symbolType string, limit int) ([]SymbolResult, error) {
	// symbols — это unified index (унифицированный индекс), поэтому этот метод
	// является самым общим способом найти сущность без знания конкретной таблицы-хранилища.
	query := `
		SELECT 
			s.id,
			s.file_id,
			s.entity_id,
			s.symbol_name,
			s.symbol_type,
			s.entity_type,
			f.rel_path,
			s.line_number,
			s.signature
		FROM symbols s
		JOIN files f ON s.file_id = f.id
		WHERE s.symbol_name ILIKE $1
	`
	args := []interface{}{"%" + name + "%"}

	if symbolType != "" {
		// Фильтр по типу добавляется динамически, чтобы не плодить отдельные SQL-шаблоны.
		query += " AND s.symbol_type = $2"
		args = append(args, symbolType)
	}

	query += fmt.Sprintf(" ORDER BY s.symbol_name LIMIT %d", limit)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SymbolResult
	for rows.Next() {
		var r SymbolResult
		var signature sql.NullString
		// signature nullable, потому что не у всех сущностей есть полная сигнатура.
		if err := rows.Scan(&r.ID, &r.FileID, &r.EntityID, &r.Name, &r.Type, &r.EntityType, &r.File, &r.LineNumber, &signature); err != nil {
			return nil, err
		}
		if signature.Valid {
			r.Signature = signature.String
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchRelationsByEntity ищет связи по точному entity id/type, что критично для inspect.
func (q *Query) SearchRelationsByEntity(sourceType string, sourceID int64, targetType string, targetID int64, relationType string, limit int) ([]RelationResult, error) {
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
		return nil, fmt.Errorf("at least one relation filter must be provided")
	}

	queryText := `
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

	queryText += " WHERE " + strings.Join(conditions, " AND ")
	queryText += fmt.Sprintf(" ORDER BY r.id DESC LIMIT $%d", argPos)
	args = append(args, limit)

	rows, err := q.db.Query(queryText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]RelationResult, 0)
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

// SearchTable ищет информацию о таблице
func (q *Query) SearchTable(name string, limit int) ([]TableResult, error) {
	// На первом проходе получаем только агрегированную "шапку" по таблицам,
	// а затем отдельными лёгкими запросами достраиваем детали по каждой записи.
	query := `
		SELECT 
			MIN(st.id) as table_id,
			st.table_name,
			st.context,
			COUNT(*) as usages
		FROM sql_tables st
		WHERE st.table_name ILIKE $1
		GROUP BY st.table_name, st.context
		ORDER BY usages DESC
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+name+"%", limit)
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

	// Второй этап intentionally split (намеренно разделён): так проще поддерживать
	// независимые источники данных — files, columns и relations.
	for i := range results {
		// Файлы, где используется таблица
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

		// Поля таблицы
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

		// Нормализованные relation types для procedure -> table уже сохранены в relations,
		// поэтому query не должен повторно выводить использование таблиц из текста SQL.
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

// SearchTableSchema ищет определения колонок таблицы из CREATE TABLE и schema patches.
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

// SearchSQLTableIndex ищет определения индексов обычных SQL-таблиц.
func (q *Query) SearchSQLTableIndex(name string, limit int) ([]SQLTableIndexResult, error) {
	rows, err := q.db.Query(`
		SELECT i.id, i.table_name, i.index_name, COALESCE(i.index_fields,''), COALESCE(i.index_type,''), i.is_unique,
		       i.definition_kind, COALESCE(f.rel_path,''), i.file_id, i.line_number
		FROM sql_index_definitions i
		JOIN files f ON f.id = i.file_id
		WHERE i.index_name ILIKE $1 OR i.table_name ILIKE $1
		ORDER BY i.table_name, i.index_name, i.id DESC
		LIMIT $2
	`, "%"+name+"%", limit)
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

// SearchRelations поиск связей между сущностями
func (q *Query) SearchRelations(sourceType string, sourceName string, targetType string, targetName string, relationType string, limit int) ([]RelationResult, error) {
	conditions := make([]string, 0, 5)
	args := make([]interface{}, 0, 6)
	argPos := 1

	if sourceType != "" {
		conditions = append(conditions, fmt.Sprintf("r.source_type = $%d", argPos))
		args = append(args, sourceType)
		argPos++
	}
	if targetType != "" {
		conditions = append(conditions, fmt.Sprintf("r.target_type = $%d", argPos))
		args = append(args, targetType)
		argPos++
	}
	if relationType != "" {
		conditions = append(conditions, fmt.Sprintf("r.relation_type = $%d", argPos))
		args = append(args, relationType)
		argPos++
	}
	if sourceName != "" {
		conditions = append(conditions, fmt.Sprintf(`(
			CASE
				WHEN r.source_type = 'sql_procedure' THEN sp_src.proc_name
				WHEN r.source_type = 'sql_table' THEN st_src.table_name
				WHEN r.source_type = 'pas_method' THEN pm_src.method_name
				WHEN r.source_type = 'js_function' THEN jf_src.function_name
				WHEN r.source_type = 'api_contract' THEN ac_src.contract_name
				WHEN r.source_type = 'report_form' THEN rf_src.report_name
				WHEN r.source_type = 'report_field' THEN rfield_src.field_name
				WHEN r.source_type = 'report_param' THEN rparam_src.param_name
				WHEN r.source_type = 'vb_function' THEN vf_src.function_name
				WHEN r.source_type = 'query_fragment' THEN qf_src.component_name
				WHEN r.source_type = 'smf_instrument' THEN smf_src.instrument_name
				ELSE NULL
			END ILIKE $%d)`, argPos))
		args = append(args, "%"+sourceName+"%")
		argPos++
	}
	if targetName != "" {
		conditions = append(conditions, fmt.Sprintf(`(
			CASE
				WHEN r.target_type = 'sql_procedure' THEN sp_tgt.proc_name
				WHEN r.target_type = 'sql_table' THEN st_tgt.table_name
				WHEN r.target_type = 'pas_method' THEN pm_tgt.method_name
				WHEN r.target_type = 'js_function' THEN jf_tgt.function_name
				WHEN r.target_type = 'api_contract' THEN ac_tgt.contract_name
				WHEN r.target_type = 'report_form' THEN rf_tgt.report_name
				WHEN r.target_type = 'report_field' THEN rfield_tgt.field_name
				WHEN r.target_type = 'report_param' THEN rparam_tgt.param_name
				WHEN r.target_type = 'vb_function' THEN vf_tgt.function_name
				WHEN r.target_type = 'query_fragment' THEN qf_tgt.component_name
				WHEN r.target_type = 'smf_instrument' THEN smf_tgt.instrument_name
				ELSE NULL
			END ILIKE $%d)`, argPos))
		args = append(args, "%"+targetName+"%")
		argPos++
	}

	if len(conditions) == 0 {
		return nil, fmt.Errorf("at least one relation filter must be provided")
	}

	queryText := `
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

	queryText += " WHERE " + strings.Join(conditions, " AND ")
	queryText += fmt.Sprintf(" ORDER BY r.id DESC LIMIT $%d", argPos)
	args = append(args, limit)

	rows, err := q.db.Query(queryText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]RelationResult, 0)
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

// FindCallers ищет вызовы процедуры
func (q *Query) FindCallers(procedureName string, limit int) ([]CallerResult, error) {
	// Источник файла вычисляется через COALESCE, потому что caller потенциально может
	// происходить из разных типов сущностей, каждая из которых хранит file linkage по-своему.
	// Поиск идёт через цепочку parent -> query_fragment -> sql_procedure.
	query := `
		SELECT 
			r_parent.source_id,
			CASE 
				WHEN r_parent.source_type = 'sql_procedure' THEN sp.proc_name
				WHEN r_parent.source_type = 'pas_method' THEN pm.method_name
				WHEN r_parent.source_type = 'js_function' THEN jf.function_name
				WHEN r_parent.source_type = 'report_form' THEN rf.report_name
				WHEN r_parent.source_type = 'vb_function' THEN vf.function_name
				ELSE 'unknown'
			 END as caller_name,
			 r_parent.source_type as caller_type,
			 COALESCE(f_sql.rel_path, f_pas.rel_path, f_js.rel_path, f_rf.rel_path, f_vf.rel_path) as file,
			 r_parent.line_number,
			 NULL as call_context
		FROM relations r_fragment
		JOIN query_fragments qf ON r_fragment.source_id = qf.id
		JOIN sql_procedures sp_target ON r_fragment.target_id = sp_target.id
		JOIN relations r_parent ON r_parent.target_type = 'query_fragment' AND r_parent.target_id = qf.id
		LEFT JOIN sql_procedures sp ON r_parent.source_type = 'sql_procedure' AND r_parent.source_id = sp.id
		LEFT JOIN pas_methods pm ON r_parent.source_type = 'pas_method' AND r_parent.source_id = pm.id
		LEFT JOIN js_functions jf ON r_parent.source_type = 'js_function' AND r_parent.source_id = jf.id
		LEFT JOIN report_forms rf ON r_parent.source_type = 'report_form' AND r_parent.source_id = rf.id
		LEFT JOIN vb_functions vf ON r_parent.source_type = 'vb_function' AND r_parent.source_id = vf.id
		LEFT JOIN files f_sql ON sp.file_id = f_sql.id
		LEFT JOIN pas_units pu ON pm.unit_id = pu.id
		LEFT JOIN files f_pas ON pu.file_id = f_pas.id
		LEFT JOIN files f_js ON jf.file_id = f_js.id
		LEFT JOIN files f_rf ON rf.file_id = f_rf.id
		LEFT JOIN report_forms rf_vf ON vf.report_form_id = rf_vf.id
		LEFT JOIN files f_vf ON rf_vf.file_id = f_vf.id
		WHERE sp_target.proc_name ILIKE $1
		  AND r_fragment.source_type = 'query_fragment'
		  AND r_fragment.target_type = 'sql_procedure'
		  AND r_fragment.relation_type = 'calls_procedure'
		  AND r_parent.relation_type IN ('executes_query', 'builds_query')
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
		// Nullable-поля читаются отдельно, чтобы не терять строки без file/context.
		if err := rows.Scan(&r.CallerID, &r.CallerName, &r.CallerType, &file, &r.LineNumber, &context); err != nil {
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

// FindMethodsByTable ищет методы, работающие с таблицей
func (q *Query) FindMethodsByTable(tableName string, limit int) ([]MethodResult, error) {
	// Запрос использует текущую relation graph модель:
	// pas_method -> query_fragment -> sql_table.
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

// GetProcedureDetails получает детальную информацию о процедуре
func (q *Query) GetProcedureDetails(name string) (*model.SQLProcedure, error) {
	// Детали процедуры берутся из canonical table (канонической таблицы) sql_procedures,
	// а параметры десериализуются из JSONB только на уровне Go-модели.
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
		// Некорректный JSON не валит весь запрос: возвращаем хотя бы базовые поля процедуры.
		if err := json.Unmarshal([]byte(paramsJSON.String), &proc.Params); err != nil {
			proc.Params = make([]model.SQLParam, 0)
		}
	}

	return &proc, nil
}

// GetProcedureResult получает детальную информацию о процедуре в CLI/API-friendly формате.
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

// GetFileByPath получает файл по пути
func (q *Query) GetFileByPath(path string) (*model.File, error) {
	// Поддерживаем поиск и по абсолютному path, и по rel_path,
	// чтобы API было удобно использовать и из CLI, и из внешних интеграций.
	query := `
		SELECT id, scan_run_id, path, rel_path, extension, size_bytes, 
		       hash_sha256, modified_at, encoding, language, created_at, updated_at
		FROM files
		WHERE path = $1 OR rel_path = $1
	`

	row := q.db.QueryRow(query, path)

	var f model.File
	err := row.Scan(&f.ID, &f.ScanRunID, &f.Path, &f.RelPath, &f.Extension, &f.SizeBytes,
		&f.HashSHA256, &f.ModifiedAt, &f.Encoding, &f.Language, &f.CreatedAt, &f.UpdatedAt)
	if err != nil {
		return nil, err
	}

	return &f, nil
}

// SearchInContent ищет текст в содержимом файлов
func (q *Query) SearchInContent(pattern string, limit int) ([]SymbolResult, error) {
	// Сейчас это не full-text search по сырому содержимому файлов,
	// а поиск по уже извлечённым symbol names/signatures в unified index.
	query := `
		SELECT 
			s.id,
			s.file_id,
			s.symbol_name,
			s.symbol_type,
			s.entity_type,
			f.rel_path,
			s.line_number,
			s.signature
		FROM symbols s
		JOIN files f ON s.file_id = f.id
		WHERE s.symbol_name ILIKE $1
		   OR s.signature ILIKE $1
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+pattern+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SymbolResult
	for rows.Next() {
		var r SymbolResult
		var signature sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.Name, &r.Type, &r.EntityType, &r.File, &r.LineNumber, &signature); err != nil {
			return nil, err
		}
		if signature.Valid {
			r.Signature = signature.String
		}
		results = append(results, r)
	}

	return results, nil
}

// SearchReportForm поиск report forms
func (q *Query) SearchReportForm(name string, limit int) ([]ReportFormResult, error) {
	query := `
		SELECT
			rf.id,
			rf.file_id,
			rf.report_name,
			rf.report_type,
			rf.form_name,
			rf.form_class,
			f.rel_path,
			rf.line_start,
			rf.line_end
		FROM report_forms rf
		JOIN files f ON rf.file_id = f.id
		WHERE rf.report_name ILIKE $1
		   OR rf.form_name ILIKE $1
		ORDER BY rf.report_name, rf.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]ReportFormResult, 0)
	for rows.Next() {
		var r ReportFormResult
		var formName, formClass sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.ReportName, &r.ReportType, &formName, &formClass, &r.File, &r.LineStart, &r.LineEnd); err != nil {
			return nil, err
		}
		if formName.Valid {
			r.FormName = formName.String
		}
		if formClass.Valid {
			r.FormClass = formClass.String
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

func (q *Query) SearchDFMForm(name string, limit int) ([]DFMFormResult, error) {
	query := `
		SELECT
			df.id,
			df.file_id,
			df.form_name,
			df.form_class,
			df.caption,
			f.rel_path,
			df.line_start,
			df.line_end
		FROM dfm_forms df
		JOIN files f ON df.file_id = f.id
		WHERE df.form_name ILIKE $1
		   OR df.form_class ILIKE $1
		   OR df.caption ILIKE $1
		ORDER BY df.form_name, df.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]DFMFormResult, 0)
	for rows.Next() {
		var r DFMFormResult
		var caption sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.FormName, &r.FormClass, &caption, &r.File, &r.LineStart, &r.LineEnd); err != nil {
			return nil, err
		}
		if caption.Valid {
			r.Caption = caption.String
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

func (q *Query) SearchDFMComponent(name string, limit int) ([]DFMComponentResult, error) {
	query := `
		SELECT
			dc.id,
			dc.file_id,
			dc.form_id,
			df.form_name,
			df.form_class,
			dc.component_name,
			dc.component_type,
			dc.parent_name,
			dc.caption,
			pf.field_name,
			pf.field_type,
			f.rel_path,
			dc.line_start,
			dc.line_end
		FROM dfm_components dc
		JOIN dfm_forms df ON dc.form_id = df.id
		JOIN files f ON dc.file_id = f.id
		LEFT JOIN pas_fields pf ON pf.dfm_component_id = dc.id
		WHERE dc.component_name ILIKE $1
		   OR dc.component_type ILIKE $1
		   OR dc.caption ILIKE $1
		   OR df.form_name ILIKE $1
		   OR df.form_class ILIKE $1
		ORDER BY df.form_name, dc.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]DFMComponentResult, 0)
	for rows.Next() {
		var r DFMComponentResult
		var parentName, caption, pasFieldName, pasFieldType sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.FormID, &r.FormName, &r.FormClass, &r.ComponentName, &r.ComponentType, &parentName, &caption, &pasFieldName, &pasFieldType, &r.File, &r.LineStart, &r.LineEnd); err != nil {
			return nil, err
		}
		if parentName.Valid {
			r.ParentName = parentName.String
		}
		if caption.Valid {
			r.Caption = caption.String
		}
		if pasFieldName.Valid {
			r.PASFieldName = pasFieldName.String
		}
		if pasFieldType.Valid {
			r.PASFieldType = pasFieldType.String
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

func (q *Query) SearchQueryFragment(text string, limit int) ([]QueryFragmentResult, error) {
	query := `
		SELECT
			qf.id,
			qf.file_id,
			qf.parent_type,
			qf.parent_id,
			qf.component_name,
			qf.component_type,
			qf.query_text,
			qf.tables_referenced,
			qf.context,
			f.rel_path,
			qf.line_number
		FROM query_fragments qf
		JOIN files f ON qf.file_id = f.id
		WHERE qf.query_text ILIKE $1
		ORDER BY qf.file_id, qf.line_number
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+text+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]QueryFragmentResult, 0)
	for rows.Next() {
		var r QueryFragmentResult
		var tablesJSON []byte
		if err := rows.Scan(&r.ID, &r.FileID, &r.ParentType, &r.ParentID, &r.ComponentName, &r.ComponentType, &r.QueryText, &tablesJSON, &r.Context, &r.File, &r.LineNumber); err != nil {
			return nil, err
		}
		if len(tablesJSON) > 0 {
			_ = json.Unmarshal(tablesJSON, &r.TablesReferenced)
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchReportField поиск report fields
func (q *Query) SearchReportField(name string, limit int) ([]ReportFieldResult, error) {
	query := `
		SELECT
			rfld.id,
			rfld.report_form_id,
			rfld.field_name,
			rfld.source_name,
			rfld.format_mask,
			rfld.options,
			rf.report_name,
			rf.report_type,
			f.rel_path,
			rfld.line_number,
			rfld.raw_text
		FROM report_fields rfld
		JOIN report_forms rf ON rfld.report_form_id = rf.id
		JOIN files f ON rf.file_id = f.id
		WHERE rfld.field_name ILIKE $1
		   OR rfld.source_name ILIKE $1
		   OR rf.report_name ILIKE $1
		   OR COALESCE(rfld.raw_text, '') ILIKE $1
		ORDER BY rf.report_name, rfld.line_number
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]ReportFieldResult, 0)
	for rows.Next() {
		var r ReportFieldResult
		var sourceName, formatMask, rawText sql.NullString
		var optionsJSON []byte
		if err := rows.Scan(&r.ID, &r.ReportFormID, &r.FieldName, &sourceName, &formatMask, &optionsJSON, &r.ReportName, &r.ReportType, &r.File, &r.LineNumber, &rawText); err != nil {
			return nil, err
		}
		if sourceName.Valid {
			r.SourceName = sourceName.String
		}
		if formatMask.Valid {
			r.FormatMask = formatMask.String
		}
		if rawText.Valid {
			r.RawText = rawText.String
		}
		if len(optionsJSON) > 0 {
			_ = json.Unmarshal(optionsJSON, &r.Options)
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchReportParam поиск report params
func (q *Query) SearchReportParam(name string, limit int) ([]ReportParamResult, error) {
	query := `
		SELECT
			rp.id,
			rp.report_form_id,
			rp.param_name,
			rp.param_kind,
			rp.component_type,
			rp.data_type,
			rp.lookup_form,
			rp.lookup_table,
			rp.lookup_column,
			rp.key_column,
			rp.default_value,
			rp.required,
			rf.report_name,
			rf.report_type,
			f.rel_path,
			rp.line_number,
			rp.raw_text
		FROM report_params rp
		JOIN report_forms rf ON rp.report_form_id = rf.id
		JOIN files f ON rf.file_id = f.id
		WHERE rp.param_name ILIKE $1
		   OR COALESCE(rp.lookup_table, '') ILIKE $1
		   OR COALESCE(rp.lookup_column, '') ILIKE $1
		   OR COALESCE(rp.raw_text, '') ILIKE $1
		ORDER BY rf.report_name, rp.line_number
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]ReportParamResult, 0)
	for rows.Next() {
		var r ReportParamResult
		var componentType, dataType, lookupForm, lookupTable, lookupColumn, keyColumn, defaultValue, rawText sql.NullString
		if err := rows.Scan(&r.ID, &r.ReportFormID, &r.ParamName, &r.ParamKind, &componentType, &dataType, &lookupForm, &lookupTable, &lookupColumn, &keyColumn, &defaultValue, &r.Required, &r.ReportName, &r.ReportType, &r.File, &r.LineNumber, &rawText); err != nil {
			return nil, err
		}
		if componentType.Valid {
			r.ComponentType = componentType.String
		}
		if dataType.Valid {
			r.DataType = dataType.String
		}
		if lookupForm.Valid {
			r.LookupForm = lookupForm.String
		}
		if lookupTable.Valid {
			r.LookupTable = lookupTable.String
		}
		if lookupColumn.Valid {
			r.LookupColumn = lookupColumn.String
		}
		if keyColumn.Valid {
			r.KeyColumn = keyColumn.String
		}
		if defaultValue.Valid {
			r.DefaultValue = defaultValue.String
		}
		if rawText.Valid {
			r.RawText = rawText.String
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchVBFunction поиск VB функций
func (q *Query) SearchVBFunction(name string, limit int) ([]VBFunctionResult, error) {
	query := `
		SELECT
			vf.id,
			vf.report_form_id,
			vf.function_name,
			vf.function_type,
			vf.signature,
			rf.report_name,
			f.rel_path,
			vf.line_start,
			vf.line_end
		FROM vb_functions vf
		JOIN report_forms rf ON vf.report_form_id = rf.id
		JOIN files f ON rf.file_id = f.id
		WHERE vf.function_name ILIKE $1
		ORDER BY vf.function_name, vf.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]VBFunctionResult, 0)
	for rows.Next() {
		var r VBFunctionResult
		var signature sql.NullString
		if err := rows.Scan(&r.ID, &r.ReportFormID, &r.FunctionName, &r.FunctionType, &signature, &r.ReportName, &r.File, &r.LineStart, &r.LineEnd); err != nil {
			return nil, err
		}
		if signature.Valid {
			r.Signature = signature.String
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// JSFunctionResult результат поиска JS-функции
type JSFunctionResult struct {
	ID           int64  `json:"id"`
	FileID       int64  `json:"file_id"`
	FunctionName string `json:"function_name"`
	File         string `json:"file"`
	LineNumber   int    `json:"line_number"`
	LineEnd      int    `json:"line_end"`
	Signature    string `json:"signature,omitempty"`
	ScenarioType string `json:"scenario_type,omitempty"`
}

// SearchJSFunction поиск JS-функции
func (q *Query) SearchJSFunction(name string, limit int) ([]JSFunctionResult, error) {
	query := `
		SELECT
			j.id,
			j.file_id,
			j.function_name,
			f.rel_path,
			j.line_start,
			j.line_end,
			j.signature,
			j.scenario_type
		FROM js_functions j
		JOIN files f ON j.file_id = f.id
		WHERE j.function_name ILIKE $1
		ORDER BY j.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []JSFunctionResult
	for rows.Next() {
		var r JSFunctionResult
		var signature sql.NullString
		var scenarioType sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.FunctionName, &r.File, &r.LineNumber, &r.LineEnd, &signature, &scenarioType); err != nil {
			return nil, err
		}
		if signature.Valid {
			r.Signature = signature.String
		}
		if scenarioType.Valid {
			r.ScenarioType = scenarioType.String
		}
		results = append(results, r)
	}

	return results, nil
}

// SMFInstrumentResult результат поиска SMF инструмента
type SMFInstrumentResult struct {
	ID             int64                    `json:"id"`
	FileID         int64                    `json:"file_id"`
	InstrumentName string                   `json:"instrument_name"`
	Brief          string                   `json:"brief,omitempty"`
	File           string                   `json:"file"`
	DealObjectID   int64                    `json:"deal_object_id,omitempty"`
	DsModuleID     int64                    `json:"ds_module_id,omitempty"`
	StartState     string                   `json:"start_state,omitempty"`
	ScenarioType   string                   `json:"scenario_type"`
	States         []map[string]interface{} `json:"states,omitempty"`
	Actions        []map[string]interface{} `json:"actions,omitempty"`
	Accounts       []map[string]interface{} `json:"accounts,omitempty"`
}

// SearchSMFInstrument поиск SMF инструмента
func (q *Query) SearchSMFInstrument(name string, limit int) ([]SMFInstrumentResult, error) {
	query := `
		SELECT
			s.id,
			s.file_id,
			s.instrument_name,
			s.brief,
			f.rel_path,
			s.deal_object_id,
			s.ds_module_id,
			s.start_state,
			s.scenario_type,
			s.states,
			s.actions,
			s.accounts
		FROM smf_instruments s
		JOIN files f ON s.file_id = f.id
		WHERE s.instrument_name ILIKE $1
		ORDER BY s.instrument_name
		LIMIT $2
	`

	rows, err := q.db.Query(query, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SMFInstrumentResult
	for rows.Next() {
		var r SMFInstrumentResult
		var brief, startState sql.NullString
		var dealObjectID, dsModuleID sql.NullInt64
		var statesJSON, actionsJSON, accountsJSON []byte

		if err := rows.Scan(&r.ID, &r.FileID, &r.InstrumentName, &brief, &r.File, &dealObjectID, &dsModuleID, &startState, &r.ScenarioType, &statesJSON, &actionsJSON, &accountsJSON); err != nil {
			return nil, err
		}

		if brief.Valid {
			r.Brief = brief.String
		}
		if startState.Valid {
			r.StartState = startState.String
		}
		if dealObjectID.Valid {
			r.DealObjectID = dealObjectID.Int64
		}
		if dsModuleID.Valid {
			r.DsModuleID = dsModuleID.Int64
		}

		// Парсим JSONB
		if len(statesJSON) > 0 {
			if err := json.Unmarshal(statesJSON, &r.States); err != nil {
				return nil, fmt.Errorf("failed to unmarshal SMF states JSON for instrument %s: %w", r.InstrumentName, err)
			}
		}
		if len(actionsJSON) > 0 {
			if err := json.Unmarshal(actionsJSON, &r.Actions); err != nil {
				return nil, fmt.Errorf("failed to unmarshal SMF actions JSON for instrument %s: %w", r.InstrumentName, err)
			}
		}
		if len(accountsJSON) > 0 {
			if err := json.Unmarshal(accountsJSON, &r.Accounts); err != nil {
				return nil, fmt.Errorf("failed to unmarshal SMF accounts JSON for instrument %s: %w", r.InstrumentName, err)
			}
		}

		results = append(results, r)
	}

	return results, nil
}

// SearchSMFByType поиск SMF по типу сценария
func (q *Query) SearchSMFByType(scenarioType string, limit int) ([]SMFInstrumentResult, error) {
	query := `
		SELECT
			s.id,
			s.file_id,
			s.instrument_name,
			s.brief,
			f.rel_path,
			s.deal_object_id,
			s.ds_module_id,
			s.start_state,
			s.scenario_type,
			s.states,
			s.actions,
			s.accounts
		FROM smf_instruments s
		JOIN files f ON s.file_id = f.id
		WHERE s.scenario_type = $1
		ORDER BY s.instrument_name
		LIMIT $2
	`

	rows, err := q.db.Query(query, scenarioType, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SMFInstrumentResult
	for rows.Next() {
		var r SMFInstrumentResult
		var brief, startState sql.NullString
		var dealObjectID, dsModuleID sql.NullInt64
		var statesJSON, actionsJSON, accountsJSON []byte

		if err := rows.Scan(&r.ID, &r.FileID, &r.InstrumentName, &brief, &r.File, &dealObjectID, &dsModuleID, &startState, &r.ScenarioType, &statesJSON, &actionsJSON, &accountsJSON); err != nil {
			return nil, err
		}

		if brief.Valid {
			r.Brief = brief.String
		}
		if startState.Valid {
			r.StartState = startState.String
		}
		if dealObjectID.Valid {
			r.DealObjectID = dealObjectID.Int64
		}
		if dsModuleID.Valid {
			r.DsModuleID = dsModuleID.Int64
		}

		// Парсим JSONB
		if len(statesJSON) > 0 {
			if err := json.Unmarshal(statesJSON, &r.States); err != nil {
				return nil, fmt.Errorf("failed to unmarshal SMF states JSON for instrument %s: %w", r.InstrumentName, err)
			}
		}
		if len(actionsJSON) > 0 {
			if err := json.Unmarshal(actionsJSON, &r.Actions); err != nil {
				return nil, fmt.Errorf("failed to unmarshal SMF actions JSON for instrument %s: %w", r.InstrumentName, err)
			}
		}
		if len(accountsJSON) > 0 {
			if err := json.Unmarshal(accountsJSON, &r.Accounts); err != nil {
				return nil, fmt.Errorf("failed to unmarshal SMF accounts JSON for instrument %s: %w", r.InstrumentName, err)
			}
		}

		results = append(results, r)
	}

	return results, nil
}
