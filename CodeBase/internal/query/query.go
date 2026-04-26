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
	TableName      string `json:"table_name"`
	ColumnName     string `json:"column_name"`
	DataType       string `json:"data_type"`
	DefinitionKind string `json:"definition_kind,omitempty"`
	File           string `json:"file,omitempty"`
	FileID         int64  `json:"file_id"`
	LineNumber     int    `json:"line_number"`
	ColumnOrder    int    `json:"column_order"`
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
	CallerID     int64  `json:"caller_id,omitempty"`
	CallerName   string `json:"caller_name"`
	CallerType   string `json:"caller_type"`
	File         string `json:"file"`
	LineNumber   int    `json:"line_number"`
	CallContext  string `json:"call_context,omitempty"`
	IsIndirect   bool   `json:"is_indirect,omitempty"`
	ViaProcedure string `json:"via_procedure,omitempty"`
	RelationType string `json:"relation_type,omitempty"`
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

func buildLookupValue(value string, like bool) string {
	trimmed := strings.TrimSpace(value)
	if like {
		return "%" + trimmed + "%"
	}
	return trimmed
}

func buildNameLookupCondition(fields []string, like bool, argPosition int) string {
	if len(fields) == 0 {
		return ""
	}
	operator := "="
	if like {
		operator = "ILIKE"
	}
	conditions := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		conditions = append(conditions, fmt.Sprintf("%s %s $%d", field, operator, argPosition))
	}
	return strings.Join(conditions, " OR ")
}

// SearchSymbol ищет сущность по имени
func (q *Query) SearchSymbol(name string, symbolType string, like bool, limit int) ([]SymbolResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"s.symbol_name"}, like, 1)
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
		WHERE ` + lookupCondition + `
	`
	args := []interface{}{lookupValue}

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
func (q *Query) SearchReportForm(name string, like bool, limit int) ([]ReportFormResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"rf.report_name", "rf.form_name"}, like, 1)
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
		WHERE ` + lookupCondition + `
		ORDER BY rf.report_name, rf.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, lookupValue, limit)
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

func (q *Query) SearchDFMForm(name string, like bool, limit int) ([]DFMFormResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"df.form_name", "df.form_class", "df.caption"}, like, 1)
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
		WHERE ` + lookupCondition + `
		ORDER BY df.form_name, df.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, lookupValue, limit)
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

func (q *Query) SearchDFMComponent(name string, like bool, limit int) ([]DFMComponentResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"dc.component_name", "dc.component_type", "dc.caption", "df.form_name", "df.form_class"}, like, 1)
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
		WHERE ` + lookupCondition + `
		ORDER BY df.form_name, dc.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, lookupValue, limit)
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
func (q *Query) SearchReportField(name string, like bool, limit int) ([]ReportFieldResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"rfld.field_name", "rfld.source_name", "rf.report_name", "COALESCE(rfld.raw_text, '')"}, like, 1)
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
		WHERE ` + lookupCondition + `
		ORDER BY rf.report_name, rfld.line_number
		LIMIT $2
	`

	rows, err := q.db.Query(query, lookupValue, limit)
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
func (q *Query) SearchReportParam(name string, like bool, limit int) ([]ReportParamResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"rp.param_name", "COALESCE(rp.lookup_table, '')", "COALESCE(rp.lookup_column, '')", "COALESCE(rp.raw_text, '')"}, like, 1)
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
		WHERE ` + lookupCondition + `
		ORDER BY rf.report_name, rp.line_number
		LIMIT $2
	`

	rows, err := q.db.Query(query, lookupValue, limit)
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
func (q *Query) SearchVBFunction(name string, like bool, limit int) ([]VBFunctionResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"vf.function_name"}, like, 1)
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
		WHERE ` + lookupCondition + `
		ORDER BY vf.function_name, vf.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, lookupValue, limit)
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
func (q *Query) SearchJSFunction(name string, like bool, limit int) ([]JSFunctionResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"j.function_name"}, like, 1)
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
		WHERE ` + lookupCondition + `
		ORDER BY j.line_start
		LIMIT $2
	`

	rows, err := q.db.Query(query, lookupValue, limit)
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
func (q *Query) SearchSMFInstrument(name string, like bool, limit int) ([]SMFInstrumentResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"s.instrument_name", "s.brief", "f.rel_path"}, like, 1)
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
		WHERE ` + lookupCondition + `
		ORDER BY s.instrument_name
		LIMIT $2
	`

	rows, err := q.db.Query(query, lookupValue, limit)
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
