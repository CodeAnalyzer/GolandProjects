package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
	"github.com/lib/pq"
)

// Query API для запросов к индексу
type Query struct {
	db      *store.DB
	profile *ProfileContext
}

// SymbolResult результат поиска сущности
type SymbolResult struct {
	ID              int64  `json:"id"`
	FileID          int64  `json:"file_id"`
	EntityID        int64  `json:"entity_id,omitempty"`
	Name            string `json:"name"`
	Type            string `json:"type"`
	EntityType      string `json:"entity_type"`
	File            string `json:"file"`
	LineNumber      int    `json:"line_number"`
	Signature       string `json:"signature,omitempty"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

// TableResult результат поиска таблицы
type TableResult struct {
	ID              int64    `json:"id"`
	FileIDs         []int64  `json:"file_ids,omitempty"`
	TableName       string   `json:"table_name"`
	Context         string   `json:"context"`
	Files           []string `json:"files"`
	Usages          int      `json:"usages"`
	Columns         []string `json:"columns"`
	Procedures      []string `json:"procedures"`
	Product         string   `json:"product,omitempty"`
	BranchUsed      string   `json:"branch_used,omitempty"`
	BranchRequested string   `json:"branch_requested,omitempty"`
	InMemory        bool     `json:"in_memory"`
	InMemoryProduct string   `json:"in_memory_product,omitempty"`
	InMemoryModule  string   `json:"in_memory_module,omitempty"`
}

// TableSchemaColumnResult результат поиска определений колонок таблицы
type TableSchemaColumnResult struct {
	TableName       string `json:"table_name"`
	ColumnName      string `json:"column_name"`
	DataType        string `json:"data_type"`
	DefinitionKind  string `json:"definition_kind,omitempty"`
	File            string `json:"file,omitempty"`
	FileID          int64  `json:"file_id"`
	LineNumber      int    `json:"line_number"`
	ColumnOrder     int    `json:"column_order"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

// SQLTableIndexFieldResult результат поиска определений полей индекса таблицы
type SQLTableIndexFieldResult struct {
	FieldName  string `json:"field_name"`
	FieldOrder int    `json:"field_order,omitempty"`
	LineNumber int    `json:"line_number,omitempty"`
}

// SQLTableIndexResult результат поиска определений индексов таблицы
type SQLTableIndexResult struct {
	ID              int64                      `json:"id"`
	TableName       string                     `json:"table_name"`
	IndexName       string                     `json:"index_name"`
	IndexFields     string                     `json:"index_fields,omitempty"`
	IndexType       string                     `json:"index_type,omitempty"`
	IsUnique        bool                       `json:"is_unique,omitempty"`
	DefinitionKind  string                     `json:"definition_kind,omitempty"`
	File            string                     `json:"file,omitempty"`
	FileID          int64                      `json:"file_id,omitempty"`
	LineNumber      int                        `json:"line_number,omitempty"`
	Fields          []SQLTableIndexFieldResult `json:"fields,omitempty"`
	Product         string                     `json:"product,omitempty"`
	BranchUsed      string                     `json:"branch_used,omitempty"`
	BranchRequested string                     `json:"branch_requested,omitempty"`
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
	Product      string `json:"product,omitempty"`
	BranchUsed   string `json:"branch_used,omitempty"`
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
	ID              int64  `json:"id"`
	FileID          int64  `json:"file_id"`
	FormName        string `json:"form_name"`
	FormClass       string `json:"form_class"`
	Caption         string `json:"caption,omitempty"`
	File            string `json:"file"`
	LineStart       int    `json:"line_start"`
	LineEnd         int    `json:"line_end"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

type DFMComponentResult struct {
	ID              int64  `json:"id"`
	FileID          int64  `json:"file_id"`
	FormID          int64  `json:"form_id"`
	FormName        string `json:"form_name"`
	FormClass       string `json:"form_class"`
	ComponentName   string `json:"component_name"`
	ComponentType   string `json:"component_type"`
	ParentName      string `json:"parent_name,omitempty"`
	Caption         string `json:"caption,omitempty"`
	PASFieldName    string `json:"pas_field_name,omitempty"`
	PASFieldType    string `json:"pas_field_type,omitempty"`
	File            string `json:"file"`
	LineStart       int    `json:"line_start"`
	LineEnd         int    `json:"line_end"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
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
	Product          string   `json:"product,omitempty"`
	BranchUsed       string   `json:"branch_used,omitempty"`
	BranchRequested  string   `json:"branch_requested,omitempty"`
}

// ReportFormResult результат поиска report form
type ReportFormResult struct {
	ID              int64  `json:"id"`
	FileID          int64  `json:"file_id"`
	ReportName      string `json:"report_name"`
	ReportType      string `json:"report_type"`
	FormName        string `json:"form_name,omitempty"`
	FormClass       string `json:"form_class,omitempty"`
	File            string `json:"file"`
	LineStart       int    `json:"line_start"`
	LineEnd         int    `json:"line_end"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

// ReportFieldResult результат поиска report field
type ReportFieldResult struct {
	ID              int64    `json:"id"`
	ReportFormID    int64    `json:"report_form_id"`
	FieldName       string   `json:"field_name"`
	SourceName      string   `json:"source_name,omitempty"`
	FormatMask      string   `json:"format_mask,omitempty"`
	Options         []string `json:"options,omitempty"`
	ReportName      string   `json:"report_name"`
	ReportType      string   `json:"report_type"`
	File            string   `json:"file"`
	LineNumber      int      `json:"line_number"`
	RawText         string   `json:"raw_text,omitempty"`
	Product         string   `json:"product,omitempty"`
	BranchUsed      string   `json:"branch_used,omitempty"`
	BranchRequested string   `json:"branch_requested,omitempty"`
}

// ReportParamResult результат поиска report param
type ReportParamResult struct {
	ID              int64  `json:"id"`
	ReportFormID    int64  `json:"report_form_id"`
	ParamName       string `json:"param_name"`
	ParamKind       string `json:"param_kind"`
	ComponentType   string `json:"component_type,omitempty"`
	DataType        string `json:"data_type,omitempty"`
	LookupForm      string `json:"lookup_form,omitempty"`
	LookupTable     string `json:"lookup_table,omitempty"`
	LookupColumn    string `json:"lookup_column,omitempty"`
	KeyColumn       string `json:"key_column,omitempty"`
	DefaultValue    string `json:"default_value,omitempty"`
	Required        bool   `json:"required"`
	ReportName      string `json:"report_name"`
	ReportType      string `json:"report_type"`
	File            string `json:"file"`
	LineNumber      int    `json:"line_number"`
	RawText         string `json:"raw_text,omitempty"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

// VBFunctionResult результат поиска VB функции
type VBFunctionResult struct {
	ID              int64  `json:"id"`
	ReportFormID    int64  `json:"report_form_id"`
	FunctionName    string `json:"function_name"`
	FunctionType    string `json:"function_type"`
	Signature       string `json:"signature,omitempty"`
	ReportName      string `json:"report_name"`
	File            string `json:"file"`
	LineStart       int    `json:"line_start"`
	LineEnd         int    `json:"line_end"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

// MethodResult результат поиска методов
type MethodResult struct {
	ID              int64  `json:"id"`
	UnitID          int64  `json:"unit_id"`
	MethodName      string `json:"method_name"`
	ClassName       string `json:"class_name"`
	UnitName        string `json:"unit_name"`
	Signature       string `json:"signature,omitempty"`
	Visibility      string `json:"visibility,omitempty"`
	File            string `json:"file"`
	LineNumber      int    `json:"line_number"`
	TableUsage      string `json:"table_usage,omitempty"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

// New создаёт новый Query API
func New(db *store.DB, opts ...Option) *Query {
	q := &Query{db: db}
	for _, opt := range opts {
		if opt != nil {
			opt(q)
		}
	}
	return q
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

// needsReportFormsLookup сообщает, требует ли поиск с данным первичным
// каноническим типом fallback на поля report_forms (form_name, form_class).
// Решение потребляет SearchSymbol: при report_form CTE base собирается как
// UNION ALL трёх индексных ветвей (см. reportFormFieldConditions).
func needsReportFormsLookup(primaryType string) bool {
	return strings.TrimSpace(primaryType) == "report_form"
}

// reportFormFieldConditions строит условия трёхполевого поиска символа
// отчётной формы: имя отчёта (symbol_name), имя DFM-формы (rf.form_name) и
// класс DFM-формы (rf.form_class). Каждое условие применяется в отдельной
// ветви UNION ALL (change optimize-query-search-bottlenecks, B3): прежний
// единый OR по полям двух таблиц не позволял планировщику использовать
// индексы — план перебирал все символы типа report_form с per-row lookup в
// report_forms. Ветвь rf.form_name/rf.form_class идёт INNER JOIN от
// report_forms: символы report_form без резолвнутой записи report_forms
// находятся ветвью symbol_name и не теряются.
func reportFormFieldConditions(like bool, argPosition int) []string {
	if like {
		return []string{
			fmt.Sprintf("s.symbol_name ILIKE $%d", argPosition),
			fmt.Sprintf("rf.form_name ILIKE $%d", argPosition),
			fmt.Sprintf("rf.form_class ILIKE $%d", argPosition),
		}
	}
	return []string{
		fmt.Sprintf("LOWER(s.symbol_name) = LOWER($%d)", argPosition),
		fmt.Sprintf("LOWER(rf.form_name) = LOWER($%d)", argPosition),
		fmt.Sprintf("LOWER(rf.form_class) = LOWER($%d)", argPosition),
	}
}

func buildSymbolLookupCondition(symbolType string, like bool, argPosition int) string {
	normalizedType := strings.TrimSpace(symbolType)
	if !like && normalizedType != "" && normalizedType != "form" {
		return fmt.Sprintf("LOWER(s.symbol_name) = LOWER($%d)", argPosition)
	}

	nameCondition := buildNameLookupCondition([]string{"s.symbol_name"}, like, argPosition)
	formClassCondition := buildNameLookupCondition([]string{"s.signature"}, like, argPosition)

	if normalizedType == "form" {
		return nameCondition + " OR " + formClassCondition
	}

	return nameCondition + " OR (s.symbol_type = 'form' AND " + formClassCondition + ")"
}

// normalizeSymbolTypeFilter нормализует значение фильтра типа символа в набор
// канонических типов: пустое значение — без фильтра, алиасы relations-стиля
// транслируются через model.ResolveSymbolTypeAlias, неизвестное значение —
// ошибка со списком допустимых типов и алиасов.
func normalizeSymbolTypeFilter(symbolType string) ([]string, error) {
	trimmed := strings.TrimSpace(symbolType)
	if trimmed == "" {
		return nil, nil
	}
	canonical, ok := model.ResolveSymbolTypeAlias(trimmed)
	if !ok {
		return nil, fmt.Errorf("%w %q, valid types: %s; aliases: %s",
			errs.ErrUnknownSymbolType, trimmed,
			strings.Join(model.SymbolTypes, ", "),
			strings.Join(model.SymbolTypeAliasesList(), ", "))
	}
	return canonical, nil
}

// SearchSymbol ищет сущность по имени. Копии, отличающиеся только веткой
// одного продукта, схлопываются (двухфазный дедуп: выбирается ветка группы
// ключа — приоритет default-ветки, — затем возвращаются все строки выбранной
// ветки); ключ дедупа — тип, имя, владелец (класс/форма для method/component),
// продукт. Разные продукты и одноимённые методы разных классов остаются
// отдельными строками; на одно-веточном индексе дедуп — no-op.
// Трёхполевой fallback отчётных форм (report_form) выполняется UNION ALL
// индексных ветвей — см. reportFormFieldConditions (B3).
func (q *Query) SearchSymbol(ctx context.Context, name string, symbolType string, like bool, limit int) ([]SymbolResult, error) {
	canonicalTypes, err := normalizeSymbolTypeFilter(symbolType)
	if err != nil {
		return nil, err
	}
	primaryType := ""
	if len(canonicalTypes) > 0 {
		primaryType = canonicalTypes[0]
	}
	lookupValue := buildLookupValue(name, like)
	args := []interface{}{lookupValue}
	typeFilter := ""
	if len(canonicalTypes) > 0 {
		// Фильтр по типу добавляется динамически, чтобы не плодить отдельные SQL-шаблоны.
		// Один алиас (api_contract) разворачивается в несколько канонических типов.
		typeFilter = " AND s.symbol_type = ANY($2)"
		args = append(args, pq.Array(canonicalTypes))
	}

	// symbols — это unified index (унифицированный индекс), поэтому этот метод
	// является самым общим способом найти сущность без знания конкретной таблицы-хранилища.
	// symbolBaseBranch — одна индексная ветвь CTE base с общим списком колонок.
	symbolBaseBranch := func(fromClause, condition string) string {
		return `
			SELECT
				s.id,
				s.file_id,
				s.entity_id,
				s.symbol_name,
				s.symbol_type,
				s.entity_type,
				LOWER(s.symbol_name) AS name_key, COALESCE(s.owner_name, '') AS owner_key,
				COALESCE(f.ds_product_id, 0) AS product_key,
				f.ds_product_branch_id AS branch_id,
				f.rel_path,
				s.line_number,
				s.signature,
				COALESCE(pr.product_name, '') AS product,
				COALESCE(br.branch_key, '') AS branch_used,
				(NOT f.is_generated) AS not_generated,
				COALESCE(br.is_default, FALSE) AS is_default
			FROM ` + fromClause + `
			JOIN files f ON s.file_id = f.id
			LEFT JOIN ds_products pr ON pr.id = f.ds_product_id
			LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
			WHERE (` + condition + `)` + typeFilter
	}

	var baseQuery string
	finalDistinct := ""
	if needsReportFormsLookup(primaryType) {
		// Трёхполевой fallback отчётных форм как UNION ALL индексных ветвей
		// (change optimize-query-search-bottlenecks, B3): ветвь symbol_name
		// использует idx_symbols_symbol_name_type_lower, ветви form_name/
		// form_class идут от небольшой report_forms с выходом на symbols через
		// idx_symbols_entity_type_entity_id.
		reportFormsFrom := "report_forms rf JOIN symbols s ON s.symbol_type = 'report_form' AND s.entity_id = rf.id"
		conds := reportFormFieldConditions(like, 1)
		baseQuery = symbolBaseBranch("symbols s", conds[0]) + `
			UNION ALL
		` + symbolBaseBranch(reportFormsFrom, conds[1]) + `
			UNION ALL
		` + symbolBaseBranch(reportFormsFrom, conds[2])
		// Строка, совпавшая по нескольким полям, попадает в base дважды:
		// дедуп точных копий по id (веточный дедуп выполняет chosen).
		finalDistinct = "DISTINCT "
	} else {
		baseQuery = symbolBaseBranch("symbols s", buildSymbolLookupCondition(primaryType, like, 1))
	}

	query := `
		WITH base AS (` + baseQuery + `
		),
		chosen AS (
			SELECT DISTINCT ON (b.name_key, b.symbol_type, b.owner_key, b.product_key)
			       b.name_key, b.symbol_type, b.owner_key, b.product_key, b.branch_id
			FROM base b
			ORDER BY b.name_key, b.symbol_type, b.owner_key, b.product_key,
			         b.not_generated DESC, ` + q.profileMatchSQL("b.product_key", "b.branch_id") + ` DESC, b.is_default DESC, b.id DESC
		)
		SELECT ` + finalDistinct + `b.id, b.file_id, b.entity_id, b.symbol_name, b.symbol_type, b.entity_type,
		       b.rel_path, b.line_number, b.signature, b.product, b.branch_used
		FROM base b
		JOIN chosen ch ON ch.name_key = b.name_key AND ch.symbol_type = b.symbol_type
		 AND ch.owner_key = b.owner_key AND ch.product_key = b.product_key
		 AND ch.branch_id IS NOT DISTINCT FROM b.branch_id
		ORDER BY b.symbol_name, b.id
		LIMIT ` + fmt.Sprintf("%d", limit)

	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SymbolResult
	for rows.Next() {
		var r SymbolResult
		var signature sql.NullString
		// signature nullable, потому что не у всех сущностей есть полная сигнатура.
		if err := rows.Scan(&r.ID, &r.FileID, &r.EntityID, &r.Name, &r.Type, &r.EntityType, &r.File, &r.LineNumber, &signature, &r.Product, &r.BranchUsed); err != nil {
			return nil, err
		}
		if signature.Valid {
			r.Signature = signature.String
		}
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, rows.Err()
}

// GetFileByPath получает файл по пути
func (q *Query) GetFileByPath(ctx context.Context, path string) (*model.File, error) {
	// Поддерживаем поиск и по абсолютному path, и по rel_path,
	// чтобы API было удобно использовать и из CLI, и из внешних интеграций.
	query := `
		SELECT id, scan_run_id, ds_product_id, path, rel_path, extension, size_bytes, 
		       hash_sha256, modified_at, encoding, language, created_at, updated_at
		FROM files
		WHERE path = $1 OR rel_path = $1
	`

	row := q.db.QueryRowContext(ctx, query, path)

	var f model.File
	var dsProductID sql.NullInt64
	err := row.Scan(&f.ID, &f.ScanRunID, &dsProductID, &f.Path, &f.RelPath, &f.Extension, &f.SizeBytes,
		&f.HashSHA256, &f.ModifiedAt, &f.Encoding, &f.Language, &f.CreatedAt, &f.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if dsProductID.Valid {
		f.DsProductID = dsProductID.Int64
	}

	return &f, nil
}

// SearchInContent ищет текст в содержимом файлов (по symbol names/signatures
// unified index) с тем же веточным дедупом, что и SearchSymbol.
func (q *Query) SearchInContent(ctx context.Context, pattern string, limit int) ([]SymbolResult, error) {
	// Сейчас это не full-text search по сырому содержимому файлов,
	// а поиск по уже извлечённым symbol names/signatures в unified index.
	query := `
		WITH base AS (
			SELECT
				s.id,
				s.file_id,
				s.symbol_name,
				s.symbol_type,
				s.entity_type,
				LOWER(s.symbol_name) AS name_key, COALESCE(s.owner_name, '') AS owner_key,
				COALESCE(f.ds_product_id, 0) AS product_key,
				f.ds_product_branch_id AS branch_id,
				f.rel_path,
				s.line_number,
				s.signature,
				COALESCE(pr.product_name, '') AS product,
				COALESCE(br.branch_key, '') AS branch_used,
				(NOT f.is_generated) AS not_generated,
				COALESCE(br.is_default, FALSE) AS is_default
			FROM symbols s
			JOIN files f ON s.file_id = f.id
			LEFT JOIN ds_products pr ON pr.id = f.ds_product_id
			LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
			WHERE s.symbol_name ILIKE $1
			   OR s.signature ILIKE $1
		),
		chosen AS (
			SELECT DISTINCT ON (b.name_key, b.symbol_type, b.owner_key, b.product_key)
			       b.name_key, b.symbol_type, b.owner_key, b.product_key, b.branch_id
			FROM base b
			ORDER BY b.name_key, b.symbol_type, b.owner_key, b.product_key,
			         b.not_generated DESC, ` + q.profileMatchSQL("b.product_key", "b.branch_id") + ` DESC, b.is_default DESC, b.id DESC
		)
		SELECT b.id, b.file_id, b.symbol_name, b.symbol_type, b.entity_type,
		       b.rel_path, b.line_number, b.signature, b.product, b.branch_used
		FROM base b
		JOIN chosen ch ON ch.name_key = b.name_key AND ch.symbol_type = b.symbol_type
		 AND ch.owner_key = b.owner_key AND ch.product_key = b.product_key
		 AND ch.branch_id IS NOT DISTINCT FROM b.branch_id
		ORDER BY b.symbol_name, b.id
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, "%"+pattern+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SymbolResult
	for rows.Next() {
		var r SymbolResult
		var signature sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.Name, &r.Type, &r.EntityType, &r.File, &r.LineNumber, &signature, &r.Product, &r.BranchUsed); err != nil {
			return nil, err
		}
		if signature.Valid {
			r.Signature = signature.String
		}
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, nil
}

// SearchReportForm поиск report forms: не более одной строки на отчёт
// одного продукта (приоритет default-ветки), с product/branch_used.
func (q *Query) SearchReportForm(ctx context.Context, name string, like bool, limit int) ([]ReportFormResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"rf.report_name", "rf.form_name", "rf.form_class"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (report_name, продукт),
	// фаза 2 возвращает все отчёты выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "rf.report_name", Alias: "report_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"rf.id",
		`rf.id,
		rf.file_id,
		rf.report_name,
		rf.report_type,
		rf.form_name,
		rf.form_class,
		f.rel_path,
		rf.line_start,
		rf.line_end`,
		`report_forms rf
		JOIN files f ON rf.file_id = f.id`+fileBranchJoin("f"),
		lookupCondition,
		`rf.report_name, rf.line_start, rf.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]ReportFormResult, 0)
	for rows.Next() {
		var r ReportFormResult
		var formName, formClass sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.ReportName, &r.ReportType, &formName, &formClass, &r.File, &r.LineStart, &r.LineEnd, &r.Product, &r.BranchUsed); err != nil {
			return nil, err
		}
		if formName.Valid {
			r.FormName = formName.String
		}
		if formClass.Valid {
			r.FormClass = formClass.String
		}
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchDFMForm поиск DFM-форм: не более одной строки на класс формы одного
// продукта (приоритет default-ветки), с product/branch_used.
func (q *Query) SearchDFMForm(ctx context.Context, name string, like bool, limit int) ([]DFMFormResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"df.form_name", "df.form_class", "df.caption"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (form_class, продукт),
	// фаза 2 возвращает все формы выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "df.form_class", Alias: "form_class"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"df.id",
		`df.id,
		df.file_id,
		df.form_name,
		df.form_class,
		df.caption,
		f.rel_path,
		df.line_start,
		df.line_end`,
		`dfm_forms df
		JOIN files f ON df.file_id = f.id`+fileBranchJoin("f"),
		lookupCondition,
		`df.form_name, df.line_start, df.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]DFMFormResult, 0)
	for rows.Next() {
		var r DFMFormResult
		var caption sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.FormName, &r.FormClass, &caption, &r.File, &r.LineStart, &r.LineEnd, &r.Product, &r.BranchUsed); err != nil {
			return nil, err
		}
		if caption.Valid {
			r.Caption = caption.String
		}
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchDFMComponent поиск DFM-компонентов: не более одной строки на
// компонент (имя + класс формы) одного продукта, с product/branch_used.
func (q *Query) SearchDFMComponent(ctx context.Context, name string, like bool, limit int) ([]DFMComponentResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"dc.component_name", "dc.component_type", "dc.caption", "df.form_name", "df.form_class"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (имя компонента, класс формы,
	// продукт), фаза 2 возвращает все компоненты выбранной ветки — включая все
	// связанные pas_fields (раньше DISTINCT ON схлопывал их до одной строки).
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "dc.component_name", Alias: "component_name"},
			{Expr: "df.form_class", Alias: "form_class"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"dc.id",
		`dc.id,
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
		dc.line_end`,
		`dfm_components dc
		JOIN dfm_forms df ON dc.form_id = df.id
		JOIN files f ON dc.file_id = f.id`+fileBranchJoin("f")+`
		LEFT JOIN pas_fields pf ON pf.dfm_component_id = dc.id`,
		lookupCondition,
		`df.form_name, dc.line_start, dc.id, pf.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]DFMComponentResult, 0)
	for rows.Next() {
		var r DFMComponentResult
		var parentName, caption, pasFieldName, pasFieldType sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.FormID, &r.FormName, &r.FormClass, &r.ComponentName, &r.ComponentType, &parentName, &caption, &pasFieldName, &pasFieldType, &r.File, &r.LineStart, &r.LineEnd, &r.Product, &r.BranchUsed); err != nil {
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
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, rows.Err()
}

// QueryFragmentSearchOutput — выдача поиска query fragments для CLI/MCP:
// строки + признак усечения по потолку кандидатов (change
// optimize-query-search-bottlenecks, B5): сверхширокий шаблон останавливает
// выборку на sql_fragment_match_cap совпадениях вместо таймаута.
type QueryFragmentSearchOutput struct {
	Items     []QueryFragmentResult `json:"items"`
	Truncated bool                  `json:"truncated"`
	Hint      string                `json:"hint,omitempty"`
}

// QueryFragmentTruncatedHint — подсказка при усечении выдачи sql-fragment.
const QueryFragmentTruncatedHint = "candidate set truncated at sql_fragment_match_cap; refine the pattern to see all matches"

// SearchQueryFragment поиск фрагментов запросов. Побайтово идентичные копии
// из разных веток одного продукта схлопываются: для группы (хэш текста,
// продукт) выбирается одна ветка (приоритет default) и показываются все
// фрагменты этой ветки — дубли внутри одной ветки сохраняются, поэтому на
// одно-веточном индексе поведение не меняется.
// Возвращает признак усечения: true, когда совпадений больше потолка
// кандидатов query.sql_fragment_match_cap (внутренний LIMIT cap+1 в matches;
// явный 0 в конфиге — без ограничения, план как до изменения).
func (q *Query) SearchQueryFragment(ctx context.Context, text string, limit int) ([]QueryFragmentResult, bool, error) {
	// Идентификаторы отбираются из того же CTE matches, а не повторным join к
	// query_fragments: повторный ILIKE по всей таблице с сортировкой по file_id
	// провоцировал Merge Join с полным параллельным сканом (2.4 млн строк) —
	// запрос по частому фрагменту ("select @@spid", 124 тыс. совпадений) уходил
	// в statement timeout. Множество результатов эквивалентно: chosen строится
	// из matches, а все строки выбранной ветки уже присутствуют в matches.
	//
	// Guard-потолок (B5): внутренний LIMIT cap+1 делает выборку matches ленивой
	// — executor перестаёт тянуть строки из ILIKE-скана после cap+1, DISTINCT ON
	// сортирует ≤ cap+1 строк. Окно count(*) OVER () стоит строго снаружи
	// внутреннего LIMIT (считает ≤ cap+1 уже выбранных строк, не форсируя
	// полный проход). total_matched > cap → усечение (truncated=true).
	// config.Get() может быть nil (запрос без загруженного конфига) — тогда
	// применяется дефолт потолка.
	var queryCfg config.QueryConfig
	if loaded := config.Get(); loaded != nil {
		queryCfg = loaded.Query
	}
	matchCap := queryCfg.SQLFragmentMatchCapOrDefault()

	matchesInner := `
			SELECT qf.id,
			       COALESCE(qf.query_hash, md5(qf.query_text)) AS content_key,
			       COALESCE(f.ds_product_id, 0) AS product_key,
			       COALESCE(f.ds_product_branch_id, 0) AS branch_key,
			       COALESCE(br.is_default, FALSE) AS is_default,
			       qf.file_id,
			       qf.line_number
			FROM query_fragments qf
			JOIN files f ON qf.file_id = f.id
			LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
			WHERE qf.query_text ILIKE $1`

	var matchesCTE string
	totalColumn := ""
	if matchCap > 0 {
		matchesInner += `
			LIMIT $3`
		matchesCTE = `
		WITH matches AS (
			SELECT t.id, t.content_key, t.product_key, t.branch_key, t.is_default, t.file_id, t.line_number,
			       count(*) OVER () AS total_matched
			FROM (` + matchesInner + `
			) t
		)`
		totalColumn = ", m.total_matched"
	} else {
		matchesCTE = `
		WITH matches AS (` + matchesInner + `
		)`
	}

	// Порядок аргументов обязан совпадать с нумерацией плейсхолдеров:
	// $1 — шаблон, $2 — финальный LIMIT выдачи, $3 — внутренний LIMIT cap+1
	// (только при включённом потолке).
	queryArgs := []interface{}{"%" + text + "%", limit}
	if matchCap > 0 {
		queryArgs = append(queryArgs, matchCap+1)
	}

	idRows, err := q.db.QueryContext(ctx, matchesCTE+`,
		chosen AS (
			SELECT DISTINCT ON (content_key, product_key) content_key, product_key, branch_key
			FROM matches
			ORDER BY content_key, product_key, `+q.profileMatchSQL("product_key", "branch_key")+` DESC, is_default DESC, branch_key DESC
		)
		SELECT m.id`+totalColumn+`
		FROM matches m
		JOIN chosen ch ON ch.content_key = m.content_key
		             AND ch.product_key = m.product_key
		             AND ch.branch_key = m.branch_key
		ORDER BY m.file_id, m.line_number
		LIMIT $2
	`, queryArgs...)
	if err != nil {
		return nil, false, err
	}
	defer idRows.Close()

	ids := make([]int64, 0, limit)
	positions := make(map[int64]int, limit)
	totalMatched := 0
	for idRows.Next() {
		var id int64
		if matchCap > 0 {
			if err := idRows.Scan(&id, &totalMatched); err != nil {
				return nil, false, err
			}
		} else {
			if err := idRows.Scan(&id); err != nil {
				return nil, false, err
			}
		}
		positions[id] = len(ids)
		ids = append(ids, id)
	}
	if err := idRows.Err(); err != nil {
		return nil, false, err
	}
	if len(ids) == 0 {
		return []QueryFragmentResult{}, false, nil
	}

	placeholders := make([]string, 0, len(ids))
	args := make([]interface{}, 0, len(ids))
	for i, id := range ids {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, id)
	}

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
			qf.line_number,
			` + fileBranchColumns + `
		FROM query_fragments qf
		JOIN files f ON qf.file_id = f.id` + fileBranchJoin("f") + `
		WHERE qf.id IN (` + strings.Join(placeholders, ",") + `)
	`

	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	results := make([]QueryFragmentResult, len(ids))
	for rows.Next() {
		var r QueryFragmentResult
		var tablesJSON []byte
		if err := rows.Scan(&r.ID, &r.FileID, &r.ParentType, &r.ParentID, &r.ComponentName, &r.ComponentType, &r.QueryText, &tablesJSON, &r.Context, &r.File, &r.LineNumber, &r.Product, &r.BranchUsed); err != nil {
			return nil, false, err
		}
		if len(tablesJSON) > 0 {
			_ = json.Unmarshal(tablesJSON, &r.TablesReferenced)
		}
		r.BranchRequested = q.branchRequested(r.Product)
		if position, ok := positions[r.ID]; ok {
			results[position] = r
		}
	}

	truncated := matchCap > 0 && totalMatched > matchCap
	return results, truncated, rows.Err()
}

// SearchReportField поиск report fields: не более одной строки на поле отчёта
// одного продукта (приоритет default-ветки), с product/branch_used.
func (q *Query) SearchReportField(ctx context.Context, name string, like bool, limit int) ([]ReportFieldResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"rfld.field_name", "rfld.source_name", "rf.report_name", "COALESCE(rfld.raw_text, '')"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (поле, отчёт, продукт),
	// фаза 2 возвращает все поля выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "rfld.field_name", Alias: "field_name"},
			{Expr: "rf.report_name", Alias: "report_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"rfld.id",
		`rfld.id,
		rfld.report_form_id,
		rfld.field_name,
		rfld.source_name,
		rfld.format_mask,
		rfld.options,
		rf.report_name,
		rf.report_type,
		f.rel_path,
		rfld.line_number,
		rfld.raw_text`,
		`report_fields rfld
		JOIN report_forms rf ON rfld.report_form_id = rf.id
		JOIN files f ON rf.file_id = f.id`+fileBranchJoin("f"),
		lookupCondition,
		`rf.report_name, rfld.line_number, rfld.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]ReportFieldResult, 0)
	for rows.Next() {
		var r ReportFieldResult
		var sourceName, formatMask, rawText sql.NullString
		var optionsJSON []byte
		if err := rows.Scan(&r.ID, &r.ReportFormID, &r.FieldName, &sourceName, &formatMask, &optionsJSON, &r.ReportName, &r.ReportType, &r.File, &r.LineNumber, &rawText, &r.Product, &r.BranchUsed); err != nil {
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
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchReportParam поиск report params: не более одной строки на параметр
// отчёта одного продукта (приоритет default-ветки), с product/branch_used.
func (q *Query) SearchReportParam(ctx context.Context, name string, like bool, limit int) ([]ReportParamResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"rp.param_name", "COALESCE(rp.lookup_table, '')", "COALESCE(rp.lookup_column, '')", "COALESCE(rp.raw_text, '')"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (параметр, отчёт, продукт),
	// фаза 2 возвращает все параметры выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "rp.param_name", Alias: "param_name"},
			{Expr: "rf.report_name", Alias: "report_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"rp.id",
		`rp.id,
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
		rp.raw_text`,
		`report_params rp
		JOIN report_forms rf ON rp.report_form_id = rf.id
		JOIN files f ON rf.file_id = f.id`+fileBranchJoin("f"),
		lookupCondition,
		`rf.report_name, rp.line_number, rp.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]ReportParamResult, 0)
	for rows.Next() {
		var r ReportParamResult
		var componentType, dataType, lookupForm, lookupTable, lookupColumn, keyColumn, defaultValue, rawText sql.NullString
		if err := rows.Scan(&r.ID, &r.ReportFormID, &r.ParamName, &r.ParamKind, &componentType, &dataType, &lookupForm, &lookupTable, &lookupColumn, &keyColumn, &defaultValue, &r.Required, &r.ReportName, &r.ReportType, &r.File, &r.LineNumber, &rawText, &r.Product, &r.BranchUsed); err != nil {
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
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchVBFunction поиск VB функций: не более одной строки на функцию отчёта
// одного продукта (приоритет default-ветки), с product/branch_used.
func (q *Query) SearchVBFunction(ctx context.Context, name string, like bool, limit int) ([]VBFunctionResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"vf.function_name"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (функция, отчёт, продукт),
	// фаза 2 возвращает все функции выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "vf.function_name", Alias: "function_name"},
			{Expr: "rf.report_name", Alias: "report_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"vf.id",
		`vf.id,
		vf.report_form_id,
		vf.function_name,
		vf.function_type,
		vf.signature,
		rf.report_name,
		f.rel_path,
		vf.line_start,
		vf.line_end`,
		`vb_functions vf
		JOIN report_forms rf ON vf.report_form_id = rf.id
		JOIN files f ON rf.file_id = f.id`+fileBranchJoin("f"),
		lookupCondition,
		`vf.function_name, vf.line_start, vf.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]VBFunctionResult, 0)
	for rows.Next() {
		var r VBFunctionResult
		var signature sql.NullString
		if err := rows.Scan(&r.ID, &r.ReportFormID, &r.FunctionName, &r.FunctionType, &signature, &r.ReportName, &r.File, &r.LineStart, &r.LineEnd, &r.Product, &r.BranchUsed); err != nil {
			return nil, err
		}
		if signature.Valid {
			r.Signature = signature.String
		}
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, rows.Err()
}

// JSFunctionResult результат поиска JS-функции
type JSFunctionResult struct {
	ID              int64  `json:"id"`
	FileID          int64  `json:"file_id"`
	FunctionName    string `json:"function_name"`
	File            string `json:"file"`
	LineNumber      int    `json:"line_number"`
	LineEnd         int    `json:"line_end"`
	Signature       string `json:"signature,omitempty"`
	ScenarioType    string `json:"scenario_type,omitempty"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

// SearchJSFunction поиск JS-функции: не более одной строки на функцию одного
// продукта (приоритет default-ветки), с product/branch_used.
func (q *Query) SearchJSFunction(ctx context.Context, name string, like bool, limit int) ([]JSFunctionResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"j.function_name"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (функция, продукт),
	// фаза 2 возвращает все функции выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "j.function_name", Alias: "function_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"j.id",
		`j.id,
		j.file_id,
		j.function_name,
		f.rel_path,
		j.line_start,
		j.line_end,
		j.signature,
		j.scenario_type`,
		`js_functions j
		JOIN files f ON j.file_id = f.id`+fileBranchJoin("f"),
		lookupCondition,
		`j.line_start, j.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []JSFunctionResult
	for rows.Next() {
		var r JSFunctionResult
		var signature sql.NullString
		var scenarioType sql.NullString
		if err := rows.Scan(&r.ID, &r.FileID, &r.FunctionName, &r.File, &r.LineNumber, &r.LineEnd, &signature, &scenarioType, &r.Product, &r.BranchUsed); err != nil {
			return nil, err
		}
		if signature.Valid {
			r.Signature = signature.String
		}
		if scenarioType.Valid {
			r.ScenarioType = scenarioType.String
		}
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, nil
}

// SMFInstrumentResult результат поиска SMF инструмента
type SMFInstrumentResult struct {
	ID              int64                    `json:"id"`
	FileID          int64                    `json:"file_id"`
	InstrumentName  string                   `json:"instrument_name"`
	Brief           string                   `json:"brief,omitempty"`
	File            string                   `json:"file"`
	DealObjectID    int64                    `json:"deal_object_id,omitempty"`
	DsModuleID      int64                    `json:"ds_module_id,omitempty"`
	StartState      string                   `json:"start_state,omitempty"`
	ScenarioType    string                   `json:"scenario_type"`
	States          []map[string]interface{} `json:"states,omitempty"`
	Actions         []map[string]interface{} `json:"actions,omitempty"`
	Accounts        []map[string]interface{} `json:"accounts,omitempty"`
	Product         string                   `json:"product,omitempty"`
	BranchUsed      string                   `json:"branch_used,omitempty"`
	BranchRequested string                   `json:"branch_requested,omitempty"`
}

// SearchSMFInstrument поиск SMF инструмента: не более одной строки на
// инструмент одного продукта (приоритет default-ветки), с product/branch_used.
func (q *Query) SearchSMFInstrument(ctx context.Context, name string, like bool, limit int) ([]SMFInstrumentResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"s.instrument_name", "s.brief", "f.rel_path"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (инструмент, продукт),
	// фаза 2 возвращает все инструменты выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "s.instrument_name", Alias: "instrument_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"s.id",
		`s.id,
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
		s.accounts`,
		`smf_instruments s
		JOIN files f ON s.file_id = f.id`+fileBranchJoin("f"),
		lookupCondition,
		`s.instrument_name, s.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, lookupValue, limit)
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

		if err := rows.Scan(&r.ID, &r.FileID, &r.InstrumentName, &brief, &r.File, &dealObjectID, &dsModuleID, &startState, &r.ScenarioType, &statesJSON, &actionsJSON, &accountsJSON, &r.Product, &r.BranchUsed); err != nil {
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

		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, nil
}

// SearchSMFByType поиск SMF по типу сценария: не более одной строки на
// инструмент одного продукта (приоритет default-ветки), с product/branch_used.
func (q *Query) SearchSMFByType(ctx context.Context, scenarioType string, limit int) ([]SMFInstrumentResult, error) {
	// Двухфазный дедуп: фаза 1 выбирает ветку для (инструмент, продукт),
	// фаза 2 возвращает все инструменты выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "s.instrument_name", Alias: "instrument_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"s.id",
		`s.id,
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
		s.accounts`,
		`smf_instruments s
		JOIN files f ON s.file_id = f.id`+fileBranchJoin("f"),
		`s.scenario_type = $1`,
		`s.instrument_name, s.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, scenarioType, limit)
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

		if err := rows.Scan(&r.ID, &r.FileID, &r.InstrumentName, &brief, &r.File, &dealObjectID, &dsModuleID, &startState, &r.ScenarioType, &statesJSON, &actionsJSON, &accountsJSON, &r.Product, &r.BranchUsed); err != nil {
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

		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, nil
}

// RetCodeResult результат поиска кода возврата
type RetCodeResult struct {
	RetCode         int64  `json:"ret_code"`
	Message         string `json:"message"`
	ProcName        string `json:"proc_name,omitempty"`
	ModuleID        int    `json:"module_id"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

// LookupRetCode ищет описание кода возврата по числовому значению.
func (q *Query) LookupRetCode(ctx context.Context, retCode int64) (*RetCodeResult, error) {
	r, err := q.db.LookupRetCode(ctx, retCode, q.profileBranches())
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, nil
	}
	return &RetCodeResult{
		RetCode:         r.RetCode,
		Message:         r.Message,
		ProcName:        r.ProcName,
		ModuleID:        r.ModuleID,
		Product:         r.Product,
		BranchUsed:      r.BranchUsed,
		BranchRequested: q.branchRequested(r.Product),
	}, nil
}

// LookupRetCodeByMessage ищет коды возврата по фрагменту текста сообщения.
func (q *Query) LookupRetCodeByMessage(ctx context.Context, messagePattern string, limit int) ([]RetCodeResult, error) {
	items, err := q.db.LookupRetCodeByMessage(ctx, messagePattern, limit, q.profileBranches())
	if err != nil {
		return nil, err
	}
	results := make([]RetCodeResult, 0, len(items))
	for _, r := range items {
		results = append(results, RetCodeResult{
			RetCode:         r.RetCode,
			Message:         r.Message,
			ProcName:        r.ProcName,
			ModuleID:        r.ModuleID,
			Product:         r.Product,
			BranchUsed:      r.BranchUsed,
			BranchRequested: q.branchRequested(r.Product),
		})
	}
	return results, nil
}
