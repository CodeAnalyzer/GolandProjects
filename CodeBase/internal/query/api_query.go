package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type APIParamResult struct {
	ID              int64  `json:"id"`
	ContractID      int64  `json:"contract_id,omitempty"`
	BusinessObject  string `json:"business_object,omitempty"`
	ContractName    string `json:"contract_name,omitempty"`
	ContractKind    string `json:"contract_kind,omitempty"`
	Direction       string `json:"direction,omitempty"`
	ParamName       string `json:"param_name"`
	TypeName        string `json:"type_name,omitempty"`
	Required        bool   `json:"required,omitempty"`
	Description     string `json:"description,omitempty"`
	File            string `json:"file,omitempty"`
	LineNumber      int    `json:"line_number,omitempty"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

type APITableFieldResult struct {
	FieldName   string `json:"field_name"`
	TypeName    string `json:"type_name,omitempty"`
	Required    bool   `json:"required,omitempty"`
	Description string `json:"description,omitempty"`
	LineNumber  int    `json:"line_number,omitempty"`
}

type APITableIndexFieldResult struct {
	FieldName  string `json:"field_name"`
	FieldOrder int    `json:"field_order,omitempty"`
	LineNumber int    `json:"line_number,omitempty"`
}

type APITableIndexResult struct {
	ID              int64                      `json:"id"`
	BusinessTableID int64                      `json:"business_table_id,omitempty"`
	BusinessObject  string                     `json:"business_object,omitempty"`
	TableName       string                     `json:"table_name"`
	IndexName       string                     `json:"index_name"`
	IndexFields     string                     `json:"index_fields,omitempty"`
	IndexType       int                        `json:"index_type,omitempty"`
	IsClustered     bool                       `json:"is_clustered,omitempty"`
	File            string                     `json:"file,omitempty"`
	LineNumber      int                        `json:"line_number,omitempty"`
	Product         string                     `json:"product,omitempty"`
	BranchUsed      string                     `json:"branch_used,omitempty"`
	BranchRequested string                     `json:"branch_requested,omitempty"`
	Fields          []APITableIndexFieldResult `json:"fields,omitempty"`
}

type APITableResult struct {
	ID              int64                 `json:"id"`
	ContractID      *int64                `json:"contract_id,omitempty"`
	BusinessObject  string                `json:"business_object,omitempty"`
	ContractName    string                `json:"contract_name,omitempty"`
	ContractKind    string                `json:"contract_kind,omitempty"`
	Direction       string                `json:"direction,omitempty"`
	TableName       string                `json:"table_name"`
	Description     string                `json:"description,omitempty"`
	File            string                `json:"file,omitempty"`
	LineNumber      int                   `json:"line_number,omitempty"`
	Source          string                `json:"source,omitempty"`
	Product         string                `json:"product,omitempty"`
	BranchUsed      string                `json:"branch_used,omitempty"`
	BranchRequested string                `json:"branch_requested,omitempty"`
	InMemory        bool                  `json:"in_memory"`
	InMemoryProduct string                `json:"in_memory_product,omitempty"`
	InMemoryModule  string                `json:"in_memory_module,omitempty"`
	Fields          []APITableFieldResult `json:"fields,omitempty"`
}

type APIContractResult struct {
	ID                int64            `json:"id"`
	FileID            int64            `json:"file_id"`
	BusinessObject    string           `json:"business_object,omitempty"`
	ContractName      string           `json:"contract_name"`
	ContractKind      string           `json:"contract_kind"`
	ObjectTypeID      int              `json:"object_type_id,omitempty"`
	OwnerModule       string           `json:"owner_module,omitempty"`
	UsedObjectName    string           `json:"used_object_name,omitempty"`
	UsedModuleSysName string           `json:"used_module_sys_name,omitempty"`
	ShortDescription  string           `json:"short_description,omitempty"`
	FullDescription   string           `json:"full_description,omitempty"`
	File              string           `json:"file"`
	LineStart         int              `json:"line_start,omitempty"`
	LineEnd           int              `json:"line_end,omitempty"`
	Product           string           `json:"product,omitempty"`
	BranchUsed        string           `json:"branch_used,omitempty"`
	BranchRequested   string           `json:"branch_requested,omitempty"`
	Params            []APIParamResult `json:"params,omitempty"`
	Tables            []APITableResult `json:"tables,omitempty"`
}

type APIImplementationResult struct {
	ContractID      int64  `json:"contract_id"`
	ContractName    string `json:"contract_name"`
	ContractKind    string `json:"contract_kind"`
	ProcedureID     int64  `json:"procedure_id"`
	ProcedureName   string `json:"procedure_name"`
	File            string `json:"file"`
	RelationType    string `json:"relation_type"`
	IsIndirect      bool   `json:"is_indirect,omitempty"`
	ViaProcedure    string `json:"via_procedure,omitempty"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

type APIRelatedProcedureResult struct {
	ContractID      int64  `json:"contract_id"`
	ContractName    string `json:"contract_name"`
	ProcedureID     int64  `json:"procedure_id"`
	ProcedureName   string `json:"procedure_name"`
	File            string `json:"file"`
	RelationType    string `json:"relation_type"`
	IsIndirect      bool   `json:"is_indirect,omitempty"`
	ViaProcedure    string `json:"via_procedure,omitempty"`
	Product         string `json:"product,omitempty"`
	BranchUsed      string `json:"branch_used,omitempty"`
	BranchRequested string `json:"branch_requested,omitempty"`
}

func (q *Query) SearchAPIContract(ctx context.Context, name string, like bool, limit int) ([]APIContractResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"c.contract_name"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (имя, вид, продукт),
	// фаза 2 возвращает все контракты выбранной ветки.
	queryText := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "LOWER(c.contract_name)", Alias: "name_key"},
			{Expr: "c.contract_kind", Alias: "kind_key"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"c.id",
		`c.id, c.file_id, COALESCE(c.business_object,''), c.contract_name, c.contract_kind,
		 COALESCE(c.object_type_id,0), COALESCE(c.owner_module,''), COALESCE(c.used_object_name,''),
		 COALESCE(c.used_module_sys_name,''), COALESCE(c.short_description,''), COALESCE(c.full_description,''),
		 f.rel_path, c.line_start, c.line_end`,
		`api_contracts c
		JOIN files f ON f.id = c.file_id`+fileBranchJoin("f"),
		lookupCondition,
		`c.contract_name, c.contract_kind, c.id`,
	) + `
		LIMIT $2
	`
	rows, err := q.db.QueryContext(ctx, queryText, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APIContractResult, 0)
	for rows.Next() {
		var item APIContractResult
		if err := rows.Scan(&item.ID, &item.FileID, &item.BusinessObject, &item.ContractName, &item.ContractKind, &item.ObjectTypeID, &item.OwnerModule, &item.UsedObjectName, &item.UsedModuleSysName, &item.ShortDescription, &item.FullDescription, &item.File, &item.LineStart, &item.LineEnd, &item.Product, &item.BranchUsed); err != nil {
			return nil, err
		}
		params, err := q.loadAPIContractParams(ctx, item.ID)
		if err != nil {
			return nil, err
		}
		tables, err := q.loadAPIContractTables(ctx, item.ID)
		if err != nil {
			return nil, err
		}
		// Параметры и таблицы живут в том же файле, что и контракт: ветку
		// и продукт наследуют от выбранной копии контракта.
		for i := range params {
			params[i].Product = item.Product
			params[i].BranchUsed = item.BranchUsed
			params[i].BranchRequested = item.BranchRequested
		}
		for i := range tables {
			tables[i].Product = item.Product
			tables[i].BranchUsed = item.BranchUsed
			tables[i].BranchRequested = item.BranchRequested
		}
		item.Params = params
		item.Tables = tables
		item.BranchRequested = q.branchRequested(item.Product)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) SearchAPITable(ctx context.Context, name string, like bool, limit int) ([]APITableResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition1 := buildNameLookupCondition([]string{"t.table_name"}, like, 1)
	lookupCondition2 := buildNameLookupCondition([]string{"bt.table_name"}, like, 1)

	// Двухфазный дедуп в каждой ветке UNION: фаза 1 выбирает ветку для
	// (таблица, контракт/бизнес-объект, продукт), фаза 2 возвращает все
	// таблицы выбранной ветки. Внешний ORDER BY/LIMIT — на объединении.
	contractTables := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "t.table_name", Alias: "table_name"},
			{Expr: "COALESCE(c.contract_name,'')", Alias: "contract_name"},
			{Expr: "COALESCE(c.contract_kind,'')", Alias: "contract_kind"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"t.id",
		`t.id, t.contract_id, COALESCE(c.business_object,''), COALESCE(c.contract_name,''), COALESCE(c.contract_kind,''),
		 t.direction, t.table_name, COALESCE(t.description,''), COALESCE(f.rel_path,''), t.line_number, 'contract' as source`,
		`api_contract_tables t
		LEFT JOIN api_contracts c ON c.id = t.contract_id
		LEFT JOIN files f ON f.id = c.file_id`+fileBranchJoin("f"),
		lookupCondition1,
		`t.table_name, COALESCE(c.contract_name,''), COALESCE(c.contract_kind,''), t.id`,
	)
	businessObjectTables := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "bt.table_name", Alias: "table_name"},
			{Expr: "bt.business_object", Alias: "business_object"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"bt.id",
		`bt.id, NULL::bigint, bt.business_object, ''::text, ''::text,
		 ''::text, bt.table_name, COALESCE(bt.description,''), COALESCE(f.rel_path,''), bt.line_number, 'business_object' as source`,
		`api_business_object_tables bt
		LEFT JOIN files f ON f.id = bt.file_id`+fileBranchJoin("f"),
		lookupCondition2,
		`bt.table_name, bt.business_object, bt.id`,
	)

	queryText := `SELECT * FROM (
			(` + contractTables + `
			)
			UNION ALL
			(` + businessObjectTables + `
			)
		) dedup
		ORDER BY table_name, source, id
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, queryText, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APITableResult, 0)
	for rows.Next() {
		var item APITableResult
		if err := rows.Scan(&item.ID, &item.ContractID, &item.BusinessObject, &item.ContractName, &item.ContractKind, &item.Direction, &item.TableName, &item.Description, &item.File, &item.LineNumber, &item.Source, &item.Product, &item.BranchUsed); err != nil {
			return nil, err
		}

		var fieldRows *sql.Rows
		var err error

		if item.Source == "contract" {
			fieldRows, err = q.db.QueryContext(ctx, `SELECT field_name, COALESCE(type_name,''), required, COALESCE(description,''), line_number FROM api_contract_table_fields WHERE contract_table_id = $1 ORDER BY param_order, id`, item.ID)
		} else {
			fieldRows, err = q.db.QueryContext(ctx, `SELECT field_name, COALESCE(type_name,''), FALSE, COALESCE(description,''), line_number FROM api_business_object_table_fields WHERE business_table_id = $1 ORDER BY param_order, id`, item.ID)
		}

		if err != nil {
			return nil, err
		}
		for fieldRows.Next() {
			var field APITableFieldResult
			if err := fieldRows.Scan(&field.FieldName, &field.TypeName, &field.Required, &field.Description, &field.LineNumber); err != nil {
				fieldRows.Close()
				return nil, err
			}
			item.Fields = append(item.Fields, field)
		}
		fieldRows.Close()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := q.enrichInMemoryFlag(ctx, len(items),
		func(i int) string { return items[i].TableName },
		func(i int, inMemory bool, product, module string) {
			items[i].InMemory = inMemory
			items[i].InMemoryProduct = product
			items[i].InMemoryModule = module
		}); err != nil {
		return nil, err
	}

	return items, nil
}

// enrichInMemoryFlag enriches a result set with the in-memory table flag
// («объявлена как таблица в памяти» из ModuleTableInMemory.lst): single batch
// query for unique table names; the join is by table name only (no product
// scope — the physical table is one per DB), and with multiple records (copies
// of .lst across branches) the default branch wins by SORTING
// (COALESCE(is_default) DESC, id), never by filtering — the only copy may
// live in a non-default branch.
func (q *Query) enrichInMemoryFlag(ctx context.Context, n int,
	getName func(i int) string,
	setResult func(i int, inMemory bool, product, module string),
) error {
	if n == 0 {
		return nil
	}
	names := make([]string, 0, n)
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		name := getName(i)
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}

	values := make([]string, len(names))
	args := make([]interface{}, 0, len(names))
	for i, name := range names {
		values[i] = fmt.Sprintf("($%d::text)", i+1)
		args = append(args, name)
	}

	rows, err := q.db.QueryContext(ctx, `
		SELECT k.name, (m.table_name IS NOT NULL), COALESCE(m.product_name, ''), COALESCE(m.module_name, '')
		FROM (VALUES `+strings.Join(values, ",")+`) AS k(name)
		LEFT JOIN LATERAL (
			SELECT mt.table_name, mt.product_name, mt.module_name
			FROM memory_tables mt
			JOIN files mf ON mf.id = mt.file_id
			LEFT JOIN ds_products_branch mbr ON mbr.id = mf.ds_product_branch_id
			WHERE LOWER(mt.table_name) = LOWER(k.name)
			ORDER BY COALESCE(mbr.is_default, FALSE) DESC, mt.id
			LIMIT 1
		) m ON TRUE`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	type inMemoryInfo struct {
		inMemory bool
		product  string
		module   string
	}
	byName := make(map[string]inMemoryInfo, len(names))
	for rows.Next() {
		var name string
		var info inMemoryInfo
		if err := rows.Scan(&name, &info.inMemory, &info.product, &info.module); err != nil {
			return err
		}
		byName[name] = info
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		if info, ok := byName[getName(i)]; ok {
			setResult(i, info.inMemory, info.product, info.module)
		}
	}
	return nil
}

func (q *Query) SearchAPITableIndex(ctx context.Context, name string, likeSearch bool, limit int) ([]APITableIndexResult, error) {
	searchValue := strings.TrimSpace(name)
	whereClause := "LOWER(i.index_name) = LOWER($1) OR LOWER(t.table_name) = LOWER($1)"
	if likeSearch {
		whereClause = "i.index_name ILIKE $1 OR t.table_name ILIKE $1"
		searchValue = "%" + searchValue + "%"
	}
	// Двухфазный дедуп: фаза 1 выбирает ветку для (таблица, индекс, продукт),
	// фаза 2 возвращает все индексы выбранной ветки.
	queryText := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "t.table_name", Alias: "table_name"},
			{Expr: "i.index_name", Alias: "index_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"i.id",
		`i.id, i.business_table_id, t.business_object, t.table_name, i.index_name,
		 COALESCE(i.index_fields,''), i.index_type, i.is_clustered, COALESCE(f.rel_path,''), i.line_number`,
		`api_business_object_table_indexes i
		JOIN api_business_object_tables t ON t.id = i.business_table_id
		JOIN files f ON f.id = t.file_id`+fileBranchJoin("f"),
		whereClause,
		`t.table_name, i.index_name, i.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, queryText, searchValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APITableIndexResult, 0)
	for rows.Next() {
		var item APITableIndexResult
		if err := rows.Scan(&item.ID, &item.BusinessTableID, &item.BusinessObject, &item.TableName, &item.IndexName, &item.IndexFields, &item.IndexType, &item.IsClustered, &item.File, &item.LineNumber, &item.Product, &item.BranchUsed); err != nil {
			return nil, err
		}
		fieldRows, err := q.db.QueryContext(ctx, `SELECT field_name, field_order, line_number FROM api_business_object_table_index_fields WHERE table_index_id = $1 ORDER BY field_order, id`, item.ID)
		if err != nil {
			return nil, err
		}
		for fieldRows.Next() {
			var field APITableIndexFieldResult
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

func (q *Query) SearchAPIParam(ctx context.Context, name string, like bool, limit int) ([]APIParamResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"p.param_name"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (параметр, контракт, продукт),
	// фаза 2 возвращает все параметры выбранной ветки.
	queryText := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "p.param_name", Alias: "param_name"},
			{Expr: "COALESCE(c.contract_name,'')", Alias: "contract_name"},
			{Expr: "COALESCE(c.contract_kind,'')", Alias: "contract_kind"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"p.id",
		`p.id, p.contract_id, COALESCE(c.business_object,''), COALESCE(c.contract_name,''), COALESCE(c.contract_kind,''),
		 p.direction, p.param_name, COALESCE(p.type_name,''), p.required, COALESCE(p.description,''), COALESCE(f.rel_path,''), p.line_number`,
		`api_contract_params p
		LEFT JOIN api_contracts c ON c.id = p.contract_id
		LEFT JOIN files f ON f.id = c.file_id`+fileBranchJoin("f"),
		lookupCondition,
		`p.param_name, p.id`,
	) + `
		LIMIT $2
	`
	rows, err := q.db.QueryContext(ctx, queryText, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APIParamResult, 0)
	for rows.Next() {
		var item APIParamResult
		if err := rows.Scan(&item.ID, &item.ContractID, &item.BusinessObject, &item.ContractName, &item.ContractKind, &item.Direction, &item.ParamName, &item.TypeName, &item.Required, &item.Description, &item.File, &item.LineNumber, &item.Product, &item.BranchUsed); err != nil {
			return nil, err
		}
		item.BranchRequested = q.branchRequested(item.Product)
		items = append(items, item)
	}
	return items, rows.Err()
}

// apiChosenContractsCTE — CTE выбора единственной копии контракта
// (приоритет default-ветки) для графовых запросов: рёбра связей берутся
// только в пределах выбранной копии и не дублируются второй веткой.
// kind входит в ключ выбора: одноимённые event/callback_event/service —
// независимые контракты. ilikeParam — плейсхолдер фильтра имени контракта.
func (q *Query) apiChosenContractsCTE(ilikeParam string) string {
	return `
		WITH chosen AS (
			SELECT DISTINCT ON (LOWER(c.contract_name), LOWER(c.contract_kind), COALESCE(f.ds_product_id, 0))
			       c.id, c.contract_name, c.contract_kind,
			       ` + fileBranchColumns + `
			FROM api_contracts c
			JOIN files f ON f.id = c.file_id` + fileBranchJoin("f") + `
			WHERE c.contract_name ILIKE ` + ilikeParam + `
			ORDER BY LOWER(c.contract_name), LOWER(c.contract_kind), COALESCE(f.ds_product_id, 0), ` + q.dedupPriorityTail("c.id") + `
		)`
}

func (q *Query) SearchAPIImplementations(ctx context.Context, name string, limit int) ([]APIImplementationResult, error) {
	rows, err := q.db.QueryContext(ctx, q.apiChosenContractsCTE("$1")+`
		SELECT *
		FROM (
			SELECT ch.id, ch.contract_name, ch.contract_kind, p.id, p.proc_name AS procedure_name, f.rel_path, r.relation_type,
			       FALSE AS is_indirect, '' AS via_procedure, ch.product, ch.branch_used
			FROM relations r
			JOIN chosen ch ON ch.id = r.target_id AND r.target_type = 'api_contract'
			JOIN sql_procedures p ON p.id = r.source_id AND r.source_type = 'sql_procedure'
			JOIN files f ON f.id = p.file_id
			WHERE r.relation_type = 'implements_contract'

			UNION

			SELECT ch.id, ch.contract_name, ch.contract_kind, p_indirect.id, p_indirect.proc_name AS procedure_name, f_indirect.rel_path,
			       CONCAT(r_impl.relation_type, '->', r_chain.relation_type) AS relation_type,
			       TRUE AS is_indirect, p_direct.proc_name AS via_procedure, ch.product, ch.branch_used
			FROM relations r_impl
			JOIN chosen ch ON ch.id = r_impl.target_id AND r_impl.target_type = 'api_contract'
			JOIN sql_procedures p_direct ON p_direct.id = r_impl.source_id AND r_impl.source_type = 'sql_procedure'
			JOIN relations r_chain ON r_chain.source_type = 'sql_procedure' AND r_chain.source_id = p_direct.id
			JOIN sql_procedures p_indirect ON p_indirect.id = r_chain.target_id AND r_chain.target_type = 'sql_procedure'
			JOIN files f_indirect ON f_indirect.id = p_indirect.file_id
			WHERE r_impl.relation_type = 'implements_contract'
			  AND r_chain.relation_type IN ('dispatches_to', 'dispatches_to_subscriber', 'calls_procedure')
		) impl
		ORDER BY contract_name, is_indirect, procedure_name
		LIMIT $2
	`, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APIImplementationResult, 0)
	for rows.Next() {
		var item APIImplementationResult
		if err := rows.Scan(&item.ContractID, &item.ContractName, &item.ContractKind, &item.ProcedureID, &item.ProcedureName, &item.File, &item.RelationType, &item.IsIndirect, &item.ViaProcedure, &item.Product, &item.BranchUsed); err != nil {
			return nil, err
		}
		item.BranchRequested = q.branchRequested(item.Product)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) SearchAPIPublishers(ctx context.Context, name string, limit int) ([]APIRelatedProcedureResult, error) {
	rows, err := q.db.QueryContext(ctx, q.apiChosenContractsCTE("$1")+`
		SELECT *
		FROM (
			SELECT ch.id, ch.contract_name, p.id, p.proc_name AS procedure_name, f.rel_path, r.relation_type,
			       FALSE AS is_indirect, '' AS via_procedure, ch.product, ch.branch_used
			FROM relations r
			JOIN chosen ch ON ch.id = r.target_id AND r.target_type = 'api_contract'
			JOIN sql_procedures p ON p.id = r.source_id AND r.source_type = 'sql_procedure'
			JOIN files f ON f.id = p.file_id
			WHERE r.relation_type = 'publishes_event'

			UNION

			SELECT ch.id, ch.contract_name, p_indirect.id, p_indirect.proc_name AS procedure_name, f_indirect.rel_path,
			       CONCAT(r_direct.relation_type, '->', r_chain.relation_type) AS relation_type,
			       TRUE AS is_indirect, p_direct.proc_name AS via_procedure, ch.product, ch.branch_used
			FROM relations r_direct
			JOIN chosen ch ON ch.id = r_direct.target_id AND r_direct.target_type = 'api_contract'
			JOIN sql_procedures p_direct ON p_direct.id = r_direct.source_id AND r_direct.source_type = 'sql_procedure'
			JOIN relations r_chain ON r_chain.source_type = 'sql_procedure' AND r_chain.source_id = p_direct.id
			JOIN sql_procedures p_indirect ON p_indirect.id = r_chain.target_id AND r_chain.target_type = 'sql_procedure'
			JOIN files f_indirect ON f_indirect.id = p_indirect.file_id
			WHERE r_direct.relation_type = 'publishes_event'
			  AND r_chain.relation_type IN ('dispatches_to', 'dispatches_to_subscriber', 'calls_procedure')

			UNION

			SELECT ch.id, ch.contract_name, cb.id, cb.contract_name AS procedure_name, f_cb.rel_path,
			       r.relation_type, FALSE AS is_indirect, '' AS via_procedure, ch.product, ch.branch_used
			FROM relations r
			JOIN chosen ch ON ch.id = r.target_id AND r.target_type = 'api_contract'
			JOIN api_contracts cb ON cb.id = r.source_id AND r.source_type = 'api_contract'
			JOIN files f_cb ON f_cb.id = cb.file_id
			WHERE r.relation_type = 'subscribes_to_event'
			  AND LOWER(cb.contract_kind) = 'callback_event'
			  AND LOWER(ch.contract_kind) = 'event'
		) rel
		ORDER BY contract_name, is_indirect, procedure_name
		LIMIT $2
	`, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APIRelatedProcedureResult, 0)
	for rows.Next() {
		var item APIRelatedProcedureResult
		if err := rows.Scan(&item.ContractID, &item.ContractName, &item.ProcedureID, &item.ProcedureName, &item.File, &item.RelationType, &item.IsIndirect, &item.ViaProcedure, &item.Product, &item.BranchUsed); err != nil {
			return nil, err
		}
		item.BranchRequested = q.branchRequested(item.Product)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) SearchAPIConsumers(ctx context.Context, name string, limit int) ([]APIRelatedProcedureResult, error) {
	return q.searchAPIRelatedProcedures(ctx, name, "executes_contract", limit)
}

func (q *Query) searchAPIRelatedProcedures(ctx context.Context, name string, relationType string, limit int) ([]APIRelatedProcedureResult, error) {
	rows, err := q.db.QueryContext(ctx, q.apiChosenContractsCTE("$2")+`
		SELECT *
		FROM (
			SELECT ch.id, ch.contract_name, p.id, p.proc_name AS procedure_name, f.rel_path, r.relation_type,
			       FALSE AS is_indirect, '' AS via_procedure, ch.product, ch.branch_used
			FROM relations r
			JOIN chosen ch ON ch.id = r.target_id AND r.target_type = 'api_contract'
			JOIN sql_procedures p ON p.id = r.source_id AND r.source_type = 'sql_procedure'
			JOIN files f ON f.id = p.file_id
			WHERE r.relation_type = $1

			UNION

			SELECT ch.id, ch.contract_name, p_indirect.id, p_indirect.proc_name AS procedure_name, f_indirect.rel_path,
			       CONCAT(r_direct.relation_type, '->', r_chain.relation_type) AS relation_type,
			       TRUE AS is_indirect, p_direct.proc_name AS via_procedure, ch.product, ch.branch_used
			FROM relations r_direct
			JOIN chosen ch ON ch.id = r_direct.target_id AND r_direct.target_type = 'api_contract'
			JOIN sql_procedures p_direct ON p_direct.id = r_direct.source_id AND r_direct.source_type = 'sql_procedure'
			JOIN relations r_chain ON r_chain.source_type = 'sql_procedure' AND r_chain.source_id = p_direct.id
			JOIN sql_procedures p_indirect ON p_indirect.id = r_chain.target_id AND r_chain.target_type = 'sql_procedure'
			JOIN files f_indirect ON f_indirect.id = p_indirect.file_id
			WHERE r_direct.relation_type = $1
			  AND r_chain.relation_type IN ('dispatches_to', 'dispatches_to_subscriber', 'calls_procedure')
		) rel
		ORDER BY contract_name, is_indirect, procedure_name
		LIMIT $3
	`, relationType, "%"+name+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APIRelatedProcedureResult, 0)
	for rows.Next() {
		var item APIRelatedProcedureResult
		if err := rows.Scan(&item.ContractID, &item.ContractName, &item.ProcedureID, &item.ProcedureName, &item.File, &item.RelationType, &item.IsIndirect, &item.ViaProcedure, &item.Product, &item.BranchUsed); err != nil {
			return nil, err
		}
		item.BranchRequested = q.branchRequested(item.Product)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) loadAPIContractParams(ctx context.Context, contractID int64) ([]APIParamResult, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT id, contract_id, direction, param_name, COALESCE(type_name,''), required, COALESCE(description,''), line_number FROM api_contract_params WHERE contract_id = $1 ORDER BY direction, param_order, id`, contractID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APIParamResult, 0)
	for rows.Next() {
		var item APIParamResult
		if err := rows.Scan(&item.ID, &item.ContractID, &item.Direction, &item.ParamName, &item.TypeName, &item.Required, &item.Description, &item.LineNumber); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) loadAPIContractTables(ctx context.Context, contractID int64) ([]APITableResult, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT id, contract_id, direction, table_name, COALESCE(description,''), line_number FROM api_contract_tables WHERE contract_id = $1 ORDER BY direction, param_order, id`, contractID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APITableResult, 0)
	for rows.Next() {
		var item APITableResult
		var contractIDVal int64
		if err := rows.Scan(&item.ID, &contractIDVal, &item.Direction, &item.TableName, &item.Description, &item.LineNumber); err != nil {
			return nil, err
		}
		item.ContractID = &contractIDVal
		fieldRows, err := q.db.QueryContext(ctx, `SELECT field_name, COALESCE(type_name,''), required, COALESCE(description,''), line_number FROM api_contract_table_fields WHERE contract_table_id = $1 ORDER BY param_order, id`, item.ID)
		if err != nil {
			return nil, err
		}
		for fieldRows.Next() {
			var field APITableFieldResult
			if err := fieldRows.Scan(&field.FieldName, &field.TypeName, &field.Required, &field.Description, &field.LineNumber); err != nil {
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
