package query

import (
	"fmt"
	"strings"
)

type APIParamResult struct {
	ID             int64  `json:"id"`
	ContractID     int64  `json:"contract_id,omitempty"`
	BusinessObject string `json:"business_object,omitempty"`
	ContractName   string `json:"contract_name,omitempty"`
	ContractKind   string `json:"contract_kind,omitempty"`
	Direction      string `json:"direction,omitempty"`
	ParamName      string `json:"param_name"`
	TypeName       string `json:"type_name,omitempty"`
	Required       bool   `json:"required,omitempty"`
	Description    string `json:"description,omitempty"`
	File           string `json:"file,omitempty"`
	LineNumber     int    `json:"line_number,omitempty"`
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
	ID             int64                    `json:"id"`
	BusinessTableID int64                   `json:"business_table_id,omitempty"`
	BusinessObject string                   `json:"business_object,omitempty"`
	TableName      string                   `json:"table_name"`
	IndexName      string                   `json:"index_name"`
	IndexFields    string                   `json:"index_fields,omitempty"`
	IndexType      int                      `json:"index_type,omitempty"`
	IsClustered    bool                     `json:"is_clustered,omitempty"`
	File           string                   `json:"file,omitempty"`
	LineNumber     int                      `json:"line_number,omitempty"`
	Fields         []APITableIndexFieldResult `json:"fields,omitempty"`
}

type APITableResult struct {
	ID             int64                 `json:"id"`
	ContractID     int64                 `json:"contract_id,omitempty"`
	BusinessObject string                `json:"business_object,omitempty"`
	ContractName   string                `json:"contract_name,omitempty"`
	ContractKind   string                `json:"contract_kind,omitempty"`
	Direction      string                `json:"direction,omitempty"`
	TableName      string                `json:"table_name"`
	Description    string                `json:"description,omitempty"`
	File           string                `json:"file,omitempty"`
	LineNumber     int                   `json:"line_number,omitempty"`
	Fields         []APITableFieldResult `json:"fields,omitempty"`
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
	Params            []APIParamResult `json:"params,omitempty"`
	Tables            []APITableResult `json:"tables,omitempty"`
}

type APIImplementationResult struct {
	ContractID    int64  `json:"contract_id"`
	ContractName  string `json:"contract_name"`
	ContractKind  string `json:"contract_kind"`
	ProcedureID   int64  `json:"procedure_id"`
	ProcedureName string `json:"procedure_name"`
	File          string `json:"file"`
	RelationType  string `json:"relation_type"`
	IsIndirect    bool   `json:"is_indirect,omitempty"`
	ViaProcedure  string `json:"via_procedure,omitempty"`
}

type APIRelatedProcedureResult struct {
	ContractID    int64  `json:"contract_id"`
	ContractName  string `json:"contract_name"`
	ProcedureID   int64  `json:"procedure_id"`
	ProcedureName string `json:"procedure_name"`
	File          string `json:"file"`
	RelationType  string `json:"relation_type"`
	IsIndirect    bool   `json:"is_indirect,omitempty"`
	ViaProcedure  string `json:"via_procedure,omitempty"`
}

func (q *Query) SearchAPIContract(name string, like bool, limit int) ([]APIContractResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"c.contract_name"}, like, 1)
	rows, err := q.db.Query(`
		SELECT c.id, c.file_id, COALESCE(c.business_object,''), c.contract_name, c.contract_kind,
		       COALESCE(c.object_type_id,0), COALESCE(c.owner_module,''), COALESCE(c.used_object_name,''),
		       COALESCE(c.used_module_sys_name,''), COALESCE(c.short_description,''), COALESCE(c.full_description,''),
		       f.rel_path, c.line_start, c.line_end
		FROM api_contracts c
		JOIN files f ON f.id = c.file_id
		WHERE `+lookupCondition+`
		ORDER BY c.contract_name, c.id DESC
		LIMIT $2
	`, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APIContractResult, 0)
	for rows.Next() {
		var item APIContractResult
		if err := rows.Scan(&item.ID, &item.FileID, &item.BusinessObject, &item.ContractName, &item.ContractKind, &item.ObjectTypeID, &item.OwnerModule, &item.UsedObjectName, &item.UsedModuleSysName, &item.ShortDescription, &item.FullDescription, &item.File, &item.LineStart, &item.LineEnd); err != nil {
			return nil, err
		}
		params, err := q.loadAPIContractParams(item.ID)
		if err != nil {
			return nil, err
		}
		tables, err := q.loadAPIContractTables(item.ID)
		if err != nil {
			return nil, err
		}
		item.Params = params
		item.Tables = tables
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) SearchAPITable(name string, like bool, limit int) ([]APITableResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"t.table_name"}, like, 1)
	rows, err := q.db.Query(`
		SELECT t.id, t.contract_id, COALESCE(c.business_object,''), COALESCE(c.contract_name,''), COALESCE(c.contract_kind,''),
		       t.direction, t.table_name, COALESCE(t.description,''), COALESCE(f.rel_path,''), t.line_number
		FROM api_contract_tables t
		LEFT JOIN api_contracts c ON c.id = t.contract_id
		LEFT JOIN files f ON f.id = c.file_id
		WHERE `+lookupCondition+`
		ORDER BY t.table_name, t.id DESC
		LIMIT $2
	`, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APITableResult, 0)
	for rows.Next() {
		var item APITableResult
		if err := rows.Scan(&item.ID, &item.ContractID, &item.BusinessObject, &item.ContractName, &item.ContractKind, &item.Direction, &item.TableName, &item.Description, &item.File, &item.LineNumber); err != nil {
			return nil, err
		}
		fieldRows, err := q.db.Query(`SELECT field_name, COALESCE(type_name,''), required, COALESCE(description,''), line_number FROM api_contract_table_fields WHERE contract_table_id = $1 ORDER BY param_order, id`, item.ID)
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

func (q *Query) SearchAPITableIndex(name string, likeSearch bool, limit int) ([]APITableIndexResult, error) {
	queryText := `
		SELECT i.id, i.business_table_id, t.business_object, t.table_name, i.index_name,
		       COALESCE(i.index_fields,''), i.index_type, i.is_clustered, COALESCE(f.rel_path,''), i.line_number
		FROM api_business_object_table_indexes i
		JOIN api_business_object_tables t ON t.id = i.business_table_id
		JOIN files f ON f.id = t.file_id
		WHERE %s
		ORDER BY t.table_name, i.index_name, i.id DESC
		LIMIT $2
	`

	searchValue := strings.TrimSpace(name)
	whereClause := "LOWER(i.index_name) = LOWER($1) OR LOWER(t.table_name) = LOWER($1)"
	if likeSearch {
		whereClause = "i.index_name ILIKE $1 OR t.table_name ILIKE $1"
		searchValue = "%" + searchValue + "%"
	}
	queryText = fmt.Sprintf(queryText, whereClause)

	rows, err := q.db.Query(queryText, searchValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APITableIndexResult, 0)
	for rows.Next() {
		var item APITableIndexResult
		if err := rows.Scan(&item.ID, &item.BusinessTableID, &item.BusinessObject, &item.TableName, &item.IndexName, &item.IndexFields, &item.IndexType, &item.IsClustered, &item.File, &item.LineNumber); err != nil {
			return nil, err
		}
		fieldRows, err := q.db.Query(`SELECT field_name, field_order, line_number FROM api_business_object_table_index_fields WHERE table_index_id = $1 ORDER BY field_order, id`, item.ID)
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

func (q *Query) SearchAPIParam(name string, like bool, limit int) ([]APIParamResult, error) {
	lookupValue := buildLookupValue(name, like)
	lookupCondition := buildNameLookupCondition([]string{"p.param_name"}, like, 1)
	rows, err := q.db.Query(`
		SELECT p.id, p.contract_id, COALESCE(c.business_object,''), COALESCE(c.contract_name,''), COALESCE(c.contract_kind,''),
		       p.direction, p.param_name, COALESCE(p.type_name,''), p.required, COALESCE(p.description,''), COALESCE(f.rel_path,''), p.line_number
		FROM api_contract_params p
		LEFT JOIN api_contracts c ON c.id = p.contract_id
		LEFT JOIN files f ON f.id = c.file_id
		WHERE `+lookupCondition+`
		ORDER BY p.param_name, p.id DESC
		LIMIT $2
	`, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APIParamResult, 0)
	for rows.Next() {
		var item APIParamResult
		if err := rows.Scan(&item.ID, &item.ContractID, &item.BusinessObject, &item.ContractName, &item.ContractKind, &item.Direction, &item.ParamName, &item.TypeName, &item.Required, &item.Description, &item.File, &item.LineNumber); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) SearchAPIImplementations(name string, limit int) ([]APIImplementationResult, error) {
	rows, err := q.db.Query(`
		SELECT *
		FROM (
			SELECT c.id, c.contract_name, c.contract_kind, p.id, p.proc_name AS procedure_name, f.rel_path, r.relation_type,
			       FALSE AS is_indirect, '' AS via_procedure
			FROM relations r
			JOIN api_contracts c ON c.id = r.target_id AND r.target_type = 'api_contract'
			JOIN sql_procedures p ON p.id = r.source_id AND r.source_type = 'sql_procedure'
			JOIN files f ON f.id = p.file_id
			WHERE r.relation_type = 'implements_contract'
			  AND c.contract_name ILIKE $1

			UNION

			SELECT c.id, c.contract_name, c.contract_kind, p_indirect.id, p_indirect.proc_name AS procedure_name, f_indirect.rel_path,
			       CONCAT(r_impl.relation_type, '->', r_chain.relation_type) AS relation_type,
			       TRUE AS is_indirect, p_direct.proc_name AS via_procedure
			FROM relations r_impl
			JOIN api_contracts c ON c.id = r_impl.target_id AND r_impl.target_type = 'api_contract'
			JOIN sql_procedures p_direct ON p_direct.id = r_impl.source_id AND r_impl.source_type = 'sql_procedure'
			JOIN relations r_chain ON r_chain.source_type = 'sql_procedure' AND r_chain.source_id = p_direct.id
			JOIN sql_procedures p_indirect ON p_indirect.id = r_chain.target_id AND r_chain.target_type = 'sql_procedure'
			JOIN files f_indirect ON f_indirect.id = p_indirect.file_id
			WHERE r_impl.relation_type = 'implements_contract'
			  AND r_chain.relation_type IN ('dispatches_to', 'dispatches_to_subscriber', 'calls_procedure')
			  AND c.contract_name ILIKE $1
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
		if err := rows.Scan(&item.ContractID, &item.ContractName, &item.ContractKind, &item.ProcedureID, &item.ProcedureName, &item.File, &item.RelationType, &item.IsIndirect, &item.ViaProcedure); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) SearchAPIPublishers(name string, limit int) ([]APIRelatedProcedureResult, error) {
	rows, err := q.db.Query(`
		SELECT *
		FROM (
			SELECT c.id, c.contract_name, p.id, p.proc_name AS procedure_name, f.rel_path, r.relation_type,
			       FALSE AS is_indirect, '' AS via_procedure
			FROM relations r
			JOIN api_contracts c ON c.id = r.target_id AND r.target_type = 'api_contract'
			JOIN sql_procedures p ON p.id = r.source_id AND r.source_type = 'sql_procedure'
			JOIN files f ON f.id = p.file_id
			WHERE r.relation_type = 'publishes_event'
			  AND c.contract_name ILIKE $1

			UNION

			SELECT c.id, c.contract_name, p_indirect.id, p_indirect.proc_name AS procedure_name, f_indirect.rel_path,
			       CONCAT(r_direct.relation_type, '->', r_chain.relation_type) AS relation_type,
			       TRUE AS is_indirect, p_direct.proc_name AS via_procedure
			FROM relations r_direct
			JOIN api_contracts c ON c.id = r_direct.target_id AND r_direct.target_type = 'api_contract'
			JOIN sql_procedures p_direct ON p_direct.id = r_direct.source_id AND r_direct.source_type = 'sql_procedure'
			JOIN relations r_chain ON r_chain.source_type = 'sql_procedure' AND r_chain.source_id = p_direct.id
			JOIN sql_procedures p_indirect ON p_indirect.id = r_chain.target_id AND r_chain.target_type = 'sql_procedure'
			JOIN files f_indirect ON f_indirect.id = p_indirect.file_id
			WHERE r_direct.relation_type = 'publishes_event'
			  AND r_chain.relation_type IN ('dispatches_to', 'dispatches_to_subscriber', 'calls_procedure')
			  AND c.contract_name ILIKE $1

			UNION

			SELECT ev.id, ev.contract_name, cb.id, cb.contract_name AS procedure_name, f_cb.rel_path,
			       r.relation_type, FALSE AS is_indirect, '' AS via_procedure
			FROM relations r
			JOIN api_contracts cb ON cb.id = r.source_id AND r.source_type = 'api_contract'
			JOIN api_contracts ev ON ev.id = r.target_id AND r.target_type = 'api_contract'
			JOIN files f_cb ON f_cb.id = cb.file_id
			WHERE r.relation_type = 'subscribes_to_event'
			  AND LOWER(cb.contract_kind) = 'callback_event'
			  AND LOWER(ev.contract_kind) = 'event'
			  AND ev.contract_name ILIKE $1
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
		if err := rows.Scan(&item.ContractID, &item.ContractName, &item.ProcedureID, &item.ProcedureName, &item.File, &item.RelationType, &item.IsIndirect, &item.ViaProcedure); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) SearchAPIConsumers(name string, limit int) ([]APIRelatedProcedureResult, error) {
	return q.searchAPIRelatedProcedures(name, "executes_contract", limit)
}

func (q *Query) searchAPIRelatedProcedures(name string, relationType string, limit int) ([]APIRelatedProcedureResult, error) {
	rows, err := q.db.Query(`
		SELECT *
		FROM (
			SELECT c.id, c.contract_name, p.id, p.proc_name AS procedure_name, f.rel_path, r.relation_type,
			       FALSE AS is_indirect, '' AS via_procedure
			FROM relations r
			JOIN api_contracts c ON c.id = r.target_id AND r.target_type = 'api_contract'
			JOIN sql_procedures p ON p.id = r.source_id AND r.source_type = 'sql_procedure'
			JOIN files f ON f.id = p.file_id
			WHERE r.relation_type = $1
			  AND c.contract_name ILIKE $2

			UNION

			SELECT c.id, c.contract_name, p_indirect.id, p_indirect.proc_name AS procedure_name, f_indirect.rel_path,
			       CONCAT(r_direct.relation_type, '->', r_chain.relation_type) AS relation_type,
			       TRUE AS is_indirect, p_direct.proc_name AS via_procedure
			FROM relations r_direct
			JOIN api_contracts c ON c.id = r_direct.target_id AND r_direct.target_type = 'api_contract'
			JOIN sql_procedures p_direct ON p_direct.id = r_direct.source_id AND r_direct.source_type = 'sql_procedure'
			JOIN relations r_chain ON r_chain.source_type = 'sql_procedure' AND r_chain.source_id = p_direct.id
			JOIN sql_procedures p_indirect ON p_indirect.id = r_chain.target_id AND r_chain.target_type = 'sql_procedure'
			JOIN files f_indirect ON f_indirect.id = p_indirect.file_id
			WHERE r_direct.relation_type = $1
			  AND r_chain.relation_type IN ('dispatches_to', 'dispatches_to_subscriber', 'calls_procedure')
			  AND c.contract_name ILIKE $2
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
		if err := rows.Scan(&item.ContractID, &item.ContractName, &item.ProcedureID, &item.ProcedureName, &item.File, &item.RelationType, &item.IsIndirect, &item.ViaProcedure); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (q *Query) loadAPIContractParams(contractID int64) ([]APIParamResult, error) {
	rows, err := q.db.Query(`SELECT id, contract_id, direction, param_name, COALESCE(type_name,''), required, COALESCE(description,''), line_number FROM api_contract_params WHERE contract_id = $1 ORDER BY direction, param_order, id`, contractID)
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

func (q *Query) loadAPIContractTables(contractID int64) ([]APITableResult, error) {
	rows, err := q.db.Query(`SELECT id, contract_id, direction, table_name, COALESCE(description,''), line_number FROM api_contract_tables WHERE contract_id = $1 ORDER BY direction, param_order, id`, contractID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]APITableResult, 0)
	for rows.Next() {
		var item APITableResult
		if err := rows.Scan(&item.ID, &item.ContractID, &item.Direction, &item.TableName, &item.Description, &item.LineNumber); err != nil {
			return nil, err
		}
		fieldRows, err := q.db.Query(`SELECT field_name, COALESCE(type_name,''), required, COALESCE(description,''), line_number FROM api_contract_table_fields WHERE contract_table_id = $1 ORDER BY param_order, id`, item.ID)
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
