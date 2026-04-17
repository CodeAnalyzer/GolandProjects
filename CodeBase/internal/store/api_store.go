package store

import (
	"database/sql"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

func (db *DB) BatchInsertAPIBusinessObjects(items []*model.APIBusinessObject, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_business_objects", "file_id", "business_object", "module_name", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.FileID, sanitizeUTF8String(item.BusinessObject), NullableString(item.ModuleName), item.LineStart, item.LineEnd); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func BuildAPIBusinessObjectTableLookupKey(businessObject string, tableName string) string {
	return strings.ToLower(strings.TrimSpace(businessObject)) + "|" + strings.ToLower(strings.TrimSpace(tableName))
}

func (db *DB) FindAPIBusinessObjectTableIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`SELECT id, business_object, table_name FROM api_business_object_tables WHERE file_id = $1 ORDER BY id DESC`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var businessObject string
		var tableName string
		if err := rows.Scan(&id, &businessObject, &tableName); err != nil {
			return nil, err
		}
		key := BuildAPIBusinessObjectTableLookupKey(businessObject, tableName)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

func (db *DB) BatchInsertAPIBusinessObjectTableFields(items []*model.APIBusinessObjectTableField, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_business_object_table_fields", "business_table_id", "field_name", "type_name", "ws_param_name", "rus_name", "description", "param_order", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.BusinessTableID, sanitizeUTF8String(item.FieldName), NullableString(item.TypeName), NullableString(item.WsParamName), NullableString(item.RusName), NullableString(item.Description), item.ParamOrder, item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func BuildAPIBusinessObjectTableIndexLookupKey(businessObject string, tableName string, indexName string) string {
	return strings.ToLower(strings.TrimSpace(businessObject)) + "|" + strings.ToLower(strings.TrimSpace(tableName)) + "|" + strings.ToLower(strings.TrimSpace(indexName))
}

func (db *DB) FindAPIBusinessObjectTableIndexIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT i.id, t.business_object, t.table_name, i.index_name
		FROM api_business_object_table_indexes i
		JOIN api_business_object_tables t ON t.id = i.business_table_id
		WHERE t.file_id = $1
		ORDER BY i.id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var businessObject string
		var tableName string
		var indexName string
		if err := rows.Scan(&id, &businessObject, &tableName, &indexName); err != nil {
			return nil, err
		}
		key := BuildAPIBusinessObjectTableIndexLookupKey(businessObject, tableName, indexName)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

func (db *DB) BatchInsertAPIBusinessObjectTableIndexes(items []*model.APIBusinessObjectTableIndex, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_business_object_table_indexes", "business_table_id", "index_name", "index_fields", "index_type", "is_clustered", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.BusinessTableID, sanitizeUTF8String(item.IndexName), NullableString(item.IndexFields), item.IndexType, item.IsClustered, item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func (db *DB) BatchInsertAPIBusinessObjectTableIndexFields(items []*model.APIBusinessObjectTableIndexField, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_business_object_table_index_fields", "table_index_id", "field_name", "field_order", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.TableIndexID, sanitizeUTF8String(item.FieldName), item.FieldOrder, item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func (db *DB) FindAPIBusinessObjectIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`SELECT id, business_object FROM api_business_objects WHERE file_id = $1 ORDER BY id DESC`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		key := strings.ToLower(strings.TrimSpace(name))
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

func (db *DB) BatchInsertAPIContracts(items []*model.APIContract, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_contracts", "file_id", "business_object_id", "business_object", "contract_name", "contract_kind", "object_type_id", "object_name_id", "api_version", "arch_approval", "implemented", "internal_use", "deprecated", "is_external", "owner_module", "used_object_name", "used_module_sys_name", "short_description", "full_description", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.FileID, NullableInt64(item.BusinessObjectID), NullableString(item.BusinessObject), sanitizeUTF8String(item.ContractName), sanitizeUTF8String(item.ContractKind), NullableInt(item.ObjectTypeID), NullableInt64(item.ObjectNameID), NullableInt(item.APIVersion), NullableInt(item.ArchApproval), item.Implemented, item.InternalUse, item.Deprecated, item.IsExternal, NullableString(item.OwnerModule), NullableString(item.UsedObjectName), NullableString(item.UsedModuleSysName), NullableString(item.ShortDescription), NullableString(item.FullDescription), item.LineStart, item.LineEnd); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func (db *DB) FindAPIContractIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`SELECT id, contract_name, contract_kind FROM api_contracts WHERE file_id = $1 ORDER BY id DESC`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var name, kind string
		if err := rows.Scan(&id, &name, &kind); err != nil {
			return nil, err
		}
		key := BuildAPIContractLookupKey(name, kind)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

func (db *DB) FindLatestAPIContractIDByNameAndKind(name string, kind string) (int64, error) {
	var id int64
	err := db.QueryRow(`SELECT id FROM api_contracts WHERE LOWER(contract_name)=LOWER($1) AND LOWER(contract_kind)=LOWER($2) ORDER BY id DESC LIMIT 1`, strings.TrimSpace(name), strings.TrimSpace(kind)).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func BuildAPIContractLookupKey(name string, kind string) string {
	return strings.ToLower(strings.TrimSpace(name)) + "|" + strings.ToLower(strings.TrimSpace(kind))
}

func (db *DB) BatchInsertAPIContractParams(items []*model.APIContractParam, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_contract_params", "contract_id", "direction", "param_name", "prm_sub_object", "type_name", "required", "rus_name", "description", "ws_param_name", "param_order", "is_virtual_link", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.ContractID, sanitizeUTF8String(item.Direction), sanitizeUTF8String(item.ParamName), NullableString(item.PrmSubObject), NullableString(item.TypeName), item.Required, NullableString(item.RusName), NullableString(item.Description), NullableString(item.WsParamName), item.ParamOrder, item.IsVirtualLink, item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func (db *DB) BatchInsertAPIContractTables(items []*model.APIContractTable, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_contract_tables", "contract_id", "direction", "table_name", "ws_param_name", "required", "rus_name", "description", "param_order", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.ContractID, sanitizeUTF8String(item.Direction), sanitizeUTF8String(item.TableName), NullableString(item.WsParamName), item.Required, NullableString(item.RusName), NullableString(item.Description), item.ParamOrder, item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func BuildAPIContractTableLookupKey(direction string, tableName string) string {
	return strings.ToLower(strings.TrimSpace(direction)) + "|" + strings.ToLower(strings.TrimSpace(tableName))
}

func (db *DB) FindAPIContractTableIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`SELECT t.id, t.direction, t.table_name FROM api_contract_tables t JOIN api_contracts c ON c.id = t.contract_id WHERE c.file_id = $1 ORDER BY t.id DESC`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var direction string
		var tableName string
		if err := rows.Scan(&id, &direction, &tableName); err != nil {
			return nil, err
		}
		key := BuildAPIContractTableLookupKey(direction, tableName)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

func (db *DB) BatchInsertAPIContractTableFields(items []*model.APIContractTableField, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_contract_table_fields", "contract_table_id", "field_name", "type_name", "required", "rus_name", "description", "ws_param_name", "param_order", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.ContractTableID, sanitizeUTF8String(item.FieldName), NullableString(item.TypeName), item.Required, NullableString(item.RusName), NullableString(item.Description), NullableString(item.WsParamName), item.ParamOrder, item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func (db *DB) BatchInsertAPIContractReturnValues(items []*model.APIContractReturnValue, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_contract_return_values", "contract_id", "value", "return_type", "description", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.ContractID, NullableString(item.Value), item.ReturnType, NullableString(item.Description), item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func (db *DB) BatchInsertAPIContractContexts(items []*model.APIContractContext, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_contract_contexts", "contract_id", "context_name", "type_name", "rus_name", "description", "context_order", "context_value", "is_virtual_link", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.ContractID, NullableString(item.ContextName), NullableString(item.TypeName), NullableString(item.RusName), NullableString(item.Description), item.ContextOrder, NullableString(item.ContextValue), item.IsVirtualLink, item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func (db *DB) BatchInsertAPIBusinessObjectParams(items []*model.APIBusinessObjectParam, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_business_object_params", "file_id", "business_object", "param_name", "prm_sub_object", "type_name", "ws_param_name", "rus_name", "description", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.FileID, sanitizeUTF8String(item.BusinessObject), sanitizeUTF8String(item.ParamName), NullableString(item.PrmSubObject), NullableString(item.TypeName), NullableString(item.WsParamName), NullableString(item.RusName), NullableString(item.Description), item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

func (db *DB) BatchInsertAPIBusinessObjectTables(items []*model.APIBusinessObjectTable, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_business_object_tables", "file_id", "business_object", "table_name", "type_name", "ws_param_name", "rus_name", "description", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.FileID, sanitizeUTF8String(item.BusinessObject), sanitizeUTF8String(item.TableName), NullableString(item.TypeName), NullableString(item.WsParamName), NullableString(item.RusName), NullableString(item.Description), item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}
