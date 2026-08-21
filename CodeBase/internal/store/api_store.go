package store

import (
	"context"
	"database/sql"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

func (db *DB) BatchInsertAPIBusinessObjects(ctx context.Context, items []*model.APIBusinessObject, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) FindAPIBusinessObjectTableIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, business_object, table_name FROM api_business_object_tables WHERE file_id = $1 ORDER BY id DESC`, fileID)
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

func (db *DB) BatchInsertAPIBusinessObjectTableFields(ctx context.Context, items []*model.APIBusinessObjectTableField, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) FindAPIBusinessObjectTableIndexIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `
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

func (db *DB) BatchInsertAPIBusinessObjectTableIndexes(ctx context.Context, items []*model.APIBusinessObjectTableIndex, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) BatchInsertAPIBusinessObjectTableIndexFields(ctx context.Context, items []*model.APIBusinessObjectTableIndexField, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) FindAPIBusinessObjectIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, business_object FROM api_business_objects WHERE file_id = $1 ORDER BY id DESC`, fileID)
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

func (db *DB) BatchInsertAPIContracts(ctx context.Context, items []*model.APIContract, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) FindAPIContractIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, contract_name, contract_kind FROM api_contracts WHERE file_id = $1 ORDER BY id DESC`, fileID)
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

func (db *DB) FindLatestAPIContractIDByNameAndKind(ctx context.Context, name string, kind string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `SELECT id FROM api_contracts WHERE LOWER(contract_name)=LOWER($1) AND LOWER(contract_kind)=LOWER($2) ORDER BY id DESC LIMIT 1`, strings.TrimSpace(name), strings.TrimSpace(kind)).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// APIContractNameKind вЂ” РїР°СЂР° (name, kind) РґР»СЏ batch-resolve.
type APIContractNameKind struct {
	Name string
	Kind string
}

// FindLatestAPIContractIDsByNamesAndKinds Р·Р°РіСЂСѓР¶Р°РµС‚ API РєРѕРЅС‚СЂР°РєС‚С‹ РѕРґРЅРёРј Р·Р°РїСЂРѕСЃРѕРј
// Рё СЃС‚СЂРѕРёС‚ in-memory map РґР»СЏ batch-resolve. Р—Р°РјРµРЅСЏРµС‚ N+1 РІС‹Р·РѕРІРѕРІ
// FindLatestAPIContractIDByNameAndKind.
func (db *DB) FindLatestAPIContractIDsByNamesAndKinds(ctx context.Context, pairs []APIContractNameKind) (map[string]int64, error) {
	type nkKey struct{ name, kind string }
	unique := make(map[nkKey]struct{})
	names := make([]string, 0)
	kinds := make([]string, 0)
	for _, p := range pairs {
		n := strings.ToLower(strings.TrimSpace(p.Name))
		k := strings.ToLower(strings.TrimSpace(p.Kind))
		if n == "" || k == "" {
			continue
		}
		key := nkKey{n, k}
		if _, exists := unique[key]; exists {
			continue
		}
		unique[key] = struct{}{}
		names = append(names, n)
		kinds = append(kinds, k)
	}
	result := make(map[string]int64, len(unique))
	if len(names) == 0 {
		return result, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT id, LOWER(contract_name) AS name_key, LOWER(contract_kind) AS kind_key
		FROM api_contracts
		WHERE LOWER(contract_name) = ANY($1)
		  AND LOWER(contract_kind) = ANY($2)
		ORDER BY id DESC
	`, pq.Array(names), pq.Array(kinds))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var nameKey, kindKey string
		if err := rows.Scan(&id, &nameKey, &kindKey); err != nil {
			return nil, err
		}
		mapKey := nameKey + "|" + kindKey
		if _, exists := result[mapKey]; !exists {
			result[mapKey] = id
		}
	}
	return result, rows.Err()
}

func (db *DB) FindLatestAPIContractIDByNameKindAndOwnerModule(ctx context.Context, name string, kind string, ownerModule string) (int64, error) {
	trimmedName := strings.TrimSpace(name)
	trimmedKind := strings.TrimSpace(kind)
	trimmedOwnerModule := strings.TrimSpace(ownerModule)
	if trimmedOwnerModule != "" {
		var id int64
		err := db.QueryRowContext(ctx, `SELECT id FROM api_contracts WHERE LOWER(contract_name)=LOWER($1) AND LOWER(contract_kind)=LOWER($2) AND LOWER(owner_module)=LOWER($3) ORDER BY id DESC LIMIT 1`, trimmedName, trimmedKind, trimmedOwnerModule).Scan(&id)
		if err == nil {
			return id, nil
		}
		if err != sql.ErrNoRows {
			return 0, err
		}
	}
	return db.FindLatestAPIContractIDByNameAndKind(ctx, trimmedName, trimmedKind)
}

func (db *DB) FindAPIContractsByKind(ctx context.Context, kind string) ([]*model.APIContract, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, contract_name, contract_kind, used_object_name, used_module_sys_name
		FROM api_contracts
		WHERE LOWER(contract_kind)=LOWER($1)
		ORDER BY id DESC
	`, strings.TrimSpace(kind))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*model.APIContract, 0)
	for rows.Next() {
		item := &model.APIContract{}
		if err := rows.Scan(&item.ID, &item.ContractName, &item.ContractKind, &item.UsedObjectName, &item.UsedModuleSysName); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// EventContractLookupKey вЂ” РєР»СЋС‡ РґР»СЏ in-memory map event-РєРѕРЅС‚СЂР°РєС‚РѕРІ.
// Р¤РѕСЂРјР°С‚: lower(name) + "|" + lower(module), РёР»Рё lower(name) + "|" РґР»СЏ fallback Р±РµР· module.
type EventContractLookup struct {
	ByNameAndModule map[string]int64 // key: lower(name)|lower(module) -> id
	ByName          map[string]int64 // key: lower(name) -> id (fallback)
}

// FindLatestEventContractIDsByNames Р·Р°РіСЂСѓР¶Р°РµС‚ event-РєРѕРЅС‚СЂР°РєС‚С‹ РѕРґРЅРёРј Р·Р°РїСЂРѕСЃРѕРј
// Рё СЃС‚СЂРѕРёС‚ in-memory map РґР»СЏ batch-resolve. Р—Р°РјРµРЅСЏРµС‚ N+1 РІС‹Р·РѕРІРѕРІ
// FindLatestAPIContractIDByNameKindAndOwnerModule.
func (db *DB) FindLatestEventContractIDsByNames(ctx context.Context, names []string) (*EventContractLookup, error) {
	normalizedNames := make([]string, 0, len(names))
	seenNames := make(map[string]struct{})
	for _, n := range names {
		key := strings.ToLower(strings.TrimSpace(n))
		if key == "" {
			continue
		}
		if _, exists := seenNames[key]; exists {
			continue
		}
		seenNames[key] = struct{}{}
		normalizedNames = append(normalizedNames, key)
	}

	lookup := &EventContractLookup{
		ByNameAndModule: make(map[string]int64),
		ByName:          make(map[string]int64),
	}
	if len(normalizedNames) == 0 {
		return lookup, nil
	}

	// Р—Р°РіСЂСѓР¶Р°РµРј РІСЃРµ event-РєРѕРЅС‚СЂР°РєС‚С‹, Сѓ РєРѕС‚РѕСЂС‹С… contract_name РІС…РѕРґРёС‚ РІ names.
	// owner_module РјРѕР¶РµС‚ Р±С‹С‚СЊ Р»СЋР±С‹Рј (РІРєР»СЋС‡Р°СЏ РїСѓСЃС‚РѕР№), РїРѕСЌС‚РѕРјСѓ РЅРµ С„РёР»СЊС‚СЂСѓРµРј РїРѕ modules.
	rows, err := db.QueryContext(ctx, `
		SELECT id, LOWER(contract_name) AS name_key, COALESCE(LOWER(owner_module), '') AS module_key
		FROM api_contracts
		WHERE LOWER(contract_kind) = 'event'
		  AND LOWER(contract_name) = ANY($1)
		ORDER BY id DESC
	`, pq.Array(normalizedNames))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var nameKey, moduleKey string
		if err := rows.Scan(&id, &nameKey, &moduleKey); err != nil {
			return nil, err
		}
		// ByNameAndModule: РїРµСЂРІС‹Р№ (latest) id РґР»СЏ name+module
		nmKey := nameKey + "|" + moduleKey
		if _, exists := lookup.ByNameAndModule[nmKey]; !exists {
			lookup.ByNameAndModule[nmKey] = id
		}
		// ByName: РїРµСЂРІС‹Р№ (latest) id РґР»СЏ name (fallback)
		if _, exists := lookup.ByName[nameKey]; !exists {
			lookup.ByName[nameKey] = id
		}
	}
	return lookup, rows.Err()
}

func (db *DB) DeleteSubscribesToEventRelations(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `
		DELETE FROM relations
		WHERE source_type = 'api_contract'
		  AND target_type = 'api_contract'
		  AND relation_type = 'subscribes_to_event'
	`)
	return err
}

func BuildAPIContractLookupKey(name string, kind string) string {
	return strings.ToLower(strings.TrimSpace(name)) + "|" + strings.ToLower(strings.TrimSpace(kind))
}

func (db *DB) BatchInsertAPIContractParams(ctx context.Context, items []*model.APIContractParam, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) BatchInsertAPIContractTables(ctx context.Context, items []*model.APIContractTable, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) FindAPIContractTableIDsByFile(ctx context.Context, fileID int64) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT t.id, t.direction, t.table_name FROM api_contract_tables t JOIN api_contracts c ON c.id = t.contract_id WHERE c.file_id = $1 ORDER BY t.id DESC`, fileID)
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

func (db *DB) BatchInsertAPIContractTableFields(ctx context.Context, items []*model.APIContractTableField, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) BatchInsertAPIContractReturnValues(ctx context.Context, items []*model.APIContractReturnValue, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) BatchInsertAPIContractContexts(ctx context.Context, items []*model.APIContractContext, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) BatchInsertAPIBusinessObjectParams(ctx context.Context, items []*model.APIBusinessObjectParam, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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

func (db *DB) BatchInsertAPIBusinessObjectTables(ctx context.Context, items []*model.APIBusinessObjectTable, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
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
