package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
)

// tableScopeKey идентифицирует выбранную копию таблицы в разрезе
// (имя, продукт, ветка); branchID = 0 соответствует файлам без ветки
// (ds_product_branch_id IS NULL).
type tableScopeKey struct {
	TableName  string
	ProductKey int64
	BranchID   int64
}

// valuesScopeOrd строит SQL-фрагмент `(VALUES ($n::int, $n+1::text,
// $n+2::bigint, $n+3::bigint), ...)` — выбранные копии таблиц с порядковым
// номером ord, по которому Go сопоставляет строки результатов с ключами.
func valuesScopeOrd(keys []tableScopeKey, startParam int) string {
	parts := make([]string, 0, len(keys))
	p := startParam
	for range keys {
		parts = append(parts, fmt.Sprintf("($%d::int, $%d::text, $%d::bigint, $%d::bigint)", p, p+1, p+2, p+3))
		p += 4
	}
	return strings.Join(parts, ", ")
}

// tableScopeOrdArgs — аргументы к valuesScopeOrd: (ord, table_name,
// product_key, branch); branchID = 0 передаётся как NULL (файлы без ветки).
func tableScopeOrdArgs(keys []tableScopeKey) []interface{} {
	args := make([]interface{}, 0, len(keys)*4)
	for i, k := range keys {
		var branch interface{}
		if k.BranchID != 0 {
			branch = k.BranchID
		}
		args = append(args, i, k.TableName, k.ProductKey, branch)
	}
	return args
}

// SearchTable поиск таблиц: не более одной строки на таблицу одного продукта
// (копии, отличающиеся только веткой, схлопываются; приоритет default-ветки);
// usages считается в пределах выбранной ветки, результат несёт
// product/branch_used. Enrichment (файлы/колонки/процедуры) ограничен
// выбранной копией (продукт + ветка).
func (q *Query) SearchTable(ctx context.Context, name string, likeSearch bool, limit int) ([]TableResult, error) {
	queryText := `
		WITH candidates AS (
			SELECT st.id, st.table_name, st.context, st.file_id,
			       COALESCE(f.ds_product_id, 0) AS product_key,
			       f.ds_product_branch_id AS branch_id,
			       (NOT f.is_generated) AS not_generated,
			       COALESCE(br.is_default, FALSE) AS is_default
			FROM sql_tables st
			JOIN files f ON f.id = st.file_id
			LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
			WHERE %s
		),
		chosen AS (
			SELECT DISTINCT ON (c.table_name, c.product_key)
			       c.id, c.table_name, c.context, c.file_id, c.product_key, c.branch_id
			FROM candidates c
			ORDER BY c.table_name, c.product_key,
			         c.not_generated DESC, ` + q.profileMatchSQL("c.product_key", "c.branch_id") + ` DESC, c.is_default DESC, c.id DESC
		)
		SELECT ch.id, ch.table_name, ch.context, COALESCE(u.usages, 0),
		       COALESCE(p.product_name, '') AS product, COALESCE(br.branch_key, '') AS branch_used,
		       ch.product_key, COALESCE(ch.branch_id, 0),
		       (m.table_name IS NOT NULL), COALESCE(m.product_name, ''), COALESCE(m.module_name, '')
		FROM chosen ch
		JOIN files f ON f.id = ch.file_id
		LEFT JOIN ds_products p ON p.id = f.ds_product_id
		LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
		LEFT JOIN LATERAL (
			SELECT COUNT(*) AS usages
			FROM candidates c
			WHERE c.table_name = ch.table_name
			  AND c.product_key = ch.product_key
			  AND c.branch_id IS NOT DISTINCT FROM ch.branch_id
		) u ON TRUE
		LEFT JOIN LATERAL (
			-- Признак «объявлена как таблица в памяти» (ModuleTableInMemory.lst):
			-- сопоставление чисто по имени таблицы, без product-scope (физическая
			-- таблица одна на БД). При нескольких записях (копии .lst в разных
			-- ветках) берётся default-ветка, затем меньший id — СОРТИРОВКА, не
			-- фильтр: единственная копия может лежать и в non-default ветке.
			SELECT mt.table_name, mt.product_name, mt.module_name
			FROM memory_tables mt
			JOIN files mf ON mf.id = mt.file_id
			LEFT JOIN ds_products_branch mbr ON mbr.id = mf.ds_product_branch_id
			WHERE LOWER(mt.table_name) = LOWER(ch.table_name)
			ORDER BY COALESCE(mbr.is_default, FALSE) DESC, mt.id
			LIMIT 1
		) m ON TRUE
		ORDER BY COALESCE(u.usages, 0) DESC, ch.table_name
		LIMIT $2
	`

	searchValue := strings.TrimSpace(name)
	whereClause := "LOWER(st.table_name) = LOWER($1)"
	if likeSearch {
		whereClause = "st.table_name ILIKE $1"
		searchValue = "%" + searchValue + "%"
	}
	queryText = fmt.Sprintf(queryText, whereClause)

	rows, err := q.db.QueryContext(ctx, queryText, searchValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TableResult
	keys := make([]tableScopeKey, 0)
	for rows.Next() {
		var r TableResult
		var key tableScopeKey
		if err := rows.Scan(&r.ID, &r.TableName, &r.Context, &r.Usages, &r.Product, &r.BranchUsed, &key.ProductKey, &key.BranchID, &r.InMemory, &r.InMemoryProduct, &r.InMemoryModule); err != nil {
			return nil, err
		}
		key.TableName = r.TableName
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return results, nil
	}
	// Указатели строим ПОСЛЕ заполнения слайса: append может перевыделить
	// массив и инвалидировать взятые ранее адреса.
	scopes := make(map[tableScopeKey]*TableResult, len(results))
	for i := range results {
		scopes[keys[i]] = &results[i]
	}

	if err := q.loadTableFiles(ctx, scopes, keys); err != nil {
		return nil, err
	}
	if err := q.loadTableColumns(ctx, scopes, keys); err != nil {
		return nil, err
	}
	if err := q.loadTableProcedures(ctx, scopes, keys); err != nil {
		return nil, err
	}

	return results, nil
}

func (q *Query) loadTableFiles(ctx context.Context, results map[tableScopeKey]*TableResult, keys []tableScopeKey) error {
	if len(keys) == 0 {
		return nil
	}
	rows, err := q.db.QueryContext(ctx, `
		SELECT k.ord, x.file_id, x.rel_path
		FROM (VALUES `+valuesScopeOrd(keys, 1)+`) AS k(ord, table_name, product_key, branch_id)
		JOIN LATERAL (
			SELECT DISTINCT f.id AS file_id, f.rel_path
			FROM sql_tables st
			JOIN files f ON f.id = st.file_id
			WHERE st.table_name = k.table_name
			  AND COALESCE(f.ds_product_id, 0) = k.product_key
			  AND f.ds_product_branch_id IS NOT DISTINCT FROM k.branch_id
			ORDER BY f.rel_path, f.id
			LIMIT 20
		) x ON TRUE
		ORDER BY k.ord
	`, tableScopeOrdArgs(keys)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ord int
		var fileID int64
		var file string
		if err := rows.Scan(&ord, &fileID, &file); err != nil {
			return err
		}
		if result := results[keys[ord]]; result != nil {
			result.FileIDs = append(result.FileIDs, fileID)
			result.Files = append(result.Files, file)
		}
	}
	return rows.Err()
}

func (q *Query) loadTableColumns(ctx context.Context, results map[tableScopeKey]*TableResult, keys []tableScopeKey) error {
	if len(keys) == 0 {
		return nil
	}
	rows, err := q.db.QueryContext(ctx, `
		SELECT k.ord, x.column_name
		FROM (VALUES `+valuesScopeOrd(keys, 1)+`) AS k(ord, table_name, product_key, branch_id)
		JOIN LATERAL (
			SELECT DISTINCT sc.column_name
			FROM sql_columns sc
			JOIN files f ON f.id = sc.file_id
			WHERE sc.table_name = k.table_name
			  AND COALESCE(f.ds_product_id, 0) = k.product_key
			  AND f.ds_product_branch_id IS NOT DISTINCT FROM k.branch_id
			ORDER BY sc.column_name
			LIMIT 50
		) x ON TRUE
		ORDER BY k.ord, x.column_name
	`, tableScopeOrdArgs(keys)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ord int
		var column string
		if err := rows.Scan(&ord, &column); err != nil {
			return err
		}
		if result := results[keys[ord]]; result != nil {
			result.Columns = append(result.Columns, column)
		}
	}
	return rows.Err()
}

func (q *Query) loadTableProcedures(ctx context.Context, results map[tableScopeKey]*TableResult, keys []tableScopeKey) error {
	if len(keys) == 0 {
		return nil
	}
	rows, err := q.db.QueryContext(ctx, `
		SELECT k.ord, x.proc_name
		FROM (VALUES `+valuesScopeOrd(keys, 1)+`) AS k(ord, table_name, product_key, branch_id)
		JOIN LATERAL (
			SELECT DISTINCT sp.proc_name
			FROM relations r
			JOIN sql_procedures sp ON r.source_id = sp.id
			JOIN sql_tables st ON r.target_id = st.id
			JOIN files ft ON ft.id = st.file_id
			WHERE r.source_type = 'sql_procedure'
			  AND r.target_type = 'sql_table'
			  AND r.relation_type IN ('selects_from', 'inserts_into', 'updates', 'deletes_from', 'references_table')
			  AND st.table_name = k.table_name
			  AND COALESCE(ft.ds_product_id, 0) = k.product_key
			  AND ft.ds_product_branch_id IS NOT DISTINCT FROM k.branch_id
			ORDER BY sp.proc_name
			LIMIT 20
		) x ON TRUE
		ORDER BY k.ord, x.proc_name
	`, tableScopeOrdArgs(keys)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ord int
		var proc string
		if err := rows.Scan(&ord, &proc); err != nil {
			return err
		}
		if result := results[keys[ord]]; result != nil {
			result.Procedures = append(result.Procedures, proc)
		}
	}
	return rows.Err()
}

// SearchTableSchema возвращает определения колонок таблицы: не более одной
// строки на колонку одного продукта (приоритет default-ветки), с
// product/branch_used.
func (q *Query) SearchTableSchema(ctx context.Context, name string, limit int) ([]TableSchemaColumnResult, error) {
	// Двухфазный дедуп: фаза 1 выбирает ветку для (колонка, продукт),
	// фаза 2 возвращает все колонки выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "scd.column_name", Alias: "column_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"scd.id",
		`scd.table_name,
		scd.column_name,
		scd.data_type,
		scd.definition_kind,
		f.rel_path,
		scd.file_id,
		scd.line_number,
		scd.column_order`,
		`sql_column_definitions scd
		JOIN files f ON scd.file_id = f.id`+fileBranchJoin("f"),
		`scd.table_name = $1`,
		`scd.column_order, scd.line_number, scd.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, name, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]TableSchemaColumnResult, 0)
	for rows.Next() {
		var item TableSchemaColumnResult
		var file sql.NullString
		if err := rows.Scan(&item.TableName, &item.ColumnName, &item.DataType, &item.DefinitionKind, &file, &item.FileID, &item.LineNumber, &item.ColumnOrder, &item.Product, &item.BranchUsed); err != nil {
			return nil, err
		}
		if file.Valid {
			item.File = file.String
		}
		item.BranchRequested = q.branchRequested(item.Product)
		results = append(results, item)
	}

	return results, rows.Err()
}

// SearchSQLTableIndex поиск определений индексов: не более одной строки на
// индекс таблицы одного продукта (приоритет default-ветки), с product/branch_used.
func (q *Query) SearchSQLTableIndex(ctx context.Context, name string, likeSearch bool, limit int) ([]SQLTableIndexResult, error) {
	searchValue := strings.TrimSpace(name)
	whereClause := "LOWER(i.index_name) = LOWER($1) OR LOWER(i.table_name) = LOWER($1)"
	if likeSearch {
		whereClause = "i.index_name ILIKE $1 OR i.table_name ILIKE $1"
		searchValue = "%" + searchValue + "%"
	}
	// Двухфазный дедуп: фаза 1 выбирает ветку для (таблица, индекс, продукт),
	// фаза 2 возвращает все индексы выбранной ветки.
	queryText := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "i.table_name", Alias: "table_name"},
			{Expr: "i.index_name", Alias: "index_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"i.id",
		`i.id, i.table_name, i.index_name, COALESCE(i.index_fields,''), COALESCE(i.index_type,''), i.is_unique,
		i.definition_kind, COALESCE(f.rel_path,''), i.file_id, i.line_number`,
		`sql_index_definitions i
		JOIN files f ON f.id = i.file_id`+fileBranchJoin("f"),
		whereClause,
		`i.table_name, i.index_name, i.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, queryText, searchValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]SQLTableIndexResult, 0)
	for rows.Next() {
		var item SQLTableIndexResult
		if err := rows.Scan(&item.ID, &item.TableName, &item.IndexName, &item.IndexFields, &item.IndexType, &item.IsUnique, &item.DefinitionKind, &item.File, &item.FileID, &item.LineNumber, &item.Product, &item.BranchUsed); err != nil {
			return nil, err
		}
		fieldRows, err := q.db.QueryContext(ctx, `SELECT field_name, field_order, line_number FROM sql_index_definition_fields WHERE table_index_id = $1 ORDER BY field_order, id`, item.ID)
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

// FindCallers возвращает вызовы процедуры: копии вызовов из разных веток
// сосуществуют (каждая со своим product/branch_used) — после веточной
// пост-обработки рёбра не пересекают ветки одного продукта.
func (q *Query) FindCallers(ctx context.Context, procedureName string, limit int) ([]CallerResult, error) {
	query := `
		SELECT *
		FROM (
			SELECT
				r.source_id,
				COALESCE(CASE
					WHEN r.source_type = 'sql_procedure' THEN sp.proc_name
					WHEN r.source_type = 'pas_method' THEN pm.method_name
					WHEN r.source_type = 'js_function' THEN jf.function_name
					WHEN r.source_type = 'report_form' THEN rf.report_name
					WHEN r.source_type = 'vb_function' THEN vf.function_name
					WHEN r.source_type = 'query_fragment' THEN COALESCE(NULLIF(qf.component_name, ''), NULLIF(qf.component_type, ''), 'query_fragment')
					ELSE 'unknown'
				 END, 'unknown') as caller_name,
				r.source_type as caller_type,
				 COALESCE(f_sql.rel_path, f_pas.rel_path, f_js.rel_path, f_rf.rel_path, f_vf.rel_path, f_qf.rel_path) as file,
				 COALESCE(pr_sql.product_name, pr_pas.product_name, pr_js.product_name, pr_rf.product_name, pr_vf.product_name, pr_qf.product_name, '') as product,
				 COALESCE(br_sql.branch_key, br_pas.branch_key, br_js.branch_key, br_rf.branch_key, br_vf.branch_key, br_qf.branch_key, '') as branch_used,
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
			LEFT JOIN ds_products pr_sql ON pr_sql.id = f_sql.ds_product_id
			LEFT JOIN ds_products pr_pas ON pr_pas.id = f_pas.ds_product_id
			LEFT JOIN ds_products pr_js ON pr_js.id = f_js.ds_product_id
			LEFT JOIN ds_products pr_rf ON pr_rf.id = f_rf.ds_product_id
			LEFT JOIN ds_products pr_vf ON pr_vf.id = f_vf.ds_product_id
			LEFT JOIN ds_products pr_qf ON pr_qf.id = f_qf.ds_product_id
			LEFT JOIN ds_products_branch br_sql ON br_sql.id = f_sql.ds_product_branch_id
			LEFT JOIN ds_products_branch br_pas ON br_pas.id = f_pas.ds_product_branch_id
			LEFT JOIN ds_products_branch br_js ON br_js.id = f_js.ds_product_branch_id
			LEFT JOIN ds_products_branch br_rf ON br_rf.id = f_rf.ds_product_branch_id
			LEFT JOIN ds_products_branch br_vf ON br_vf.id = f_vf.ds_product_branch_id
			LEFT JOIN ds_products_branch br_qf ON br_qf.id = f_qf.ds_product_branch_id
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
				COALESCE(pr_up.product_name, '') as product,
				COALESCE(br_up.branch_key, '') as branch_used,
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
			LEFT JOIN ds_products pr_up ON pr_up.id = f_up.ds_product_id
			LEFT JOIN ds_products_branch br_up ON br_up.id = f_up.ds_product_branch_id
			WHERE sp_target.proc_name ILIKE $1
			  AND r_down.relation_type IN ('calls_procedure', 'dispatches_to_subscriber')
			  AND r_up.relation_type IN ('dispatches_to', 'dispatches_to_subscriber', 'calls_procedure')
		) callers
		ORDER BY is_indirect, caller_type, caller_name
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, "%"+procedureName+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []CallerResult
	for rows.Next() {
		var r CallerResult
		var file sql.NullString
		var context sql.NullString
		if err := rows.Scan(&r.CallerID, &r.CallerName, &r.CallerType, &file, &r.Product, &r.BranchUsed, &r.LineNumber, &context, &r.IsIndirect, &r.ViaProcedure, &r.RelationType); err != nil {
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

// FindMethodsByTable возвращает методы, работающие с таблицей через фрагменты
// запросов: не более одной строки на метод (класс+имя) одного продукта
// (приоритет default-ветки), с product/branch_used.
func (q *Query) FindMethodsByTable(ctx context.Context, tableName string, limit int) ([]MethodResult, error) {
	// Двухфазный дедуп: фаза 1 выбирает ветку для (класс, метод, продукт),
	// фаза 2 возвращает все строки выбранной ветки.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "COALESCE(pc.class_name, '')", Alias: "class_name"},
			{Expr: "pm.method_name", Alias: "method_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"pm.id",
		`DISTINCT pm.id,
		pm.unit_id,
		pm.method_name,
		COALESCE(pc.class_name, '') AS class_name,
		pu.unit_name,
		f.rel_path,
		pm.line_number,
		st.table_name`,
		`pas_methods pm
		LEFT JOIN pas_classes pc ON pm.class_id = pc.id
		JOIN pas_units pu ON pm.unit_id = pu.id
		JOIN files f ON pu.file_id = f.id`+fileBranchJoin("f")+`
		JOIN relations r_query ON r_query.source_type = 'pas_method' AND r_query.source_id = pm.id
		JOIN query_fragments qf ON r_query.target_type = 'query_fragment' AND r_query.target_id = qf.id
		JOIN relations r_table ON r_table.source_type = 'query_fragment' AND r_table.source_id = qf.id
		JOIN sql_tables st ON r_table.target_type = 'sql_table' AND r_table.target_id = st.id`,
		`LOWER(st.table_name) = LOWER($1)
		  AND r_query.relation_type IN ('executes_query', 'builds_query')
		  AND r_table.relation_type = 'references_table'`,
		`pu.unit_name, pm.method_name, pm.line_number, pm.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, tableName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []MethodResult
	for rows.Next() {
		var r MethodResult
		if err := rows.Scan(&r.ID, &r.UnitID, &r.MethodName, &r.ClassName, &r.UnitName, &r.File, &r.LineNumber, &r.TableUsage, &r.Product, &r.BranchUsed); err != nil {
			return nil, err
		}
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, nil
}

// FindPASMethodsByName поиск PAS-методов по имени: не более одной строки на
// метод (класс+имя) одного продукта (приоритет default-ветки), с
// product/branch_used; одноимённые методы разных классов не схлопываются.
func (q *Query) FindPASMethodsByName(ctx context.Context, methodName string, like bool, limit int) ([]MethodResult, error) {
	lookupValue := buildLookupValue(methodName, like)
	lookupCondition := buildNameLookupCondition([]string{"pm.method_name"}, like, 1)
	// Двухфазный дедуп: фаза 1 выбирает ветку для (класс, метод, продукт),
	// фаза 2 возвращает все строки выбранной ветки; одноимённые методы
	// разных классов не схлопываются.
	query := q.chosenBranchQuery(
		[]branchKeyPair{
			{Expr: "COALESCE(pc.class_name, '')", Alias: "class_name"},
			{Expr: "pm.method_name", Alias: "method_name"},
			{Expr: "COALESCE(f.ds_product_id, 0)", Alias: "product_key"},
		},
		"pm.id",
		`pm.id,
		pm.unit_id,
		pm.method_name,
		COALESCE(pc.class_name, '') AS class_name,
		pu.unit_name,
		COALESCE(pm.signature, ''),
		COALESCE(pm.visibility, ''),
		f.rel_path,
		pm.line_number`,
		`pas_methods pm
		LEFT JOIN pas_classes pc ON pm.class_id = pc.id
		JOIN pas_units pu ON pm.unit_id = pu.id
		JOIN files f ON pu.file_id = f.id`+fileBranchJoin("f"),
		lookupCondition,
		`pu.unit_name, COALESCE(pc.class_name, ''), pm.method_name, pm.line_number, pm.id`,
	) + `
		LIMIT $2
	`

	rows, err := q.db.QueryContext(ctx, query, lookupValue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []MethodResult
	for rows.Next() {
		var r MethodResult
		if err := rows.Scan(&r.ID, &r.UnitID, &r.MethodName, &r.ClassName, &r.UnitName, &r.Signature, &r.Visibility, &r.File, &r.LineNumber, &r.Product, &r.BranchUsed); err != nil {
			return nil, err
		}
		r.BranchRequested = q.branchRequested(r.Product)
		results = append(results, r)
	}

	return results, rows.Err()
}

type SQLParamResult struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Direction string `json:"direction"`
}

type SQLProcedureResult struct {
	ID              int64            `json:"id"`
	FileID          int64            `json:"file_id"`
	ProcName        string           `json:"proc_name"`
	Params          []SQLParamResult `json:"params"`
	File            string           `json:"file,omitempty"`
	LineStart       int              `json:"line_start"`
	LineEnd         int              `json:"line_end"`
	BodyHash        string           `json:"body_hash,omitempty"`
	Description     string           `json:"description,omitempty"`
	Product         string           `json:"product,omitempty"`
	BranchUsed      string           `json:"branch_used,omitempty"`
	BranchRequested string           `json:"branch_requested,omitempty"`
}

// procedurePriorityOrder — детерминированный порядок выбора копии процедуры
// (команда query procedure): не-генерируемый источник, затем совпадение с
// линией профиля dsinfo (profile_match — только при заданном профиле),
// default-ветка (is_default), больший id. Оси branch_match/product_match
// из FindLatest* здесь вырождены (scope источника не передаётся).
func (q *Query) procedurePriorityOrder() string {
	return `(NOT f.is_generated) DESC, ` +
		q.profileMatchSQL("f.ds_product_id", "f.ds_product_branch_id") + ` DESC, ` +
		`(COALESCE(br.is_default, FALSE)) DESC, sp.id DESC`
}

// GetProcedureDetails возвращает копию процедуры по точному имени,
// детерминированно выбранную (см. procedurePriorityOrder).
func (q *Query) GetProcedureDetails(ctx context.Context, name string) (*model.SQLProcedure, error) {
	query := `
		SELECT
			sp.id, sp.file_id, sp.proc_name,
			sp.parameters, sp.line_start, sp.line_end, sp.body_hash, sp.description
		FROM sql_procedures sp
		JOIN files f ON f.id = sp.file_id
		LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
		WHERE sp.proc_name = $1
		ORDER BY ` + q.procedurePriorityOrder() + `
		LIMIT 1
	`

	row := q.db.QueryRowContext(ctx, query, name)

	var proc model.SQLProcedure
	var paramsJSON sql.NullString
	var description sql.NullString

	err := row.Scan(&proc.ID, &proc.FileID, &proc.ProcName,
		&paramsJSON, &proc.LineStart, &proc.LineEnd, &proc.BodyHash, &description)
	if err != nil {
		return nil, err
	}
	if description.Valid {
		proc.Description = description.String
	}

	if paramsJSON.Valid {
		if err := json.Unmarshal([]byte(paramsJSON.String), &proc.Params); err != nil {
			proc.Params = make([]model.SQLParam, 0)
		}
	}

	return &proc, nil
}

// GetProcedureResult возвращает копию процедуры по точному имени для
// команды query procedure: детерминированный выбор (procedurePriorityOrder)
// и фактические продукт/ветка выбранной копии (product/branch_used).
func (q *Query) GetProcedureResult(ctx context.Context, name string) (*SQLProcedureResult, error) {
	query := `
		SELECT
			sp.id, sp.file_id, sp.proc_name,
			sp.parameters, f.rel_path, sp.line_start, sp.line_end, sp.body_hash, sp.description,
			COALESCE(p.product_name, ''), COALESCE(br.branch_key, '')
		FROM sql_procedures sp
		JOIN files f ON sp.file_id = f.id
		LEFT JOIN ds_products p ON p.id = f.ds_product_id
		LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
		WHERE sp.proc_name = $1
		ORDER BY ` + q.procedurePriorityOrder() + `
		LIMIT 1
	`

	row := q.db.QueryRowContext(ctx, query, name)

	var proc SQLProcedureResult
	var paramsJSON sql.NullString
	var file sql.NullString
	var bodyHash sql.NullString
	var description sql.NullString

	err := row.Scan(&proc.ID, &proc.FileID, &proc.ProcName,
		&paramsJSON, &file, &proc.LineStart, &proc.LineEnd, &bodyHash, &description,
		&proc.Product, &proc.BranchUsed)
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
	if description.Valid {
		proc.Description = description.String
	}
	proc.BranchRequested = q.branchRequested(proc.Product)

	return &proc, nil
}
