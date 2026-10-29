package store

import (
	"context"
	"database/sql"
)

// RetCodeLookup — результат поиска кода возврата в ds_return_codes
type RetCodeLookup struct {
	RetCode    int64
	Message    string
	ProcName   string
	ModuleID   int
	Product    string
	BranchUsed string
}

// LookupRetCode ищет описание кода возврата в таблице ds_return_codes.
// При копиях кода в нескольких ветках выбирается копия линии профиля dsinfo
// (branches: product_id → branch_id, только при заданном профиле), затем
// default-ветки, затем больший id — выбор детерминирован, результат несёт
// product/branch_used.
func (db *DB) LookupRetCode(ctx context.Context, retCode int64, branches map[int64]int64) (*RetCodeLookup, error) {
	row := db.QueryRowContext(ctx, `
		SELECT rc.ret_code, rc.message, rc.proc_name, rc.module_id,
		       COALESCE(p.product_name, ''), COALESCE(br.branch_key, '')
		FROM ds_return_codes rc
		JOIN files f ON f.id = rc.file_id
		LEFT JOIN ds_products p ON p.id = f.ds_product_id
		LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
		WHERE rc.ret_code = $1
		ORDER BY (NOT f.is_generated) DESC, COALESCE(`+ProfileMatchSQL("f.ds_product_id", "f.ds_product_branch_id", branches)+`, FALSE) DESC,
		         COALESCE(br.is_default, FALSE) DESC, rc.id DESC
		LIMIT 1`,
		retCode)

	var r RetCodeLookup
	var procName sql.NullString
	err := row.Scan(&r.RetCode, &r.Message, &procName, &r.ModuleID, &r.Product, &r.BranchUsed)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if procName.Valid {
		r.ProcName = procName.String
	}
	return &r, nil
}

// LookupRetCodes — batch-lookup для нескольких кодов возврата.
// Возвращает map[retCode]*RetCodeLookup.
func (db *DB) LookupRetCodes(ctx context.Context, retCodes []int64) (map[int64]*RetCodeLookup, error) {
	result := make(map[int64]*RetCodeLookup)
	if len(retCodes) == 0 {
		return result, nil
	}

	// Уникальные коды
	seen := make(map[int64]bool)
	codes := make([]int64, 0, len(retCodes))
	for _, c := range retCodes {
		if !seen[c] {
			seen[c] = true
			codes = append(codes, c)
		}
	}

	for _, code := range codes {
		r, err := db.LookupRetCode(ctx, code, nil)
		if err != nil {
			return nil, err
		}
		if r != nil {
			result[code] = r
		}
	}
	return result, nil
}

// LookupRetCodeByMessage ищет коды возврата по фрагменту текста сообщения (ILIKE).
// Возвращает список совпадений: не более одной строки на код одного продукта
// (приоритет линии профиля dsinfo, затем default-ветки), с product/branch_used.
func (db *DB) LookupRetCodeByMessage(ctx context.Context, messagePattern string, limit int, branches map[int64]int64) ([]RetCodeLookup, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.QueryContext(ctx, `
		WITH chosen AS (
			SELECT DISTINCT ON (rc.ret_code, COALESCE(f.ds_product_id, 0))
			       rc.ret_code AS ret_code_key,
			       COALESCE(f.ds_product_id, 0) AS product_key,
			       f.ds_product_branch_id AS branch_id
			FROM ds_return_codes rc
			JOIN files f ON f.id = rc.file_id
			LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
			WHERE rc.message ILIKE '%' || $1 || '%'
			ORDER BY rc.ret_code, COALESCE(f.ds_product_id, 0),
			         (NOT f.is_generated) DESC, COALESCE(`+ProfileMatchSQL("f.ds_product_id", "f.ds_product_branch_id", branches)+`, FALSE) DESC,
			         COALESCE(br.is_default, FALSE) DESC, rc.id DESC
		)
		SELECT
			rc.ret_code, rc.message, rc.proc_name, rc.module_id,
			COALESCE(p.product_name, '') AS product, COALESCE(br.branch_key, '') AS branch_used
		FROM ds_return_codes rc
		JOIN files f ON f.id = rc.file_id
		LEFT JOIN ds_products p ON p.id = f.ds_product_id
		LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
		JOIN chosen ch ON ch.ret_code_key = rc.ret_code
		             AND ch.product_key = COALESCE(f.ds_product_id, 0)
		             AND ch.branch_id IS NOT DISTINCT FROM f.ds_product_branch_id
		WHERE rc.message ILIKE '%' || $1 || '%'
		ORDER BY rc.ret_code, rc.id
		LIMIT $2`,
		messagePattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []RetCodeLookup
	for rows.Next() {
		var r RetCodeLookup
		var procName sql.NullString
		if err := rows.Scan(&r.RetCode, &r.Message, &procName, &r.ModuleID, &r.Product, &r.BranchUsed); err != nil {
			return nil, err
		}
		if procName.Valid {
			r.ProcName = procName.String
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
