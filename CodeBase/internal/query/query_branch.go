package query

import (
	"fmt"
	"strings"
)

// Единые SQL-фрагменты для мультиветочного дедупа списковых запросов
// (срез branch-aware-lookup). Копии сущности, отличающиеся только веткой
// одного продукта, схлопываются в одну строку; приоритет выбора —
// не-генерируемый источник, default-ветка (is_default), больший id.
// На одно-веточном индексе все правила — no-op.
//
// Дедуп двухфазный: фаза 1 выбирает ВЕТКУ для каждой группы ключа дедупа
// (по приоритету лучшей строки группы), фаза 2 возвращает ВСЕ строки
// выбранной ветки. Поэтому одноимённые сущности одной ветки (разные файлы)
// не схлопываются — состав результата на одно-веточном индексе не меняется.

// fileBranchJoin строит JOIN'ы продукта и ветки файла с фиксированными
// алиасами pr/br. Использовать с единственным файловым алиасом в запросе
// (иначе алиасы конфликтуют — тогда пишите JOIN'ы явно с суффиксами).
func fileBranchJoin(fileAlias string) string {
	return fmt.Sprintf(`
		LEFT JOIN ds_products pr ON pr.id = %s.ds_product_id
		LEFT JOIN ds_products_branch br ON br.id = %s.ds_product_branch_id`, fileAlias, fileAlias)
}

// fileBranchColumns — SELECT-колонки продукта и ветки файла для fileBranchJoin.
// branch_used — нормализованный branch_key (единый ключ веток, как в dsinfo
// и срезе C), а не сырое имя git-ветки branch_name.
const fileBranchColumns = `COALESCE(pr.product_name, '') AS product, COALESCE(br.branch_key, '') AS branch_used`

// dedupPriorityTail — хвост ORDER BY внутри DISTINCT ON-дедупа после ключей
// группировки: не-генерируемый источник → ось профиля dsinfo (profile_match,
// только при заданном профиле) → default-ветка → больший id.
// idExpr — id-выражение сущности (например, rf.id).
func (q *Query) dedupPriorityTail(idExpr string) string {
	return fmt.Sprintf("(NOT f.is_generated) DESC, %s DESC, COALESCE(br.is_default, FALSE) DESC, %s DESC",
		q.profileMatchSQL("f.ds_product_id", "f.ds_product_branch_id"), idExpr)
}

// branchKeyPair — выражение ключа дедупа и его алиас в CTE chosen
// (выражение задаётся в терминах алиасов таблиц запроса; одинаковые алиасы
// должны использоваться в обеих фазах).
type branchKeyPair struct {
	Expr  string // SQL-выражение ключа (например, rf.report_name)
	Alias string // имя колонки в CTE chosen (например, report_name)
}

// chosenBranchCTE — фаза 1 двухфазного дедупа: CTE chosen, выбирающая для
// каждой группы ключа ветку лучшей копии (dedupPriorityTail). fromWhere —
// FROM ... JOIN files f ... + fileBranchJoin + WHERE ... фильтр запроса
// (плейсхолдеры разделяются с фазой 2).
func (q *Query) chosenBranchCTE(keys []branchKeyPair, idExpr, fromWhere string) string {
	keyExprs := make([]string, 0, len(keys))
	aliasList := make([]string, 0, len(keys))
	for _, k := range keys {
		keyExprs = append(keyExprs, k.Expr)
		aliasList = append(aliasList, fmt.Sprintf("%s AS %s", k.Expr, k.Alias))
	}
	keysSQL := strings.Join(keyExprs, ", ")
	aliasesSQL := strings.Join(aliasList, ", ")
	return fmt.Sprintf(`
		WITH chosen AS (
			SELECT DISTINCT ON (%s)
			       %s, f.ds_product_branch_id AS branch_id
			FROM %s
			ORDER BY %s, %s
		)`, keysSQL, aliasesSQL, fromWhere, keysSQL, q.dedupPriorityTail(idExpr))
}

// chosenBranchJoin — условие фазы 2: строка принадлежит выбранной ветке
// своей группы ключа (NULL-ветка сопоставляется через IS NOT DISTINCT FROM).
// Таблицы фазы 2 обязаны использовать те же алиасы, что и в chosenBranchCTE.
func chosenBranchJoin(keys []branchKeyPair) string {
	parts := make([]string, 0, len(keys)+1)
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("ch.%s = %s", k.Alias, k.Expr))
	}
	parts = append(parts, "ch.branch_id IS NOT DISTINCT FROM f.ds_product_branch_id")
	return strings.Join(parts, " AND ")
}

// chosenBranchQuery — полный двухфазный запрос: CTE chosen (фаза 1 — выбор
// ветки для каждой группы ключа) + все строки выбранной ветки (фаза 2).
// fromClause — FROM ... JOIN с алиасами таблиц (без WHERE; используется в
// обеих фазах, алиасы совпадают); filter — WHERE-условие (плейсхолдеры общие
// для обеих фаз); selectCols — колонки SELECT фазы 2 без product/branch_used
// (добавляются автоматически); orderExpr — ORDER BY фазы 2 (включая
// id-тайбрейк для детерминизма). LIMIT добавляет вызывающий код.
func (q *Query) chosenBranchQuery(keys []branchKeyPair, idExpr, selectCols, fromClause, filter, orderExpr string) string {
	fromWhere := fromClause + " WHERE " + filter
	return q.chosenBranchCTE(keys, idExpr, fromWhere) + `
		SELECT ` + selectCols + `, ` + fileBranchColumns + `
		FROM ` + fromClause + `
		JOIN chosen ch ON ` + chosenBranchJoin(keys) + `
		WHERE ` + filter + `
		ORDER BY ` + orderExpr
}
