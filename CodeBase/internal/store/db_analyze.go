package store

import (
	"context"
	"strings"
)

// analyzeCodebaseTablesList — фиксированный список таблиц кодовой базы для
// ANALYZE после init/update (change optimize-query-search-bottlenecks):
// массовые вставки/удаления оставляют планировщику устаревшую статистику,
// и FTS/trgm/веточные запросы деградируют после больших update. Список —
// константа; расширение тривиально. Аналитические таблицы (rti_*/trc_*) не
// входят: их статистику обновляют свои store-методы после массовых удалений.
var analyzeCodebaseTablesList = []string{
	"files",
	"symbols",
	"sql_procedures",
	"sql_tables",
	"sql_columns",
	"sql_index_definitions",
	"query_fragments",
	"relations",
	"api_contracts",
	"spec_capabilities",
	"spec_requirements",
	"spec_scenarios",
	"spec_usecases",
	"spec_code_mentions",
}

// AnalyzeCodebaseTables выполняет единый ANALYZE по фиксированному списку
// таблиц кодовой базы. Вызов обязан идти после фиксации итога прогона
// (UpdateScanRun) и вне транзакций индексации: ошибка трактуется
// вызывающей стороной как предупреждение, статус прогона не меняется
// (идиома «VACUUM ANALYZE после массового удаления» из RTI/TRC-store,
// без VACUUM: мёртвые кортежи — зона autovacuum, планировщику достаточно
// ANALYZE). Read-only путь (query-команды) метод не вызывает.
func (db *DB) AnalyzeCodebaseTables(ctx context.Context) error {
	_, err := db.ExecContext(ctx, "ANALYZE "+strings.Join(analyzeCodebaseTablesList, ", "))
	return err
}
