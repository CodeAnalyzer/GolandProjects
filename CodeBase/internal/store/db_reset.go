package store

import (
	"context"
	"fmt"
	"strings"
)

// codebaseResetTables — таблицы, заполняемые парсерами кодовой базы, усекаемые
// при полной пересборке индекса (update --modified=false). Сохраняются:
// таблицы анализаторов (rti_*, trc_*), schema_migrations и scan_runs (история
// прогонов: FK files.scan_run_id → scan_runs(id) позволяет усечь files,
// не трогая родителя; строки истории остаются сиротскими безвредно).
// Список фиксируется тестом-сторожем (db_reset_integration_test.go): появление
// новой таблицы в схеме требует явного решения — в список усечения или в сохраняемые.
var codebaseResetTables = []string{
	// Файлы/символы/продукты/кэш статистики
	"files", "symbols", "ds_products", "stats_snapshot",
	// SQL
	"sql_procedures", "sql_tables", "sql_columns", "sql_column_definitions",
	"sql_index_definitions", "sql_index_definition_fields",
	// PAS/H/JS/SMF/DFM
	"pas_units", "pas_classes", "pas_methods", "pas_fields", "h_files_defines",
	"js_functions", "js_constants", "smf_instruments", "dfm_forms", "dfm_components",
	// Отчёты
	"report_forms", "report_fields", "report_params", "vb_functions",
	// Фрагменты/include
	"query_fragments", "include_directives",
	// API XML
	"api_business_objects", "api_contracts", "api_contract_params",
	"api_contract_tables", "api_contract_table_fields", "api_business_object_params",
	"api_business_object_tables", "api_business_object_table_fields",
	"api_business_object_table_indexes", "api_business_object_table_index_fields",
	"api_contract_return_values", "api_contract_contexts", "api_macro_invocations",
	// Связи/retcode
	"relations", "ds_return_codes",
	// Спеки
	"spec_configs", "spec_capabilities", "spec_requirements", "spec_scenarios",
	"spec_usecases", "spec_usecase_steps", "spec_changes", "spec_change_delta",
	"spec_code_mentions", "spec_vocab", "spec_embeddings",
	// LSA корпуса описаний (процедуры + контракты)
	"desc_vocab", "desc_embeddings",
}

// ResetCodebaseTables усекает все таблицы кодовой базы одним statement
// TRUNCATE ... CASCADE (идемпотентно, место возвращается немедленно — новые
// relfile-ы, VACUUM FULL не требуется). Возвращает число усечённых таблиц.
// Таблицы анализаторов и служебные не затрагиваются (см. codebaseResetTables).
func (db *DB) ResetCodebaseTables(ctx context.Context) (int, error) {
	stmt := "TRUNCATE TABLE " + strings.Join(codebaseResetTables, ", ") + " CASCADE"
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return 0, fmt.Errorf("failed to truncate codebase tables: %w", err)
	}
	return len(codebaseResetTables), nil
}
